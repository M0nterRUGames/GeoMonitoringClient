import asyncio
import logging
import numpy as np
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
from obspy import read
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Text, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from app_config import (
    DB_CONFIG, DATABASE_URL, BAIKAL_STREAM_OUTPUT_DIR, BAIKAL_ARCHIVE_DIR,
    BAIKAL_POLL_INTERVAL, BAIKAL_MIN_FILE_AGE, MSEED_FILE_EXTENSIONS,
    BAIKAL_SENSOR_DB_ID,
)

STREAM_OUTPUT_DIR = BAIKAL_STREAM_OUTPUT_DIR
ARCHIVE_DIR = BAIKAL_ARCHIVE_DIR
POLL_INTERVAL = BAIKAL_POLL_INTERVAL
MIN_FILE_AGE = BAIKAL_MIN_FILE_AGE
FILE_EXTENSIONS = MSEED_FILE_EXTENSIONS
TARGET_SENSOR_ID = BAIKAL_SENSOR_DB_ID
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('baikal_vibration.log', encoding='utf-8')
    ]
)
logger = logging.getLogger("BaikalVibration")
Base = declarative_base()

class Sensor(Base):
    __tablename__ = "sensors"
    id = Column(Integer, primary_key=True, index=True)
    sensor_id = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(100), nullable=False)
    description = Column(Text)
    latitude = Column(Float)
    longitude = Column(Float)
    address = Column(String(255))
    sensor_type = Column(String(50))
    status = Column(String(20), default="active")
    created_at = Column(DateTime, default=datetime.utcnow)
    measurements = relationship("Measurement", back_populates="sensor")

class Measurement(Base):
    __tablename__ = "measurements"
    id = Column(Integer, primary_key=True, index=True)
    sensor_id = Column(Integer, ForeignKey("sensors.id"), nullable=False)
    vibration_level = Column(Float) 
    measured_at = Column(DateTime, index=True)
    is_anomaly = Column(Boolean, default=False)
    anomaly_type = Column(String(50))
    sensor = relationship("Sensor", back_populates="measurements")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
class BaikalVibrationClient:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.archive_dir = ARCHIVE_DIR
        self.processed_files = set()
        self._ensure_dirs()

    def _ensure_dirs(self):
        self.archive_dir.mkdir(parents=True, exist_ok=True)

    def _is_file_ready(self, filepath: Path) -> bool:
        try:
            return (time.time() - filepath.stat().st_mtime) > MIN_FILE_AGE
        except Exception:
            return False

    def scan_new_files(self):
        if not self.output_dir.exists():
            return []
        ready = []
        for f in self.output_dir.iterdir():
            if (f.is_file() and f.suffix.lower() in FILE_EXTENSIONS and 
                f.name not in self.processed_files and self._is_file_ready(f)):
                ready.append(f)
        return sorted(ready, key=lambda x: x.stat().st_mtime)

    def calculate_vibration(self, filepath: Path):
        try:
            st = read(str(filepath))
            if not st or len(st) == 0:
                return None, None
            
            trace = st[0]
            data = trace.data.astype(float)
            if len(data) == 0:
                return None, None

            vibration_rms = np.sqrt(np.mean(data**2))
            vibration_rms = round(float(vibration_rms) / 1000.0, 4)

            ts = trace.stats.starttime.datetime
            if ts.year < 2000:
                ts = datetime.fromtimestamp(filepath.stat().st_mtime)
                logger.debug(f"Дата из заголовка некорректна, использовано время изменения файла: {ts}")

            return vibration_rms, ts
        except Exception as e:
            logger.error(f"Ошибка парсинга {filepath.name}: {e}")
            return None, None

    def ensure_sensor_exists(self):
        db = SessionLocal()
        try:
            sensor = db.query(Sensor).filter(Sensor.id == TARGET_SENSOR_ID).first()
            if not sensor:
                logger.info(f"Создание датчика с ID={TARGET_SENSOR_ID}...")
                sensor = Sensor(
                    id=TARGET_SENSOR_ID,
                    sensor_id=f"baikal_8_{TARGET_SENSOR_ID}",
                    name="Байкал-8 (Вибрация)",
                    description="Поток USB-Stream",
                    latitude=54.846667, longitude=83.106667,
                    address="Локальный ПК",
                    sensor_type="vibration",
                    status="active"
                )
                db.add(sensor)
                db.commit()
                logger.info(f"Датчик ID={TARGET_SENSOR_ID} создан")
            return True
        except Exception as e:
            logger.error(f"Ошибка проверки датчика ID {TARGET_SENSOR_ID}: {e}")
            db.rollback()
            return False
        finally:
            db.close()

    def save_to_db(self, vibration_level, timestamp):
        db = SessionLocal()
        try:
            is_anomaly = vibration_level > 1.0
            anomaly_type = "high_vibration" if is_anomaly else None

            measurement = Measurement(
                sensor_id=TARGET_SENSOR_ID,
                vibration_level=vibration_level,
                measured_at=timestamp,
                is_anomaly=is_anomaly,
                anomaly_type=anomaly_type
            )
            db.add(measurement)
            db.commit()
            logger.info(f"💾 БД: Вибрация={vibration_level} | Время={timestamp} | Аномалия={'ДА' if is_anomaly else 'НЕТ'}")
        except Exception as e:
            logger.error(f"Ошибка записи в БД: {e}")
            db.rollback()
        finally:
            db.close()

    def archive_file(self, filepath: Path):
        try:
            shutil.move(str(filepath), str(self.archive_dir / filepath.name))
            self.processed_files.add(filepath.name)
        except Exception as e:
            logger.warning(f"Не удалось архивировать {filepath.name}: {e}")

    async def run(self):
        logger.info(f"Мониторинг: {self.output_dir}")

        if not self.output_dir.exists():
            logger.error(f"Папка не найдена: {self.output_dir}")
            return

        if not self.ensure_sensor_exists():
            logger.error("Не удалось подготовить датчик. Остановка.")
            return

        logger.info("Ожидание новых файлов...")
        while True:
            try:
                new_files = self.scan_new_files()
                if new_files:
                    logger.info(f"Найдено {len(new_files)} файлов")
                    for f in new_files:
                        vib, ts = self.calculate_vibration(f)
                        if vib is not None and ts is not None:
                            self.save_to_db(vib, ts)
                        self.archive_file(f)
                await asyncio.sleep(POLL_INTERVAL)
            except KeyboardInterrupt:
                logger.info("Остановка пользователем")
                break
            except Exception as e:
                logger.error(f"Ошибка цикла: {e}")
                await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Структура БД проверена")
    except Exception as e:
        logger.error(f"Ошибка инициализации БД: {e}")
        sys.exit(1)

    asyncio.run(BaikalVibrationClient(output_dir=STREAM_OUTPUT_DIR).run())
