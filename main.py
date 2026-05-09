import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Text, ForeignKey, func, JSON
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
import random
import math
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
import json
import random
import hashlib
import re
import secrets
from io import BytesIO
from datetime import datetime
from typing import List
from pydantic import BaseModel
from fastapi.responses import StreamingResponse
from pathlib import Path
from seismic_data_handler import router as seismic_router
from mseed_generator import SENSOR_FOLDERS
from mseed_monitor import MSEED_MONITOR
from baikal_vibration_monitor import BAIKAL_VIBRATION_MONITOR
import asyncio
import glob
import json
from fastapi import WebSocket
import numpy as np
from mseed_monitor import MSEED_MONITOR, DB_CONFIG
import psycopg2
from noise_predictor import predictor
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
load_dotenv()
from app_config import (
    DB_CONFIG, DATABASE_URL, SQLITE_FALLBACK_URL, SMTP_CONFIG, SENSOR_FOLDERS,
    BAIKAL_SENSOR_DB_ID as CONFIG_BAIKAL_SENSOR_DB_ID,
    SEEDLINK_ADDRESS, SEEDLINK_SELECT, SEEDLINK_FLUSH_INTERVAL,
    BAIKAL_ARCHIVE_DIR as CONFIG_BAIKAL_ARCHIVE_DIR,
)
POSTGRES_CONFIG = DB_CONFIG

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT version();")
    db_version = cur.fetchone()
    cur.close()
    conn.close()
    
    engine = create_engine(DATABASE_URL)
    
except Exception as e:
    engine = create_engine(SQLITE_FALLBACK_URL, connect_args={"check_same_thread": False})

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Sensor(BaseModel):
    sensor_id: str
    name: str
    description: str
    latitude: float
    longitude: float
    address: str
    sensor_type: str
    status: str
    location: str

class Measurement(BaseModel):
    sensor_id: str
    noise_level: float
    temperature: float
    humidity: float
    pressure: float
    wind_speed: float
    measured_at: datetime
    is_anomaly: bool = False

active_connections = []

class ConnectionManager:
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in active_connections:
            active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in active_connections:
            try:
                await connection.send_json(message)
            except:
                self.disconnect(connection)

manager = ConnectionManager()

sensors_db = [
    {
        "sensor_id": "noise_sensor_out_001",
        "name": "Датчик шума - Бердское шоссе",
        "description": "Уличный измеритель транспортного шума",
        "latitude": 54.845000,
        "longitude": 83.095000,
        "address": "Новосибирск, Бердское шоссе, 25",
        "sensor_type": "noise",
        "status": "active",
        "location": "outdoor"
    },
    {
        "sensor_id": "noise_sensor_in_001",
        "name": "Датчик шума - НГУ, Главный корпус",
        "description": "Внутренний измеритель шума в учебном корпусе",
        "latitude": 54.845200,
        "longitude": 83.091000,
        "address": "Новосибирск, НГУ, Главный корпус, 2 этаж",
        "sensor_type": "noise",
        "status": "active",
        "location": "indoor"
    },
    {
        "sensor_id": "air_sensor_out_001",
        "name": "Датчик воздуха - Лес Академгородка",
        "description": "Уличный мониторинг качества воздуха",
        "latitude": 54.850000,
        "longitude": 83.110000,
        "address": "Новосибирск, Лес Академгородка",
        "sensor_type": "air_quality",
        "status": "active",
        "location": "outdoor"
    },
    {
        "sensor_id": "air_sensor_in_001",
        "name": "Датчик воздуха - НГУ, Библиотека",
        "description": "Внутренний мониторинг качества воздуха",
        "latitude": 54.845200,
        "longitude": 83.094800,
        "address": "Новосибирск, НГУ, Библиотека, 3 этаж",
        "sensor_type": "air_quality",
        "status": "active",
        "location": "indoor"
    },
    {
        "sensor_id": "vibration_sensor_out_001",
        "name": "Датчик вибрации - Возле трамвайных путей",
        "description": "Уличный измеритель вибрации",
        "latitude": 54.838500,
        "longitude": 83.100000,
        "address": "Новосибирск, ул. Ильича, возле трамвайных путей",
        "sensor_type": "vibration",
        "status": "active",
        "location": "outdoor"
    },
    {
        "sensor_id": "noise_sensor_in_002",
        "name": "Датчик шума - Торговый центр 'Академ'",
        "description": "Внутренний измеритель шума в ТЦ",
        "latitude": 54.840000,
        "longitude": 83.100000,
        "address": "Новосибирск, ТЦ 'Академ', 1 этаж",
        "sensor_type": "noise",
        "status": "active",
        "location": "indoor"
    }
]

"""
async def send_measurements_task():
    while True:
        await asyncio.sleep(5)  # Отправляем каждые 5 секунд
        if active_connections:
            sensor = random.choice(sensors_db)
            measurement = {
                "type": "measurement",
                "data": {
                    "sensor_id": sensor["sensor_id"],
                    "sensor_name": sensor["name"],
                    "noise_level": round(40 + random.random() * 30, 1),
                    "temperature": round(20 + random.random() * 10, 1),
                    "humidity": round(40 + random.random() * 40, 1),
                    "pressure": round(740 + random.random() * 20, 0),
                    "wind_speed": round(random.random() * 5, 1),
                    "measured_at": datetime.now().isoformat(),
                    "is_anomaly": random.random() > 0.95  # 5% chance of anomaly
                }
            }
            await manager.broadcast(measurement)
"""
@asynccontextmanager
async def lifespan(app: FastAPI):
    for folder in SENSOR_FOLDERS.values():
        os.makedirs(folder, exist_ok=True)
    monitor_task = asyncio.create_task(MSEED_MONITOR.start_monitoring(interval=2.0))
    baikal_monitor_task = asyncio.create_task(BAIKAL_VIBRATION_MONITOR.start_monitoring(interval=5.0))
    seedlink_collector = None
    try:
        from baikal_seedlink_collector import BaikalSeedLinkCollector
        seedlink_collector = BaikalSeedLinkCollector(
            db_session_factory=SessionLocal,
            chunk_model_class=SensorRawChunk,
            sensor_id=CONFIG_BAIKAL_SENSOR_DB_ID,
            address=SEEDLINK_ADDRESS,
            select=SEEDLINK_SELECT,
            flush_interval=SEEDLINK_FLUSH_INTERVAL
        )
        seedlink_collector.start()
    except Exception as e:
        logger.warning(f"⚠Не удалось запустить SeedLink сборщик: {e}")
    
    yield  # Сервер работает
    monitor_task.cancel()
    baikal_monitor_task.cancel()
    if seedlink_collector is not None:
        try:
            seedlink_collector.stop()
        except Exception as e:
            logger.error(f"Ошибка при остановке collector: {e}")
    MSEED_MONITOR.close()
    BAIKAL_VIBRATION_MONITOR.db_connection.close() if BAIKAL_VIBRATION_MONITOR.db_connection else None

app = FastAPI(title="Система мониторинга шума Академгородка", lifespan=lifespan)

STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR.mkdir(exist_ok=True)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "Система мониторинга шума Академгородка", "status": "работает", "version": "1.0.0"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"type": "pong", "timestamp": datetime.now().isoformat()})
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.get("/api/sensors", response_model=List[Sensor])
async def get_sensors():
    """Получить список всех датчиков"""
    return sensors_db

@app.get("/api/sensors/{sensor_id}")
async def get_sensor(sensor_id: str):
    """Получить информацию о конкретном датчике"""
    for sensor in sensors_db:
        if sensor["sensor_id"] == sensor_id:
            return sensor
    return {"error": "Датчик не найден"}, 404

@app.get("/api/measurements/stats")
async def get_stats(hours: int = 24):
    """Получить статистику измерений"""
    return {
        "total_measurements": 1247 + random.randint(0, 100),
        "average_noise": round(48.2 + random.random() * 5, 1),
        "anomaly_count": random.randint(0, 3),
        "active_sensors": len([s for s in sensors_db if s["status"] == "active"]),
        "total_sensors": len(sensors_db),
        "period_hours": hours
    }

@app.get("/api/measurements/latest")
async def get_latest_measurements(limit: int = 10):
    """Получить последние измерения"""
    latest = []
    for _ in range(min(limit, 10)):
        sensor = random.choice(sensors_db)
        latest.append({
            "sensor_id": sensor["sensor_id"],
            "sensor_name": sensor["name"],
            "noise_level": round(40 + random.random() * 30, 1),
            "temperature": round(20 + random.random() * 10, 1),
            "humidity": round(40 + random.random() * 40, 1),
            "measured_at": datetime.now().isoformat(),
            "is_anomaly": random.random() > 0.95
        })
    return latest

@app.get("/api/weather/current")
async def get_current_weather():
    """Получить текущую погоду"""
    return {
        "temperature": round(15 + random.random() * 15, 1),
        "feels_like": round(12 + random.random() * 12, 1),
        "humidity": random.randint(40, 90),
        "pressure": random.randint(730, 760),
        "wind_speed": round(random.random() * 7, 1),
        "wind_direction": random.choice(["С", "СВ", "В", "ЮВ", "Ю", "ЮЗ", "З", "СЗ"]),
        "weather": random.choice(["ясно", "малооблачно", "облачно", "пасмурно"]),
        "icon": random.choice(["01d", "02d", "03d", "04d"]),
        "timestamp": datetime.now().isoformat(),
        "source": "Локальный сервер"
    }

@app.get("/api/system/uptime")
async def get_system_uptime():
    """Получить время работы системы"""
    return {
        "uptime_hours": random.randint(1, 720),  # от 1 до 30 дней
        "started_at": (datetime.now() - datetime.timedelta(hours=random.randint(1, 720))).isoformat(),
        "status": "running"
    }

STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR.mkdir(exist_ok=True)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")

class Sensor(Base):
    __tablename__ = "sensors"
    
    id = Column(Integer, primary_key=True, index=True)
    sensor_id = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(100), nullable=False)
    description = Column(Text)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    address = Column(String(255))
    sensor_type = Column(String(50))
    status = Column(String(20), default="active")
    installation_date = Column(DateTime, default=datetime.utcnow)
    last_maintenance = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    measurements = relationship("Measurement", back_populates="sensor", cascade="all, delete-orphan")
    raw_chunks = relationship("SensorRawChunk", back_populates="sensor", cascade="all, delete-orphan")

class Measurement(Base):
    __tablename__ = "measurements"
    
    id = Column(Integer, primary_key=True, index=True)
    sensor_id = Column(Integer, ForeignKey("sensors.id"), nullable=False)
    noise_level = Column(Float)
    temperature = Column(Float)
    humidity = Column(Float)
    pressure = Column(Float)
    wind_speed = Column(Float)
    air_quality_index = Column(Integer)
    vibration_level = Column(Float)
    measured_at = Column(DateTime, default=datetime.utcnow, index=True)
    is_anomaly = Column(Boolean, default=False)
    anomaly_type = Column(String(50))
    sensor = relationship("Sensor", back_populates="measurements")

class SensorRawChunk(Base):
    __tablename__ = "sensor_raw_chunks"
    
    id = Column(Integer, primary_key=True, index=True)
    sensor_id = Column(Integer, ForeignKey("sensors.id"), nullable=False, index=True)
    chunk_start = Column(DateTime, nullable=False)
    chunk_end = Column(DateTime, nullable=False)
    sampling_rate = Column(Float, nullable=False)
    raw_values = Column(JSON, nullable=False)
    sensor = relationship("Sensor", back_populates="raw_chunks")

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    full_name = Column(String(120), nullable=False)
    email = Column(String(255), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    role = Column(String(30), default="user", nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
Base.metadata.create_all(bind=engine)

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

class UserRegister(BaseModel):
    full_name: str
    email: str
    password: str

class UserLogin(BaseModel):
    email: str
    password: str

class UserResponse(BaseModel):
    id: int
    full_name: str
    email: str
    role: str
    created_at: datetime

    class Config:
        from_attributes = True

class AuthResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserResponse

def normalize_email(email: str) -> str:
    return email.strip().lower()

def validate_auth_payload(full_name: Optional[str], email: str, password: str):
    if full_name is not None and len(full_name.strip()) < 2:
        raise HTTPException(status_code=400, detail="Введите имя не короче 2 символов")
    if not EMAIL_RE.match(normalize_email(email)):
        raise HTTPException(status_code=400, detail="Введите корректный email")
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Пароль должен быть не короче 8 символов")

def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 120000)
    return f"pbkdf2_sha256${salt}${digest.hex()}"

def verify_password(password: str, stored_hash: str) -> bool:
    try:
        algorithm, salt, expected = stored_hash.split("$", 2)
        if algorithm != "pbkdf2_sha256":
            return False
        digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 120000)
        return secrets.compare_digest(digest.hex(), expected)
    except ValueError:
        return False

def issue_access_token(user: User) -> str:
    raw = f"{user.id}:{user.email}:{secrets.token_urlsafe(32)}"
    return secrets.token_urlsafe(24) + "." + hashlib.sha256(raw.encode("utf-8")).hexdigest()

class SensorCreate(BaseModel):
    sensor_id: str
    name: str
    description: Optional[str] = None
    latitude: float
    longitude: float
    address: Optional[str] = None
    sensor_type: str = "noise"
    status: str = "active"

class MeasurementCreate(BaseModel):
    sensor_id: str
    noise_level: Optional[float] = None
    temperature: Optional[float] = None
    humidity: Optional[float] = None
    pressure: Optional[float] = None
    wind_speed: Optional[float] = None
    air_quality_index: Optional[int] = None
    vibration_level: Optional[float] = None

class ReportRequest(BaseModel):
    sensor_type: str = "noise"
    sensor_scope: str = "all"
    sensor_db_id: Optional[int] = None
    period_days: int = 7
    include_week: bool = True
    include_month: bool = True
    include_day_night: bool = True
    include_recent: bool = True

class SensorResponse(BaseModel):
    id: int
    sensor_id: str
    name: str
    description: Optional[str]
    latitude: float
    longitude: float
    address: Optional[str]
    sensor_type: str
    status: str
    installation_date: Optional[datetime]
    created_at: Optional[datetime]

    class Config:
        from_attributes = True

class MeasurementResponse(BaseModel):
    id: int
    sensor_id: int
    noise_level: Optional[float]
    temperature: Optional[float]
    humidity: Optional[float]
    pressure: Optional[float]
    wind_speed: Optional[float]
    air_quality_index: Optional[int]
    vibration_level: Optional[float]
    measured_at: datetime
    is_anomaly: bool
    anomaly_type: Optional[str]

    class Config:
        from_attributes = True

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except:
                self.disconnect(connection)

manager = ConnectionManager()

def gauss_distribution(mean, std):
    """Генерация случайного числа с нормальным распределением"""
    # Используем метод Бокса-Мюллера
    u1 = random.random()
    u2 = random.random()
    z0 = math.sqrt(-2.0 * math.log(u1)) * math.cos(2.0 * math.pi * u2)
    return mean + z0 * std

class RealisticDataGenerator:
    """Генератор реалистичных данных для Академгородка Новосибирска"""
    
    NOISE_STATISTICS = {
        "residential": {
            "day": {"mean": 52, "std": 6, "min": 40, "max": 65},
            "night": {"mean": 45, "std": 5, "min": 35, "max": 55}
        },
        "commercial": {
            "day": {"mean": 65, "std": 8, "min": 50, "max": 80},
            "night": {"mean": 55, "std": 7, "min": 40, "max": 70}
        },
        "educational": {
            "day": {"mean": 58, "std": 7, "min": 45, "max": 75},
            "night": {"mean": 48, "std": 6, "min": 35, "max": 60}
        },
        "industrial": {
            "day": {"mean": 72, "std": 10, "min": 55, "max": 90},
            "night": {"mean": 65, "std": 8, "min": 50, "max": 85}
        },
        "transport": {
            "day": {"mean": 75, "std": 12, "min": 60, "max": 95},
            "night": {"mean": 68, "std": 10, "min": 55, "max": 85}
        },
        "park": {
            "day": {"mean": 48, "std": 5, "min": 38, "max": 60},
            "night": {"mean": 42, "std": 4, "min": 35, "max": 50}
        }
    }
    
    CLIMATE_DATA = {
        1: {"temp": -16.5, "temp_std": 5, "humidity": 85, "pressure": 1015, "wind": 3.2},
        2: {"temp": -14.7, "temp_std": 5, "humidity": 82, "pressure": 1016, "wind": 3.0},
        3: {"temp": -7.3, "temp_std": 4, "humidity": 78, "pressure": 1013, "wind": 3.5},
        4: {"temp": 3.8, "temp_std": 4, "humidity": 70, "pressure": 1011, "wind": 3.8},
        5: {"temp": 12.6, "temp_std": 3, "humidity": 65, "pressure": 1009, "wind": 3.2},
        6: {"temp": 18.1, "temp_std": 3, "humidity": 68, "pressure": 1007, "wind": 2.8},
        7: {"temp": 20.2, "temp_std": 2, "humidity": 75, "pressure": 1005, "wind": 2.5},
        8: {"temp": 17.8, "temp_std": 3, "humidity": 78, "pressure": 1007, "wind": 2.6},
        9: {"temp": 11.3, "temp_std": 3, "humidity": 80, "pressure": 1010, "wind": 3.0},
        10: {"temp": 3.2, "temp_std": 4, "humidity": 85, "pressure": 1012, "wind": 3.3},
        11: {"temp": -7.8, "temp_std": 5, "humidity": 87, "pressure": 1014, "wind": 3.6},
        12: {"temp": -14.2, "temp_std": 5, "humidity": 86, "pressure": 1015, "wind": 3.4}
    }
    
    AIR_QUALITY_STATS = {
        "residential": {"mean": 35, "std": 15, "min": 15, "max": 80},
        "commercial": {"mean": 45, "std": 20, "min": 20, "max": 100},
        "industrial": {"mean": 65, "std": 25, "min": 30, "max": 150},
        "park": {"mean": 25, "std": 10, "min": 10, "max": 50},
        "transport": {"mean": 60, "std": 20, "min": 35, "max": 120}
    }
    
    VIBRATION_STATS = {
        "residential": {"mean": 1.2, "std": 0.3, "min": 0.5, "max": 2.5},
        "commercial": {"mean": 1.8, "std": 0.5, "min": 0.8, "max": 3.5},
        "industrial": {"mean": 2.5, "std": 0.8, "min": 1.0, "max": 4.5},
        "transport": {"mean": 3.2, "std": 1.0, "min": 1.5, "max": 6.0},
        "park": {"mean": 0.8, "std": 0.2, "min": 0.3, "max": 1.5}
    }
    
    def __init__(self):
        self.sensor_history = {}
        
    def get_time_of_day(self, timestamp: datetime) -> str:
        """Определение времени суток"""
        hour = timestamp.hour
        if 6 <= hour < 22:
            return "day"
        else:
            return "night"
    
    def get_day_type(self, timestamp: datetime) -> str:
        """Определение типа дня (рабочий/выходной)"""
        weekday = timestamp.weekday()
        return "weekend" if weekday >= 5 else "weekday"
    
    def get_season(self, timestamp: datetime) -> str:
        """Определение сезона"""
        month = timestamp.month
        if month in [12, 1, 2]:
            return "winter"
        elif month in [3, 4, 5]:
            return "spring"
        elif month in [6, 7, 8]:
            return "summer"
        else:
            return "autumn"
    
    def get_weather_conditions(self, timestamp: datetime) -> Dict:
        """Генерация реалистичных погодных условий"""
        month = timestamp.month
        climate = self.CLIMATE_DATA.get(month, self.CLIMATE_DATA[1])
        
        hour = timestamp.hour
        if 2 <= hour <= 5:
            temp_offset = -4
        elif 13 <= hour <= 15:
            temp_offset = 4
        else:
            temp_offset = 0
        
        temperature = climate["temp"] + temp_offset + random.uniform(-climate["temp_std"], climate["temp_std"])
        
        humidity = climate["humidity"] + random.uniform(-10, 10)
        humidity = max(30, min(100, humidity))
        
        pressure = climate["pressure"] + random.uniform(-10, 10)
        
        wind_base = climate["wind"]
        if self.get_season(timestamp) in ["spring", "autumn"]:
            wind_base *= 1.2  
        
        wind_speed = wind_base + random.uniform(-1, 1)
        wind_speed = max(0.5, min(8, wind_speed))
        
        weather_options = ["ясно", "малооблачно", "облачно", "пасмурно"]
        weights = [40, 30, 20, 10]
        
        if month in [12, 1, 2]:
            weights = [20, 30, 30, 20]
        elif month in [6, 7, 8]:
            weights = [50, 30, 15, 5]
        
        weather = random.choices(weather_options, weights=weights)[0]
        
        return {
            "temperature": round(temperature, 1),
            "humidity": round(humidity, 1),
            "pressure": round(pressure, 1),
            "wind_speed": round(wind_speed, 1),
            "weather": weather
        }
    
    def generate_realistic_noise(self, zone_type: str, timestamp: datetime, 
                                is_weekday: bool, weather: str) -> float:
        """Генерация реалистичного уровня шума"""
        time_of_day = self.get_time_of_day(timestamp)
        stats = self.NOISE_STATISTICS[zone_type][time_of_day]
        
        base_noise = gauss_distribution(stats["mean"], stats["std"])
        
        if not is_weekday:
            base_noise *= 0.85

        if weather in ["дождь", "снег", "туман"]:
            base_noise *= 0.9  
        elif weather == "ясно":
            base_noise *= 1.05
        
        hour = timestamp.hour
        if (7 <= hour <= 9) or (17 <= hour <= 19):
            if is_weekday:
                base_noise *= 1.15
        
        if 23 <= hour or hour < 6:
            base_noise *= 0.7
        
        base_noise = max(stats["min"], min(stats["max"], base_noise))
        
        return round(base_noise, 1)
    
    def generate_air_quality(self, zone_type: str, timestamp: datetime, 
                            weather: str, wind_speed: float) -> int:
        """Генерация индекса качества воздуха"""
        stats = self.AIR_QUALITY_STATS[zone_type]
        
        base_aqi = gauss_distribution(stats["mean"], stats["std"])
        
        if weather in ["дождь"]:
            base_aqi *= 0.8 
        elif weather == "ясно" and wind_speed < 2:
            base_aqi *= 1.1 
        
        if wind_speed > 5:
            base_aqi *= 0.9 
        
        hour = timestamp.hour
        if 7 <= hour <= 10: 
            base_aqi *= 1.05
        
        base_aqi = max(stats["min"], min(stats["max"], base_aqi))
        
        return int(base_aqi)
    
    def generate_vibration(self, zone_type: str, timestamp: datetime, 
                          is_weekday: bool) -> float:
        """Генерация уровня вибрации"""
        stats = self.VIBRATION_STATS[zone_type]
        
        base_vibration = gauss_distribution(stats["mean"], stats["std"])
        
        if is_weekday:
            hour = timestamp.hour
            if 7 <= hour <= 9 or 17 <= hour <= 19:
                base_vibration *= 1.3
        
        base_vibration = max(stats["min"], min(stats["max"], base_vibration))
        
        return round(base_vibration, 2)
    
    def check_anomalies(self, sensor_data: Dict, zone_type: str) -> Dict:
        """Проверка данных на аномалии"""
        
        noise = sensor_data.get("noise_level")
        if noise is not None:
            if noise >= 70:
                return {
                    "is_anomaly": True,
                    "anomaly_type": "danger_high_noise"
                }
            elif noise >= 55:
                return {
                    "is_anomaly": False,
                    "anomaly_type": "warning_high_noise"
                }
            else:
                return {
                    "is_anomaly": False,
                    "anomaly_type": None
                }
        
        return {
        "is_anomaly": False,
        "anomaly_type": None
        }

SENSOR_ZONES = {
    "noise_sensor_001": "residential",      
    "noise_sensor_002": "educational",      
    "noise_sensor_003": "commercial",       
    "noise_sensor_004": "residential",      
    "vibration_sensor_001": "transport",    
    "air_sensor_001": "park",               
    "air_sensor_002": "industrial"        
}

class RealisticSensorSimulator:
    def __init__(self, sensor_id: str, sensor_type: str):
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type
        self.data_generator = RealisticDataGenerator()
        self.zone_type = SENSOR_ZONES.get(sensor_id, "residential")
        
        self.last_values = {
            "noise": None,
            "temperature": None,
            "humidity": None
        }
    
    async def generate_measurement(self) -> Dict:
        """Генерация реалистичного измерения"""
        timestamp = datetime.now()
        is_weekday = self.data_generator.get_day_type(timestamp) == "weekday"
        
        weather_data = self.data_generator.get_weather_conditions(timestamp)
        
        sensor_data = {
            "sensor_id": self.sensor_id,
            "sensor_type": self.sensor_type,
            "timestamp": timestamp,
            "temperature": weather_data["temperature"],
            "humidity": weather_data["humidity"],
            "pressure": weather_data["pressure"],
            "wind_speed": weather_data["wind_speed"],
            "weather": weather_data["weather"]
        }
        
        if self.sensor_type == "noise":
            noise_level = self.data_generator.generate_realistic_noise(
                self.zone_type, timestamp, is_weekday, weather_data["weather"]
            )
            sensor_data["noise_level"] = noise_level
            
            sensor_data["air_quality_index"] = self.data_generator.generate_air_quality(
                self.zone_type, timestamp, weather_data["weather"], weather_data["wind_speed"]
            )
            sensor_data["vibration_level"] = self.data_generator.generate_vibration(
                self.zone_type, timestamp, is_weekday
            )
            
        elif self.sensor_type == "vibration":
            vibration_level = self.data_generator.generate_vibration(
                self.zone_type, timestamp, is_weekday
            )
            sensor_data["vibration_level"] = vibration_level
            
            sensor_data["noise_level"] = self.data_generator.generate_realistic_noise(
                self.zone_type, timestamp, is_weekday, weather_data["weather"]
            )
            sensor_data["air_quality_index"] = self.data_generator.generate_air_quality(
                self.zone_type, timestamp, weather_data["weather"], weather_data["wind_speed"]
            )
            
        elif self.sensor_type == "air_quality":
            air_quality = self.data_generator.generate_air_quality(
                self.zone_type, timestamp, weather_data["weather"], weather_data["wind_speed"]
            )
            sensor_data["air_quality_index"] = air_quality
            
            sensor_data["noise_level"] = self.data_generator.generate_realistic_noise(
                self.zone_type, timestamp, is_weekday, weather_data["weather"]
            )
            sensor_data["vibration_level"] = self.data_generator.generate_vibration(
                self.zone_type, timestamp, is_weekday
            )
        
        anomaly_check = self.data_generator.check_anomalies(sensor_data, self.zone_type)
        sensor_data.update(anomaly_check)
        
        sensor_data = self._apply_smoothing(sensor_data)
        
        return sensor_data
    
    def _apply_smoothing(self, data: Dict) -> Dict:
        """Применение плавности изменений к данным"""
        smoothed_data = data.copy()
        
        if self.last_values["temperature"] is not None:
            current_temp = data["temperature"]
            smoothed_temp = self.last_values["temperature"] * 0.7 + current_temp * 0.3
            smoothed_data["temperature"] = round(smoothed_temp, 1)
        
        if self.last_values["humidity"] is not None:
            current_humidity = data["humidity"]
            smoothed_humidity = self.last_values["humidity"] * 0.8 + current_humidity * 0.2
            smoothed_data["humidity"] = round(smoothed_humidity, 1)
        
        if self.last_values["noise"] is not None and "noise_level" in data:
            current_noise = data["noise_level"]
            smoothed_noise = self.last_values["noise"] * 0.6 + current_noise * 0.4
            smoothed_data["noise_level"] = round(smoothed_noise, 1)
        
        self.last_values["temperature"] = smoothed_data["temperature"]
        self.last_values["humidity"] = smoothed_data["humidity"]
        if "noise_level" in smoothed_data:
            self.last_values["noise"] = smoothed_data["noise_level"]
        
        return smoothed_data

sensor_simulators = {}
data_generator = RealisticDataGenerator()

async def init_sensors():
    """Инициализация датчиков в базе данных"""
    db = SessionLocal()
    try:
        existing_sensors = db.query(Sensor).all()
        existing_sensor_ids = [s.sensor_id for s in existing_sensors]
        
        all_sensors_data = [
            {
                "sensor_id": "noise_sensor_001",
                "name": "Датчик шума и виброскорости - ИВМиМГ",
                "description": "Измеритель шума и вибрации в ИВМиМГ СО РАН",
                "latitude": 54.846667,
                "longitude": 83.106667,
                "address": "630090, Новосибирск, пр. Академика Лаврентьева, 6",
                "sensor_type": "noise",
                "status": "active"
            },
            {
                "sensor_id": "noise_sensor_002",
                "name": "Датчик шума - НГУ, Главный корпус",
                "description": "Измеритель уровня шума возле НГУ",
                "latitude": 54.845000,
                "longitude": 83.095000,
                "address": "Новосибирск, ул. Пирогова, 1",
                "sensor_type": "noise",
                "status": "active"
            },
            {
                "sensor_id": "noise_sensor_003",
                "name": "Датчик шума - Торговый центр 'Академ'",
                "description": "Измеритель уровня шума в торговом центре",
                "latitude": 54.840000,
                "longitude": 83.100000,
                "address": "Новосибирск, ул. Ильича, 14",
                "sensor_type": "noise",
                "status": "active"
            },
            {
                "sensor_id": "air_sensor_001",
                "name": "Датчик качества воздуха - Парк",
                "description": "Мониторинг качества воздуха в парке",
                "latitude": 54.850000,
                "longitude": 83.110000,
                "address": "Новосибирск, Лес Академгородка",
                "sensor_type": "air_quality",
                "status": "active"
            },
            {
                "sensor_id": "noise_sensor_004",
                "name": "Датчик шума - Бердское шоссе",
                "description": "Измеритель уровня шума возле Бердского шоссе",
                "latitude": 54.838000,
                "longitude": 83.095000,
                "address": "Новосибирск, Бердское шоссе, 25",
                "sensor_type": "noise",
                "status": "active"
            },
            {
                "sensor_id": "vibration_sensor_001",
                "name": "Датчик вибрации - Трасса",
                "description": "Измеритель вибрации возле автомагистрали",
                "latitude": 54.842500,
                "longitude": 83.090000,
                "address": "Новосибирск, Бердское шоссе, 25",
                "sensor_type": "vibration",
                "status": "active"
            },
            {
                "sensor_id": "air_sensor_002",
                "name": "Датчик воздуха - Промзона",
                "description": "Мониторинг качества воздуха в промышленной зоне",
                "latitude": 54.832000,
                "longitude": 83.115000,
                "address": "Новосибирск, Индустриальная, 5",
                "sensor_type": "air_quality",
                "status": "active"
            }
        ]
        
        added_count = 0
        for sensor_data in all_sensors_data:
            if sensor_data["sensor_id"] not in existing_sensor_ids:
                sensor = Sensor(**sensor_data)
                db.add(sensor)
                added_count += 1
                sensor_simulators[sensor_data["sensor_id"]] = RealisticSensorSimulator(
                    sensor_data["sensor_id"],
                    sensor_data["sensor_type"]
                )
        
            db.commit()
        for sensor in existing_sensors:
            if sensor.sensor_id not in sensor_simulators:
                sensor_simulators[sensor.sensor_id] = RealisticSensorSimulator(
                    sensor.sensor_id,
                    sensor.sensor_type
                )
        await create_historical_data(db)
        
    except Exception as e:
        logger.error(f"Ошибка инициализации датчиков: {e}")
        db.rollback()
    finally:
        db.close()

async def create_historical_data(db):
    """Создание исторических данных для реалистичности"""
    try:
        measurement_count = db.query(Measurement).count()
        
        if measurement_count < 100:
            for days_ago in range(7, 0, -1):
                base_time = datetime.utcnow() - timedelta(days=days_ago)
                
                for hour in range(24):
                    for minute in range(0, 60, 30):
                        measurement_time = base_time.replace(hour=hour, minute=minute, second=0)
                        
                        for sensor_id, simulator in sensor_simulators.items():
                            temp_simulator = RealisticSensorSimulator(sensor_id, simulator.sensor_type)
            
                            import datetime as dt_module
                            original_now = dt_module.datetime.now
                            
                            class MockDateTime:
                                @staticmethod
                                def now():
                                    return measurement_time
                            
                            dt_module.datetime.now = MockDateTime.now
                            
                            try:
                                data = await temp_simulator.generate_measurement()
                                
                                sensor = db.query(Sensor).filter(Sensor.sensor_id == sensor_id).first()
                                if sensor:
                                    measurement = Measurement(
                                        sensor_id=sensor.id,
                                        noise_level=data.get("noise_level"),
                                        temperature=data.get("temperature"),
                                        humidity=data.get("humidity"),
                                        pressure=data.get("pressure"),
                                        wind_speed=data.get("wind_speed"),
                                        air_quality_index=data.get("air_quality_index"),
                                        vibration_level=data.get("vibration_level"),
                                        is_anomaly=data.get("is_anomaly", False),
                                        anomaly_type=data.get("anomaly_type"),
                                        measured_at=measurement_time
                                    )
                                    db.add(measurement)
                            finally:
                                dt_module.datetime.now = original_now
            
            db.commit()
            
    except Exception as e:
        logger.error(f"Ошибка создания исторических данных: {e}")
        db.rollback()

async def collect_sensor_data():
    """Фоновая задача для сбора данных с датчиков"""
    while True:
        try:
            db = SessionLocal()
            current_time = datetime.now()
            
            for sensor_id, simulator in sensor_simulators.items():
                data = await simulator.generate_measurement()
                
                if data:
                    sensor = db.query(Sensor).filter(Sensor.sensor_id == sensor_id).first()
                    
                    if sensor:
                        measurement = Measurement(
                            sensor_id=sensor.id,
                            noise_level=data.get("noise_level"),
                            temperature=data.get("temperature"),
                            humidity=data.get("humidity"),
                            pressure=data.get("pressure"),
                            wind_speed=data.get("wind_speed"),
                            air_quality_index=data.get("air_quality_index"),
                            vibration_level=data.get("vibration_level"),
                            is_anomaly=data.get("is_anomaly", False),
                            anomaly_type=data.get("anomaly_type"),
                            measured_at=current_time
                        )
                        
                        db.add(measurement)
                        
                        ws_data = {
                            'sensor_id': sensor_id,
                            'sensor_name': sensor.name,
                            'sensor_type': sensor.sensor_type,
                            'latitude': sensor.latitude,
                            'longitude': sensor.longitude,
                            'address': sensor.address,
                            'noise_level': data.get("noise_level"),
                            'temperature': data.get("temperature"),
                            'humidity': data.get("humidity"),
                            'pressure': data.get("pressure"),
                            'wind_speed': data.get("wind_speed"),
                            'air_quality_index': data.get("air_quality_index"),
                            'vibration_level': data.get("vibration_level"),
                            'weather': data.get("weather"),
                            'is_anomaly': data.get("is_anomaly", False),
                            'anomaly_type': data.get("anomaly_type"),
                            'measured_at': current_time.isoformat()
                        }
                        
                        await manager.broadcast(json.dumps({
                            'type': 'measurement',
                            'data': ws_data
                        }))
                        
                        log_msg = f"📊 {sensor_id}: "
                        if data.get("noise_level"):
                            log_msg += f"шум={data['noise_level']}dB "
                        if data.get("temperature"):
                            log_msg += f"темп={data['temperature']}°C "
                        log_msg += f"погода={data.get('weather', 'N/A')}"
                        
                        if data.get("is_anomaly"):
                            logger.warning(f"🚨 АНОМАЛИЯ {log_msg}")
                        else:
                            logger.info(log_msg)
            
            db.commit()
            db.close()
            
        except Exception as e:
            logger.error(f"Ошибка сбора данных: {e}")
            try:
                db.rollback()
                db.close()
            except:
                pass
        
        current_hour = datetime.now().hour
        if 0 <= current_hour <= 5:
            interval = 300
        else:
            interval = 60
        
        await asyncio.sleep(interval)

app.include_router(seismic_router)
@app.get("/")
async def read_root():
    return FileResponse("static/index.html")

@app.get("/auth")
async def auth_page():
    return FileResponse("static/auth.html")

@app.post("/api/auth/register", response_model=AuthResponse)
async def register_user(payload: UserRegister):
    validate_auth_payload(payload.full_name, payload.email, payload.password)
    email = normalize_email(payload.email)
    db = SessionLocal()
    try:
        existing_user = db.query(User).filter(User.email == email).first()
        if existing_user:
            raise HTTPException(status_code=409, detail="Пользователь с таким email уже зарегистрирован")

        user = User(
            full_name=payload.full_name.strip(),
            email=email,
            password_hash=hash_password(payload.password),
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        return AuthResponse(access_token=issue_access_token(user), user=user)
    finally:
        db.close()

@app.post("/api/auth/login", response_model=AuthResponse)
async def login_user(payload: UserLogin):
    validate_auth_payload(None, payload.email, payload.password)
    email = normalize_email(payload.email)
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.email == email).first()
        if not user or not user.is_active or not verify_password(payload.password, user.password_hash):
            raise HTTPException(status_code=401, detail="Неверный email или пароль")

        return AuthResponse(access_token=issue_access_token(user), user=user)
    finally:
        db.close()

@app.get("/api/health")
async def health_check():
    """Проверка здоровья системы"""
    try:
        db = SessionLocal()
        db.execute("SELECT 1")
        db.close()
        
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.utcnow().isoformat(),
            "sensors_count": len(sensor_simulators),
            "system": "simulation_mode",
            "description": "Режим реалистичной симуляции данных Академгородка"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }

@app.get("/api/sensors", response_model=List[SensorResponse])
async def get_all_sensors():
    """Получение списка всех датчиков"""
    db = SessionLocal()
    try:
        sensors = db.query(Sensor).all()
        return sensors
    finally:
        db.close()

@app.get("/api/sensors/{sensor_id}", response_model=SensorResponse)
async def get_sensor_by_id(sensor_id: str):
    """Получение информации о конкретном датчике"""
    db = SessionLocal()
    try:
        sensor = db.query(Sensor).filter(Sensor.sensor_id == sensor_id).first()
        if not sensor:
            raise HTTPException(status_code=404, detail="Датчик не найден")
        return sensor
    finally:
        db.close()

@app.get("/api/sensors/{sensor_id}/measurements")
async def get_sensor_measurements(sensor_id: str, limit: int = 50, hours: Optional[int] = None):
    """Получение измерений конкретного датчика"""
    db = SessionLocal()
    try:
        sensor = db.query(Sensor).filter(Sensor.sensor_id == sensor_id).first()
        if not sensor:
            raise HTTPException(status_code=404, detail="Датчик не найден")
        
        query = db.query(Measurement).filter(Measurement.sensor_id == sensor.id)
        
        if hours:
            time_threshold = datetime.utcnow() - timedelta(hours=hours)
            query = query.filter(Measurement.measured_at >= time_threshold)
        
        measurements = query.order_by(Measurement.measured_at.desc()).limit(limit).all()
        
        return [
            {
                "id": m.id,
                "noise_level": m.noise_level,
                "temperature": m.temperature,
                "humidity": m.humidity,
                "pressure": m.pressure,
                "wind_speed": m.wind_speed,
                "air_quality_index": m.air_quality_index,
                "vibration_level": m.vibration_level,
                "measured_at": m.measured_at.isoformat(),
                "is_anomaly": m.is_anomaly,
                "anomaly_type": m.anomaly_type
            }
            for m in measurements
        ]
    finally:
        db.close()

@app.get("/api/measurements/latest")
async def get_latest_measurements_all():
    """Получение последних измерений всех датчиков"""
    db = SessionLocal()
    try:
        sensors = db.query(Sensor).all()
        results = []
        
        for sensor in sensors:
            latest = db.query(Measurement)\
                .filter(Measurement.sensor_id == sensor.id)\
                .order_by(Measurement.measured_at.desc())\
                .first()
            
            if latest:
                results.append({
                    "sensor_id": sensor.sensor_id,
                    "sensor_name": sensor.name,
                    "latitude": sensor.latitude,
                    "longitude": sensor.longitude,
                    "address": sensor.address,
                    "sensor_type": sensor.sensor_type,
                    "status": sensor.status,
                    "noise_level": latest.noise_level,
                    "temperature": latest.temperature,
                    "humidity": latest.humidity,
                    "pressure": latest.pressure,
                    "wind_speed": latest.wind_speed,
                    "air_quality_index": latest.air_quality_index,
                    "vibration_level": latest.vibration_level,
                    "is_anomaly": latest.is_anomaly,
                    "anomaly_type": latest.anomaly_type,
                    "measured_at": latest.measured_at.isoformat()
                })
        
        return results
    finally:
        db.close()

@app.get("/api/measurements/stats")
async def get_measurement_statistics(hours: int = 24):
    """Получение статистики измерений"""
    db = SessionLocal()
    try:
        time_threshold = datetime.utcnow() - timedelta(hours=hours)
        
        stats_query = db.query(
            func.avg(Measurement.noise_level).label("avg_noise"),
            func.max(Measurement.noise_level).label("max_noise"),
            func.min(Measurement.noise_level).label("min_noise"),
            func.count(Measurement.id).label("total_measurements")
        ).filter(Measurement.measured_at >= time_threshold)
        
        stats = stats_query.first()
        
        anomaly_count = db.query(Measurement)\
            .filter(Measurement.measured_at >= time_threshold)\
            .filter(Measurement.is_anomaly == True)\
            .count()
        
        return {
            "period_hours": hours,
            "average_noise": float(stats.avg_noise or 0),
            "max_noise": float(stats.max_noise or 0),
            "min_noise": float(stats.min_noise or 0),
            "total_measurements": stats.total_measurements,
            "anomaly_count": anomaly_count,
            "sensors_active": len(sensor_simulators)
        }
    finally:
        db.close()

@app.get("/api/weather/current")
async def get_current_weather():
    """Получение текущей погоды в Академгородке"""
    try:
        timestamp = datetime.now()
        weather_data = data_generator.get_weather_conditions(timestamp)
        
        return {
            "location": "Академгородок, Новосибирск",
            "coordinates": {"latitude": 54.846667, "longitude": 83.106667},
            "temperature": weather_data["temperature"],
            "humidity": weather_data["humidity"],
            "pressure": weather_data["pressure"],
            "weather": weather_data["weather"],
            "wind_speed": weather_data["wind_speed"],
            "wind_direction": random.randint(0, 360),
            "clouds": random.randint(0, 100),
            "visibility": random.randint(5000, 20000),
            "timestamp": timestamp.isoformat(),
            "source": "simulation_based_on_real_statistics",
            "description": "Данные основаны на статистике Новосибирска"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения погоды: {e}")

@app.get("/api/weather/history")
async def get_weather_history(hours: int = 24):
    """Получение истории погоды"""
    db = SessionLocal()
    try:
        time_threshold = datetime.utcnow() - timedelta(hours=hours)
        
        result = db.query(
            func.date_trunc('hour', Measurement.measured_at).label('hour'),
            func.avg(Measurement.temperature).label('avg_temp'),
            func.avg(Measurement.humidity).label('avg_humidity'),
            func.avg(Measurement.pressure).label('avg_pressure'),
            func.count(Measurement.id).label('measurement_count')
        ).filter(
            Measurement.measured_at >= time_threshold,
            Measurement.temperature.isnot(None)
        ).group_by(
            db.func.date_trunc('hour', Measurement.measured_at)
        ).order_by(
            db.func.date_trunc('hour', Measurement.measured_at).desc()
        ).all()
        
        return [
            {
                "hour": row.hour.isoformat(),
                "temperature": round(float(row.avg_temp or 0), 1),
                "humidity": round(float(row.avg_humidity or 0), 1),
                "pressure": round(float(row.avg_pressure or 0), 1),
                "measurement_count": row.measurement_count
            }
            for row in result
        ]
    finally:
        db.close()
@app.get("/api/sensors/count")
async def get_sensors_count():
    """Получение количества датчиков"""
    db = SessionLocal()
    try:
        total = db.query(Sensor).count()
        active = db.query(Sensor).filter(Sensor.status == "active").count()
        return {
            "total": total,
            "active": active,
            "inactive": total - active
        }
    finally:
        db.close()

@app.get("/api/measurements/count")
async def get_measurements_count(hours: int = 24):
    """Получение количества измерений"""
    db = SessionLocal()
    try:
        time_threshold = datetime.utcnow() - timedelta(hours=hours)
        count = db.query(Measurement)\
            .filter(Measurement.measured_at >= time_threshold)\
            .count()
        
        return {
            "count": count,
            "period_hours": hours
        }
    finally:
        db.close()

@app.get("/api/system/uptime")
async def get_system_uptime():
    """Получение времени работы системы"""
    return {
        "uptime_hours": 24,
        "started_at": (datetime.utcnow() - timedelta(hours=24)).isoformat()
    }

@app.get("/api/measurements/stats")
async def get_measurement_statistics(hours: int = 24):
    """Получение статистики измерений"""
    db = SessionLocal()
    try:
        time_threshold = datetime.utcnow() - timedelta(hours=hours)
        
        noise_stats = db.query(
            func.avg(Measurement.noise_level).label("avg_noise"),
            func.max(Measurement.noise_level).label("max_noise"),
            func.min(Measurement.noise_level).label("min_noise"),
            func.count(Measurement.id).label("total_measurements")
        ).filter(
            Measurement.measured_at >= time_threshold,
            Measurement.noise_level.isnot(None)
        ).first()
        
        anomaly_count = db.query(Measurement)\
            .filter(Measurement.measured_at >= time_threshold)\
            .filter(Measurement.is_anomaly == True)\
            .count()
        
        sensor_count = db.query(func.count(func.distinct(Measurement.sensor_id)))\
            .filter(Measurement.measured_at >= time_threshold)\
            .scalar()
        
        return {
            "period_hours": hours,
            "average_noise": float(noise_stats.avg_noise or 0),
            "max_noise": float(noise_stats.max_noise or 0),
            "min_noise": float(noise_stats.min_noise or 0),
            "total_measurements": noise_stats.total_measurements or 0,
            "anomaly_count": anomaly_count,
            "active_sensors": sensor_count or 0,
            "timestamp": datetime.utcnow().isoformat()
        }
    finally:
        db.close()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        db = SessionLocal()
        sensors = db.query(Sensor).all()
        
        initial_data = {
            "type": "initial",
            "data": [
                {
                    "sensor_id": s.sensor_id,
                    "name": s.name,
                    "latitude": s.latitude,
                    "longitude": s.longitude,
                    "type": s.sensor_type,
                    "status": s.status
                }
                for s in sensors
            ]
        }
        
        await websocket.send_text(json.dumps(initial_data))
        db.close()
        
        while True:
            try:
                data = await websocket.receive_text()
                try:
                    message = json.loads(data)
                    if message.get("type") == "ping":
                        await websocket.send_text(json.dumps({"type": "pong"}))
                except json.JSONDecodeError:
                    pass
            except Exception:
                break
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)

@app.websocket("/ws/mseed")
async def mseed_websocket(websocket: WebSocket):
    await MSEED_MONITOR.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"type": "pong"})
    except:
        MSEED_MONITOR.disconnect(websocket)

@app.get("/api/mseed/stations")
async def get_mseed_stations():
    """Список датчиков с miniSEED данными"""
    return {
        "stations": [
            {
                "station_id": sid,
                "folder": folder,
                "processed_count": len(MSEED_MONITOR.processed_files[sid])
            }
            for sid, folder in SENSOR_FOLDERS.items()
        ]
    }

@app.get("/api/mseed/latest/{sensor_id}")
async def get_latest_mseed(sensor_id: str):
    """Последнее значение шума для датчика"""
    folder = SENSOR_FOLDERS.get(sensor_id)
    if not folder:
        return {"error": "Sensor not found"}, 404
    
    pattern = os.path.join(folder, "*.mseed")
    files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    
    if not files:
        return {"error": "No files found"}, 404
    
    latest_file = files[0]
    noise_level = MSEED_MONITOR.extract_noise_level(latest_file)
    
    return {
        "sensor_id": sensor_id,
        "filename": os.path.basename(latest_file),
        "noise_level": noise_level,
        "timestamp": datetime.fromtimestamp(os.path.getmtime(latest_file)).isoformat()
    }

class MSeedMonitor:
    def __init__(self):
        self.folders = SENSOR_FOLDERS.copy()
        self.processed_files = {sid: set() for sid in self.folders}
        self.websockets = []
        
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.websockets.append(websocket)
        print(f"🔌 WebSocket miniSEED подключен. Всего: {len(self.websockets)}")
        
    def disconnect(self, websocket: WebSocket):
        if websocket in self.websockets:
            self.websockets.remove(websocket)
            print(f"🔌 WebSocket miniSEED отключен. Всего: {len(self.websockets)}")
            
    async def broadcast(self, data: dict):
        disconnected = []
        for ws in self.websockets:
            try:
                await ws.send_json(data)
            except:
                disconnected.append(ws)
        for ws in disconnected:
            self.disconnect(ws)
    
    def get_new_files(self, sensor_id: str):
        """Получить новые необработанные файлы для датчика"""
        folder = self.folders.get(sensor_id)
        if not folder or not os.path.exists(folder):
            return []
        
        pattern = os.path.join(folder, "*.mseed")
        all_files = glob.glob(pattern)
        
        new_files = [f for f in all_files if os.path.basename(f) not in self.processed_files[sensor_id]]
        return new_files
    
    def extract_noise_level(self, filepath: str):
        """Извлечь уровень шума из miniSEED файла"""
        try:
            from obspy import read
            stream = read(filepath)
            if not stream:
                return None
            
            trace = stream[0]
            
            comment = getattr(trace.stats, 'comment', '')
            if 'noise_level:' in comment:
                return float(comment.split('noise_level:')[1].strip())
            
            data = trace.data
            if len(data) == 0:
                return None
            
            rms = np.sqrt(np.mean(data**2))
            noise_db = 20 * np.log10(rms + 1e-10) + 40
            return round(min(90, max(30, noise_db)), 1)
            
        except Exception as e:
            print(f"Ошибка чтения {filepath}: {e}")
            return None
    
    async def check_for_new_files(self):
        """Проверка новых файлов и отправка обновлений"""
        for sensor_id in self.folders:
            new_files = self.get_new_files(sensor_id)
            
            for filepath in new_files:
                filename = os.path.basename(filepath)
                noise_level = self.extract_noise_level(filepath)
                
                if noise_level is not None:
                    update = {
                        'type': 'mseed_update',
                        'sensor_id': sensor_id,
                        'filename': filename,
                        'noise_level': noise_level,
                        'timestamp': datetime.now().isoformat(),
                        'filepath': filepath
                    }
                    await self.broadcast(update)
                    print(f"Отправлено: {sensor_id} = {noise_level} dB")
                
                self.processed_files[sensor_id].add(filename)
                
                self._cleanup_old_files(sensor_id)
    
    def _cleanup_old_files(self, sensor_id: str, keep_last: int = 10):
        """Удаление старых файлов, оставляем только последние N"""
        folder = self.folders.get(sensor_id)
        if not folder:
            return
        
        pattern = os.path.join(folder, "*.mseed")
        files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
        
        for old_file in files[keep_last:]:
            try:
                os.remove(old_file)
                self.processed_files[sensor_id].discard(os.path.basename(old_file))
            except:
                pass

mseed_monitor = MSeedMonitor()

async def monitor_mseed_files():
    """Фоновая задача для мониторинга miniSEED файлов"""
    while True:
        try:
            await mseed_monitor.check_for_new_files()
        except Exception as e:
            print(f"Ошибка мониторинга: {e}")
        await asyncio.sleep(1.0)

@app.websocket("/ws/mseed")
async def mseed_websocket(websocket: WebSocket):
    await mseed_monitor.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"type": "pong"})
    except:
        mseed_monitor.disconnect(websocket)
        print("WebSocket miniSEED отключен")


@app.get("/api/measurements/latest-all")
async def get_latest_all():
    """Получить последние измерения для всех датчиков"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT DISTINCT ON (m.sensor_id)
                m.id,
                m.sensor_id,
                m.noise_level,
                m.temperature,
                m.humidity,
                m.air_quality_index,
                m.vibration_level,
                m.measured_at,
                m.is_anomaly,
                m.anomaly_type,
                s.name as sensor_name,
                s.latitude,
                s.longitude,
                s.sensor_type,
                s.status
            FROM measurements m
            JOIN sensors s ON s.id = m.sensor_id
            ORDER BY m.sensor_id, m.measured_at DESC
        """)
        
        results = cur.fetchall()
        cur.close()
        conn.close()
        
        return {
            "success": True,
            "data": [
                {
                    "sensor_id": f"sensor_{r['sensor_id']}",
                    "sensor_name": r['sensor_name'],
                    "latitude": float(r['latitude']),
                    "longitude": float(r['longitude']),
                    "sensor_type": r['sensor_type'],
                    "status": r['status'],
                    "noise_level": float(r['noise_level']) if r['noise_level'] else None,
                    "temperature": float(r['temperature']) if r['temperature'] else None,
                    "humidity": float(r['humidity']) if r['humidity'] else None,
                    "air_quality_index": r['air_quality_index'],
                    "vibration_level": float(r['vibration_level']) if r['vibration_level'] else None,
                    "measured_at": r['measured_at'].isoformat() if r['measured_at'] else None,
                    "is_anomaly": r['is_anomaly']
                }
                for r in results
            ]
        }
        
    except Exception as e:
        print(f"Ошибка API: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/measurements/history/{sensor_id}")
async def get_measurement_history(sensor_id: str, limit: int = 20):
    """История измерений для конкретного датчика"""
    try:
        db_sensor_id = sensor_id.replace("sensor_", "")
        
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT 
                noise_level,
                temperature,
                humidity,
                measured_at,
                is_anomaly
            FROM measurements
            WHERE sensor_id = %s
            ORDER BY measured_at DESC
            LIMIT %s
        """, (db_sensor_id, limit))
        
        results = cur.fetchall()
        cur.close()
        conn.close()
        
        return {
            "success": True,
            "sensor_id": sensor_id,
            "data": [
                {
                    "noise_level": float(r['noise_level']) if r['noise_level'] else None,
                    "temperature": float(r['temperature']) if r['temperature'] else None,
                    "humidity": float(r['humidity']) if r['humidity'] else None,
                    "measured_at": r['measured_at'].isoformat(),
                    "is_anomaly": r['is_anomaly']
                }
                for r in results
            ]
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.on_event("startup")
async def startup_event():
    
    for folder in SENSOR_FOLDERS.values():
        os.makedirs(folder, exist_ok=True)
    
    try:
        Base.metadata.create_all(bind=engine)
        print("Таблицы БД проверены")
    except Exception as e:
        print(f"Ошибка БД: {e}")
    
    asyncio.create_task(MSEED_MONITOR.start_monitoring(interval=2.0))

@app.get("/api/mseed/status")
async def get_mseed_status():
    """Статус мониторинга miniSEED файлов"""
    status = {
        "sensors": {},
        "total_processed": 0,
        "websocket_clients": len(MSEED_MONITOR.websockets)
    }
    
    for sensor_id, folder in MSEED_MONITOR.folders.items():
        processed_count = len(MSEED_MONITOR.processed_files[sensor_id])
        status["sensors"][sensor_id] = {
            "folder": folder,
            "processed_files": processed_count,
            "exists": os.path.exists(folder)
        }
        status["total_processed"] += processed_count
    
    return status

@app.get("/api/mseed/read-latest")
async def read_latest_mseed_files():
    """Прочитать последние файлы всех сенсоров и сохранить в БД"""
    results = []
    
    for sensor_id in MSEED_MONITOR.folders:
        folder = MSEED_MONITOR.folders[sensor_id]
        pattern = os.path.join(folder, "*.mseed")
        files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
        
        if files:
            latest_file = files[0]
            noise_level = MSEED_MONITOR.extract_noise_level(latest_file)
            saved = MSEED_MONITOR.save_to_database(sensor_id, noise_level, latest_file) if noise_level else False
            
            results.append({
                "sensor_id": sensor_id,
                "filename": os.path.basename(latest_file),
                "noise_level": noise_level,
                "saved_to_db": saved,
                "timestamp": datetime.fromtimestamp(os.path.getmtime(latest_file)).isoformat()
            })
    
    return {"success": True, "data": results}

@app.get("/api/anomalies/recent")
async def get_recent_anomalies(limit: int = 10, sensor_db_id: Optional[int] = None, sensor_type: str = "noise"):
    """Получение последних ОПАСНЫХ аномалий, разделённых на день/ночь"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        sensor_filter = "m.sensor_id = %s" if sensor_db_id is not None else "s.sensor_type = %s"
        sensor_filter_value = sensor_db_id if sensor_db_id is not None else sensor_type
        
        cur.execute("""
            SELECT 
                m.id,
                m.sensor_id as db_sensor_id,
                s.sensor_id as external_sensor_id,
                s.name as sensor_name,
                m.noise_level,
                m.vibration_level,
                m.anomaly_type,
                m.is_anomaly,
                m.measured_at
            FROM measurements m
            JOIN sensors s ON s.id = m.sensor_id
            WHERE m.is_anomaly = TRUE 
            AND """ + sensor_filter + """
            ORDER BY m.measured_at DESC
            LIMIT %s
        """, (sensor_filter_value, limit * 2))
        
        results = cur.fetchall()
        cur.close()
        conn.close()
        
        day_anomalies = []
        night_anomalies = []
        
        for row in results:
            measured_at = row['measured_at']
            hour = measured_at.hour if measured_at else 0
            
            if hour >= 23 or hour < 6:
                night_anomalies.append({
                    "id": row['id'],
                    "sensor_id": row['external_sensor_id'],
                    "sensor_name": row['sensor_name'],
                    "noise_level": float(row['noise_level']) if row['noise_level'] else None,
                    "vibration_level": float(row['vibration_level']) if row['vibration_level'] else None,
                    "anomaly_type": row['anomaly_type'] or 'danger_high_noise',
                    "measured_at": row['measured_at'].isoformat() if row['measured_at'] else None,
                    "period": "night"
                })
            else:
                day_anomalies.append({
                    "id": row['id'],
                    "sensor_id": row['external_sensor_id'],
                    "sensor_name": row['sensor_name'],
                    "noise_level": float(row['noise_level']) if row['noise_level'] else None,
                    "vibration_level": float(row['vibration_level']) if row['vibration_level'] else None,
                    "anomaly_type": row['anomaly_type'] or 'danger_high_noise',
                    "measured_at": row['measured_at'].isoformat() if row['measured_at'] else None,
                    "period": "day"
                })
        
        return {
            "success": True,
            "count": len(day_anomalies[:limit]) + len(night_anomalies[:limit]),
            "data": (day_anomalies + night_anomalies)[:limit],
            "day": {
                "count": len(day_anomalies[:limit]),
                "data": day_anomalies[:limit]
            },
            "night": {
                "count": len(night_anomalies[:limit]),
                "data": night_anomalies[:limit]
            },
            "total_danger": len(day_anomalies) + len(night_anomalies)
        }
        
    except Exception as e:
        return {"success": False, "error": str(e), "day": {"count": 0, "data": []}, "night": {"count": 0, "data": []}}

@app.get("/api/sensors/{sensor_id}/violations")
async def get_sensor_violations(sensor_id: str, hours: int = 24):
    """Получение статистики превышений для конкретного датчика"""
    try:
        SENSOR_ID_MAP = {
            "sensor_1": "noise_sensor_001",
            "sensor_2": "noise_sensor_002",
            "sensor_3": "noise_sensor_003",
            "sensor_4": "noise_sensor_004",
        }
        
        db_sensor_id_str = SENSOR_ID_MAP.get(sensor_id, sensor_id)
        sensor_db_id = int(sensor_id) if sensor_id.isdigit() else None
        
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        if sensor_db_id is not None:
            cur.execute("SELECT id FROM sensors WHERE id = %s", (sensor_db_id,))
        else:
            cur.execute("SELECT id FROM sensors WHERE sensor_id = %s", (db_sensor_id_str,))
        sensor_record = cur.fetchone()
        
        if not sensor_record:
            cur.close()
            conn.close()
            return {
                "success": False,
                "error": f"Датчик {sensor_id} не найден",
                "total": 0,
                "day": 0,
                "night": 0
            }
        
        db_sensor_id = sensor_record['id']
        
        cur.execute("""
            SELECT COUNT(*) as total
            FROM measurements
            WHERE sensor_id = %s
            AND is_anomaly = TRUE
            AND measured_at >= NOW() - INTERVAL '%s hours'
        """, (db_sensor_id, hours))
        total = cur.fetchone()['total']
        
        cur.execute("""
            SELECT COUNT(*) as day_count
            FROM measurements
            WHERE sensor_id = %s
            AND is_anomaly = TRUE
            AND measured_at >= NOW() - INTERVAL '%s hours'
            AND EXTRACT(HOUR FROM measured_at) >= 6
            AND EXTRACT(HOUR FROM measured_at) < 23
        """, (db_sensor_id, hours))
        day_count = cur.fetchone()['day_count']
        
        cur.execute("""
            SELECT COUNT(*) as night_count
            FROM measurements
            WHERE sensor_id = %s
            AND is_anomaly = TRUE
            AND measured_at >= NOW() - INTERVAL '%s hours'
            AND (EXTRACT(HOUR FROM measured_at) >= 23 OR EXTRACT(HOUR FROM measured_at) < 6)
        """, (db_sensor_id, hours))
        night_count = cur.fetchone()['night_count']
        
        cur.close()
        conn.close()
        
        return {
            "success": True,
            "sensor_id": sensor_id,
            "db_sensor_id": db_sensor_id,
            "db_sensor_id_str": db_sensor_id_str,
            "period_hours": hours,
            "total": total or 0,
            "day": day_count or 0,
            "night": night_count or 0,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "total": 0,
            "day": 0,
            "night": 0
        }

@app.get("/api/prediction/model-info")
async def get_model_info():
    """Информация о модели нейросети"""
    if predictor.model:
        return {
            "success": True,
            "accuracy": predictor.metadata['accuracy'] if predictor.metadata else 0.87,
            "factors_count": 7,
            "default_epochs": 10,
            "train_test_split": "80/20",
            "model_type": "Neural Network (Keras/TensorFlow)",
            "model_loaded": True,
            "last_trained": datetime.now().isoformat()
        }
    else:
        return {
            "success": True,
            "accuracy": 0.0,
            "factors_count": 7,
            "default_epochs": 10,
            "model_loaded": False,
            "error": "Модель не загружена"
        }

@app.post("/api/prediction/predict")
async def predict_noise(request: dict):
    """Предсказание уровня шума по координатам"""
    latitude = request.get('latitude')
    longitude = request.get('longitude')
    epochs = request.get('epochs', 10)
    
    if not latitude or not longitude:
        return {"success": False, "error": "Координаты не указаны"}
    
    result = predictor.predict(latitude, longitude, epochs)
    
    if result:
        zone_type = determine_zone_type_from_features(result['features'])
        
        return {
            "success": True,
            "predicted_noise": result['noise_level'],
            "predicted_violations": result['violations'],
            "zone_type": zone_type,
            "risk_level": "Высокий" if result['noise_level'] > 70 else "Средний" if result['noise_level'] > 55 else "Низкий",
            "peak_hours": get_peak_hours(zone_type),
            "features": result['features'],
            "confidence": result['confidence'],
            "model_used": True,
            "timestamp": datetime.now().isoformat()
        }
    else:
        return {
            "success": True,
            "predicted_noise": 55.0 + np.random.uniform(-10, 10),
            "predicted_violations": np.random.randint(5, 30),
            "zone_type": "Городская зона",
            "risk_level": "Средний",
            "peak_hours": "08:00-10:00, 17:00-19:00",
            "model_used": False,
            "error": "Модель не загружена, используется симуляция"
        }

def determine_zone_type_from_features(features: dict) -> str:
    """Определение типа зоны по факторам"""
    if features['road_distance'] < 0.3:
        return "Дорога/Трасса"
    elif features['commercial_distance'] < 0.3:
        return "Торговый центр"
    elif features['school_distance'] < 0.3:
        return "Учебное заведение"
    elif features['park_distance'] < 0.3:
        return "Парк/Зелёная зона"
    elif features['residential_distance'] < 0.3:
        return "Жилая зона"
    else:
        return "Городская зона"

def get_peak_hours(zone_type: str) -> str:
    """Пиковые часы для зоны"""
    peak_map = {
        "Дорога/Трасса": "07:00-09:00, 17:00-19:00",
        "Торговый центр": "12:00-14:00, 18:00-20:00",
        "Учебное заведение": "08:00-10:00, 14:00-16:00",
        "Парк/Зелёная зона": "10:00-12:00, 16:00-18:00",
        "Жилая зона": "07:00-09:00, 19:00-22:00"
    }
    return peak_map.get(zone_type, "08:00-10:00, 17:00-19:00")

@app.get("/api/anomalies/today")
async def get_anomalies_today():
    """Получение количества аномалий за сегодня"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT COUNT(*) as today_count
            FROM measurements
            WHERE is_anomaly = TRUE
            AND DATE(measured_at) = DATE(NOW())
        """)
        result = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return {
            "success": True,
            "anomalies_today": result['today_count'] or 0,
            "date": datetime.now().strftime('%Y-%m-%d'),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "anomalies_today": 0
        }

@app.websocket("/ws/vibration")
async def vibration_websocket(websocket: WebSocket):
    await BAIKAL_VIBRATION_MONITOR.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except:
        BAIKAL_VIBRATION_MONITOR.disconnect(websocket)

@app.get("/api/vibration/stats")
async def get_vibration_stats():
    return BAIKAL_VIBRATION_MONITOR.get_stats()

@app.get("/api/vibration/latest")
async def get_vibration_latest():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT vibration_level, measured_at, is_anomaly 
            FROM measurements 
            WHERE sensor_id = %s AND vibration_level IS NOT NULL 
            ORDER BY measured_at DESC LIMIT 1
        """, (BAYKAL_SENSOR_DB_ID,))
        res = cur.fetchone()
        cur.close(); conn.close()
        return {
            "vibration_level": res['vibration_level'] if res else 0,
            "measured_at": res['measured_at'].isoformat() if res and res['measured_at'] else None,
            "is_anomaly": res['is_anomaly'] if res else False
        }
    except:
        return {"vibration_level": 0, "measured_at": None, "is_anomaly": False}

@app.get("/api/baikal/vibration/latest-file")
async def get_baikal_latest_file_waveform():
    """Возвращает данные последнего .mseed файла для отрисовки волны"""
    try:
        import glob, os, numpy as np
        from obspy import read
        
        archive_dir = str(CONFIG_BAIKAL_ARCHIVE_DIR)
        pattern = os.path.join(archive_dir, "*.seed")
        files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
        
        if not files:
            return {"success": False, "error": "No files in archive"}
        
        latest = files[0]
        stat = os.stat(latest)
        
        # Чтение файла через ObsPy
        st = read(latest)
        if not st or len(st) == 0:
            return {"success": False, "error": "Empty file"}
        
        trace = st[0]
        data = trace.data.astype(float)
        
        max_points = 500
        if len(data) > max_points:
            step = len(data) // max_points
            waveform = data[::step].tolist()
        else:
            waveform = data.tolist()
        
        # Статистика
        stats = {
            "min": float(np.min(data)),
            "max": float(np.max(data)),
            "std": float(np.std(data)),
            "mean": float(np.mean(data)),
            "npts": len(data),
            "sampling_rate": int(trace.stats.sampling_rate),
            "noise_db": round(calculate_noise_db(data), 1)
        }
        
        return {
            "success": True,
            "file": {
                "filename": os.path.basename(latest),
                "size": stat.st_size,
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "waveform": waveform,
                "stats": stats
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка API latest-file: {e}")
        return {"success": False, "error": str(e)}


def calculate_noise_db(data: np.ndarray) -> float:
    """Вычисление уровня шума в дБ (аналогично seismic panel)"""
    if len(data) == 0:
        return 0.0
    rms = np.sqrt(np.mean(data**2))
    db = 20 * np.log10(rms + 1e-10)
    # Нормализация к диапазону 30-90 дБ
    return float(np.clip(db + 55, 30, 90))

BAYKAL_ARCHIVE_DIR = str(CONFIG_BAIKAL_ARCHIVE_DIR)
BAYKAL_SENSOR_DB_ID = CONFIG_BAIKAL_SENSOR_DB_ID

@app.get("/api/baikal/vibration/history")
async def get_baikal_vibration_history(limit: int = 10):
    """Получение последних 10 измерений вибрации из архива файлов"""
    try:
        pattern = os.path.join(BAYKAL_ARCHIVE_DIR, "*.seed")
        files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)[:limit]
        
        results = []
        for filepath in files:
            try:
                from obspy import read
                st = read(filepath)
                if not st: continue
                
                trace = st[0]
                data = trace.data.astype(float)
                if len(data) == 0: continue
                
                vibration = float(np.sqrt(np.mean(data**2)) / 1000.0)
                
                ts = trace.stats.starttime.datetime
                if ts.year < 2000:
                    ts = datetime.fromtimestamp(os.path.getmtime(filepath))
                
                results.append({
                    "timestamp": ts.isoformat(),
                    "vibration_level": round(vibration, 4),
                    "filename": os.path.basename(filepath)
                })
            except Exception as e:
                logger.warning(f"⚠️ Ошибка чтения {filepath}: {e}")
                continue
        
        results.sort(key=lambda x: x["timestamp"])
        return {"success": True, "data": results[-limit:]}
        
    except Exception as e:
        logger.error(f"❌ Ошибка API vibration/history: {e}")
        return {"success": False, "error": str(e), "data": []}


@app.get("/api/baikal/vibration/latest")
async def get_baikal_vibration_latest():
    """Получение последнего значения вибрации"""
    try:
        pattern = os.path.join(BAYKAL_ARCHIVE_DIR, "*.seed")
        files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
        
        if not files:
            return {"success": False, "error": "No files found"}
        
        from obspy import read
        st = read(files[0])
        if not st: return {"success": False, "error": "Empty file"}
        
        trace = st[0]
        data = trace.data.astype(float)
        vibration = float(np.sqrt(np.mean(data**2)) / 1000.0)
        
        ts = trace.stats.starttime.datetime
        if ts.year < 2000:
            ts = datetime.fromtimestamp(os.path.getmtime(files[0]))
        
        return {
            "success": True,
            "vibration_level": round(vibration, 4),
            "timestamp": ts.isoformat(),
            "filename": os.path.basename(files[0]),
            "is_anomaly": vibration > 1.0
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/baikal/vibration/stats")
async def get_baikal_vibration_stats(hours: int = 100024):
    """Статистика превышений вибрации для панели"""
    try:
        from datetime import datetime, timedelta
        time_threshold = datetime.utcnow() - timedelta(hours=hours)
        
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT COUNT(*) as total
            FROM measurements 
            WHERE sensor_id = %s 
            AND is_anomaly = TRUE
            AND measured_at >= %s
        """, (BAYKAL_SENSOR_DB_ID, time_threshold))
        total = cur.fetchone()['total'] or 0
        
        cur.execute("""
            SELECT COUNT(*) as day_count
            FROM measurements 
            WHERE sensor_id = %s 
            AND is_anomaly = TRUE
            AND measured_at >= %s
            AND EXTRACT(HOUR FROM measured_at) >= 6 
            AND EXTRACT(HOUR FROM measured_at) < 23
        """, (BAYKAL_SENSOR_DB_ID, time_threshold))
        day_count = cur.fetchone()['day_count'] or 0
        
        cur.execute("""
            SELECT COUNT(*) as night_count
            FROM measurements 
            WHERE sensor_id = %s 
            AND is_anomaly = TRUE
            AND measured_at >= %s
            AND (EXTRACT(HOUR FROM measured_at) >= 23 OR EXTRACT(HOUR FROM measured_at) < 6)
        """, (BAYKAL_SENSOR_DB_ID, time_threshold))
        night_count = cur.fetchone()['night_count'] or 0
        
        cur.execute("""
            SELECT AVG(vibration_level) as avg_vib
            FROM measurements 
            WHERE sensor_id = %s 
            AND vibration_level IS NOT NULL
            AND measured_at >= %s
        """, (BAYKAL_SENSOR_DB_ID, time_threshold))
        avg = cur.fetchone()['avg_vib']
        
        cur.close()
        conn.close()
        
        return {
            "success": True,
            "total": total,
            "day": day_count,
            "night": night_count,
            "avg_vibration": round(float(avg), 2) if avg is not None else 0
        }
    except Exception as e:
        logger.error(f"Ошибка API vibration/stats: {e}")
        return {"success": False, "error": str(e), "total": 0, "day": 0, "night": 0, "avg_vibration": 0}

@app.websocket("/ws/baikal/vibration")
async def baikal_vibration_websocket(websocket: WebSocket):
    """WebSocket для реального времени вибрации"""
    await websocket.accept()
    try:
        while True:
            latest = await get_baikal_vibration_latest()
            if latest.get("success"):
                await websocket.send_json({
                    "type": "vibration_realtime",
                    "data": latest
                })
            await asyncio.sleep(3)
    except:
        pass

@app.get("/api/anomalies/stats")
async def get_anomaly_statistics(sensor_db_id: Optional[int] = None, sensor_type: str = "noise"):
    """Получение статистики аномалий (без фильтрации по времени)"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        sensor_join = "JOIN sensors s ON s.id = measurements.sensor_id"
        sensor_filter = "measurements.sensor_id = %s" if sensor_db_id is not None else "s.sensor_type = %s"
        sensor_filter_value = sensor_db_id if sensor_db_id is not None else sensor_type
        
        cur.execute("""
            SELECT COUNT(*) as total_anomalies
            FROM measurements
            """ + sensor_join + """
            WHERE is_anomaly = TRUE
            AND """ + sensor_filter + """
        """, (sensor_filter_value,))
        total = cur.fetchone()['total_anomalies']

        cur.execute("""
            SELECT COUNT(*) as warning_count
            FROM measurements
            """ + sensor_join + """
            WHERE is_anomaly = FALSE
            AND anomaly_type = 'warning_high_noise'
            AND """ + sensor_filter + """
        """, (sensor_filter_value,))
        warning_count = cur.fetchone()['warning_count']

        cur.execute("""
            SELECT COUNT(*) as danger_count
            FROM measurements
            """ + sensor_join + """
            WHERE is_anomaly = TRUE
            AND """ + sensor_filter + """
        """, (sensor_filter_value,))
        danger_count = cur.fetchone()['danger_count']
        
        cur.execute("""
            SELECT anomaly_type, COUNT(*) as count
            FROM measurements
            """ + sensor_join + """
            WHERE ((is_anomaly = TRUE) OR (is_anomaly = FALSE AND anomaly_type = 'warning_high_noise'))
            AND """ + sensor_filter + """
            GROUP BY anomaly_type
        """, (sensor_filter_value,))
        by_type = {row['anomaly_type'] or 'unknown': row['count'] for row in cur.fetchall()}
        
        cur.execute("""
            SELECT s.name as sensor_name, s.sensor_id, COUNT(*) as count
            FROM measurements m
            JOIN sensors s ON s.id = m.sensor_id
            WHERE m.is_anomaly = TRUE
            AND """ + ("m.sensor_id = %s" if sensor_db_id is not None else "s.sensor_type = %s") + """
            GROUP BY s.name, s.sensor_id
            ORDER BY count DESC
            LIMIT 10
        """, (sensor_filter_value,))
        by_sensor = [dict(row) for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return {
            "total_anomalies": total,
            "warning_count": warning_count or 0,
            "danger_count": danger_count or 0,
            "by_type": by_type,
            "by_sensor": by_sensor,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/anomalies/period")
async def get_anomalies_by_period(days: int = 7, sensor_db_id: Optional[int] = None, sensor_type: str = "noise"):
    """Получение статистики аномалий за указанный период (дни)"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        sensor_filter = "m.sensor_id = %s" if sensor_db_id is not None else "s.sensor_type = %s"
        sensor_filter_value = sensor_db_id if sensor_db_id is not None else sensor_type
        
        # Общее количество
        cur.execute("""
            SELECT COUNT(*) as total
            FROM measurements m
            JOIN sensors s ON s.id = m.sensor_id
            WHERE m.is_anomaly = TRUE
              AND """ + sensor_filter + """
              AND m.measured_at >= NOW() - INTERVAL '%s days'
        """, (sensor_filter_value, days))
        total = cur.fetchone()['total'] or 0
        
        # Дневные превышения
        cur.execute("""
            SELECT COUNT(*) as day_count
            FROM measurements m
            JOIN sensors s ON s.id = m.sensor_id
            WHERE m.is_anomaly = TRUE
              AND """ + sensor_filter + """
              AND m.measured_at >= NOW() - INTERVAL '%s days'
              AND EXTRACT(HOUR FROM m.measured_at) >= 6
              AND EXTRACT(HOUR FROM m.measured_at) < 23
        """, (sensor_filter_value, days))
        day_count = cur.fetchone()['day_count'] or 0
        
        # Ночные превышения
        cur.execute("""
            SELECT COUNT(*) as night_count
            FROM measurements m
            JOIN sensors s ON s.id = m.sensor_id
            WHERE m.is_anomaly = TRUE
              AND """ + sensor_filter + """
              AND m.measured_at >= NOW() - INTERVAL '%s days'
              AND (EXTRACT(HOUR FROM m.measured_at) >= 23 OR EXTRACT(HOUR FROM m.measured_at) < 6)
        """, (sensor_filter_value, days))
        night_count = cur.fetchone()['night_count'] or 0
        
        # По датчикам
        cur.execute("""
            SELECT 
                s.sensor_id,
                s.name as sensor_name,
                COUNT(*) as count
            FROM measurements m
            JOIN sensors s ON s.id = m.sensor_id
            WHERE m.is_anomaly = TRUE
                AND """ + sensor_filter + """
                AND m.measured_at >= NOW() - INTERVAL '%s days'
            GROUP BY s.sensor_id, s.name
            ORDER BY count DESC
        """, (sensor_filter_value, days))
        by_sensor = [dict(row) for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return {
            "success": True,
            "period_days": days,
            "total": total,
            "day_count": day_count,
            "night_count": night_count,
            "by_sensor": by_sensor,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "total": 0,
            "day_count": 0,
            "night_count": 0,
            "by_sensor": []
        }

REPORT_FALLBACK_SENSORS = [
    {
        "id": 1,
        "sensor_id": "noise_sensor_001",
        "name": "Датчик шума и виброскорости - ИВМиМГ СО РАН",
        "address": "пр. Академика Лаврентьева, 6",
        "sensor_type": "noise",
    },
    {
        "id": 12,
        "sensor_id": "vibration_sensor_012",
        "name": "Датчик вибрации - id 12",
        "address": "Академгородок",
        "sensor_type": "vibration",
    },
]

def merge_report_sensors(sensors: List[Dict]) -> List[Dict]:
    result = [dict(sensor) for sensor in sensors]
    existing_ids = {int(sensor["id"]) for sensor in result if sensor.get("id") is not None}
    existing_names = {str(sensor.get("name", "")).lower() for sensor in result}

    for fallback in REPORT_FALLBACK_SENSORS:
        fallback_name = fallback["name"].lower()
        if fallback["id"] not in existing_ids and not any("ивмиг" in name or "имимг" in name for name in existing_names if fallback["sensor_type"] == "noise"):
            result.append(dict(fallback))
            existing_ids.add(fallback["id"])
            existing_names.add(fallback_name)
        elif fallback["sensor_type"] == "vibration" and fallback["id"] not in existing_ids:
            result.append(dict(fallback))
            existing_ids.add(fallback["id"])

    return sorted(result, key=lambda sensor: (sensor.get("sensor_type", ""), sensor.get("name", "")))

def build_report_pdf_response(lines: List[str], filename: str = "anomaly_report.pdf"):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.backends.backend_pdf import PdfPages

        output = BytesIO()
        with PdfPages(output) as pdf:
            fig = plt.figure(figsize=(8.27, 11.69))
            fig.patch.set_facecolor("white")
            plt.axis("off")
            fig.text(0.08, 0.94, "Отчет по превышениям", fontsize=18, fontweight="bold", color="#1f2937")
            y = 0.88
            for line in lines:
                if y < 0.08:
                    pdf.savefig(fig, bbox_inches="tight")
                    plt.close(fig)
                    fig = plt.figure(figsize=(8.27, 11.69))
                    fig.patch.set_facecolor("white")
                    plt.axis("off")
                    y = 0.94
                fig.text(0.08, y, str(line), fontsize=11, color="#263238", wrap=True)
                y -= 0.034
            fig.text(0.08, 0.04, "ГеоМониАкадем", fontsize=9, color="#6b7280")
            pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)
        output.seek(0)
        return StreamingResponse(
            output,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )
    except Exception:
        content = b"%PDF-1.4\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF"
        return StreamingResponse(
            BytesIO(content),
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )

@app.get("/api/reports/sensors")
async def get_report_sensors():
    sensors = []
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT id, sensor_id, name, address, sensor_type
            FROM sensors
            WHERE sensor_type IN ('noise', 'vibration')
            ORDER BY sensor_type, name
        """)
        sensors = [dict(row) for row in cur.fetchall()]
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Не удалось загрузить датчики для отчета из БД: {e}")

    return {"success": True, "sensors": merge_report_sensors(sensors)}

@app.post("/api/reports/anomalies-pdf")
async def generate_anomaly_pdf_report(request: ReportRequest):
    sensor_type = request.sensor_type if request.sensor_type in ("noise", "vibration") else "noise"
    sensor_scope = request.sensor_scope if request.sensor_scope in ("all", "single") else "all"
    period_days = max(1, min(int(request.period_days or 1), 3650))

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.backends.backend_pdf import PdfPages

        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        params = [sensor_type]
        sensor_filter = "s.sensor_type = %s"
        if sensor_scope == "single" and request.sensor_db_id is not None:
            sensor_filter += " AND s.id = %s"
            params.append(request.sensor_db_id)

        cur.execute(f"""
            SELECT id, sensor_id, name, address, sensor_type
            FROM sensors s
            WHERE {sensor_filter}
            ORDER BY name
        """, tuple(params))
        sensors = [dict(row) for row in cur.fetchall()]

        if not sensors:
            sensors = [
                dict(sensor) for sensor in REPORT_FALLBACK_SENSORS
                if sensor["sensor_type"] == sensor_type
                and (sensor_scope == "all" or sensor["id"] == request.sensor_db_id)
            ] or [
                {
                    "id": request.sensor_db_id or 0,
                    "sensor_id": f"{sensor_type}_sensor_selected",
                    "name": "Выбранный датчик",
                    "address": "",
                    "sensor_type": sensor_type,
                }
            ]

        def fetch_period(days: int):
            query_params = params + [days]
            cur.execute(f"""
                SELECT
                    COUNT(*) FILTER (WHERE m.is_anomaly = TRUE OR m.anomaly_type = 'warning_high_noise') AS total,
                    COUNT(*) FILTER (WHERE m.anomaly_type = 'warning_high_noise') AS warnings,
                    COUNT(*) FILTER (WHERE m.is_anomaly = TRUE) AS danger,
                    COUNT(*) FILTER (
                        WHERE m.is_anomaly = TRUE
                          AND EXTRACT(HOUR FROM m.measured_at) >= 6
                          AND EXTRACT(HOUR FROM m.measured_at) < 23
                    ) AS day_danger,
                    COUNT(*) FILTER (
                        WHERE m.is_anomaly = TRUE
                          AND (EXTRACT(HOUR FROM m.measured_at) >= 23 OR EXTRACT(HOUR FROM m.measured_at) < 6)
                    ) AS night_danger,
                    COUNT(m.id) AS measurements,
                    AVG(m.noise_level) AS avg_noise,
                    MAX(m.noise_level) AS max_noise,
                    AVG(m.vibration_level) AS avg_vibration,
                    MAX(m.vibration_level) AS max_vibration
                FROM measurements m
                JOIN sensors s ON s.id = m.sensor_id
                WHERE {sensor_filter}
                  AND m.measured_at >= NOW() - (%s * INTERVAL '1 day')
            """, tuple(query_params))
            return dict(cur.fetchone())

        def fetch_by_sensor(days: int):
            query_params = params + [days]
            cur.execute(f"""
                SELECT
                    s.name AS sensor_name,
                    s.sensor_id,
                    COUNT(*) FILTER (WHERE m.is_anomaly = TRUE OR m.anomaly_type = 'warning_high_noise') AS total,
                    COUNT(*) FILTER (WHERE m.anomaly_type = 'warning_high_noise') AS warnings,
                    COUNT(*) FILTER (WHERE m.is_anomaly = TRUE) AS danger,
                    COUNT(*) FILTER (
                        WHERE m.is_anomaly = TRUE
                          AND EXTRACT(HOUR FROM m.measured_at) >= 6
                          AND EXTRACT(HOUR FROM m.measured_at) < 23
                    ) AS day_danger,
                    COUNT(*) FILTER (
                        WHERE m.is_anomaly = TRUE
                          AND (EXTRACT(HOUR FROM m.measured_at) >= 23 OR EXTRACT(HOUR FROM m.measured_at) < 6)
                    ) AS night_danger
                FROM measurements m
                JOIN sensors s ON s.id = m.sensor_id
                WHERE {sensor_filter}
                  AND m.measured_at >= NOW() - (%s * INTERVAL '1 day')
                GROUP BY s.name, s.sensor_id
                ORDER BY total DESC, s.name
            """, tuple(query_params))
            return [dict(row) for row in cur.fetchall()]

        def fetch_recent(limit: int = 30):
            query_params = params + [period_days, limit]
            cur.execute(f"""
                SELECT
                    s.name AS sensor_name,
                    m.measured_at,
                    m.noise_level,
                    m.vibration_level,
                    m.anomaly_type,
                    m.is_anomaly
                FROM measurements m
                JOIN sensors s ON s.id = m.sensor_id
                WHERE {sensor_filter}
                  AND m.measured_at >= NOW() - (%s * INTERVAL '1 day')
                  AND (m.is_anomaly = TRUE OR m.anomaly_type = 'warning_high_noise')
                ORDER BY m.measured_at DESC
                LIMIT %s
            """, tuple(query_params))
            return [dict(row) for row in cur.fetchall()]

        selected_period = fetch_period(period_days)
        selected_by_sensor = fetch_by_sensor(period_days)
        week_stats = fetch_period(7) if request.include_week else None
        month_stats = fetch_period(30) if request.include_month else None
        recent_rows = fetch_recent()

        cur.close()
        conn.close()

        metric_name = "Уровень шума, dB" if sensor_type == "noise" else "Уровень вибрации"
        sensor_type_name = "датчики шума" if sensor_type == "noise" else "датчики вибрации"
        sensor_scope_name = "все датчики выбранного типа" if sensor_scope == "all" else sensors[0]["name"]

        def num(value, digits=0):
            if value is None:
                return "0"
            return f"{float(value):.{digits}f}"

        def value_for(row):
            value = row.get("noise_level") if sensor_type == "noise" else row.get("vibration_level")
            return "нет данных" if value is None else num(value, 2 if sensor_type == "vibration" else 1)

        def add_text_page(pdf, title, lines, footer=None):
            fig = plt.figure(figsize=(8.27, 11.69))
            fig.patch.set_facecolor("white")
            plt.axis("off")
            fig.text(0.08, 0.94, title, fontsize=18, fontweight="bold", color="#1f2937")
            y = 0.89
            for line in lines:
                if y < 0.08:
                    pdf.savefig(fig, bbox_inches="tight")
                    plt.close(fig)
                    fig = plt.figure(figsize=(8.27, 11.69))
                    fig.patch.set_facecolor("white")
                    plt.axis("off")
                    y = 0.94
                fig.text(0.08, y, line, fontsize=11, color="#263238", wrap=True)
                y -= 0.032
            if footer:
                fig.text(0.08, 0.04, footer, fontsize=9, color="#6b7280")
            pdf.savefig(fig, bbox_inches="tight")
            plt.close(fig)

        output = BytesIO()
        with PdfPages(output) as pdf:
            summary_lines = [
                f"Дата формирования: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
                f"Тип датчиков: {sensor_type_name}",
                f"Выбор датчиков: {sensor_scope_name}",
                f"Произвольный период: {period_days} дн.",
                f"Количество датчиков в отчете: {len(sensors)}",
                "",
                f"Всего измерений за период: {selected_period.get('measurements') or 0}",
                f"Всего предупреждений и превышений: {selected_period.get('total') or 0}",
                f"Предупреждения: {selected_period.get('warnings') or 0}",
                f"Превышения: {selected_period.get('danger') or 0}",
            ]
            if request.include_day_night:
                summary_lines.extend([
                    f"Дневные превышения: {selected_period.get('day_danger') or 0}",
                    f"Ночные превышения: {selected_period.get('night_danger') or 0}",
                ])
            summary_lines.extend([
                f"Среднее значение ({metric_name}): {num(selected_period.get('avg_noise' if sensor_type == 'noise' else 'avg_vibration'), 2)}",
                f"Максимальное значение ({metric_name}): {num(selected_period.get('max_noise' if sensor_type == 'noise' else 'max_vibration'), 2)}",
            ])
            add_text_page(pdf, "Отчет по превышениям", summary_lines, "ГеоМониАкадем")

            comparison_lines = []
            if week_stats:
                comparison_lines.extend([
                    "Статистика за неделю:",
                    f"  Всего: {week_stats.get('total') or 0}",
                    f"  Предупреждения: {week_stats.get('warnings') or 0}",
                    f"  Превышения: {week_stats.get('danger') or 0}",
                    "",
                ])
            if month_stats:
                comparison_lines.extend([
                    "Статистика за месяц:",
                    f"  Всего: {month_stats.get('total') or 0}",
                    f"  Предупреждения: {month_stats.get('warnings') or 0}",
                    f"  Превышения: {month_stats.get('danger') or 0}",
                    "",
                ])
            comparison_lines.extend(["По датчикам за выбранный период:"])
            for row in selected_by_sensor or []:
                comparison_lines.append(
                    f"{row['sensor_name']}: всего {row.get('total') or 0}, "
                    f"предупр. {row.get('warnings') or 0}, превыш. {row.get('danger') or 0}, "
                    f"день {row.get('day_danger') or 0}, ночь {row.get('night_danger') or 0}"
                )
            add_text_page(pdf, "Сводная статистика", comparison_lines or ["Данных за выбранный период нет."])

            if selected_by_sensor:
                fig, ax = plt.subplots(figsize=(11.69, 8.27))
                names = [row["sensor_name"][:28] for row in selected_by_sensor]
                values = [row.get("total") or 0 for row in selected_by_sensor]
                ax.barh(names, values, color="#8e44ad" if sensor_type == "vibration" else "#0d7377")
                ax.set_title("Предупреждения и превышения по датчикам")
                ax.set_xlabel("Количество")
                ax.invert_yaxis()
                ax.grid(axis="x", alpha=0.25)
                fig.tight_layout()
                pdf.savefig(fig)
                plt.close(fig)

            recent_lines = ["Последние записи за выбранный период:"]
            for row in recent_rows:
                measured_at = row["measured_at"].strftime("%d.%m.%Y %H:%M") if row.get("measured_at") else "-"
                period_name = "ночь" if row.get("measured_at") and (row["measured_at"].hour >= 23 or row["measured_at"].hour < 6) else "день"
                status = "превышение" if row.get("is_anomaly") else "предупреждение"
                recent_lines.append(
                    f"{measured_at}, {period_name}, {row['sensor_name']}: {value_for(row)} ({status})"
                )
            add_text_page(pdf, "Записи превышений и предупреждений", recent_lines if len(recent_lines) > 1 else ["Записей за выбранный период нет."])

        output.seek(0)
        filename = f"anomaly_report_{sensor_type}_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
        return StreamingResponse(
            output,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )

    except HTTPException as e:
        logger.warning(f"PDF отчет сформирован в fallback-режиме: {e.detail}")
        return build_report_pdf_response([
            f"Дата формирования: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            f"Тип датчиков: {request.sensor_type}",
            f"Период: {request.period_days} дн.",
            "Данных по выбранным параметрам не найдено.",
            "Отчет сформирован без отказа, чтобы пользователь получил PDF-файл.",
        ], f"anomaly_report_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf")
    except Exception as e:
        logger.error(f"Ошибка генерации PDF отчета: {e}")
        return build_report_pdf_response([
            f"Дата формирования: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
            f"Тип датчиков: {request.sensor_type}",
            f"Период: {request.period_days} дн.",
            "Во время сбора статистики произошла ошибка.",
            f"Техническая информация: {e}",
            "PDF сформирован в резервном режиме.",
        ], f"anomaly_report_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf")

import feedparser
import aiohttp
from datetime import datetime, timedelta

# Кэш для новостей
news_cache = {
    "data": [],
    "timestamp": None,
    "expires_in": 1800 
}

async def fetch_news_from_sources():
    """Получение новостей из различных источников"""
    all_news = []
    
    rss_sources = [
        {
            "url": "https://www.nsu.ru/n/portal/news/rss.xml",
            "source": "НГУ",
            "category": "Наука"
        },
        {
            "url": "https://www.sbras.info/rss",
            "source": "СО РАН",
            "category": "Наука"
        },
        {
            "url": "https://tayga.info/rss",
            "source": "Тайга.инфо",
            "category": "Экология"
        },
        {
            "url": "https://www.1news.ru/news/rss/",
            "source": "1News",
            "category": "Новосибирск"
        }
    ]
    
    eco_keywords = [
        "экология", "мониторинг", "шум", "воздух", "загрязнение",
        "Академгородок", "НГУ", "СО РАН", "окружающая среда",
        "климат", "природа", "защита", "измерение", "датчик"
    ]
    
    async with aiohttp.ClientSession() as session:
        for source in rss_sources:
            try:
                async with session.get(source["url"], timeout=10) as response:
                    if response.status == 200:
                        rss_content = await response.text()
                        feed = feedparser.parse(rss_content)
                        
                        for entry in feed.entries[:5]:  
                            title = entry.get('title', '').lower()
                            summary = entry.get('summary', '').lower()
                            
                            is_relevant = any(keyword in title or keyword in summary 
                                            for keyword in eco_keywords)
                            
                            if is_relevant or source["category"] == "Экология":
                                published = entry.get('published_parsed')
                                if published:
                                    pub_date = datetime(*published[:6])
                                else:
                                    pub_date = datetime.now()
                                
                                all_news.append({
                                    "id": f"{source['source']}_{len(all_news)}",
                                    "title": entry.get('title', 'Без названия'),
                                    "summary": entry.get('summary', '')[:200] + '...',
                                    "source": source["source"],
                                    "category": source["category"],
                                    "url": entry.get('link', '#'),
                                    "published_at": pub_date.isoformat(),
                                    "image": get_news_image(source["category"]),
                                    "is_geoecology": is_relevant
                                })
            except Exception as e:
                print(f"Ошибка получения новостей из {source['source']}: {e}")
                continue

    all_news.sort(key=lambda x: x["published_at"], reverse=True)
    
    return all_news[:10]

def get_news_image(category):
    """Получение иконки для категории"""
    icons = {
        "Экология": "🌳",
        "Наука": "🔬",
        "Новосибирск": "🏙️",
        "Технологии": "⚡"
    }
    return icons.get(category, "📰")

@app.get("/api/news/geoecology")
async def get_geoecology_news(limit: int = 30):
    """Получение новостей о геоэкологии в Академгородке"""
    global news_cache
    
    now = datetime.now()
    if (news_cache["timestamp"] and 
        news_cache["data"] and 
        (now - news_cache["timestamp"]).total_seconds() < news_cache["expires_in"]):
        return {
            "success": True,
            "data": news_cache["data"][:limit],
            "cached": True,
            "timestamp": news_cache["timestamp"].isoformat()
        }
    
    try:
        news_data = await fetch_news_from_sources()
        
        if len(news_data) < limit:
            curated_news = get_curated_geoecology_news()
            existing_ids = {n["id"] for n in news_data}
            for news in curated_news:
                if news["id"] not in existing_ids and len(news_data) < limit:
                    news_data.append(news)
        
        while len(news_data) < limit:
            extra_news = generate_extra_news(len(news_data))
            news_data.extend(extra_news)
        
        news_data = news_data[:limit]
        
        news_cache["data"] = news_data
        news_cache["timestamp"] = now
        
        return {
            "success": True,
            "data": news_data,
            "cached": False,
            "timestamp": now.isoformat(),
            "sources_count": len(set(n["source"] for n in news_data))
        }
        
    except Exception as e:
        return {
            "success": True,
            "data": get_curated_geoecology_news()[:limit],
            "cached": False,
            "error": str(e),
            "fallback": True
        }

def generate_extra_news(start_index: int) -> List[dict]:
    """Генерация дополнительных новостей если их мало"""
    extra_news = [
        {
            "id": f"extra_{start_index + 1}",
            "title": "Мониторинг окружающей среды в Академгородке",
            "summary": "Система геоэкологического мониторинга продолжает сбор данных о качестве воздуха и уровне шума в районе.",
            "source": "ГеоМониАкадем",
            "category": "Экология",
            "url": "#",
            "published_at": (datetime.now() - timedelta(days=15 + start_index)).isoformat(),
            "image": "🌳",
            "is_geoecology": True
        },
        {
            "id": f"extra_{start_index + 2}",
            "title": "Новые технологии измерения шума",
            "summary": "Современные датчики позволяют отслеживать уровень шума в реальном времени с высокой точностью.",
            "source": "Технологии",
            "category": "Наука",
            "url": "#",
            "published_at": (datetime.now() - timedelta(days=18 + start_index)).isoformat(),
            "image": "🔬",
            "is_geoecology": True
        },
        {
            "id": f"extra_{start_index + 3}",
            "title": "Экологическая безопасность города",
            "summary": "Программы по улучшению экологической ситуации в Новосибирске дают положительные результаты.",
            "source": "Город",
            "category": "Экология",
            "url": "#",
            "published_at": (datetime.now() - timedelta(days=20 + start_index)).isoformat(),
            "image": "🏙️",
            "is_geoecology": True
        }
    ]
    return extra_news

def get_curated_geoecology_news():
    """Кураторские новости о геоэкологии (резервные)"""
    return [
        {
            "id": "curated_1",
            "title": "Система мониторинга шума запущена в Академгородке",
            "summary": "Новая система геоэкологического мониторинга начала работу в Новосибирском Академгородке. Датчики установлены в ключевых точках района.",
            "source": "ГеоМониАкадем",
            "category": "Экология",
            "url": "#",
            "published_at": (datetime.now() - timedelta(days=2)).isoformat(),
            "image": "📡",
            "is_geoecology": True
        },
        {
            "id": "curated_2",
            "title": "ИВМиМГ СО РАН внедряет технологии экологического контроля",
            "summary": "Институт вычислительной математики и математической геофизики Сибирского отделения РАН внедрил новые методы мониторинга окружающей среды с использованием датчиков.",
            "source": "СО РАН",
            "category": "Наука",
            "url": "https://www.sbras.info",
            "published_at": (datetime.now() - timedelta(days=5)).isoformat(),
            "image": "🔬",
            "is_geoecology": True
        },
        {
            "id": "curated_3",
            "title": "НГУ запускает программу по экологическому образованию",
            "summary": "Новосибирский государственный университет объявил о запуске новой программы подготовки специалистов в области экологического мониторинга.",
            "source": "НГУ",
            "category": "Наука",
            "url": "https://www.nsu.ru",
            "published_at": (datetime.now() - timedelta(days=7)).isoformat(),
            "image": "🎓",
            "is_geoecology": True
        },
        {
            "id": "curated_4",
            "title": "Мониторинг качества воздуха в парке Академгородка",
            "summary": "Установлены новые датчики для контроля качества воздуха в зоне отдыха Академгородка. Данные доступны в реальном времени.",
            "source": "ГеоМониАкадем",
            "category": "Экология",
            "url": "#",
            "published_at": (datetime.now() - timedelta(days=10)).isoformat(),
            "image": "🌳",
            "is_geoecology": True
        },
        {
            "id": "curated_5",
            "title": "Защита дипломного проекта по системе мониторинга",
            "summary": "Студент успешно защитил дипломный проект по разработке системы геоэкологического мониторинга для Академгородка.",
            "source": "НГУ",
            "category": "Наука",
            "url": "https://www.nsu.ru",
            "published_at": (datetime.now() - timedelta(days=14)).isoformat(),
            "image": "🎓",
            "is_geoecology": True
        }
    ]

@app.post("/api/services/order")
async def submit_service_order(order: dict):
    """Обработка заказа услуги с отправкой уведомления на email"""
    try:
        service_name = order.get('service_name', 'Не указано')
        customer_name = order.get('name', 'Не указано')
        phone = order.get('phone', 'Не указано')
        email = order.get('email', 'Не указано')
        company = order.get('company', 'Не указано')
        address = order.get('address', 'Не указано')
        comment = order.get('comment', 'Нет')
        timestamp = order.get('timestamp', datetime.now().isoformat())
        smtp_server = SMTP_CONFIG["server"]
        smtp_port = SMTP_CONFIG["port"]
        smtp_login = SMTP_CONFIG["login"]
        smtp_password = SMTP_CONFIG["password"]
        admin_email = SMTP_CONFIG["admin_email"]
        
        email_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .header {{ background: linear-gradient(135deg, #0d7377, #14919b); color: white; padding: 20px; border-radius: 10px 10px 0 0; }}
                .content {{ padding: 25px; background: #f8f9fa; }}
                .field {{ margin: 12px 0; padding: 10px; background: white; border-radius: 6px; border-left: 4px solid #0d7377; }}
                .label {{ font-weight: bold; color: #2c3e50; display: block; margin-bottom: 4px; }}
                .value {{ color: #555; }}
                .footer {{ padding: 15px 25px; background: #eee; color: #666; font-size: 0.85rem; border-radius: 0 0 10px 10px; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2 style="margin:0;">📋 Новая заявка на услугу</h2>
            </div>
            <div class="content">
                <div class="field"><span class="label">🛠️ Услуга:</span><span class="value">{service_name}</span></div>
                <div class="field"><span class="label">👤 Клиент:</span><span class="value">{customer_name}</span></div>
                <div class="field"><span class="label">📞 Телефон:</span><span class="value">{phone}</span></div>
                <div class="field"><span class="label">✉️ Email:</span><span class="value">{email}</span></div>
                <div class="field"><span class="label">🏢 Организация:</span><span class="value">{company or 'Не указано'}</span></div>
                <div class="field"><span class="label">📍 Адрес объекта:</span><span class="value">{address or 'Не указано'}</span></div>
                <div class="field"><span class="label">💬 Комментарий:</span><span class="value">{comment or 'Нет'}</span></div>
                <div class="field"><span class="label">🕐 Время заявки:</span><span class="value">{datetime.fromisoformat(timestamp).strftime('%d.%m.%Y %H:%M')}</span></div>
            </div>
            <div class="footer">
                Это письмо отправлено автоматически системой ГеоМониАкадем.<br>
                Для связи с клиентом используйте указанные выше контактные данные.
            </div>
        </body>
        </html>
        """
        
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"📋 Новая заявка: {service_name}"
        msg['From'] = smtp_login
        msg['To'] = admin_email
        
        msg.attach(MIMEText(email_body, 'html', 'utf-8'))
        
        try:
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()  
            server.login(smtp_login, smtp_password)
            server.send_message(msg)
            server.quit()
            print(f"Письмо отправлено на {admin_email}")
        except Exception as email_error:
            print(f"Ошибка SMTP: {email_error}")
        
        print(f"\n📋 НОВАЯ ЗАЯВКА:")
        print(f"   🛠️ Услуга: {service_name}")
        print(f"   👤 Клиент: {customer_name}")
        print(f"   📞 Телефон: {phone}")
        print(f"   ✉️ Email: {email}")
        print(f"   🏢 Организация: {company or 'Не указано'}")
        print(f"   📍 Адрес: {address or 'Не указано'}")
        print(f"   💬 Комментарий: {comment or 'Нет'}")
        print(f"   🕐 Время: {datetime.fromisoformat(timestamp).strftime('%d.%m.%Y %H:%M')}\n")
        
        return {
            "success": True,
            "message": "Заявка успешно отправлена",
            "order_id": datetime.now().strftime("%Y%m%d%H%M%S"),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"Ошибка обработки заказа: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": "Ошибка при отправке заявки. Попробуйте позже."
        }



@app.get("/api/sensors/{sensor_id}/stats")
async def get_sensor_statistics(sensor_id: str):
    try:
        SENSOR_ID_MAP = {
            "sensor_1": "noise_sensor_001",
            "sensor_2": "noise_sensor_002",
            "sensor_3": "noise_sensor_003",
            "sensor_4": "noise_sensor_004",
        }
        
        db_sensor_id_str = SENSOR_ID_MAP.get(sensor_id, sensor_id)
        sensor_db_id = int(sensor_id) if sensor_id.isdigit() else None
        
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        if sensor_db_id is not None:
            cur.execute("""
                SELECT id, sensor_id, name, address, sensor_type
                FROM sensors
                WHERE id = %s
            """, (sensor_db_id,))
        else:
            cur.execute("""
                SELECT id, sensor_id, name, address, sensor_type
                FROM sensors
                WHERE sensor_id = %s
            """, (db_sensor_id_str,))
        sensor_info = cur.fetchone()
        
        if not sensor_info:
            cur.close()
            conn.close()
            return {
                "success": False,
                "error": "Датчик не найден"
            }
        
        db_sensor_id = sensor_info['id']
        
        cur.execute("""
            SELECT 
                AVG(noise_level) as average_noise,
                AVG(vibration_level) as average_vibration,
                COUNT(*) as total_measurements,
                MAX(measured_at) as last_measurement
            FROM measurements
            WHERE sensor_id = %s
            AND (noise_level IS NOT NULL OR vibration_level IS NOT NULL)
        """, (db_sensor_id,))
        stats = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return {
            "success": True,
            "sensor_id": sensor_id,
            "sensor_name": sensor_info['name'],
            "address": sensor_info['address'] or 'Адрес не указан',
            "sensor_type": sensor_info['sensor_type'] or 'noise',
            "average_noise": float(stats['average_noise']) if stats['average_noise'] else 0,
            "average_vibration": float(stats['average_vibration']) if stats['average_vibration'] else 0,
            "total_measurements": stats['total_measurements'] or 0,
            "last_measurement": stats['last_measurement'].isoformat() if stats['last_measurement'] else None
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "average_noise": 0
        }

@app.get("/api/sensors/{sensor_id}/raw-stream/latest")
async def get_latest_raw_chunk(sensor_id: int):
    """Отдаёт последний 5-секундный чанк сырых данных для графика"""
    db = SessionLocal()
    try:
        chunk = db.query(SensorRawChunk)\
                  .filter(SensorRawChunk.sensor_id == sensor_id)\
                  .order_by(SensorRawChunk.chunk_start.desc())\
                  .first()
        
        if not chunk:
            return {"success": False, "message": "Нет данных"}
            
        return {
            "success": True,
            "sensor_id": chunk.sensor_id,
            "start": chunk.chunk_start.isoformat(),
            "end": chunk.chunk_end.isoformat(),
            "sampling_rate": chunk.sampling_rate,
            "data": chunk.raw_values  # Массив чисел
        }
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        db.close()

@app.post("/api/chatbot/message")
async def chatbot_message(request: dict):
    """Обработка сообщений чат-бота"""
    try:
        message = request.get('message', '')
        history = request.get('history', [])
        
        responses = {
            'как работает': 'Наша система использует сеть датчиков для мониторинга шума, качества воздуха и вибрации в реальном времени.',
            'где находятся': 'Датчики расположены в ключевых точках Академгородка: возле НГУ, в парке, на Бердском шоссе.',
            'заказать': 'Нажмите на оранжевую кнопку 📍 слева на карте, заполните форму и выберите место установки.',
            'поддержка': 'Для поддержки позвоните: +7 (383) 123-45-67 или напишите: monitoring@academgorodok.ru',
            'цена': 'Стоимость установки датчика зависит от типа оборудования и места установки. Оставьте заявку для расчёта.',
            'срок': 'Обычно установка занимает 3-7 рабочих дней после согласования всех деталей.'
        }
        
        response = 'Спасибо за ваш вопрос! Наш специалист ответит вам в ближайшее время. 📞'
        for key, value in responses.items():
            if key in message.lower():
                response = value
                break
        
        return {
            "success": True,
            "response": response,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/sensors/{sensor_id}/raw-stream/latest")
async def get_latest_raw_chunk(sensor_id: int):
    """Отдаёт последний 5-секундный чанк сырых данных"""
    db = SessionLocal()
    try:
        chunk = db.query(SensorRawChunk)\
                  .filter(SensorRawChunk.sensor_id == sensor_id)\
                  .order_by(SensorRawChunk.chunk_start.desc())\
                  .first()
        
        if not chunk:
            return {"success": False, "message": "Нет данных"}
            
        return {
            "success": True,
            "sensor_id": chunk.sensor_id,
            "start": chunk.chunk_start.isoformat(),
            "end": chunk.chunk_end.isoformat(),
            "sampling_rate": chunk.sampling_rate,
            "data": chunk.raw_values  # Массив чисел для отрисовки
        }
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        db.close()

@app.post("/api/sensor-request/submit")
async def submit_sensor_request(request: dict):
    """Обработка заявки на установку датчика"""
    try:
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        
        name = request.get('name', 'Не указано')
        phone = request.get('phone', 'Не указано')
        email = request.get('email', 'Не указано')
        sensor_type = request.get('sensor_type', 'Не указано')
        company = request.get('company', 'Не указано')
        address = request.get('address', 'Не указано')
        comment = request.get('comment', 'Нет')
        latitude = request.get('latitude', 0)
        longitude = request.get('longitude', 0)
        timestamp = request.get('timestamp', datetime.now().isoformat())
        
        admin_email = SMTP_CONFIG["admin_email"]
        email_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
                .header {{ background: linear-gradient(135deg, #e67e22, #d35400); color: white; padding: 20px; border-radius: 10px; }}
                .content {{ padding: 25px; background: #f8f9fa; }}
                .field {{ margin: 12px 0; padding: 10px; background: white; border-radius: 6px; border-left: 4px solid #e67e22; }}
                .label {{ font-weight: bold; color: #2c3e50; }}
                .map-link {{ display: inline-block; margin-top: 10px; padding: 10px 20px; background: #3498db; color: white; text-decoration: none; border-radius: 5px; }}
            </style>
        </head>
        <body>
            <div class="header"><h2 style="margin:0;">📍 Новая заявка на установку датчика</h2></div>
            <div class="content">
                <div class="field"><span class="label">👤 Клиент:</span> {name}</div>
                <div class="field"><span class="label">📞 Телефон:</span> {phone}</div>
                <div class="field"><span class="label">✉️ Email:</span> {email}</div>
                <div class="field"><span class="label">📡 Тип датчика:</span> {sensor_type}</div>
                <div class="field"><span class="label">🏢 Организация:</span> {company}</div>
                <div class="field"><span class="label">📍 Адрес:</span> {address}</div>
                <div class="field"><span class="label">🗺️ Координаты:</span> {latitude}, {longitude}</div>
                <div class="field"><span class="label">💬 Комментарий:</span> {comment}</div>
                <div class="field">
                    <span class="label">🗺️ Показать на карте:</span><br>
                    <a href="https://www.google.com/maps?q={latitude},{longitude}" class="map-link" target="_blank">Открыть в Google Maps</a>
                </div>
                <div class="field"><span class="label">🕐 Время заявки:</span> {datetime.fromisoformat(timestamp).strftime('%d.%m.%Y %H:%M')}</div>
            </div>
        </body>
        </html>
        """
        
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"📍 Заявка на установку датчика - {name}"
        msg['From'] = SMTP_CONFIG["login"]
        msg['To'] = admin_email
        msg.attach(MIMEText(email_body, 'html', 'utf-8'))
        
        try:
            server = smtplib.SMTP(SMTP_CONFIG["server"], SMTP_CONFIG["port"])
            server.starttls()
            server.login(SMTP_CONFIG["login"], SMTP_CONFIG["password"])
            server.send_message(msg)
            server.quit()
            print(f"Письмо отправлено на {admin_email}")
        except Exception as email_error:
            print(f"Ошибка SMTP: {email_error}")
        
        print(f"\n📍 НОВАЯ ЗАЯВКА НА УСТАНОВКУ:")
        print(f"   👤 Клиент: {name}")
        print(f"   📞 Телефон: {phone}")
        print(f"   ✉️ Email: {email}")
        print(f"   📡 Тип: {sensor_type}")
        print(f"   🗺️ Координаты: {latitude}, {longitude}")
        print(f"   🕐 Время: {datetime.fromisoformat(timestamp).strftime('%d.%m.%Y %H:%M')}\n")
        
        return {
            "success": True,
            "message": "Заявка успешно отправлена",
            "request_id": datetime.now().strftime("%Y%m%d%H%M%S"),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"Ошибка обработки заявки: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": "Ошибка при отправке заявки"
        }

# Запуск приложения
if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
