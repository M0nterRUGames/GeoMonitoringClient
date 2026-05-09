"""Application configuration loaded from environment variables.

Create a local `.env` file near this module and put real values there.
Do not commit `.env` to GitHub.
"""
from pathlib import Path
import os
from urllib.parse import quote_plus

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def env_path(name: str, default: Path | str) -> Path:
    return Path(os.getenv(name, str(default))).expanduser()


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


# PostgreSQL
DB_NAME = env_str("DB_NAME", "geo_monitoring")
DB_USER = env_str("DB_USER", "postgres")
DB_PASSWORD = env_str("DB_PASSWORD", "")
DB_HOST = env_str("DB_HOST", "localhost")
DB_PORT = env_str("DB_PORT", "5432")
DB_CLIENT_ENCODING = env_str("DB_CLIENT_ENCODING", "UTF8")
DB_OPTIONS = env_str("DB_OPTIONS", f"-c client_encoding={DB_CLIENT_ENCODING}")

DB_CONFIG = {
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST,
    "port": DB_PORT,
    "client_encoding": DB_CLIENT_ENCODING,
    "options": DB_OPTIONS,
}

DATABASE_URL = env_str("DATABASE_URL", "") or (
    f"postgresql://{quote_plus(DB_USER)}:{quote_plus(DB_PASSWORD)}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}?client_encoding={quote_plus(DB_CLIENT_ENCODING)}"
)

SQLITE_FALLBACK_URL = env_str("SQLITE_FALLBACK_URL", "sqlite:///./geo_monitoring.db")


# SMTP / email notifications
SMTP_CONFIG = {
    "server": env_str("SMTP_SERVER", "smtp.example.com"),
    "port": env_int("SMTP_PORT", 587),
    "login": env_str("SMTP_LOGIN", ""),
    "password": env_str("SMTP_PASSWORD", ""),
    "admin_email": env_str("SMTP_ADMIN_EMAIL", env_str("SMTP_LOGIN", "")),
    "use_tls": env_bool("SMTP_USE_TLS", True),
}


# Directories
STATIC_DIR = env_path("STATIC_DIR", BASE_DIR / "static")
MSEED_BASE_DIR = env_path("MSEED_BASE_DIR", STATIC_DIR / "miniSeed")
SEED_DIR = env_path("SEED_DIR", MSEED_BASE_DIR)

SENSOR_FOLDERS = {
    "sensor_1": str(env_path("MSEED_SENSOR_1_DIR", MSEED_BASE_DIR / "sensor_1")),
    "sensor_2": str(env_path("MSEED_SENSOR_2_DIR", MSEED_BASE_DIR / "sensor_2")),
    "sensor_3": str(env_path("MSEED_SENSOR_3_DIR", MSEED_BASE_DIR / "sensor_3")),
    "sensor_4": str(env_path("MSEED_SENSOR_4_DIR", MSEED_BASE_DIR / "sensor_4")),
}

MSEED_FILE_EXTENSIONS = set(
    ext.strip().lower()
    for ext in env_str("MSEED_FILE_EXTENSIONS", ".mseed,.seed,.miniseed").split(",")
    if ext.strip()
)


# Baikal-8 / SeedLink
BAIKAL_STREAM_OUTPUT_DIR = env_path("BAIKAL_STREAM_OUTPUT_DIR", BASE_DIR / "baikal-control")
BAIKAL_ARCHIVE_DIR = env_path("BAIKAL_ARCHIVE_DIR", BAIKAL_STREAM_OUTPUT_DIR / "archive")
BAIKAL_SENSOR_DB_ID = env_int("BAIKAL_SENSOR_DB_ID", 7)
BAIKAL_POLL_INTERVAL = env_float("BAIKAL_POLL_INTERVAL", 5.0)
BAIKAL_MIN_FILE_AGE = env_float("BAIKAL_MIN_FILE_AGE", 8.0)
VIBRATION_THRESHOLD = env_float("VIBRATION_THRESHOLD", 70.0)

SEEDLINK_ADDRESS = env_str("SEEDLINK_ADDRESS", "127.0.0.1:18000")
SEEDLINK_SELECT = env_str("SEEDLINK_SELECT", "NT_B8:CH3")
SEEDLINK_FLUSH_INTERVAL = env_float("SEEDLINK_FLUSH_INTERVAL", 5.0)
