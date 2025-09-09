import os
from dotenv import load_dotenv
from pydantic import BaseSettings

load_dotenv()

DB_NAME = os.environ.get("ASYNC_DB_NAME", "spimex_async_db")
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "password")
ASYNC_SQLALCHEMY_DATABASE_URL = (
    f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


class APISettings(BaseSettings):
    """Настройки API"""

    HOST: str = "0.0.0.0"
    PORT: int = 8000
    DEBUG: bool = False

    DATABASE_URL: str = ASYNC_SQLALCHEMY_DATABASE_URL

    REDIS_URL: str = "redis://redis:6379/0"
    REDIS_PASSWORD: str = ""

    CACHE_TTL: int = 3600
    CACHE_RESET_TIME: str = "14:11"

    SECRET_KEY: str = "spimex-api-secret-key"

    LOG_LEVEL: str = "INFO"

    class Config:
        case_sensitive = True


api_settings = APISettings()
