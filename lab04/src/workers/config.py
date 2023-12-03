from __future__ import annotations

import dataclasses
from typing import Any, Optional

from pydantic import PostgresDsn, validator, field_validator
from pydantic_core import MultiHostUrl
from pydantic_core.core_schema import ValidationInfo
from pydantic_settings import BaseSettings


__all__ = [
    "settings",
    "log_config",
]


class Settings(BaseSettings):
    # general
    LOGGING_LEVEL: str
    LOGGING_FORMAT: str
    # kafka
    KAFKA_HOST: str
    KAFKA_PORT: str
    # transformator
    TF_APP_NAME: str
    TF_TOPIC_NAME: str
    TF_CONCURRENCY: int = 1
    TF_PARTITIONS: int = 1
    TF_MAXIMUM_PARSING_PAGES: Optional[int] = None
    TF_PAGE_BATCH_SIZE: int = 1000
    # DB
    POSTGRES_HOST: str
    POSTGRES_PORT: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB_NAME: str
    DATABASE_URI: Optional[str] = None

    @field_validator("DATABASE_URI", mode="before")
    @classmethod
    def assemble_db_uri(cls, v: str | None, values: ValidationInfo) -> str:
        if isinstance(v, str):
            return v
        return (
            f"postgresql+asyncpg://"
            f"{values.data['POSTGRES_USER']}:"
            f"{values.data['POSTGRES_PASSWORD']}@"
            f"{values.data['POSTGRES_HOST']}:"
            f"{values.data['POSTGRES_PORT']}/"
            f"{values.data['POSTGRES_DB_NAME']}"
        )

    class Config:
        case_sensitive = True


settings = Settings()


log_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "simple": {
            "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
            "format": settings.LOGGING_FORMAT,
        },
    },
    "handlers": {
        "default": {
            "formatter": "simple",
            "class": "logging.StreamHandler",
        },
    },
    "loggers": {
        "": {
            "handlers": ["default"],
            "level": settings.LOGGING_LEVEL,
        },
    },
}
