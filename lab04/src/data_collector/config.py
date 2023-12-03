from pydantic import PostgresDsn, field_validator
from pydantic_core import MultiHostUrl
from sqlalchemy.orm import declarative_base
from pydantic_settings import BaseSettings
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from pydantic_core.core_schema import ValidationInfo

__all__ = [
    "settings",
    "DeclarativeBase",
    "Database",
    "db",
]


class Settings(BaseSettings):
    COLLECTOR_ERROR_COUNT: int = 10
    COLLECTOR_BASE_API_URL: str
    COLLECTOR_ACCESS_TOKEN: str
    COLLECTOR_TIMEOUT: int = 11
    # DB
    COLLECTOR_POSTGRES_HOST: str
    COLLECTOR_POSTGRES_PORT: str
    COLLECTOR_POSTGRES_USER: str
    COLLECTOR_POSTGRES_PASSWORD: str
    COLLECTOR_POSTGRES_DB_NAME: str
    DATABASE_URI: MultiHostUrl | None = None

    @field_validator("DATABASE_URI", mode="before")
    @classmethod
    def assemble_db_uri(cls, v: str | None, values: ValidationInfo) -> MultiHostUrl:
        if isinstance(v, str):
            return PostgresDsn(v)
        return PostgresDsn(
            f"postgresql+asyncpg://"
            f"{values.data['COLLECTOR_POSTGRES_USER']}:"
            f"{values.data['COLLECTOR_POSTGRES_PASSWORD']}@"
            f"{values.data['COLLECTOR_POSTGRES_HOST']}/"
            f"{values.data['COLLECTOR_POSTGRES_DB_NAME']}"
        )

    class Config:
        case_sensitive = True


settings = Settings()

DeclarativeBase = declarative_base()


class Database:
    def __init__(self):
        self.__session = None
        self.engine = create_async_engine(
            str(settings.DATABASE_URI),
        )

    def connect(self):
        self.__session = async_sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
        )

    async def disconnect(self):
        await self.engine.dispose()

    @staticmethod
    async def get_db_session():
        async with db.__session() as session:
            yield session


db = Database()
