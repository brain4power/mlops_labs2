from uuid import UUID, uuid4
from datetime import datetime

from sqlalchemy import func, Computed, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSONB
from pgvector.sqlalchemy import Vector

from sqlalchemy.ext.declarative import declarative_base

DeclarativeBase = declarative_base()


__all__ = [
    "RawData",
    "EducationDirection",
    "AdditionalData",
    "ServiceTask",
]


class RawData(DeclarativeBase):
    __tablename__ = "h_raw_data"

    record_id: Mapped[UUID] = mapped_column(
        primary_key=True,
        default=uuid4,
        server_default=func.gen_random_uuid(),
    )
    time_created: Mapped[datetime] = mapped_column(default=datetime.now, server_default=func.now())
    time_updated: Mapped[datetime] = mapped_column(
        default=datetime.now,
        onupdate=datetime.now,
        server_default=func.now(),
    )
    meta_data: Mapped[dict | None] = mapped_column(JSONB)
    # main data
    page_number: Mapped[int]
    deactivated: Mapped[str]
    country_id: Mapped[int]
    country_title: Mapped[str]
    city_id: Mapped[int]
    city_title: Mapped[str]
    about: Mapped[str]
    activities: Mapped[str]
    books: Mapped[str]
    games: Mapped[str]
    interests: Mapped[str]
    education_form: Mapped[str]
    education_status: Mapped[str]
    university_id: Mapped[int]
    university_name: Mapped[str]
    faculty_id: Mapped[int]
    faculty_name: Mapped[str]
    graduation_year: Mapped[int]
    g_merged_data: Mapped[str] = mapped_column(
        Computed(
            "h_concat_string_normalize(country_title, city_title, about, activities, books, games, interests)::VARCHAR",
            persisted=True,
        ),
        nullable=False,
    )


class EducationDirection(DeclarativeBase):
    __tablename__ = "h_education_direction"

    direction_id: Mapped[UUID] = mapped_column(
        primary_key=True,
        default=uuid4,
        server_default=func.gen_random_uuid(),
    )
    university_id: Mapped[int]
    university_name: Mapped[str]
    faculty_id: Mapped[int] = mapped_column(unique=True)
    faculty_name: Mapped[str]


class AdditionalData(DeclarativeBase):
    __tablename__ = "h_additional_data"

    record_id: Mapped[UUID] = mapped_column(
        primary_key=True,
        default=uuid4,
        server_default=func.gen_random_uuid(),
    )
    time_created: Mapped[datetime] = mapped_column(default=datetime.now, server_default=func.now())
    time_updated: Mapped[datetime] = mapped_column(
        default=datetime.now,
        onupdate=datetime.now,
        server_default=func.now(),
    )
    raw_record_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("h_raw_data.record_id", onupdate="CASCADE"),
        unique=True,
    )
    direction_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("h_education_direction.direction_id", onupdate="CASCADE"),
    )
    embedding: Mapped[Vector] = mapped_column(Vector(384))


class ServiceTask(DeclarativeBase):
    __tablename__ = "h_service_tasks"

    record_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4, server_default=func.gen_random_uuid())
    time_created: Mapped[datetime] = mapped_column(default=datetime.now, server_default=func.now())
    time_updated: Mapped[datetime] = mapped_column(
        default=datetime.now,
        onupdate=datetime.now,
        server_default=func.now(),
    )
    time_task_start: Mapped[datetime | None]
    time_task_end: Mapped[datetime | None]
    from_page: Mapped[int]
    to_page: Mapped[int | None]
    state: Mapped[str]
    comment: Mapped[str | None]
