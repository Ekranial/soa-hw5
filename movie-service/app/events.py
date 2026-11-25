"""Модель события и валидация.

Два уровня проверки:
1. Pydantic (HTTP API): типы, enum, UUID, бизнес-правила (progress_seconds для VIEW_*).
2. Avro-схема из Schema Registry (fastavro.validate) — перед отправкой в Kafka,
   и для событий из API, и для событий генератора.
"""

import datetime as dt
import uuid
from enum import Enum

from fastavro import parse_schema
from fastavro.validation import validate
from pydantic import BaseModel, Field, model_validator


class EventType(str, Enum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(str, Enum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


VIEW_EVENTS = {EventType.VIEW_STARTED, EventType.VIEW_FINISHED, EventType.VIEW_PAUSED, EventType.VIEW_RESUMED}


class MovieEventIn(BaseModel):
    """Событие, пришедшее в HTTP API. event_id и timestamp можно не передавать."""

    event_id: uuid.UUID | None = Field(default=None, description="Если не передан — генерируется сервисом")
    user_id: str = Field(min_length=1, max_length=64)
    movie_id: str = Field(min_length=1, max_length=64)
    event_type: EventType
    timestamp: dt.datetime | None = Field(default=None, description="ISO 8601; без зоны считается UTC")
    device_type: DeviceType
    session_id: str = Field(min_length=1, max_length=64)
    progress_seconds: int | None = Field(default=None, ge=0, le=24 * 3600)

    @model_validator(mode="after")
    def check_progress(self):
        if self.event_type in VIEW_EVENTS and self.progress_seconds is None:
            raise ValueError(f"progress_seconds is required for {self.event_type.value}")
        return self

    def to_record(self) -> dict:
        ts = self.timestamp or dt.datetime.now(dt.timezone.utc)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.timezone.utc)
        return make_record(
            event_id=str(self.event_id or uuid.uuid4()),
            user_id=self.user_id,
            movie_id=self.movie_id,
            event_type=self.event_type.value,
            timestamp=ts,
            device_type=self.device_type.value,
            session_id=self.session_id,
            progress_seconds=self.progress_seconds,
        )


def make_record(*, event_id: str, user_id: str, movie_id: str, event_type: str, timestamp: dt.datetime,
                device_type: str, session_id: str, progress_seconds: int | None) -> dict:
    """Запись в формате, который ожидает AvroSerializer (timestamp-millis = aware datetime)."""
    return {
        "event_id": event_id,
        "user_id": user_id,
        "movie_id": movie_id,
        "event_type": event_type,
        "timestamp": timestamp.astimezone(dt.timezone.utc),
        "device_type": device_type,
        "session_id": session_id,
        "progress_seconds": progress_seconds,
    }


class AvroValidator:
    """Проверка записи по Avro-схеме, полученной из Schema Registry."""

    def __init__(self, schema: dict):
        self._parsed = parse_schema(schema)

    def validate(self, record: dict) -> None:
        # raise_errors=True → ValidationError с перечнем нарушений
        validate(record, self._parsed, raise_errors=True)
