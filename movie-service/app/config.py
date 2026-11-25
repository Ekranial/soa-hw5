import os
from dataclasses import dataclass


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"environment variable {name} is required")
    return value


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str
    schema_registry_url: str
    topic: str
    topic_partitions: int
    topic_replication_factor: int
    topic_min_insync_replicas: int
    schema_path: str
    # публикация
    publish_max_retries: int
    publish_base_backoff: float
    publish_timeout: float
    # генератор
    generator_enabled: bool
    generator_events_per_second: float
    generator_backfill_days: int
    generator_users: int
    generator_seed: int


def load_settings() -> Settings:
    return Settings(
        bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS"),
        schema_registry_url=_env("SCHEMA_REGISTRY_URL"),
        topic=_env("KAFKA_TOPIC", "movie-events"),
        topic_partitions=int(_env("KAFKA_TOPIC_PARTITIONS", "3")),
        topic_replication_factor=int(_env("KAFKA_TOPIC_REPLICATION_FACTOR", "2")),
        topic_min_insync_replicas=int(_env("KAFKA_TOPIC_MIN_INSYNC_REPLICAS", "1")),
        schema_path=_env("SCHEMA_PATH", "/app/schemas/movie_event.avsc"),
        publish_max_retries=int(_env("PUBLISH_MAX_RETRIES", "5")),
        publish_base_backoff=float(_env("PUBLISH_BASE_BACKOFF_SECONDS", "0.2")),
        publish_timeout=float(_env("PUBLISH_TIMEOUT_SECONDS", "30")),
        generator_enabled=_env("GENERATOR_ENABLED", "true").lower() == "true",
        generator_events_per_second=float(_env("GENERATOR_EVENTS_PER_SECOND", "5")),
        generator_backfill_days=int(_env("GENERATOR_BACKFILL_DAYS", "14")),
        generator_users=int(_env("GENERATOR_USERS", "1500")),
        generator_seed=int(_env("GENERATOR_SEED", "42")),
    )
