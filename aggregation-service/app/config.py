import os
from dataclasses import dataclass


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"environment variable {name} is required")
    return value


@dataclass(frozen=True)
class Settings:
    ch_host: str
    ch_port: int
    ch_user: str
    ch_password: str
    ch_database: str
    pg_dsn: str
    # расписание
    aggregation_interval: float   # как часто искать «грязные» даты и пересчитывать
    lookback_days: int            # автоматически пересчитываются только даты за последние N дней
    top_n: int
    # экспорт в S3
    export_interval: float
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str
    # retry записи в PostgreSQL
    pg_retry_attempts: int
    pg_retry_base_delay: float


def load_settings() -> Settings:
    return Settings(
        ch_host=_env("CLICKHOUSE_HOST", "clickhouse"),
        ch_port=int(_env("CLICKHOUSE_PORT", "8123")),
        ch_user=_env("CLICKHOUSE_USER", "app"),
        ch_password=_env("CLICKHOUSE_PASSWORD", "app"),
        ch_database=_env("CLICKHOUSE_DATABASE", "analytics"),
        pg_dsn=_env("POSTGRES_DSN"),
        aggregation_interval=float(_env("AGGREGATION_INTERVAL_SECONDS", "60")),
        lookback_days=int(_env("AGGREGATION_LOOKBACK_DAYS", "30")),
        top_n=int(_env("TOP_MOVIES_LIMIT", "10")),
        export_interval=float(_env("EXPORT_INTERVAL_SECONDS", "300")),
        s3_endpoint=_env("S3_ENDPOINT_URL", "http://s3:8333"),
        s3_access_key=_env("S3_ACCESS_KEY"),
        s3_secret_key=_env("S3_SECRET_KEY"),
        s3_bucket=_env("S3_BUCKET", "movie-analytics"),
        pg_retry_attempts=int(_env("PG_RETRY_ATTEMPTS", "3")),
        pg_retry_base_delay=float(_env("PG_RETRY_BASE_DELAY_SECONDS", "1")),
    )
