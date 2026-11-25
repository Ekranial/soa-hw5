"""Общие фикстуры интеграционных тестов. Тесты работают с поднятым стендом (docker compose)."""

import os
import time

import boto3
from botocore.config import Config
import clickhouse_connect
import psycopg
import pytest
import requests

PRODUCER_URL = os.getenv("PRODUCER_URL", "http://movie-service:8000")
AGGREGATION_URL = os.getenv("AGGREGATION_URL", "http://aggregation-service:8001")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://analytics:analytics@postgres:5432/analytics")
S3_BUCKET = os.getenv("S3_BUCKET", "movie-analytics")


def wait_http(url: str, timeout: float = 180) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if requests.get(url, timeout=3).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise RuntimeError(f"{url} is not available")


@pytest.fixture(scope="session")
def producer():
    wait_http(f"{PRODUCER_URL}/health")
    return PRODUCER_URL


@pytest.fixture(scope="session")
def aggregation():
    wait_http(f"{AGGREGATION_URL}/health")
    return AGGREGATION_URL


@pytest.fixture(scope="session")
def ch():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"), port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "app"), password=os.getenv("CLICKHOUSE_PASSWORD", "app"),
        database="analytics",
    )


@pytest.fixture(scope="session")
def pg():
    with psycopg.connect(POSTGRES_DSN, autocommit=True) as conn:
        yield conn


@pytest.fixture(scope="session")
def s3():
    return boto3.client(
        "s3", endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://s3:8333"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "analytics"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "analytics-secret"), region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def wait_for(predicate, timeout: float = 90, interval: float = 1.0, what: str = "condition"):
    """Ждёт, пока predicate() вернёт истину (pipeline асинхронный: Kafka → ClickHouse)."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"timeout waiting for {what}; last value: {last!r}")
