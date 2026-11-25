"""Интеграционный тест ingestion pipeline: HTTP API продюсера → Kafka → ClickHouse.

Изоляция: каждый запуск использует свои user_id/event_id и дату в прошлом
(не мешает реальным данным и агрегатам); в конце тестовые строки удаляются.
Повторный запуск всегда проходит с чистого листа.
"""

import datetime as dt
import random
import uuid

import pytest
import requests

from conftest import wait_for

RUN = uuid.uuid4().hex[:8]
USER = f"it-pipe-{RUN}"
# случайная дата в 1990-х: вне окна автоматической агрегации (последние 30 дней)
TEST_DAY = dt.date(1990, 1, 1) + dt.timedelta(days=random.randint(0, 3000))


def make_event(**overrides) -> dict:
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": USER,
        "movie_id": "m-007",
        "event_type": "VIEW_PAUSED",
        "timestamp": f"{TEST_DAY.isoformat()}T12:34:56.789Z",
        "device_type": "TV",
        "session_id": f"s-{RUN}",
        "progress_seconds": 1234,
    }
    event.update(overrides)
    return event


def fetch(ch, event_id: str):
    rows = ch.query(
        "SELECT event_id, user_id, movie_id, toString(event_type), timestamp, toString(device_type), "
        "session_id, progress_seconds, count() OVER () AS copies "
        "FROM analytics.movie_events FINAL WHERE event_id = {id:UUID}",
        parameters={"id": event_id},
    ).result_rows
    return rows


@pytest.fixture(scope="module", autouse=True)
def cleanup(ch):
    yield
    ch.command("DELETE FROM analytics.movie_events WHERE user_id = {u:String}", parameters={"u": USER})


def test_event_goes_from_producer_to_clickhouse(producer, ch):
    event = make_event()
    resp = requests.post(f"{producer}/events", json=event, timeout=30)
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["event_id"] == event["event_id"]
    assert body["partition"] in (0, 1, 2) and body["offset"] >= 0

    rows = wait_for(lambda: fetch(ch, event["event_id"]), what="event in ClickHouse")
    event_id, user_id, movie_id, event_type, ts, device, session, progress, copies = rows[0]
    assert str(event_id) == event["event_id"]
    assert user_id == USER
    assert movie_id == "m-007"
    assert event_type == "VIEW_PAUSED"
    assert device == "TV"
    assert session == f"s-{RUN}"
    assert progress == 1234
    expected_ts = dt.datetime.combine(TEST_DAY, dt.time(12, 34, 56, 789000), tzinfo=dt.timezone.utc)
    actual_ts = ts if ts.tzinfo else ts.replace(tzinfo=dt.timezone.utc)
    assert actual_ts == expected_ts
    assert copies == 1


def test_event_without_progress_is_nullable_in_clickhouse(producer, ch):
    event = make_event(event_type="LIKED", progress_seconds=None)
    resp = requests.post(f"{producer}/events", json=event, timeout=30)
    assert resp.status_code == 201, resp.text
    rows = wait_for(lambda: fetch(ch, event["event_id"]), what="LIKED event in ClickHouse")
    assert rows[0][3] == "LIKED"
    assert rows[0][7] is None


def test_same_event_id_twice_is_stored_once(producer, ch):
    """Повтор публикации (retry клиента или продюсера) не создаёт дубль: ReplacingMergeTree + FINAL."""
    event = make_event(event_type="VIEW_RESUMED")
    for _ in range(2):
        assert requests.post(f"{producer}/events", json=event, timeout=30).status_code == 201
    rows = wait_for(lambda: fetch(ch, event["event_id"]), what="deduplicated event")
    assert len(rows) == 1 and rows[0][8] == 1


@pytest.mark.parametrize("broken, field", [
    ({"event_type": "VIEW_REWOUND"}, "event_type"),          # нет в enum
    ({"device_type": "WATCH"}, "device_type"),
    ({"user_id": ""}, "user_id"),
    ({"event_id": "not-a-uuid"}, "event_id"),
    ({"progress_seconds": None}, "progress_seconds"),        # обязателен для VIEW_*
    ({"progress_seconds": -5}, "progress_seconds"),
])
def test_invalid_event_is_rejected(producer, broken, field):
    resp = requests.post(f"{producer}/events", json=make_event(**broken), timeout=30)
    assert resp.status_code == 422, resp.text
    assert field in resp.text


def test_missing_required_field_is_rejected(producer):
    event = make_event()
    del event["session_id"]
    resp = requests.post(f"{producer}/events", json=event, timeout=30)
    assert resp.status_code == 422
