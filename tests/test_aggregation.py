"""Интеграционный тест агрегации и экспорта: события → ClickHouse → метрики → PostgreSQL → S3.

На изолированной дате (случайный день в 2000-х, вне окна автоматической агрегации)
публикуется заранее известный набор событий, поэтому все метрики можно проверить точно.
После теста все данные этой даты удаляются — тест можно запускать сколько угодно раз.

Сценарий (дата D):
  u1: SEARCHED, VIEW_STARTED m-001, PAUSED 600, RESUMED 600, FINISHED 3600, LIKED   (TV)
      D+1: VIEW_STARTED m-002;  D+7: VIEW_STARTED m-001
  u2: VIEW_STARTED m-001, FINISHED 5400                                             (MOBILE)
      D+1: VIEW_STARTED m-003
  u3: VIEW_STARTED m-002 (не досмотрел)                                             (MOBILE)
  u4: только SEARCHED — активен, но в когорту не входит (нет просмотра)             (DESKTOP)

Ожидаем за D: DAU = 4, просмотров начато 3, завершено 2, конверсия 2/3,
среднее время = (3600 + 5400) / 2 = 4500 с, топ: m-001 (2), m-002 (1).
Когорта D = {u1, u2, u3}: Day1 = 2/3 = 66.67 %, Day7 = 1/3 = 33.33 %, Day2..6 = 0 %.
"""

import datetime as dt
import json
import random
import uuid

import pytest
import requests

from conftest import S3_BUCKET, wait_for

RUN = uuid.uuid4().hex[:8]
D = dt.date(2000, 1, 1) + dt.timedelta(days=random.randint(0, 6000))
U = {n: f"it-agg-{RUN}-u{n}" for n in range(1, 5)}
DEVICE = {1: "TV", 2: "MOBILE", 3: "MOBILE", 4: "DESKTOP"}


def ev(user: int, etype: str, movie: str, day: dt.date, hhmm: str, progress=None, session="a") -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": U[user],
        "movie_id": movie,
        "event_type": etype,
        "timestamp": f"{day.isoformat()}T{hhmm}:00Z",
        "device_type": DEVICE[user],
        "session_id": f"{RUN}-u{user}-{day}-{session}",
        "progress_seconds": progress,
    }


D1, D7 = D + dt.timedelta(days=1), D + dt.timedelta(days=7)
EVENTS = [
    ev(1, "SEARCHED", "m-001", D, "10:00"),
    ev(1, "VIEW_STARTED", "m-001", D, "10:01", 0),
    ev(1, "VIEW_PAUSED", "m-001", D, "10:11", 600),
    ev(1, "VIEW_RESUMED", "m-001", D, "10:20", 600),
    ev(1, "VIEW_FINISHED", "m-001", D, "11:10", 3600),
    ev(1, "LIKED", "m-001", D, "11:11", 3600),
    ev(2, "VIEW_STARTED", "m-001", D, "12:00", 0),
    ev(2, "VIEW_FINISHED", "m-001", D, "13:30", 5400),
    ev(3, "VIEW_STARTED", "m-002", D, "14:00", 0),
    ev(3, "VIEW_PAUSED", "m-002", D, "14:20", 1200),
    ev(4, "SEARCHED", "m-005", D, "15:00"),
    ev(1, "VIEW_STARTED", "m-002", D1, "09:00", 0),
    ev(2, "VIEW_STARTED", "m-003", D1, "20:00", 0),
    ev(1, "VIEW_STARTED", "m-001", D7, "18:00", 0),
]


def pg_metrics(pg, day):
    rows = pg.execute("SELECT metric_name, value, computed_at FROM daily_metrics WHERE metric_date = %s",
                      (day,)).fetchall()
    return {name: (value, computed_at) for name, value, computed_at in rows}


@pytest.fixture(scope="module")
def published(producer, ch):
    for event in EVENTS:
        resp = requests.post(f"{producer}/events", json=event, timeout=30)
        assert resp.status_code == 201, resp.text
    users = list(U.values())
    wait_for(
        lambda: ch.query("SELECT count() FROM analytics.movie_events FINAL WHERE has({u:Array(String)}, user_id)",
                         parameters={"u": users}).result_rows[0][0] == len(EVENTS),
        what=f"{len(EVENTS)} test events in ClickHouse",
    )
    yield
    # ---- очистка: raw-события, агрегаты, PostgreSQL, S3
    ch.command("DELETE FROM analytics.movie_events WHERE has({u:Array(String)}, user_id)", parameters={"u": users})
    for table, col in [("agg_daily_metrics", "metric_date"), ("agg_top_movies", "metric_date"),
                       ("agg_device_daily", "metric_date"), ("agg_retention_cohort", "cohort_date")]:
        ch.command(f"ALTER TABLE analytics.{table} DELETE WHERE {col} = {{d:Date}} SETTINGS mutations_sync = 1",
                   parameters={"d": D.isoformat()})


@pytest.fixture(scope="module")
def aggregated(published, aggregation):
    resp = requests.post(f"{aggregation}/aggregate", params={"date": D.isoformat()}, timeout=120)
    assert resp.status_code == 200, resp.text
    return resp.json()


@pytest.fixture(scope="module", autouse=True)
def pg_cleanup(pg, s3):
    yield
    for table, col in [("daily_metrics", "metric_date"), ("top_movies_daily", "metric_date"),
                       ("device_daily", "metric_date"), ("retention_cohort", "cohort_date"),
                       ("export_log", "export_date")]:
        pg.execute(f"DELETE FROM {table} WHERE {col} = %s", (D,))
    s3.delete_object(Bucket=S3_BUCKET, Key=f"daily/{D.isoformat()}/aggregates.json")


def test_manual_recompute_reports_processed_events(aggregated):
    assert aggregated["date"] == D.isoformat()
    assert aggregated["raw_events"] == 11          # событий за саму дату D
    assert aggregated["rows_written"] > 0


def test_daily_metrics_in_postgres(aggregated, pg):
    m = pg_metrics(pg, D)
    assert m["dau"][0] == 4
    assert m["views_started"][0] == 3
    assert m["views_finished"][0] == 2
    assert m["view_conversion"][0] == pytest.approx(2 / 3, abs=1e-4)
    assert m["avg_watch_time_seconds"][0] == pytest.approx(4500)
    assert m["likes"][0] == 1 and m["searches"][0] == 2


def test_retention_d1_d7(aggregated, pg):
    m = pg_metrics(pg, D)
    assert m["new_viewers"][0] == 3                 # u4 без просмотра в когорту не входит
    assert m["retention_d1"][0] == pytest.approx(2 / 3, abs=1e-4)
    assert m["retention_d7"][0] == pytest.approx(1 / 3, abs=1e-4)


def test_retention_cohort_row(aggregated, pg):
    rows = pg.execute("SELECT day_number, cohort_size, retained_users, retention_pct FROM retention_cohort "
                      "WHERE cohort_date = %s ORDER BY day_number", (D,)).fetchall()
    assert [r[0] for r in rows] == list(range(8))
    assert all(r[1] == 3 for r in rows)
    pct = {r[0]: float(r[3]) for r in rows}
    assert pct[0] == 100.0
    assert pct[1] == pytest.approx(66.67)
    assert all(pct[n] == 0.0 for n in range(2, 7))
    assert pct[7] == pytest.approx(33.33)


def test_top_movies(aggregated, pg):
    rows = pg.execute("SELECT rank, movie_id, views, unique_viewers FROM top_movies_daily "
                      "WHERE metric_date = %s ORDER BY rank", (D,)).fetchall()
    assert rows == [(1, "m-001", 2, 2), (2, "m-002", 1, 1)]


def test_devices(aggregated, pg):
    rows = dict(pg.execute("SELECT device_type, unique_users FROM device_daily WHERE metric_date = %s",
                           (D,)).fetchall())
    assert rows == {"TV": 1, "MOBILE": 2, "DESKTOP": 1}


def test_aggregates_are_materialized_in_clickhouse(aggregated, ch):
    dau = ch.query("SELECT value FROM analytics.agg_daily_metrics FINAL "
                   "WHERE metric_date = {d:Date} AND metric_name = 'dau'", parameters={"d": D.isoformat()})
    assert dau.result_rows == [(4.0,)]


def test_recompute_is_idempotent(aggregated, aggregation, pg):
    def snapshot():
        return {
            t: pg.execute(f"SELECT count(*) FROM {t} WHERE {c} = %s", (D,)).fetchone()[0]
            for t, c in [("daily_metrics", "metric_date"), ("top_movies_daily", "metric_date"),
                         ("retention_cohort", "cohort_date"), ("device_daily", "metric_date")]
        }

    before_counts, before = snapshot(), pg_metrics(pg, D)
    resp = requests.post(f"{aggregation}/aggregate", params={"date": D.isoformat()}, timeout=120)
    assert resp.status_code == 200
    after_counts, after = snapshot(), pg_metrics(pg, D)
    assert after_counts == before_counts                      # строки обновились, а не задублировались
    assert after["dau"][0] == before["dau"][0]
    assert after["dau"][1] > before["dau"][1]                 # computed_at обновился


def test_export_to_s3(aggregated, aggregation, s3):
    key = f"daily/{D.isoformat()}/aggregates.json"
    resp = requests.post(f"{aggregation}/export", params={"date": D.isoformat()}, timeout=60)
    assert resp.status_code == 200, resp.text
    assert resp.json()["object"] == f"s3://{S3_BUCKET}/{key}"

    payload = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    assert payload["date"] == D.isoformat()
    names = {m["metric_name"] for m in payload["metrics"]}
    assert {"dau", "view_conversion", "avg_watch_time_seconds", "retention_d1", "retention_d7"} <= names
    assert all(m["computed_at"] for m in payload["metrics"])
    assert payload["top_movies"][0]["movie_id"] == "m-001"
    assert len(payload["retention_cohort"]) == 8
    first_modified = s3.head_object(Bucket=S3_BUCKET, Key=key)["LastModified"]

    # повторный экспорт перезаписывает тот же объект, а не создаёт второй
    resp = requests.post(f"{aggregation}/export", params={"date": D.isoformat()}, timeout=60)
    assert resp.status_code == 200
    listing = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"daily/{D.isoformat()}/")
    assert [o["Key"] for o in listing["Contents"]] == [key]
    assert s3.head_object(Bucket=S3_BUCKET, Key=key)["LastModified"] >= first_modified
