"""PostgreSQL — хранилище «готовых» метрик. Все записи — идемпотентные UPSERT."""

import datetime as dt
import logging
import time
from typing import Callable, TypeVar

import psycopg
from psycopg.rows import dict_row

from app.config import Settings

log = logging.getLogger("agg.postgres")
T = TypeVar("T")


class PostgresStore:
    def __init__(self, s: Settings):
        self._dsn = s.pg_dsn
        self._attempts = s.pg_retry_attempts
        self._base_delay = s.pg_retry_base_delay

    def _with_retry(self, what: str, fn: Callable[[psycopg.Connection], T]) -> T:
        """Retry с экспоненциальной задержкой (1 с → 2 с → 4 с) на ошибках соединения."""
        for attempt in range(1, self._attempts + 1):
            try:
                with psycopg.connect(self._dsn, connect_timeout=5, row_factory=dict_row) as conn:
                    with conn.transaction():
                        return fn(conn)
            except psycopg.OperationalError as exc:
                if attempt == self._attempts:
                    log.error("PostgreSQL %s failed after %d attempts: %s", what, attempt, exc)
                    raise
                delay = self._base_delay * 2 ** (attempt - 1)
                log.warning("PostgreSQL %s failed (attempt %d/%d): %s — retry in %.0fs",
                            what, attempt, self._attempts, exc, delay)
                time.sleep(delay)
        raise AssertionError("unreachable")

    # ------------------------------------------------------------------ запись агрегатов

    def upsert_aggregates(self, daily, top, cohorts, devices, top_count: int, date: dt.date) -> int:
        """Сохраняет результат пересчёта одной даты в одной транзакции. Возвращает число строк."""

        def write(conn: psycopg.Connection) -> int:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO daily_metrics (metric_date, metric_name, value, computed_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (metric_date, metric_name) DO UPDATE
                    SET value = EXCLUDED.value, computed_at = EXCLUDED.computed_at, updated_at = now()
                    """,
                    daily,
                )
                cur.executemany(
                    """
                    INSERT INTO top_movies_daily (metric_date, rank, movie_id, views, unique_viewers, computed_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (metric_date, rank) DO UPDATE
                    SET movie_id = EXCLUDED.movie_id, views = EXCLUDED.views,
                        unique_viewers = EXCLUDED.unique_viewers, computed_at = EXCLUDED.computed_at,
                        updated_at = now()
                    """,
                    top,
                )
                # если мест в рейтинге стало меньше — удаляем хвост прошлого расчёта
                cur.execute("DELETE FROM top_movies_daily WHERE metric_date = %s AND rank > %s", (date, top_count))
                cur.executemany(
                    """
                    INSERT INTO retention_cohort
                        (cohort_date, day_number, cohort_size, retained_users, retention_pct, computed_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (cohort_date, day_number) DO UPDATE
                    SET cohort_size = EXCLUDED.cohort_size, retained_users = EXCLUDED.retained_users,
                        retention_pct = EXCLUDED.retention_pct, computed_at = EXCLUDED.computed_at,
                        updated_at = now()
                    """,
                    cohorts,
                )
                cur.executemany(
                    """
                    INSERT INTO device_daily (metric_date, device_type, unique_users, events, computed_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (metric_date, device_type) DO UPDATE
                    SET unique_users = EXCLUDED.unique_users, events = EXCLUDED.events,
                        computed_at = EXCLUDED.computed_at, updated_at = now()
                    """,
                    devices,
                )
            return len(daily) + len(top) + len(cohorts) + len(devices)

        return self._with_retry(f"upsert for {date}", write)

    def computed_dates(self, since: dt.date) -> dict[dt.date, dt.datetime]:
        """Когда в последний раз считались метрики каждой даты (по метрике dau)."""

        def read(conn):
            rows = conn.execute(
                "SELECT metric_date, computed_at FROM daily_metrics WHERE metric_name = 'dau' AND metric_date >= %s",
                (since,),
            ).fetchall()
            return {r["metric_date"]: r["computed_at"] for r in rows}

        return self._with_retry("read computed dates", read)

    # ------------------------------------------------------------------ экспорт

    def dates_pending_export(self, lookback_days: int) -> list[dt.date]:
        """Прошедшие даты, которые ещё не выгружены или пересчитаны после последней выгрузки."""

        def read(conn):
            rows = conn.execute(
                """
                SELECT m.metric_date
                FROM daily_metrics AS m
                LEFT JOIN export_log AS e ON e.export_date = m.metric_date
                WHERE m.metric_date < current_date
                  AND m.metric_date >= current_date - %s::int
                GROUP BY m.metric_date, e.status, e.exported_at
                HAVING e.status IS DISTINCT FROM 'SUCCESS' OR max(m.computed_at) > e.exported_at
                ORDER BY m.metric_date
                """,
                (lookback_days,),
            ).fetchall()
            return [r["metric_date"] for r in rows]

        return self._with_retry("read pending exports", read)

    def export_payload(self, date: dt.date) -> dict:
        def read(conn):
            metrics = conn.execute(
                "SELECT metric_name, value, computed_at FROM daily_metrics WHERE metric_date = %s ORDER BY metric_name",
                (date,),
            ).fetchall()
            top = conn.execute(
                "SELECT rank, movie_id, views, unique_viewers, computed_at FROM top_movies_daily "
                "WHERE metric_date = %s ORDER BY rank",
                (date,),
            ).fetchall()
            devices = conn.execute(
                "SELECT device_type, unique_users, events, computed_at FROM device_daily "
                "WHERE metric_date = %s ORDER BY device_type",
                (date,),
            ).fetchall()
            cohort = conn.execute(
                "SELECT day_number, cohort_size, retained_users, retention_pct, computed_at FROM retention_cohort "
                "WHERE cohort_date = %s ORDER BY day_number",
                (date,),
            ).fetchall()
            return {"metrics": metrics, "top_movies": top, "devices": devices, "retention_cohort": cohort}

        return self._with_retry(f"read export payload for {date}", read)

    def mark_export(self, date: dt.date, status: str, object_key: str | None, rows: int, error: str | None) -> None:
        def write(conn):
            conn.execute(
                """
                INSERT INTO export_log (export_date, status, object_key, rows_exported, exported_at, error)
                VALUES (%s, %s, %s, %s, CASE WHEN %s::text = 'SUCCESS' THEN now() END, %s)
                ON CONFLICT (export_date) DO UPDATE
                SET status = EXCLUDED.status, object_key = COALESCE(EXCLUDED.object_key, export_log.object_key),
                    rows_exported = EXCLUDED.rows_exported,
                    exported_at = COALESCE(EXCLUDED.exported_at, export_log.exported_at),
                    error = EXCLUDED.error, updated_at = now()
                """,
                (date, status, object_key, rows, status, error),
            )

        self._with_retry(f"mark export {date}", write)

    def read_metrics(self, date: dt.date) -> list[dict]:
        return self._with_retry(
            "read metrics",
            lambda conn: conn.execute(
                "SELECT metric_name, value, computed_at FROM daily_metrics WHERE metric_date = %s ORDER BY metric_name",
                (date,),
            ).fetchall(),
        )
