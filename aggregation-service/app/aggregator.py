"""Расчёт агрегатов: ClickHouse (raw → agg-таблицы) → PostgreSQL. Плюс планировщик."""

import datetime as dt
import logging
import threading
import time

from app import queries as q
from app.ch import connect
from app.config import Settings
from app.exporter import NothingToExport, S3Exporter
from app.pg import PostgresStore

log = logging.getLogger("agg.aggregator")


def as_utc(value: dt.datetime) -> dt.datetime:
    return value.replace(tzinfo=dt.timezone.utc) if value.tzinfo is None else value.astimezone(dt.timezone.utc)


class Aggregator:
    def __init__(self, s: Settings):
        self._s = s
        self._ch = connect(s, database=s.ch_database)
        self.pg = PostgresStore(s)
        self.exporter = S3Exporter(s, self.pg)
        # один клиент ClickHouse и один цикл одновременно: планировщик и HTTP не пересекаются
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._last_export_run = 0.0

    # ------------------------------------------------------------------ пересчёт одной даты

    def aggregate_date(self, date: dt.date) -> dict:
        """Пересчитывает метрики за дату и когорты [date-7, date], сохраняет в PostgreSQL."""
        with self._lock:
            return self._aggregate_date(date)

    def _aggregate_date(self, date: dt.date) -> dict:
        started = time.monotonic()
        ts = dt.datetime.now(dt.timezone.utc)
        params = {"d": date.isoformat(), "ts": ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3], "top_n": self._s.top_n}

        raw_events = self._ch.query(q.RAW_EVENTS_COUNT, parameters=params).result_rows[0][0]
        self._ch.command(q.DAILY_METRICS, parameters=params)
        self._ch.command(q.DEVICES, parameters=params)
        self._ch.command(q.TOP_MOVIES, parameters=params)
        top_count = self._ch.query(q.TOP_MOVIES_COUNT, parameters=params).result_rows[0][0]
        inserted_top = self._ch.query(
            "SELECT count() FROM analytics.agg_top_movies FINAL "
            "WHERE metric_date = {d:Date} AND computed_at = toDateTime64({ts:String}, 3, 'UTC')",
            parameters=params,
        ).result_rows[0][0]
        if inserted_top < top_count:
            self._ch.command(q.TOP_MOVIES_TRIM, parameters={**params, "n": inserted_top})
        self._ch.command(q.COHORTS, parameters=params)
        self._ch.command(q.RETENTION_METRICS, parameters=params)

        daily = self._ch.query(q.READ_DAILY, parameters=params).result_rows
        top = self._ch.query(q.READ_TOP, parameters=params).result_rows
        cohorts = self._ch.query(q.READ_COHORTS, parameters=params).result_rows
        devices = self._ch.query(q.READ_DEVICES, parameters=params).result_rows

        rows = self.pg.upsert_aggregates(daily, top, cohorts, devices, top_count=inserted_top, date=date)
        elapsed = time.monotonic() - started
        log.info("date %s recomputed: %d raw events → %d rows in PostgreSQL (%.2fs)", date, raw_events, rows, elapsed)
        return {"date": date.isoformat(), "raw_events": raw_events, "rows_written": rows,
                "seconds": round(elapsed, 3), "computed_at": ts.isoformat()}

    # ------------------------------------------------------------------ цикл по расписанию

    def dirty_dates(self) -> list[dt.date]:
        """Даты, где появились новые события после последнего успешного сохранения в PostgreSQL.

        Сравниваем с PostgreSQL (а не с agg-таблицами ClickHouse): если запись в PostgreSQL
        упала, дата останется «грязной» и будет пересчитана в следующем цикле.
        """
        lookback = self._s.lookback_days
        raw = self._ch.query(q.RAW_LAST_INGESTED, parameters={"lookback": lookback}).result_rows
        computed = self.pg.computed_dates(dt.date.today() - dt.timedelta(days=lookback))
        dirty = []
        for event_date, last_ingested in raw:
            done = computed.get(event_date)
            if done is None or as_utc(last_ingested) > as_utc(done):
                dirty.append(event_date)
        return dirty

    def run_cycle(self) -> dict:
        with self._lock:
            started = time.monotonic()
            dates = self.dirty_dates()
            log.info("Цикл агрегации начат: дат к пересчёту — %d %s", len(dates),
                     [d.isoformat() for d in dates] if len(dates) <= 10 else "")
            raw_total = rows_total = 0
            failed = []
            for date in dates:
                try:
                    stats = self._aggregate_date(date)
                    raw_total += stats["raw_events"]
                    rows_total += stats["rows_written"]
                except Exception:  # noqa: BLE001 — дата останется «грязной» и пересчитается в следующий раз
                    log.exception("aggregation for %s failed", date)
                    failed.append(date.isoformat())
            elapsed = time.monotonic() - started
            log.info("Цикл завершён: дат %d, обработано %d записей, записано %d строк в PostgreSQL за %.2f сек%s",
                     len(dates), raw_total, rows_total, elapsed, f", ошибки: {failed}" if failed else "")
            return {"dates": len(dates), "raw_events": raw_total, "rows_written": rows_total,
                    "seconds": round(elapsed, 3), "failed": failed}

    def export_pending(self) -> list[dict]:
        """Выгружает в S3 прошедшие дни, которые ещё не выгружены или пересчитаны после выгрузки."""
        results = []
        try:
            pending = self.pg.dates_pending_export(self._s.lookback_days)
        except Exception:  # noqa: BLE001
            log.exception("cannot read pending exports — retry on next run")
            return results
        if pending:
            log.info("Экспорт в S3: дат к выгрузке — %d", len(pending))
        for date in pending:
            try:
                results.append(self.exporter.export(date))
            except NothingToExport:
                continue
            except Exception:  # noqa: BLE001 — ошибка уже залогирована, повтор в следующем запуске
                continue
        return results

    def export_date(self, date: dt.date) -> dict:
        return self.exporter.export(date)

    def scheduler_loop(self) -> None:
        log.info("scheduler started: aggregation every %.0fs, export every %.0fs, lookback %d days",
                 self._s.aggregation_interval, self._s.export_interval, self._s.lookback_days)
        while not self._stop.is_set():
            try:
                self.run_cycle()
            except Exception:  # noqa: BLE001
                log.exception("aggregation cycle failed")
            if time.monotonic() - self._last_export_run >= self._s.export_interval:
                self._last_export_run = time.monotonic()
                self.export_pending()
            self._stop.wait(self._s.aggregation_interval)

    def start(self) -> None:
        threading.Thread(target=self.scheduler_loop, name="scheduler", daemon=True).start()

    def stop(self) -> None:
        self._stop.set()
