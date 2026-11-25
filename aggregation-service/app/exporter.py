"""Экспорт агрегатов из PostgreSQL в S3 (SeaweedFS): s3://<bucket>/daily/YYYY-MM-DD/aggregates.json"""

import datetime as dt
import json
import logging

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from app.config import Settings
from app.pg import PostgresStore

log = logging.getLogger("agg.export")


class NothingToExport(Exception):
    pass


class S3Exporter:
    def __init__(self, s: Settings, pg: PostgresStore):
        self._bucket = s.s3_bucket
        self._pg = pg
        self._s3 = boto3.client(
            "s3",
            endpoint_url=s.s3_endpoint,
            aws_access_key_id=s.s3_access_key,
            aws_secret_access_key=s.s3_secret_key,
            region_name="us-east-1",
            config=Config(signature_version="s3v4", connect_timeout=5, read_timeout=15,
                          s3={"addressing_style": "path"},
                          retries={"max_attempts": 3, "mode": "standard"}),
        )
        self._bucket_ready = False

    @staticmethod
    def object_key(date: dt.date) -> str:
        return f"daily/{date.isoformat()}/aggregates.json"

    def _ensure_bucket(self) -> None:
        if self._bucket_ready:
            return
        try:
            self._s3.head_bucket(Bucket=self._bucket)
        except ClientError:
            self._s3.create_bucket(Bucket=self._bucket)
            log.info("bucket created: %s", self._bucket)
        self._bucket_ready = True

    def export(self, date: dt.date) -> dict:
        """Выгружает все метрики за дату. Повторный экспорт перезаписывает тот же объект."""
        try:
            data = self._pg.export_payload(date)
            if not data["metrics"]:
                raise NothingToExport(f"no aggregates for {date} in PostgreSQL")
            payload = {
                "date": date.isoformat(),
                "exported_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                **data,
            }
            body = json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode()
            key = self.object_key(date)
            self._ensure_bucket()
            self._s3.put_object(Bucket=self._bucket, Key=key, Body=body, ContentType="application/json")
        except NothingToExport:
            raise
        except (BotoCoreError, ClientError, Exception) as exc:  # noqa: BLE001
            log.error("export %s FAILED: %s — will retry on next run", date, exc)
            try:
                self._pg.mark_export(date, "FAILED", None, 0, str(exc)[:500])
            except Exception:  # noqa: BLE001 — PostgreSQL тоже может быть недоступен
                log.exception("cannot record export failure for %s", date)
            raise
        rows = sum(len(v) for k, v in data.items())
        self._pg.mark_export(date, "SUCCESS", f"s3://{self._bucket}/{key}", rows, None)
        log.info("exported %s → s3://%s/%s (%d rows, %d bytes)", date, self._bucket, key, rows, len(body))
        return {"date": date.isoformat(), "object": f"s3://{self._bucket}/{key}", "rows": rows, "bytes": len(body)}
