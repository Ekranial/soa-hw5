import logging
import time

import clickhouse_connect

from app.config import Settings

log = logging.getLogger("agg.clickhouse")


def connect(s: Settings, database: str | None = None, attempts: int = 60):
    """Клиент ClickHouse (HTTP-протокол). Ждёт, пока сервер станет доступен."""
    for attempt in range(1, attempts + 1):
        try:
            client = clickhouse_connect.get_client(
                host=s.ch_host, port=s.ch_port, username=s.ch_user, password=s.ch_password,
                database=database or "default", connect_timeout=5, send_receive_timeout=120,
            )
            client.command("SELECT 1")
            return client
        except Exception as exc:  # noqa: BLE001
            log.info("waiting for ClickHouse (%d/%d): %s", attempt, attempts, exc)
            time.sleep(2)
    raise RuntimeError("ClickHouse is not available")
