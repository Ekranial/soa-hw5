"""Мигратор схемы ClickHouse (аналог Flyway): применяет migrations/V<n>__*.sql по порядку,
применённые версии хранит в analytics.schema_migrations. Запускается init-контейнером
clickhouse-migrate при каждом `docker compose up`; уже применённые версии пропускаются.
"""

import logging
import pathlib
import re
import sys

from app.ch import connect
from app.config import load_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
log = logging.getLogger("ch-migrate")

MIGRATIONS_DIR = pathlib.Path("/app/clickhouse/migrations")
FILE_RE = re.compile(r"^V(\d+)__(.+)\.sql$")


def split_statements(sql: str) -> list[str]:
    lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    return [stmt.strip() for stmt in "\n".join(lines).split(";") if stmt.strip()]


def main() -> int:
    s = load_settings()
    client = connect(s)
    client.command("CREATE DATABASE IF NOT EXISTS analytics")
    client.command(
        "CREATE TABLE IF NOT EXISTS analytics.schema_migrations "
        "(version UInt32, name String, applied_at DateTime DEFAULT now()) ENGINE = MergeTree ORDER BY version"
    )
    applied = {row[0] for row in client.query("SELECT version FROM analytics.schema_migrations").result_rows}

    files = sorted(
        (int(m.group(1)), m.group(2), path)
        for path in MIGRATIONS_DIR.iterdir()
        if (m := FILE_RE.match(path.name))
    )
    for version, name, path in files:
        if version in applied:
            log.info("V%d %s — already applied", version, name)
            continue
        for stmt in split_statements(path.read_text(encoding="utf-8")):
            client.command(stmt)
        client.insert("analytics.schema_migrations", [[version, name]], column_names=["version", "name"])
        log.info("V%d %s — applied", version, name)
    log.info("ClickHouse schema is up to date (%d migrations)", len(files))
    return 0


if __name__ == "__main__":
    sys.exit(main())
