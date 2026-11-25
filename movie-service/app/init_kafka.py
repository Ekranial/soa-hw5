"""Init-job: создаёт топик movie-events и регистрирует Avro-схему в Schema Registry.

Запускается один раз при `docker compose up` (контейнер kafka-init) до старта
продюсера и ClickHouse. Идемпотентен: повторный запуск ничего не ломает.
"""

import json
import logging
import sys
import time

import requests
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

from app.config import load_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
log = logging.getLogger("kafka-init")


def wait_for_brokers(admin: AdminClient, expected: int, timeout: float = 120) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            brokers = admin.list_topics(timeout=5).brokers
            if len(brokers) >= expected:
                log.info("Kafka cluster is ready: %d brokers %s", len(brokers), sorted(brokers))
                return
            log.info("waiting for brokers: %d/%d", len(brokers), expected)
        except Exception as exc:  # noqa: BLE001
            log.info("waiting for Kafka: %s", exc)
        if time.monotonic() > deadline:
            raise RuntimeError("Kafka cluster is not ready")
        time.sleep(2)


def ensure_topic(admin: AdminClient, s) -> None:
    topics = admin.list_topics(timeout=10).topics
    if s.topic not in topics:
        new = NewTopic(
            s.topic,
            num_partitions=s.topic_partitions,
            replication_factor=s.topic_replication_factor,
            config={"min.insync.replicas": str(s.topic_min_insync_replicas)},
        )
        admin.create_topics([new])[s.topic].result(timeout=30)
        log.info("topic created: %s partitions=%d replication.factor=%d min.insync.replicas=%d",
                 s.topic, s.topic_partitions, s.topic_replication_factor, s.topic_min_insync_replicas)
    else:
        log.info("topic already exists: %s", s.topic)

    meta = admin.list_topics(topic=s.topic, timeout=10).topics[s.topic]
    for pid, p in sorted(meta.partitions.items()):
        log.info("  partition %d: leader=%s replicas=%s isr=%s", pid, p.leader, p.replicas, p.isrs)
    cfg = admin.describe_configs([ConfigResource(ConfigResource.Type.TOPIC, s.topic)])
    for res, fut in cfg.items():
        value = fut.result()["min.insync.replicas"].value
        log.info("  min.insync.replicas=%s", value)


def register_schema(s) -> None:
    subject = f"{s.topic}-value"  # TopicNameStrategy — стандарт Confluent
    with open(s.schema_path, encoding="utf-8") as f:
        schema = json.load(f)

    base = s.schema_registry_url.rstrip("/")
    for attempt in range(30):
        try:
            # Совместимость BACKWARD: новая версия схемы должна читать данные старой
            requests.put(f"{base}/config/{subject}", json={"compatibility": "BACKWARD"}, timeout=5).raise_for_status()
            resp = requests.post(
                f"{base}/subjects/{subject}/versions",
                json={"schemaType": "AVRO", "schema": json.dumps(schema)},
                timeout=5,
            )
            resp.raise_for_status()
            schema_id = resp.json()["id"]
            latest = requests.get(f"{base}/subjects/{subject}/versions/latest", timeout=5).json()
            log.info("schema registered: subject=%s version=%s id=%s (compatibility=BACKWARD)",
                     subject, latest["version"], schema_id)
            return
        except requests.RequestException as exc:
            log.info("waiting for Schema Registry (%d): %s", attempt + 1, exc)
            time.sleep(2)
    raise RuntimeError("Schema Registry is not available")


def main() -> int:
    s = load_settings()
    admin = AdminClient({"bootstrap.servers": s.bootstrap_servers})
    wait_for_brokers(admin, expected=s.topic_replication_factor)
    ensure_topic(admin, s)
    register_schema(s)
    log.info("kafka-init finished")
    return 0


if __name__ == "__main__":
    sys.exit(main())
