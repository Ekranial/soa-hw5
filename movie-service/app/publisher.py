"""Kafka-продюсер: Avro + Schema Registry, acks=all, идемпотентность, retry с exponential backoff."""

import json
import logging
import threading
import time
from concurrent.futures import Future
from dataclasses import dataclass

from confluent_kafka import KafkaError, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from app.config import Settings
from app.events import AvroValidator

log = logging.getLogger("movie.publisher")

# Ошибки, после которых имеет смысл отправить сообщение ещё раз.
# Повтор безопасен: event_id тот же, ClickHouse (ReplacingMergeTree) схлопнет дубль.
_RETRIABLE_CODES = {
    KafkaError._MSG_TIMED_OUT,
    KafkaError._TRANSPORT,
    KafkaError._ALL_BROKERS_DOWN,
    KafkaError.NOT_ENOUGH_REPLICAS,
    KafkaError.NOT_ENOUGH_REPLICAS_AFTER_APPEND,
    KafkaError.LEADER_NOT_AVAILABLE,
    KafkaError.NOT_LEADER_FOR_PARTITION,
    KafkaError.REQUEST_TIMED_OUT,
}


class PublishError(Exception):
    pass


@dataclass
class PublishResult:
    event_id: str
    partition: int
    offset: int


class EventPublisher:
    def __init__(self, s: Settings):
        self._s = s
        self.subject = f"{s.topic}-value"
        sr = SchemaRegistryClient({"url": s.schema_registry_url})
        latest = self._wait_schema(sr)
        self.schema_version = latest.version
        self.schema_id = latest.schema_id
        schema_str = latest.schema.schema_str
        self.validator = AvroValidator(json.loads(schema_str))
        # Схему регистрирует init-job; продюсер только использует последнюю версию.
        self._serializer = AvroSerializer(
            sr, schema_str, conf={"auto.register.schemas": False, "use.latest.version": True}
        )
        self._producer = Producer({
            "bootstrap.servers": s.bootstrap_servers,
            "client.id": "movie-service",
            "acks": "all",                 # запись подтверждается всеми ISR-репликами
            "enable.idempotence": True,    # без дублей и с сохранением порядка при внутренних ретраях
            "retries": 10,                 # ретраи librdkafka (exponential backoff 100 мс → 2 с)
            "retry.backoff.ms": 100,
            "retry.backoff.max.ms": 2000,
            "delivery.timeout.ms": 15000,
            "linger.ms": 5,
            "compression.type": "lz4",
        })
        self._stop = threading.Event()
        self._poller = threading.Thread(target=self._poll_loop, name="kafka-poller", daemon=True)
        self._poller.start()
        log.info("publisher ready: topic=%s subject=%s schema version=%s id=%s",
                 s.topic, self.subject, self.schema_version, self.schema_id)

    def _wait_schema(self, sr: SchemaRegistryClient):
        for attempt in range(60):
            try:
                return sr.get_latest_version(self.subject)
            except Exception as exc:  # noqa: BLE001
                log.info("waiting for schema %s (%d): %s", self.subject, attempt + 1, exc)
                time.sleep(2)
        raise RuntimeError(f"schema {self.subject} is not registered")

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            self._producer.poll(0.1)  # вызывает delivery-callback'и

    # ------------------------------------------------------------------ API

    def publish(self, record: dict) -> Future:
        """Асинхронная публикация. Future завершится PublishResult или исключением."""
        future: Future = Future()
        try:
            self.validator.validate(record)  # проверка по Avro-схеме до отправки
            value = self._serializer(record, SerializationContext(self._s.topic, MessageField.VALUE))
        except Exception as exc:  # noqa: BLE001 — ошибка валидации или сериализации, не ретраим
            future.set_exception(exc)
            return future
        self._send(record, value, attempt=1, future=future)
        return future

    def flush(self, timeout: float = 30) -> int:
        return self._producer.flush(timeout)

    def close(self) -> None:
        self.flush()
        self._stop.set()

    # ------------------------------------------------------------------ internals

    def _send(self, record: dict, value: bytes, attempt: int, future: Future) -> None:
        # Ключ = user_id: все события пользователя попадают в одну партицию → порядок сохраняется.
        key = record["user_id"].encode()

        def on_delivery(err, msg):
            if err is None:
                log.info("published event_id=%s event_type=%s timestamp=%s partition=%d offset=%d",
                         record["event_id"], record["event_type"], record["timestamp"].isoformat(),
                         msg.partition(), msg.offset())
                future.set_result(PublishResult(record["event_id"], msg.partition(), msg.offset()))
                return
            retriable = err.retriable() or err.code() in _RETRIABLE_CODES
            if retriable and attempt < self._s.publish_max_retries:
                delay = min(self._s.publish_base_backoff * 2 ** (attempt - 1), 10.0)
                log.warning("publish failed event_id=%s (%s), attempt %d/%d, retry in %.1fs",
                            record["event_id"], err, attempt, self._s.publish_max_retries, delay)
                timer = threading.Timer(delay, self._send, args=(record, value, attempt + 1, future))
                timer.daemon = True
                timer.start()
            else:
                log.error("publish FAILED event_id=%s event_type=%s after %d attempt(s): %s",
                          record["event_id"], record["event_type"], attempt, err)
                future.set_exception(PublishError(str(err)))

        while True:
            try:
                self._producer.produce(self._s.topic, key=key, value=value, on_delivery=on_delivery)
                return
            except BufferError:
                # локальная очередь librdkafka заполнена — даём ей разгрузиться
                time.sleep(0.05)
