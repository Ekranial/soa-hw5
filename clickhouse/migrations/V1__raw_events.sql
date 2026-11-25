-- Приём и хранение raw-событий: Kafka Engine → Materialized View → ReplacingMergeTree

-- 1. Постоянное хранилище.
--    ReplacingMergeTree: Kafka даёт гарантию «как минимум один раз», а генератор при рестарте
--    повторяет события с теми же event_id. Строки с одинаковым ключом сортировки схлопываются
--    при слиянии кусков, а запросы с FINAL видят уже схлопнутые данные.
--    PARTITION BY месяц: дневные запросы читают одну партицию, старые месяцы легко удалять.
--    ORDER BY (event_date, user_id, ...): почти все запросы агрегации фильтруют по дате
--    и группируют по пользователю (DAU, retention, когорты).
CREATE TABLE IF NOT EXISTS analytics.movie_events
(
    event_id         UUID,
    user_id          String,
    movie_id         String,
    event_type       Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3,
                           'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp        DateTime64(3, 'UTC'),
    device_type      Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id       String,
    progress_seconds Nullable(Int32),
    event_date       Date MATERIALIZED toDate(timestamp),
    kafka_partition  UInt16,
    kafka_offset     UInt64,
    ingested_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id, event_type, event_id);

-- 2. Kafka Engine: читает топик movie-events. Формат AvroConfluent — сообщения в wire-формате
--    Confluent (magic byte + id схемы), схема берётся из Schema Registry по id.
--    Типы колонок соответствуют Avro-схеме (schemas/movie_event.avsc).
CREATE TABLE IF NOT EXISTS analytics.movie_events_queue
(
    event_id         String,
    user_id          String,
    movie_id         String,
    event_type       Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3,
                           'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp        DateTime64(3, 'UTC'),
    device_type      Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id       String,
    progress_seconds Nullable(Int32)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:19092,kafka-2:19092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-movie-events',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 1,
    kafka_max_block_size = 10000,
    kafka_flush_interval_ms = 1000;

-- 3. Перекладка из Kafka-таблицы в MergeTree. MV срабатывает на каждый прочитанный блок
--    сообщений; offset коммитится после успешной вставки.
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.movie_events_mv TO analytics.movie_events AS
SELECT
    toUUID(event_id) AS event_id,
    user_id,
    movie_id,
    event_type,
    timestamp,
    device_type,
    session_id,
    progress_seconds,
    _partition AS kafka_partition,
    _offset    AS kafka_offset
FROM analytics.movie_events_queue;
