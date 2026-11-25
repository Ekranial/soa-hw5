-- Материализованные агрегаты. Их пишет Aggregation Service, читают Grafana и выгрузка в PostgreSQL.
-- ReplacingMergeTree(computed_at): пересчёт за ту же дату вставляет строки с тем же ключом
-- и более новым computed_at — при чтении с FINAL видна только последняя версия.

-- Дневные метрики в «длинном» формате: одна строка = (дата, метрика).
-- Доли (view_conversion, retention_d1, retention_d7) хранятся как 0..1.
-- retention_d1/retention_d7 привязаны к дате КОГОРТЫ (дню первого просмотра).
CREATE TABLE IF NOT EXISTS analytics.agg_daily_metrics
(
    metric_date Date,
    metric_name LowCardinality(String),
    value       Float64,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (metric_date, metric_name);

-- Топ фильмов за день по числу просмотров (VIEW_STARTED). Ключ — место в рейтинге.
CREATE TABLE IF NOT EXISTS analytics.agg_top_movies
(
    metric_date    Date,
    rank           UInt16,
    movie_id       String,
    views          UInt64,
    unique_viewers UInt64,
    computed_at    DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (metric_date, rank);

-- Когортный retention: когорта = день первого просмотра, day_number = 0..7.
CREATE TABLE IF NOT EXISTS analytics.agg_retention_cohort
(
    cohort_date    Date,
    day_number     UInt8,
    cohort_size    UInt64,
    retained_users UInt64,
    retention_pct  Float64,
    computed_at    DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (cohort_date, day_number);

-- Распределение активности по устройствам за день.
CREATE TABLE IF NOT EXISTS analytics.agg_device_daily
(
    metric_date  Date,
    device_type  LowCardinality(String),
    unique_users UInt64,
    events       UInt64,
    computed_at  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (metric_date, device_type);
