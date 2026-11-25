-- Хранилище «готовых» метрик. Пишет Aggregation Service (UPSERT), читают дашборды и экспорт в S3.
-- Первичные ключи — естественные ключи агрегатов: повторный пересчёт той же даты обновляет строку,
-- а не добавляет новую (ON CONFLICT DO UPDATE).

-- Дневные метрики: дата, название метрики, значение, время вычисления.
-- Доли (view_conversion, retention_d1, retention_d7) — 0..1; retention_* и new_viewers — по дате когорты.
CREATE TABLE daily_metrics (
    metric_date date             NOT NULL,
    metric_name text             NOT NULL CHECK (metric_name ~ '^[a-z0-9_]+$'),
    value       double precision NOT NULL,
    computed_at timestamptz      NOT NULL,
    updated_at  timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (metric_date, metric_name)
);

CREATE TABLE top_movies_daily (
    metric_date    date        NOT NULL,
    rank           smallint    NOT NULL CHECK (rank > 0),
    movie_id       text        NOT NULL,
    views          bigint      NOT NULL CHECK (views >= 0),
    unique_viewers bigint      NOT NULL CHECK (unique_viewers >= 0),
    computed_at    timestamptz NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (metric_date, rank)
);

CREATE TABLE retention_cohort (
    cohort_date    date         NOT NULL,
    day_number     smallint     NOT NULL CHECK (day_number BETWEEN 0 AND 7),
    cohort_size    bigint       NOT NULL CHECK (cohort_size > 0),
    retained_users bigint       NOT NULL CHECK (retained_users >= 0),
    retention_pct  numeric(5,2) NOT NULL CHECK (retention_pct BETWEEN 0 AND 100),
    computed_at    timestamptz  NOT NULL,
    updated_at     timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (cohort_date, day_number)
);

CREATE TABLE device_daily (
    metric_date  date        NOT NULL,
    device_type  text        NOT NULL CHECK (device_type IN ('MOBILE', 'DESKTOP', 'TV', 'TABLET')),
    unique_users bigint      NOT NULL CHECK (unique_users >= 0),
    events       bigint      NOT NULL CHECK (events >= 0),
    computed_at  timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (metric_date, device_type)
);

-- Журнал выгрузок в S3: по нему экспорт понимает, что выгружать (новое, пересчитанное, упавшее).
CREATE TABLE export_log (
    export_date   date        PRIMARY KEY,
    status        text        NOT NULL CHECK (status IN ('SUCCESS', 'FAILED')),
    object_key    text,
    rows_exported integer     NOT NULL DEFAULT 0,
    exported_at   timestamptz,
    error         text,
    updated_at    timestamptz NOT NULL DEFAULT now()
);
