"""SQL расчёта бизнес-метрик в ClickHouse.

Параметры (server-side binding clickhouse-connect):
  {d:Date}    — дата пересчёта
  {ts:String} — время вычисления (одно на цикл), пишется в computed_at
  {top_n:UInt16}

Все запросы читают raw-события с FINAL: ReplacingMergeTree схлопывает дубли событий
(одинаковый event_id) только при фоновых слияниях, FINAL делает это при чтении.
"""

TS = "toDateTime64({ts:String}, 3, 'UTC')"

RAW_EVENTS_COUNT = """
SELECT count() FROM analytics.movie_events FINAL WHERE event_date = {d:Date}
"""

# DAU, конверсия, среднее время просмотра и вспомогательные счётчики за день.
# Один проход по данным дня, результат разворачивается в строки через ARRAY JOIN.
DAILY_METRICS = f"""
INSERT INTO analytics.agg_daily_metrics (metric_date, metric_name, value, computed_at)
SELECT {{d:Date}}, m.1, m.2, {TS}
FROM
(
    SELECT
        uniqExact(user_id)                                   AS dau,
        uniqExact(session_id)                                AS sessions,
        count()                                              AS events_total,
        countIf(event_type = 'VIEW_STARTED')                 AS views_started,
        countIf(event_type = 'VIEW_FINISHED')                AS views_finished,
        avgIf(progress_seconds, event_type = 'VIEW_FINISHED') AS avg_watch,
        countIf(event_type = 'LIKED')                        AS likes,
        countIf(event_type = 'SEARCHED')                     AS searches
    FROM analytics.movie_events FINAL
    WHERE event_date = {{d:Date}}
)
ARRAY JOIN [
    ('dau',                    toFloat64(dau)),
    ('sessions',               toFloat64(sessions)),
    ('events_total',           toFloat64(events_total)),
    ('views_started',          toFloat64(views_started)),
    ('views_finished',         toFloat64(views_finished)),
    ('view_conversion',        if(views_started = 0, 0, views_finished / views_started)),
    ('avg_watch_time_seconds', if(views_finished = 0, 0, toFloat64(ifNull(avg_watch, 0)))),
    ('likes',                  toFloat64(likes)),
    ('searches',               toFloat64(searches))
] AS m
"""

# Устройства: уникальные пользователи и события по типу устройства.
DEVICES = f"""
INSERT INTO analytics.agg_device_daily (metric_date, device_type, unique_users, events, computed_at)
SELECT {{d:Date}}, toString(device_type), uniqExact(user_id), count(), {TS}
FROM analytics.movie_events FINAL
WHERE event_date = {{d:Date}}
GROUP BY device_type
"""

# Топ фильмов: оконная функция row_number() даёт место в рейтинге.
TOP_MOVIES = f"""
INSERT INTO analytics.agg_top_movies (metric_date, rank, movie_id, views, unique_viewers, computed_at)
SELECT {{d:Date}}, toUInt16(rank), movie_id, views, unique_viewers, {TS}
FROM
(
    SELECT
        movie_id, views, unique_viewers,
        row_number() OVER (ORDER BY views DESC, unique_viewers DESC, movie_id ASC) AS rank
    FROM
    (
        SELECT
            movie_id,
            countIf(event_type = 'VIEW_STARTED')                AS views,
            uniqExactIf(user_id, event_type = 'VIEW_STARTED')   AS unique_viewers
        FROM analytics.movie_events FINAL
        WHERE event_date = {{d:Date}}
        GROUP BY movie_id
        HAVING views > 0
    )
)
WHERE rank <= {{top_n:UInt16}}
"""

TOP_MOVIES_COUNT = """
SELECT count() FROM analytics.agg_top_movies FINAL WHERE metric_date = {d:Date}
"""

# Если фильмов за день стало меньше, чем мест в рейтинге, убираем «хвост» прошлого расчёта.
TOP_MOVIES_TRIM = """
ALTER TABLE analytics.agg_top_movies DELETE
WHERE metric_date = {d:Date} AND rank > {n:UInt16}
SETTINGS mutations_sync = 1
"""

# Когортный retention для когорт [d-7, d].
#   когорта пользователя = дата его ПЕРВОГО VIEW_STARTED за всю историю;
#   «вернулся в день N» = есть любое событие в дату когорты + N.
# Сетка cohort × day_number (0..7) строится заранее, поэтому дни без вернувшихся дают 0 %,
# а не пропуск. Дни, до которых данные ещё не дошли (будущее), в сетку не попадают.
COHORTS = f"""
INSERT INTO analytics.agg_retention_cohort
    (cohort_date, day_number, cohort_size, retained_users, retention_pct, computed_at)
WITH
    first_view AS
    (
        SELECT user_id, min(event_date) AS cohort_date
        FROM analytics.movie_events FINAL
        WHERE event_type = 'VIEW_STARTED'
        GROUP BY user_id
    ),
    cohort AS
    (
        SELECT user_id, cohort_date
        FROM first_view
        WHERE cohort_date BETWEEN {{d:Date}} - 7 AND {{d:Date}}
    ),
    activity AS
    (
        SELECT DISTINCT user_id, event_date
        FROM analytics.movie_events FINAL
        WHERE event_date BETWEEN {{d:Date}} - 7 AND {{d:Date}} + 7
    ),
    sizes AS
    (
        SELECT cohort_date, uniqExact(user_id) AS cohort_size
        FROM cohort
        GROUP BY cohort_date
    ),
    returned AS
    (
        SELECT
            c.cohort_date                                       AS cohort_date,
            toUInt8(dateDiff('day', c.cohort_date, a.event_date)) AS day_number,
            uniqExact(c.user_id)                                AS retained_users
        FROM cohort AS c
        INNER JOIN activity AS a ON a.user_id = c.user_id
        WHERE a.event_date >= c.cohort_date AND a.event_date <= c.cohort_date + 7
        GROUP BY cohort_date, day_number
    ),
    grid AS
    (
        SELECT cohort_date, cohort_size, toUInt8(arrayJoin(range(8))) AS day_number
        FROM sizes
    )
SELECT
    g.cohort_date,
    g.day_number,
    g.cohort_size,
    r.retained_users,
    round(100 * r.retained_users / g.cohort_size, 2),
    {TS}
FROM grid AS g
LEFT JOIN returned AS r ON r.cohort_date = g.cohort_date AND r.day_number = g.day_number
WHERE g.cohort_date + g.day_number <= (SELECT max(event_date) FROM analytics.movie_events)
"""

# Retention D1/D7 и размер когорты как дневные метрики (дата = дата когорты).
RETENTION_METRICS = f"""
INSERT INTO analytics.agg_daily_metrics (metric_date, metric_name, value, computed_at)
SELECT
    cohort_date,
    multiIf(day_number = 0, 'new_viewers', day_number = 1, 'retention_d1', 'retention_d7'),
    if(day_number = 0, toFloat64(cohort_size), retention_pct / 100),
    {TS}
FROM analytics.agg_retention_cohort FINAL
WHERE cohort_date BETWEEN {{d:Date}} - 7 AND {{d:Date}} AND day_number IN (0, 1, 7)
"""

# ------------------------------------------------------------------ чтение результата

READ_DAILY = """
SELECT metric_date, metric_name, value, computed_at
FROM analytics.agg_daily_metrics FINAL
WHERE metric_date = {d:Date}
   OR (metric_date BETWEEN {d:Date} - 7 AND {d:Date}
       AND metric_name IN ('new_viewers', 'retention_d1', 'retention_d7'))
ORDER BY metric_date, metric_name
"""

READ_TOP = """
SELECT metric_date, rank, movie_id, views, unique_viewers, computed_at
FROM analytics.agg_top_movies FINAL
WHERE metric_date = {d:Date}
ORDER BY rank
"""

READ_COHORTS = """
SELECT cohort_date, day_number, cohort_size, retained_users, retention_pct, computed_at
FROM analytics.agg_retention_cohort FINAL
WHERE cohort_date BETWEEN {d:Date} - 7 AND {d:Date}
ORDER BY cohort_date, day_number
"""

READ_DEVICES = """
SELECT metric_date, device_type, unique_users, events, computed_at
FROM analytics.agg_device_daily FINAL
WHERE metric_date = {d:Date}
ORDER BY device_type
"""

# Последнее поступление сырых событий по датам — для поиска «грязных» дат.
RAW_LAST_INGESTED = """
SELECT event_date, max(ingested_at) AS last_ingested
FROM analytics.movie_events
WHERE event_date >= today() - {lookback:UInt16} AND event_date <= today()
GROUP BY event_date
ORDER BY event_date
"""
