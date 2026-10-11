-- Per-minute rollup maintained at insert time. Dashboards and the overview API read this
-- instead of scanning raw events, so their cost depends on the time range, not the event volume.
-- Chained from pulse.events, so it sees every row the events MV writes.

CREATE TABLE IF NOT EXISTS pulse.metrics_minute
(
    minute            DateTime('UTC'),
    event_type        LowCardinality(String),
    country           LowCardinality(String),
    device            LowCardinality(String),
    product_category  LowCardinality(String),
    events            SimpleAggregateFunction(sum, UInt64),
    quantity          SimpleAggregateFunction(sum, UInt64),
    revenue_cents     SimpleAggregateFunction(sum, UInt64),
    users             AggregateFunction(uniq, String),
    sessions          AggregateFunction(uniq, String),
    orders            AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, event_type, country, device, product_category)
TTL minute + INTERVAL 400 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS pulse.metrics_minute_mv TO pulse.metrics_minute AS
SELECT
    toStartOfMinute(event_time)  AS minute,
    event_type,
    country,
    device,
    product_category,
    toUInt64(count())            AS events,
    toUInt64(sum(quantity))      AS quantity,
    toUInt64(sum(revenue_cents)) AS revenue_cents,
    uniqState(user_id)           AS users,
    uniqState(session_id)        AS sessions,
    uniqState(order_id)          AS orders
FROM pulse.events
GROUP BY minute, event_type, country, device, product_category;
