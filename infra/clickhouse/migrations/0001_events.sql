-- Storage for enriched events, plus the shared table that captures rows the Kafka engines could not parse.

CREATE TABLE IF NOT EXISTS pulse.events
(
    event_id          UUID,
    event_type        LowCardinality(String),
    event_time        DateTime64(3, 'UTC'),
    received_at       DateTime64(3, 'UTC'),
    processed_at      DateTime64(3, 'UTC'),
    ingested_at       DateTime64(3, 'UTC') DEFAULT now64(3),
    user_id           String,
    session_id        String,
    page_url          String,
    referrer          String,
    product_id        LowCardinality(String),
    product_name      String,
    product_category  LowCardinality(String),
    product_brand     LowCardinality(String),
    quantity          UInt32,
    unit_price_cents  UInt64,
    revenue_cents     UInt64,
    order_id          String,
    country           LowCardinality(String),
    device            LowCardinality(String),
    user_agent        String,
    INDEX idx_user_id user_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_session_id session_id TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toDate(event_time)
ORDER BY (event_type, event_time, event_id)
TTL toDate(event_time) + INTERVAL 90 DAY
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS pulse.ingest_errors
(
    observed_at   DateTime64(3, 'UTC') DEFAULT now64(3),
    source_table  LowCardinality(String),
    topic         LowCardinality(String),
    partition     UInt64,
    offset        UInt64,
    error         String,
    raw_message   String
)
ENGINE = MergeTree
PARTITION BY toDate(observed_at)
ORDER BY observed_at
TTL toDate(observed_at) + INTERVAL 30 DAY;
