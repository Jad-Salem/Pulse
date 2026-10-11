-- Closed sessions computed by the stream processor (event-time session windows).

CREATE TABLE IF NOT EXISTS pulse.sessions
(
    session_id     String,
    user_id        String,
    session_start  DateTime64(3, 'UTC'),
    session_end    DateTime64(3, 'UTC'),
    ingested_at    DateTime64(3, 'UTC') DEFAULT now64(3),
    duration_ms    UInt64,
    event_count    UInt32,
    page_views     UInt32,
    product_views  UInt32,
    add_to_carts   UInt32,
    checkouts      UInt32,
    purchases      UInt32,
    revenue_cents  UInt64,
    converted      Bool,
    country        LowCardinality(String),
    device         LowCardinality(String),
    landing_page   String
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toDate(session_start)
ORDER BY (session_start, session_id)
TTL toDate(session_start) + INTERVAL 90 DAY
SETTINGS ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS pulse.sessions_queue
(
    session_id     String,
    user_id        String,
    session_start  DateTime64(3, 'UTC'),
    session_end    DateTime64(3, 'UTC'),
    duration_ms    Int64,
    event_count    Int32,
    page_views     Int32,
    product_views  Int32,
    add_to_carts   Int32,
    checkouts      Int32,
    purchases      Int32,
    revenue_cents  Int64,
    converted      Bool,
    country        String,
    device         String,
    landing_page   String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '${KAFKA_BROKERS}',
    kafka_topic_list = 'sessions',
    kafka_group_name = 'clickhouse-sessions',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = '${SCHEMA_REGISTRY_URL}',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS pulse.sessions_mv TO pulse.sessions AS
SELECT
    session_id,
    user_id,
    session_start,
    session_end,
    toUInt64(greatest(duration_ms, 0))   AS duration_ms,
    toUInt32(greatest(event_count, 0))   AS event_count,
    toUInt32(greatest(page_views, 0))    AS page_views,
    toUInt32(greatest(product_views, 0)) AS product_views,
    toUInt32(greatest(add_to_carts, 0))  AS add_to_carts,
    toUInt32(greatest(checkouts, 0))     AS checkouts,
    toUInt32(greatest(purchases, 0))     AS purchases,
    toUInt64(greatest(revenue_cents, 0)) AS revenue_cents,
    converted,
    country,
    device,
    landing_page
FROM pulse.sessions_queue
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS pulse.sessions_queue_errors_mv TO pulse.ingest_errors AS
SELECT
    'sessions_queue' AS source_table,
    _topic           AS topic,
    _partition       AS partition,
    _offset          AS offset,
    _error           AS error,
    _raw_message     AS raw_message
FROM pulse.sessions_queue
WHERE length(_error) > 0;
