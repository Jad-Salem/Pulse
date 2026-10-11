-- Ingestion of events.enriched: Kafka engine (queue) -> materialized views -> storage.
-- Created after the rollups (0002) so the very first ingested row already feeds them.
-- ${KAFKA_BROKERS} and ${SCHEMA_REGISTRY_URL} are rendered by platform-init.

-- Column names match the Avro field names; types follow the AvroConfluent mapping.
CREATE TABLE IF NOT EXISTS pulse.events_queue
(
    event_id          String,
    event_type        String,
    event_time        DateTime64(3, 'UTC'),
    received_at       DateTime64(3, 'UTC'),
    processed_at      DateTime64(3, 'UTC'),
    user_id           String,
    session_id        String,
    page_url          String,
    referrer          Nullable(String),
    product_id        Nullable(String),
    product_name      Nullable(String),
    product_category  Nullable(String),
    product_brand     Nullable(String),
    quantity          Nullable(Int32),
    unit_price_cents  Nullable(Int64),
    revenue_cents     Int64,
    order_id          Nullable(String),
    country           String,
    device            String,
    user_agent        Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '${KAFKA_BROKERS}',
    kafka_topic_list = 'events.enriched',
    kafka_group_name = 'clickhouse-events',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = '${SCHEMA_REGISTRY_URL}',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS pulse.events_mv TO pulse.events AS
SELECT
    toUUIDOrZero(event_id)                         AS event_id,
    event_type,
    event_time,
    received_at,
    processed_at,
    user_id,
    session_id,
    page_url,
    ifNull(referrer, '')                           AS referrer,
    ifNull(product_id, '')                         AS product_id,
    ifNull(product_name, '')                       AS product_name,
    ifNull(product_category, '')                   AS product_category,
    ifNull(product_brand, '')                      AS product_brand,
    toUInt32(greatest(ifNull(quantity, 0), 0))     AS quantity,
    toUInt64(greatest(ifNull(unit_price_cents, 0), 0)) AS unit_price_cents,
    toUInt64(greatest(revenue_cents, 0))           AS revenue_cents,
    ifNull(order_id, '')                           AS order_id,
    country,
    device,
    ifNull(user_agent, '')                         AS user_agent
FROM pulse.events_queue
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS pulse.events_queue_errors_mv TO pulse.ingest_errors AS
SELECT
    'events_queue' AS source_table,
    _topic         AS topic,
    _partition     AS partition,
    _offset        AS offset,
    _error         AS error,
    _raw_message   AS raw_message
FROM pulse.events_queue
WHERE length(_error) > 0;
