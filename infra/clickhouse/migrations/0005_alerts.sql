-- Real-time detections raised by the stream processor. alert_id is deterministic, so a replay
-- of the same input collapses onto the same row.

CREATE TABLE IF NOT EXISTS pulse.alerts
(
    alert_id        String,
    alert_type      LowCardinality(String),
    severity        LowCardinality(String),
    entity_id       String,
    description     String,
    observed_value  Float64,
    threshold       Float64,
    window_start    DateTime64(3, 'UTC'),
    window_end      DateTime64(3, 'UTC'),
    detected_at     DateTime64(3, 'UTC'),
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(window_end)
ORDER BY (alert_type, window_end, alert_id)
TTL toDate(window_end) + INTERVAL 180 DAY;

CREATE TABLE IF NOT EXISTS pulse.alerts_queue
(
    alert_id        String,
    alert_type      String,
    severity        String,
    entity_id       String,
    description     String,
    observed_value  Float64,
    threshold       Float64,
    window_start    DateTime64(3, 'UTC'),
    window_end      DateTime64(3, 'UTC'),
    detected_at     DateTime64(3, 'UTC')
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '${KAFKA_BROKERS}',
    kafka_topic_list = 'alerts',
    kafka_group_name = 'clickhouse-alerts',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = '${SCHEMA_REGISTRY_URL}',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS pulse.alerts_mv TO pulse.alerts AS
SELECT
    alert_id,
    alert_type,
    severity,
    entity_id,
    description,
    observed_value,
    threshold,
    window_start,
    window_end,
    detected_at
FROM pulse.alerts_queue
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS pulse.alerts_queue_errors_mv TO pulse.ingest_errors AS
SELECT
    'alerts_queue' AS source_table,
    _topic         AS topic,
    _partition     AS partition,
    _offset        AS offset,
    _error         AS error,
    _raw_message   AS raw_message
FROM pulse.alerts_queue
WHERE length(_error) > 0;
