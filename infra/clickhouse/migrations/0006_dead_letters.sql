-- Dead-letter queue made queryable: the original payload is kept as base64 and the failure
-- context comes from the dlq.* Kafka headers written by the stream processor.

CREATE TABLE IF NOT EXISTS pulse.dead_letters
(
    failed_at         DateTime64(3, 'UTC'),
    stage             LowCardinality(String),
    reason            String,
    source_topic      LowCardinality(String),
    source_partition  Int32,
    source_offset     Int64,
    payload_size      UInt32,
    payload_base64    String
)
ENGINE = MergeTree
PARTITION BY toDate(failed_at)
ORDER BY (failed_at, stage)
TTL toDate(failed_at) + INTERVAL 14 DAY;

CREATE TABLE IF NOT EXISTS pulse.dead_letters_queue
(
    payload String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '${KAFKA_BROKERS}',
    kafka_topic_list = 'events.dlq',
    kafka_group_name = 'clickhouse-dlq',
    kafka_format = 'RawBLOB',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS pulse.dead_letters_mv TO pulse.dead_letters AS
SELECT
    ifNull(_timestamp_ms, now64(3))                                            AS failed_at,
    _headers.value[indexOf(_headers.name, 'dlq.stage')]                        AS stage,
    _headers.value[indexOf(_headers.name, 'dlq.reason')]                       AS reason,
    _headers.value[indexOf(_headers.name, 'dlq.source.topic')]                 AS source_topic,
    toInt32OrZero(_headers.value[indexOf(_headers.name, 'dlq.source.partition')]) AS source_partition,
    toInt64OrZero(_headers.value[indexOf(_headers.name, 'dlq.source.offset')])    AS source_offset,
    toUInt32(length(payload))                                                  AS payload_size,
    base64Encode(payload)                                                      AS payload_base64
FROM pulse.dead_letters_queue;
