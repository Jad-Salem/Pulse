# Runbook

Day-2 operations for the local platform. Each procedure lists the production equivalent where it differs.

## Health at a glance

- **Grafana → Pulse / Platform health**: ingest rate, consumer lag, Flink checkpoints and restarts, dead letters.
- **Prometheus → Alerts** (http://localhost:9090/alerts): rules live in `infra/prometheus/rules/pulse.yml`.
- `docker compose ps`: every long-running service should be `running (healthy)`; `platform-init` should be `exited (0)`.

## Inspect dead letters

```sql
-- http://localhost:8123/play  (user query_api / password query-api-dev)
SELECT failed_at, stage, reason, source_topic, source_partition, source_offset
FROM pulse.dead_letters
ORDER BY failed_at DESC
LIMIT 20;
```

`stage = 'decode'` means the bytes were not valid Confluent Avro (or the schema id is unknown). `stage = 'validate'` means the event decoded but broke the contract. The original payload is in `payload_base64`, byte-identical to what the producer sent.

**Replaying** after fixing the cause: decode `payload_base64` and produce the bytes back to `events.raw` with any Kafka client. The stream processor de-duplicates by `event_id`, so a replay can never double-count.

## Rows that ClickHouse could not parse

```sql
SELECT observed_at, source_table, topic, partition, offset, error
FROM pulse.ingest_errors ORDER BY observed_at DESC LIMIT 20;
```

These should always be empty, because every row on these topics was written by the stream processor using a registered schema. A non-empty result means a contract/table mismatch: fix it with a new migration.

## Upgrade the stream processor without losing state

```bash
make savepoint                                          # returns a request id
curl -s localhost:8082/jobs/00000000000000000000000000000000/savepoints/<request-id>
docker compose build jobmanager taskmanager
```

Then start the new version from the savepoint path reported above. Add `--fromSavepoint <path>` to the jobmanager `command` and run `docker compose up -d jobmanager taskmanager`.

Operator `uid`s are stable, so state maps onto the new graph. Removing an operator requires `--allowNonRestoredState`.

*Production:* the Flink Kubernetes Operator does this declaratively (`upgradeMode: savepoint`).

## Reprocess history (Kappa replay)

All derived data can be rebuilt from `events.raw` (7-day retention):

1. Stop the job: `docker compose stop jobmanager taskmanager`.
2. Reset the consumer group to the desired point:
   `docker compose exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --group stream-processor --topic events.raw --reset-offsets --to-datetime 2026-01-01T00:00:00.000 --execute`
3. Start it without restoring state: `docker compose up -d jobmanager taskmanager`.

Downstream, ReplacingMergeTree tables collapse the replayed rows. Rebuild the minute rollups for the affected days by truncating them and re-inserting from `pulse.events FINAL`.

## Change a data contract

1. Edit `schemas/avro/<schema>.avsc`, adding fields **with defaults** only.
2. `make test` checks that the file is valid Avro. `platform-init` refuses to register a schema that is not BACKWARD-compatible.
3. Add a ClickHouse migration if the field should be stored: a new column in storage, a recreated queue table, and a recreated MV.
4. Deploy consumers before producers.

## Chaos drills

| Drill | Command | Expected |
|---|---|---|
| Lose a broker | `docker compose stop kafka-2` | No errors at the collector (RF=3, min ISR=2); Kafka UI shows under-replicated partitions |
| Lose two brokers | also `docker compose stop kafka-3` | Collector returns 503 with `Retry-After`; `CollectorProduceFailures` fires; nothing acknowledged is lost |
| Kill a TaskManager | `docker compose kill taskmanager` (then `up -d`) | Job restarts from the last checkpoint; no duplicates in ClickHouse |
| Schema Registry down | `docker compose stop schema-registry` | Flink keeps going on cached schemas; nothing is dead-lettered for registry errors |
| Poison data | always on (the load generator sends some) | Rows in `pulse.dead_letters`; job never restarts because of them |

Restart anything you stopped with `docker compose up -d`.
