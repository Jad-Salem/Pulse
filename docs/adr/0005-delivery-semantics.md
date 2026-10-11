# ADR-0005: Delivery semantics end to end

- Status: accepted
- Date: 2026-10-10

## Context

"Exactly-once" is a property of a whole pipeline, not of a component. Each hop has to be designed for it, and the gaps have to be stated honestly.

## Decision

| Hop | Guarantee | Mechanism |
|---|---|---|
| Client → collector | At-least-once | Clients retry with the same `event_id`; the collector returns 202 only after Kafka acks |
| Collector → `events.raw` | At-least-once, no producer-side duplicates | Idempotent producer, `acks=all`, RF=3, `min.insync.replicas=2` |
| `events.raw` → Flink → output topics | **Exactly-once** | Checkpointed source offsets + transactional sinks (2PC); `event_id` de-duplication removes client retries |
| Output topics → ClickHouse | At-least-once (effectively once) | Kafka engine with `read_committed`; ReplacingMergeTree keyed on the record identity; `FINAL` on event-level queries |

## Consequences

- **Latency floor.** Transactional output becomes visible at checkpoint commit. With a 10 s interval, expect p95 collector→ClickHouse latency of about one to one-and-a-half intervals. The business dashboard measures it live as "Pipeline latency (p95)". Lowering the interval trades throughput for freshness.
- **De-duplication horizon.** The `event_id` TTL is 1 hour. A retry that arrives more than an hour after the original is counted twice. Clients retry within seconds, so this is acceptable and documented.
- **Rollups.** `metrics_minute` is fed by a materialized view and cannot be de-duplicated after the fact. In the rare case where ClickHouse crashes between inserting a block and committing its offset, the rollup over-counts that block. Exact figures are always available from the event-level tables with `FINAL`.
- **Transaction timeout.** Sinks use `transaction.timeout.ms=900000`, the broker's default maximum. A job that is down for longer than that loses its in-flight (uncommitted) transaction, and it is replayed from the last checkpoint.
