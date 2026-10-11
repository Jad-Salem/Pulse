# ADR-0003: ClickHouse as the real-time serving store

- Status: accepted
- Date: 2026-10-10

## Context

Dashboards and the query API need sub-second aggregations over fresh data (seconds old) and over long ranges (months), with high concurrency, ad-hoc SQL for analysts, and simple operations.

## Decision

Use **ClickHouse 26.8 LTS**, fed directly from Kafka by Kafka table engines:
- **Kafka engine → materialized view → MergeTree.** No separate ingestion service. Parse failures are captured with `kafka_handle_error_mode='stream'` into `pulse.ingest_errors`.
- **ReplacingMergeTree** for event-level tables, keyed so that a replayed record collapses onto the original.
- **AggregatingMergeTree per-minute rollup**, maintained at insert time by a chained materialized view. Headline KPIs never scan raw events.
- **Least-privilege users:**
  - `query_api` and `grafana` are read-only and limited to `SELECT ON pulse.*`, with execution-time and memory caps plus a quota.
  - The admin user runs migrations only.
- **Versioned, forward-only migrations** with checksum drift detection (`tools/platform-init`).

## Alternatives considered

- **Apache Pinot.** Purpose-built for user-facing real-time analytics, with upserts. It has more moving parts (controller, broker, server, minion, ZooKeeper) and a smaller SQL surface. A strong choice at a very high QPS for external users.
- **Apache Druid.** Similar trade-offs to Pinot: very good at time-series rollups, but more operationally complex.
- **PostgreSQL / TimescaleDB.** Simpler, but row-oriented or hypertable storage does not reach the same scan speed and compression on wide analytical queries.
- **Writing to ClickHouse from Flink (JDBC sink).** This couples the processing job to the serving store's availability. Kafka in between decouples them and lets ClickHouse replay independently.

## Consequences

- ClickHouse ingestion is at-least-once on a crash (insert committed, offset not). ReplacingMergeTree plus `FINAL` on event-level queries absorbs this. Rollups can over-count by the size of one unacknowledged block in that window (ADR-0005).
- Schema changes to the Kafka-facing tables follow the queue/MV pattern: create the new MV, then drop the old one, in a migration.
