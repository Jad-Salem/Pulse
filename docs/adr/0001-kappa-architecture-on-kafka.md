# ADR-0001: Kappa architecture on Apache Kafka

- Status: accepted
- Date: 2026-10-10

## Context

The platform must turn a continuous stream of user events into queryable metrics within seconds, and must be able to recompute everything after a bug fix or a logic change.

## Decision

Use a **Kappa architecture**: a single durable, replayable event log is the system of record, and every derived dataset (enriched events, sessions, alerts, rollups) is computed by streaming jobs from that log. The log is **Apache Kafka 4.x in KRaft mode** (no ZooKeeper), run as a three-broker cluster with replication factor 3 and `min.insync.replicas=2`.

Raw events are retained for 7 days, which is the replay window. Reprocessing means deploying a new job version that reads from an earlier offset, or a new consumer group.

## Alternatives considered

- **Lambda architecture** (separate batch and speed layers). Two codebases compute the same metrics and inevitably disagree. The batch layer earns its keep only for multi-year historical recomputation; here that would come from tiering older data to object storage, which is on the roadmap but not built.
- **Apache Pulsar.** A capable log with built-in tiered storage, but its ecosystem (connectors, Flink and ClickHouse integrations, operational knowledge) is far smaller.
- **Redpanda.** Kafka-API compatible and simpler to operate. Choosing Apache Kafka keeps the event log under the Apache 2.0 licence and matches what most organizations run.
- **Cloud queues (Kinesis, Pub/Sub).** Vendor lock-in, and they cannot run locally.

## Consequences

- Any downstream store can be rebuilt from the log; ClickHouse is a cache of truth, not truth.
- Kafka adds operational weight (three brokers locally). KRaft removes ZooKeeper, so the cluster is a single kind of process.
- The 7-day retention bounds how far back a replay can go without archived data.
