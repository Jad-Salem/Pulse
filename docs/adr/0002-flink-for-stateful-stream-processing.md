# ADR-0002: Apache Flink for stateful stream processing

- Status: accepted
- Date: 2026-10-10

## Context

Processing needs per-key state (de-duplication over an hour of event IDs, open sessions, recent orders per user), event-time semantics (late and out-of-order mobile events), timers, a join against changing reference data, and exactly-once output.

## Decision

Use **Apache Flink 2.2** with the DataStream API in Java 17:
- RocksDB state backend with incremental checkpoints.
- Kafka source and transactional Kafka sinks (connector 5.0).
- Confluent-registry Avro serialization.
- Deployment in application mode.

Flink 2.2.1 is the newest release that has a matching Kafka connector (`5.0.0-2.2`).

The topology is built independently of its sources and sinks (`PulseTopology`), so the same graph runs in two places: against Kafka in production, and against in-memory streams on an embedded MiniCluster in tests. Every stateful operator has a stable `uid`, so savepoints survive code changes.

## Alternatives considered

- **Spark Structured Streaming.** Excellent for streaming ETL and for sharing code with batch jobs. This workload is dominated by fine-grained keyed state, per-key timers and event-time sessions, where Flink's DataStream API is the more direct fit.
- **Kafka Streams.** A good library for Kafka-to-Kafka services (with `TopologyTestDriver` for tests). Flink was preferred for savepoint-based upgrades of stateful jobs, its web UI and metrics for operating a running job, and its wider connector ecosystem.
- **Flink SQL only.** Excellent for the enrichment and windowing parts. The velocity detector (custom timers, cooldown) and the dead-letter routing are clearer and easier to test as DataStream functions. A mixed approach remains possible.
- **ksqlDB.** SQL-only, and licensed under the Confluent Community License rather than an OSI open-source licence.

## Consequences

- A JVM component and a second language (Java) in the stack. This is a deliberate choice that matches industry practice.
- Exactly-once makes results visible only when a checkpoint commits. The checkpoint interval (10 s here, configurable) becomes the floor of end-to-end latency.
- Without Kubernetes HA, a JobManager restart in the compose setup resumes from Kafka offsets committed at the last checkpoint. Keyed state is lost; in production, Flink Kubernetes HA restores it.
