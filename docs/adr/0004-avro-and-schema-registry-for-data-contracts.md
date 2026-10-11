# ADR-0004: Avro + Schema Registry as data contracts

- Status: accepted
- Date: 2026-10-10

## Context

Four codebases (Python services, a Java Flink job, ClickHouse SQL, tests) read and write the same topics. Without enforced contracts, a producer change silently breaks consumers.

## Decision

- Every structured topic has an **Avro schema in `schemas/avro/`**: the single source of truth.
  - Java classes are generated from it at build time.
  - Python reads it at runtime.
  - ClickHouse decodes it with the `AvroConfluent` format.
- Schemas are registered in **Confluent Schema Registry** under `<topic>-value` with **BACKWARD** compatibility, by `platform-init`, before producers start. Producers run with `auto.register.schemas=false`, so a code change cannot alter a contract without a reviewed change to `schemas/`.
- Categorical fields are strings with documented allowed values, enforced by validation, not Avro enums. Adding an enum symbol breaks old readers; these categories grow.
- Money is an integer number of cents and timestamps are `timestamp-millis`, so there are no floats and no time-zone ambiguity.
- Semantic rules that a schema cannot express (for example, "a purchase needs an order_id") are documented once and enforced at both the edge and the stream processor.

## Alternatives considered

- **JSON without schemas.** Larger on the wire, no evolution rules, and consumers break at runtime.
- **Protobuf.** Equally valid, and better for RPC. Avro has first-class support in all three engines used here, including ClickHouse's `AvroConfluent` and Flink's registry format.
- **Apicurio Registry** (Apache 2.0). API-compatible with the Confluent registry for these use cases. Confluent's registry is the de facto standard; its Community License is free for this use.

## Consequences

- One more service (the registry) on the critical path. Clients cache schemas, so a registry outage affects only schema lookups that are not yet cached. The stream processor treats registry errors as transient, never as poison data.
- Breaking changes need a new topic or a coordinated migration, which is the point.
