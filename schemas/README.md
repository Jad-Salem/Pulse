# Data contracts

Every Kafka topic that carries structured data has an Avro schema in [`avro/`](avro). These files are the single source of truth:

| Topic | Schema | Key | Producer | Consumers |
|---|---|---|---|---|
| `events.raw` | [`event.avsc`](avro/event.avsc) | `user_id` | collector (and any backend service) | stream-processor |
| `products` (compacted) | [`product.avsc`](avro/product.avsc) | `product_id` | platform-init (catalog seed) | stream-processor |
| `events.enriched` | [`enriched_event.avsc`](avro/enriched_event.avsc) | `user_id` | stream-processor | ClickHouse |
| `sessions` | [`session_summary.avsc`](avro/session_summary.avsc) | `session_id` | stream-processor | ClickHouse |
| `alerts` | [`alert.avsc`](avro/alert.avsc) | `entity_id` | stream-processor | ClickHouse |
| `events.dlq` | none (original bytes + `dlq.*` headers) | original key | stream-processor | ClickHouse |

How each consumer uses them:

- **Java** (stream-processor): `avro-maven-plugin` generates `io.github.jad_salem.pulse.schemas.*` classes from these files at build time.
- **Python** (collector, platform-init, load-generator): the files are read at runtime and serialized with the Confluent `AvroSerializer`.
- **Schema Registry**: `platform-init` registers each schema under `<topic>-value` with `BACKWARD` compatibility *before* any producer starts. Producers run with `auto.register.schemas=false`, so a producer can never change a contract on its own. Changing a schema is a reviewed change to this directory.

## Evolution rules

`BACKWARD` compatibility means consumers on the new schema can read data written with the old one. In practice:

- Add a field only with a `default` (usually `["null", T]` with `default: null`).
- Never rename or remove a field without a default; never change a field's type.
- Categorical fields (`event_type`, `device`, `severity`) are strings, not Avro enums. Adding a symbol to an Avro enum breaks readers that don't have it, and these categories grow over time. The allowed values are enforced by validation instead (below).

## Validation contract (`events.raw`)

The collector enforces these rules at the edge (HTTP 422). The stream processor enforces them again for events that reach Kafka by other paths. Those events go to `events.dlq`.

1. `event_id` is a UUID.
2. `event_type` ∈ {`page_view`, `product_view`, `add_to_cart`, `remove_from_cart`, `checkout_started`, `purchase`}.
3. `user_id`, `session_id`, `page_url` are non-blank.
4. `country` matches `^[A-Z]{2}$`; `device` ∈ {`desktop`, `mobile`, `tablet`, `unknown`}.
5. `product_view`, `add_to_cart`, `remove_from_cart`, `purchase` carry a `product_id`.
6. `add_to_cart`, `remove_from_cart`, `purchase` carry `quantity` in 1..1000 and `unit_price_cents` ≥ 0.
7. `purchase` carries an `order_id`.
8. `event_time` is at most 5 minutes after `received_at`, which rejects client clocks that run fast.
