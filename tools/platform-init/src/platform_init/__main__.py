"""Bootstrap the platform in dependency order. Safe to run any number of times.

1. Kafka topics       (infra/kafka/topics.yaml)
2. Schema contracts   (schemas/avro, registered under <topic>-value)
3. ClickHouse schema  (infra/clickhouse/migrations)
4. Product catalog    (data/products.json -> compacted topic)
"""

import logging
import sys
import time
from collections.abc import Callable

from platform_init import migrations, schemas, seed, topics
from platform_init.config import Settings

log = logging.getLogger("platform_init")


def wait_for[T](description: str, attempt: Callable[[], T], timeout: float) -> T:
    """Retry `attempt` with capped exponential backoff until it succeeds or time runs out."""
    deadline = time.monotonic() + timeout
    delay = 1.0
    while True:
        try:
            return attempt()
        except Exception as exc:
            if time.monotonic() + delay > deadline:
                raise TimeoutError(f"gave up waiting for {description}") from exc
            log.info("waiting for %s (%s)", description, exc.__class__.__name__)
            time.sleep(delay)
            delay = min(delay * 2, 10.0)


def run(settings: Settings) -> None:
    import clickhouse_connect
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    timeout = settings.wait_timeout_seconds
    specs = topics.load_topic_specs(settings.topics_file)

    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    wait_for("Kafka", lambda: admin.list_topics(timeout=5), timeout)
    created = topics.ensure_topics(admin, specs)
    log.info("topics ready (%d created, %d total)", len(created), len(specs))

    registry = SchemaRegistryClient({"url": settings.schema_registry_url})
    wait_for("Schema Registry", registry.get_subjects, timeout)
    registered = schemas.register_schemas(registry, specs, settings.schemas_dir)
    log.info("schemas ready (%d subjects)", len(registered))

    client = wait_for(
        "ClickHouse",
        lambda: clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password.get_secret_value(),
        ),
        timeout,
    )
    applied = migrations.apply(
        client,
        settings.clickhouse_database,
        migrations.discover(settings.migrations_dir),
        settings.migration_variables(),
    )
    log.info("clickhouse ready (%d migrations applied this run)", len(applied))

    products_spec = next(s for s in specs if s.name == settings.products_topic)
    assert products_spec.value_schema is not None
    serializer = AvroSerializer(
        registry,
        (settings.schemas_dir / products_spec.value_schema).read_text(encoding="utf-8"),
        conf={"auto.register.schemas": False},
    )
    context = SerializationContext(settings.products_topic, MessageField.VALUE)
    producer = Producer(
        {"bootstrap.servers": settings.kafka_bootstrap_servers, "enable.idempotence": True}
    )
    count = seed.seed_products(
        producer,
        lambda record: serializer(record, context),
        settings.products_topic,
        seed.load_products(settings.products_file),
    )
    log.info("catalog seeded (%d products)", count)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    try:
        run(Settings())
    except Exception:
        log.exception("platform init failed")
        sys.exit(1)
    log.info("platform init complete")


if __name__ == "__main__":
    main()
