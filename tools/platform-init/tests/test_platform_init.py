import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import fastavro
import pytest
from fastavro.validation import validate

from platform_init import migrations, seed, topics

REPO = Path(__file__).resolve().parents[3]
SCHEMAS = REPO / "schemas" / "avro"
VARIABLES = {"KAFKA_BROKERS": "kafka-1:9092,kafka-2:9092", "SCHEMA_REGISTRY_URL": "http://sr:8081"}


def test_every_schema_file_is_valid_avro() -> None:
    files = sorted(SCHEMAS.glob("*.avsc"))
    assert files
    for path in files:
        fastavro.parse_schema(json.loads(path.read_text()))


def test_topic_specs_apply_defaults_and_reference_existing_schemas() -> None:
    specs = topics.load_topic_specs(REPO / "infra" / "kafka" / "topics.yaml")
    by_name = {s.name: s for s in specs}

    assert by_name["events.raw"].replication_factor == 3
    assert by_name["events.raw"].config["min.insync.replicas"] == "2"
    assert by_name["products"].config["cleanup.policy"] == "compact"
    assert by_name["events.dlq"].value_schema is None
    for spec in specs:
        if spec.value_schema:
            assert (SCHEMAS / spec.value_schema).is_file(), spec.value_schema


def test_catalog_matches_product_schema() -> None:
    schema = fastavro.parse_schema(json.loads((SCHEMAS / "product.avsc").read_text()))
    products = seed.load_products(REPO / "data" / "products.json")

    assert len(products) > 10
    for product in products:
        assert validate(seed.to_record(product, datetime.now(UTC)), schema)


def test_migrations_are_discovered_in_order_and_render() -> None:
    found = migrations.discover(REPO / "infra" / "clickhouse" / "migrations")

    assert [m.version for m in found] == sorted(m.version for m in found)
    for migration in found:
        statements = migration.statements(VARIABLES)
        assert statements, migration.name
        for statement in statements:
            assert statement.lstrip().upper().startswith("CREATE"), statement[:80]
            assert "${" not in statement
            assert not statement.rstrip().endswith(";")


def test_kafka_engine_tables_get_rendered_endpoints() -> None:
    found = migrations.discover(REPO / "infra" / "clickhouse" / "migrations")
    sql = "\n".join(s for m in found for s in m.statements(VARIABLES))
    assert "kafka_broker_list = 'kafka-1:9092,kafka-2:9092'" in sql
    assert "format_avro_schema_registry_url = 'http://sr:8081'" in sql


def test_split_statements() -> None:
    sql = """
    -- leading comment;
    CREATE TABLE a (x UInt8) ENGINE = Memory;

    -- another comment
    CREATE TABLE b
    (
        -- inline comment;
        y UInt8
    ) ENGINE = Memory;
    """
    assert [s.split()[2] for s in migrations.split_statements(sql)] == ["a", "b"]


def test_unsafe_variables_are_rejected() -> None:
    migration = migrations.Migration(1, "x", "SELECT '${KAFKA_BROKERS}'")
    with pytest.raises(ValueError, match="unsafe"):
        migration.statements({"KAFKA_BROKERS": "x', 'y"})


class FakeClickHouse:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.rows: list[list[Any]] = []

    def command(self, cmd: str) -> None:
        self.commands.append(cmd)

    def query(self, query: str) -> Any:
        rows = [(version, checksum) for version, _, checksum in self.rows]
        return type("Result", (), {"result_rows": rows})()

    def insert(self, table: str, data: list[list[Any]], column_names: list[str]) -> None:
        self.rows.extend(data)


def test_apply_is_idempotent_and_detects_drift() -> None:
    first = migrations.Migration(1, "one", "CREATE TABLE t1 (x UInt8) ENGINE = Memory;")
    second = migrations.Migration(2, "two", "CREATE TABLE t2 (x UInt8) ENGINE = Memory;")
    client = FakeClickHouse()

    assert migrations.apply(client, "pulse", [first, second], {}) == [1, 2]
    assert migrations.apply(client, "pulse", [first, second], {}) == []

    edited = migrations.Migration(1, "one", "CREATE TABLE t1 (x UInt16) ENGINE = Memory;")
    with pytest.raises(migrations.MigrationDriftError):
        migrations.apply(client, "pulse", [edited, second], {})


def test_apply_rejects_bad_database_name() -> None:
    with pytest.raises(ValueError, match="invalid database"):
        migrations.apply(FakeClickHouse(), "pulse; DROP", [], {})


class FakeProducer:
    def __init__(self) -> None:
        self.records: list[tuple[str, bytes, bytes]] = []

    def produce(self, topic: str, value: bytes, key: bytes) -> None:
        self.records.append((topic, key, value))

    def flush(self, timeout: float) -> int:
        return 0


def test_seed_keys_records_by_product_id() -> None:
    producer = FakeProducer()
    products = [
        {"product_id": "P1", "name": "n", "category": "c", "brand": "b", "list_price_cents": 5}
    ]

    count = seed.seed_products(producer, lambda r: r["name"].encode(), "products", products)

    assert count == 1
    assert producer.records == [("products", b"P1", b"n")]
