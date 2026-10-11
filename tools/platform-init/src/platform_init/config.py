from pathlib import Path

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Read from INIT_* environment variables. Paths are relative to the working directory."""

    model_config = SettingsConfigDict(env_prefix="INIT_", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:19092"
    schema_registry_url: str = "http://localhost:8081"

    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "pulse_admin"
    clickhouse_password: SecretStr = SecretStr("")
    clickhouse_database: str = "pulse"
    # How ClickHouse itself reaches Kafka / the registry (rendered into Kafka engine DDL).
    # Defaults to the values above, which is right whenever both run on the same network.
    clickhouse_kafka_brokers: str | None = None
    clickhouse_schema_registry_url: str | None = None

    topics_file: Path = Path("infra/kafka/topics.yaml")
    schemas_dir: Path = Path("schemas/avro")
    migrations_dir: Path = Path("infra/clickhouse/migrations")
    products_file: Path = Path("data/products.json")
    products_topic: str = "products"

    wait_timeout_seconds: float = 180.0

    def migration_variables(self) -> dict[str, str]:
        return {
            "KAFKA_BROKERS": self.clickhouse_kafka_brokers or self.kafka_bootstrap_servers,
            "SCHEMA_REGISTRY_URL": self.clickhouse_schema_registry_url or self.schema_registry_url,
        }
