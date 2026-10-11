from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Read from LOADGEN_* environment variables."""

    model_config = SettingsConfigDict(env_prefix="LOADGEN_", extra="ignore")

    collector_url: str = "http://localhost:8000"
    api_key: str = "dev-write-key"
    products_file: Path = Path("data/products.json")
    event_schema_file: Path = Path("schemas/avro/event.avsc")

    users: int = Field(default=60, ge=1, description="Concurrent simulated shoppers")
    fraud_users: int = Field(default=1, ge=0, description="Bots that place rapid-fire orders")
    think_time_seconds: float = Field(default=2.0, gt=0, description="Mean pause between actions")
    duration_seconds: float = Field(default=0, ge=0, description="0 = run forever")
    seed: int | None = None

    batch_size: int = Field(default=100, ge=1, le=500)
    flush_interval_seconds: float = Field(default=0.5, gt=0)

    # Controlled chaos, to exercise the pipeline's guarantees.
    duplicate_rate: float = Field(default=0.02, ge=0, le=1, description="Client retries")
    late_rate: float = Field(default=0.01, ge=0, le=1, description="Offline-buffered events")
    late_delay_seconds: tuple[float, float] = (20.0, 90.0)
    poison_per_minute: float = Field(default=2.0, ge=0, description="Bad records written to Kafka")

    kafka_bootstrap_servers: str | None = None  # poison injection is off without Kafka access
    schema_registry_url: str | None = None
