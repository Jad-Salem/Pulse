from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration, read from COLLECTOR_* environment variables."""

    model_config = SettingsConfigDict(env_prefix="COLLECTOR_", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:19092"
    schema_registry_url: str = "http://localhost:8081"
    topic: str = "events.raw"
    schema_path: Path = Path("schemas/avro/event.avsc")

    # Comma-separated write keys. Required: there is deliberately no default.
    api_keys: SecretStr

    max_clock_skew_seconds: int = Field(default=300, ge=0)
    max_event_age_seconds: int = Field(default=7 * 24 * 3600, gt=0)
    delivery_timeout_ms: int = Field(default=30_000, gt=0)

    log_level: str = "INFO"
    log_json: bool = True

    @property
    def api_key_set(self) -> frozenset[str]:
        return frozenset(
            k.strip() for k in self.api_keys.get_secret_value().split(",") if k.strip()
        )

    @property
    def value_subject(self) -> str:
        return f"{self.topic}-value"
