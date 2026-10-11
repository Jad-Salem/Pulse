from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration, read from QUERY_API_* environment variables."""

    model_config = SettingsConfigDict(env_prefix="QUERY_API_", extra="ignore")

    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "query_api"
    clickhouse_password: SecretStr = SecretStr("")
    clickhouse_database: str = "pulse"
    clickhouse_secure: bool = False
    query_timeout_seconds: int = Field(default=10, gt=0)

    live_interval_seconds: float = Field(default=2.0, gt=0)
    live_window_seconds: int = Field(default=300, gt=0)

    log_level: str = "INFO"
    log_json: bool = True
