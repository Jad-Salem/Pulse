"""ClickHouse access over HTTP with the official clickhouse-connect async client."""

from collections.abc import Mapping
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient

from query_api.config import Settings


class ClickHouseRunner:
    def __init__(self, client: AsyncClient) -> None:
        self._client = client

    @classmethod
    async def connect(cls, settings: Settings) -> "ClickHouseRunner":
        client = await clickhouse_connect.get_async_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password.get_secret_value(),
            database=settings.clickhouse_database,
            secure=settings.clickhouse_secure,
            send_receive_timeout=settings.query_timeout_seconds,
            settings={"max_execution_time": settings.query_timeout_seconds},
        )
        return cls(client)

    async def fetch(self, sql: str, parameters: Mapping[str, Any]) -> list[dict[str, Any]]:
        result = await self._client.query(sql, parameters=dict(parameters))
        return list(result.named_results())

    async def ping(self) -> bool:
        return bool(await self._client.ping())

    async def close(self) -> None:
        await self._client.close()
