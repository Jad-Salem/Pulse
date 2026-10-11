"""Fixtures for end-to-end tests. Endpoints default to the docker compose port mappings."""

import os
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import clickhouse_connect
import httpx
import pytest
from clickhouse_connect.driver.client import Client

COLLECTOR_URL = os.environ.get("E2E_COLLECTOR_URL", "http://localhost:8000")
QUERY_API_URL = os.environ.get("E2E_QUERY_API_URL", "http://localhost:8001")
KAFKA = os.environ.get("E2E_KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
API_KEY = os.environ.get("E2E_API_KEY", "dev-write-key")
TIMEOUT = float(os.environ.get("E2E_TIMEOUT_SECONDS", "180"))


def eventually[T](
    probe: Callable[[], T | None], timeout: float = TIMEOUT, interval: float = 2.0
) -> T:
    """Poll until `probe` returns something truthy; the pipeline is asynchronous by design."""
    deadline = time.monotonic() + timeout
    last: T | None = None
    while time.monotonic() < deadline:
        last = probe()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"condition not met within {timeout:.0f}s (last value: {last!r})")


def select(client: Client, sql: str, **parameters: Any) -> list[tuple[Any, ...]]:
    return [tuple(row) for row in client.query(sql, parameters=parameters).result_rows]


@pytest.fixture(scope="session")
def collector() -> Iterator[httpx.Client]:
    with httpx.Client(base_url=COLLECTOR_URL, headers={"X-API-Key": API_KEY}, timeout=15) as client:
        yield client


@pytest.fixture(scope="session")
def query_api() -> Iterator[httpx.Client]:
    with httpx.Client(base_url=QUERY_API_URL, timeout=15) as client:
        yield client


@pytest.fixture(scope="session")
def clickhouse() -> Client:
    return clickhouse_connect.get_client(
        host=os.environ.get("E2E_CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("E2E_CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("E2E_CLICKHOUSE_USER", "query_api"),
        password=os.environ.get("E2E_CLICKHOUSE_PASSWORD", "query-api-dev"),
    )


class Shopper:
    """Builds contract-valid events for one fresh user/session, so tests never collide."""

    def __init__(self) -> None:
        self.user_id = f"e2e-u-{uuid4().hex[:12]}"
        self.session_id = f"e2e-s-{uuid4().hex[:12]}"
        self.start = datetime.now(UTC)

    def event(self, event_type: str, offset_seconds: float = 0, **fields: Any) -> dict[str, Any]:
        return {
            "event_id": str(uuid4()),
            "event_type": event_type,
            "event_time": (self.start + timedelta(seconds=offset_seconds)).isoformat(),
            "user_id": self.user_id,
            "session_id": self.session_id,
            "page_url": "https://shop.example.com/e2e",
            "country": "NL",
            "device": "desktop",
            **fields,
        }

    def purchase(
        self, offset_seconds: float, order_id: str, product_id: str = "P0001"
    ) -> dict[str, Any]:
        return self.event(
            "purchase",
            offset_seconds,
            product_id=product_id,
            quantity=2,
            unit_price_cents=2500,
            order_id=order_id,
        )


@pytest.fixture
def shopper() -> Shopper:
    return Shopper()
