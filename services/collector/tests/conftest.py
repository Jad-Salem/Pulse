from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from collector.app import create_app
from collector.config import Settings
from collector.producer import ProducerUnavailableError

API_KEY = "test-key"


class FakeSink:
    """In-memory EventSink: records what would have been produced."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, Any]]] = []
        self.fail_with: Exception | None = None
        self.ready = True
        self.started = False
        self.closed = False

    def start(self) -> None:
        self.started = True

    async def send(self, key: str, value: dict[str, Any]) -> None:
        if self.fail_with is not None:
            raise self.fail_with
        self.sent.append((key, value))

    def is_ready(self, timeout: float) -> bool:
        return self.ready

    def close(self, timeout: float) -> int:
        self.closed = True
        return 0


@pytest.fixture
def sink() -> FakeSink:
    return FakeSink()


@pytest.fixture
def client(sink: FakeSink) -> Iterator[TestClient]:
    settings = Settings(api_keys=API_KEY, log_json=False)
    with TestClient(create_app(settings, sink), headers={"X-API-Key": API_KEY}) as test_client:
        yield test_client


def make_event(**overrides: Any) -> dict[str, Any]:
    event: dict[str, Any] = {
        "event_id": str(uuid4()),
        "event_type": "page_view",
        "event_time": datetime.now(UTC).isoformat(),
        "user_id": "u-1",
        "session_id": "s-1",
        "page_url": "https://shop.example.com/",
        "country": "US",
        "device": "desktop",
    }
    event.update(overrides)
    return event


def purchase(**overrides: Any) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "event_type": "purchase",
        "product_id": "P0001",
        "quantity": 2,
        "unit_price_cents": 1999,
        "order_id": "o-1",
    }
    return make_event(**(fields | overrides))


__all__ = ["API_KEY", "FakeSink", "ProducerUnavailableError", "make_event", "purchase"]
