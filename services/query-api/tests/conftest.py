from collections.abc import Iterator, Mapping
from typing import Any

import pytest
from fastapi.testclient import TestClient

from query_api.app import create_app
from query_api.config import Settings


class FakeRunner:
    """Returns canned rows for the first registered SQL fragment found in a query."""

    def __init__(self) -> None:
        self.responses: list[tuple[str, list[dict[str, Any]]]] = []
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.healthy = True
        self.closed = False

    def respond(self, fragment: str, rows: list[dict[str, Any]]) -> None:
        self.responses.insert(0, (fragment, rows))  # most recent registration wins

    async def fetch(self, sql: str, parameters: Mapping[str, Any]) -> list[dict[str, Any]]:
        self.calls.append((sql, dict(parameters)))
        for fragment, rows in self.responses:
            if fragment in sql:
                return rows
        raise AssertionError(f"unexpected query:\n{sql}")

    async def ping(self) -> bool:
        return self.healthy

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def runner() -> FakeRunner:
    return FakeRunner()


@pytest.fixture
def client(runner: FakeRunner) -> Iterator[TestClient]:
    async def factory(_: Settings) -> FakeRunner:
        return runner

    app = create_app(Settings(log_json=False), runner_factory=factory)
    with TestClient(app) as test_client:
        yield test_client
