from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from query_api.app import overview_stream
from query_api.durations import parse_duration
from query_api.repository import AnalyticsRepository

from .conftest import FakeRunner

TOTALS = {"events": 1000, "active_users": 80, "sessions": 100, "revenue_cents": 50_000}
PURCHASES = {"orders": 4, "converted_sessions": 5}


def overview_rows(runner: FakeRunner) -> None:
    runner.respond("sum(events) AS events", [TOTALS])
    runner.respond("converted_sessions", [PURCHASES])


def test_overview_derives_rates(client: TestClient, runner: FakeRunner) -> None:
    overview_rows(runner)

    body = client.get("/v1/overview", params={"window": "1h"}).json()

    assert body["window_seconds"] == 3600
    assert body["orders"] == 4
    assert body["conversion_rate"] == pytest.approx(0.05)
    assert body["average_order_value_cents"] == 12_500


def test_overview_handles_empty_window(client: TestClient, runner: FakeRunner) -> None:
    runner.respond("sum(events) AS events", [dict.fromkeys(TOTALS, 0)])
    runner.respond("converted_sessions", [dict.fromkeys(PURCHASES, 0)])

    body = client.get("/v1/overview").json()

    assert body["conversion_rate"] == 0.0
    assert body["average_order_value_cents"] == 0


def test_values_are_bound_as_parameters_not_interpolated(
    client: TestClient, runner: FakeRunner
) -> None:
    overview_rows(runner)

    client.get("/v1/overview", params={"window": "15m"})

    for sql, params in runner.calls:
        assert params == {"window": 900}
        assert "900" not in sql
        assert "{window:UInt32}" in sql


@pytest.mark.parametrize("window", ["15", "1w", "-5m", "5m; DROP TABLE x", "30s", "31d"])
def test_invalid_windows_are_rejected(client: TestClient, window: str) -> None:
    assert client.get("/v1/overview", params={"window": window}).status_code == 422


def test_timeseries(client: TestClient, runner: FakeRunner) -> None:
    bucket = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    runner.respond("AS bucket", [{"bucket": bucket, "value": 12.0}])

    body = client.get(
        "/v1/timeseries", params={"metric": "orders", "window": "1h", "step": "5m"}
    ).json()

    assert body["points"] == [{"bucket": "2026-01-01T12:00:00Z", "value": 12.0}]
    sql, params = runner.calls[0]
    assert "uniqMerge(orders)" in sql
    assert "event_type = 'purchase'" in sql
    assert params == {"window": 3600, "step": 300}


@pytest.mark.parametrize(
    "params",
    [
        {"metric": "password"},  # not in the allow-list
        {"step": "30s"},  # finer than the rollup
        {"step": "90s"},  # not whole minutes
        {"window": "30d", "step": "1m"},  # too many points
    ],
)
def test_timeseries_rejects_bad_input(client: TestClient, params: dict[str, str]) -> None:
    assert client.get("/v1/timeseries", params=params).status_code == 422


def test_top_products_ranking_selects_order_clause(client: TestClient, runner: FakeRunner) -> None:
    row = {
        "product_id": "P0001",
        "product_name": "Yoga Mat",
        "category": "sports",
        "units": 7,
        "orders": 5,
        "revenue_cents": 13_993,
    }
    runner.respond("FROM pulse.events FINAL", [row])

    body = client.get("/v1/top-products", params={"by": "units", "limit": 3}).json()

    assert body == [row]
    sql, params = runner.calls[0]
    assert "ORDER BY units DESC" in sql
    assert params["limit"] == 3


def test_funnel_conversion_math(client: TestClient, runner: FakeRunner) -> None:
    runner.respond("windowFunnel", [{"step_1": 200, "step_2": 50, "step_3": 20, "step_4": 10}])

    steps = client.get("/v1/funnel").json()["steps"]

    assert [s["step"] for s in steps] == [
        "product_view",
        "add_to_cart",
        "checkout_started",
        "purchase",
    ]
    assert steps[0]["conversion_from_previous"] == 1.0
    assert steps[1]["conversion_from_previous"] == pytest.approx(0.25)
    assert steps[3]["conversion_from_previous"] == pytest.approx(0.5)
    assert steps[3]["conversion_from_start"] == pytest.approx(0.05)


def test_breakdown_maps_dimension_to_column(client: TestClient, runner: FakeRunner) -> None:
    runner.respond("GROUP BY key", [{"key": "books", "events": 3, "users": 2, "revenue_cents": 9}])

    body = client.get("/v1/breakdown", params={"dimension": "category"}).json()

    assert body["rows"][0]["key"] == "books"
    assert "product_category" in runner.calls[0][0]
    assert client.get("/v1/breakdown", params={"dimension": "user_id"}).status_code == 422


def test_session_stats(client: TestClient, runner: FakeRunner) -> None:
    runner.respond(
        "FROM pulse.sessions FINAL",
        [
            {
                "sessions": 10,
                "average_duration_seconds": 42.5,
                "average_events": 6.1,
                "bounces": 3,
                "converted": 1,
            }
        ],
    )

    body = client.get("/v1/sessions/stats").json()

    assert body["bounce_rate"] == pytest.approx(0.3)
    assert body["conversion_rate"] == pytest.approx(0.1)


def test_readiness(client: TestClient, runner: FakeRunner) -> None:
    assert client.get("/readyz").status_code == 200
    runner.healthy = False
    assert client.get("/readyz").status_code == 503


def test_metrics_endpoint(client: TestClient, runner: FakeRunner) -> None:
    overview_rows(runner)
    client.get("/v1/overview")
    assert 'route="/v1/overview"' in client.get("/metrics").text


async def test_live_stream_emits_overview_events(runner: FakeRunner) -> None:
    overview_rows(runner)
    polls = iter([False, True])

    async def disconnected() -> bool:
        return next(polls)

    chunks = [c async for c in overview_stream(AnalyticsRepository(runner), 300, 0, disconnected)]

    assert len(chunks) == 1
    assert chunks[0].startswith("event: overview\ndata: {")
    assert chunks[0].endswith("\n\n")


def test_parse_duration() -> None:
    assert parse_duration("90s") == 90
    assert parse_duration("15m") == 900
    assert parse_duration("6h") == 21_600
    assert parse_duration("7d") == 604_800
    with pytest.raises(ValueError, match="invalid duration"):
        parse_duration("7w")
