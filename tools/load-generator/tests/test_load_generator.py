import asyncio
import json
import random
from itertools import islice
from pathlib import Path
from typing import Any

import httpx
import pytest

from load_generator.behavior import Product, Visitor, fraud_session, shopping_session
from load_generator.sender import BatchSender

REPO = Path(__file__).resolve().parents[3]
CATALOG = [Product.from_json(p) for p in json.loads((REPO / "data" / "products.json").read_text())]

PRODUCT_EVENTS = {"product_view", "add_to_cart", "remove_from_cart", "purchase"}
LINE_ITEM_EVENTS = {"add_to_cart", "remove_from_cart", "purchase"}


def assert_contract(event: dict[str, Any]) -> None:
    """The same rules the collector enforces (schemas/README.md)."""
    if event["event_type"] in PRODUCT_EVENTS:
        assert event.get("product_id"), event
    if event["event_type"] in LINE_ITEM_EVENTS:
        assert 1 <= event["quantity"] <= 1000, event
        assert event["unit_price_cents"] >= 0, event
    if event["event_type"] == "purchase":
        assert event.get("order_id"), event
    assert len(event["country"]) == 2
    assert event["device"] in {"desktop", "mobile", "tablet"}


def test_shopping_sessions_follow_the_contract() -> None:
    rng = random.Random(1)
    types: set[str] = set()
    for _ in range(300):
        steps = list(shopping_session(Visitor.random(rng), CATALOG, rng, think_time=1.0))
        assert steps[0][1]["event_type"] == "page_view"
        assert len({fields["session_id"] for _, fields in steps}) == 1
        for pause, fields in steps:
            assert pause >= 0
            assert_contract(fields)
            types.add(fields["event_type"])
    assert types == {
        "page_view",
        "product_view",
        "add_to_cart",
        "remove_from_cart",
        "checkout_started",
        "purchase",
    }


def test_purchase_lines_share_an_order_id() -> None:
    rng = random.Random(2)
    orders: dict[str, set[str]] = {}
    for _ in range(500):
        for _, fields in shopping_session(Visitor.random(rng), CATALOG, rng, 1.0):
            if fields["event_type"] == "purchase":
                orders.setdefault(fields["session_id"], set()).add(fields["order_id"])
    assert orders
    assert all(len(ids) == 1 for ids in orders.values())


def test_fraud_bot_orders_fast_enough_to_trip_the_detector() -> None:
    rng = random.Random(3)
    elapsed, order_times = 0.0, []
    for pause, fields in islice(fraud_session(Visitor.random(rng), CATALOG, rng), 30):
        elapsed += pause
        assert_contract(fields)
        if fields["event_type"] == "purchase":
            order_times.append(elapsed)
    in_first_minute = [t for t in order_times if t <= 60]
    assert len(in_first_minute) >= 3  # the stream processor alerts at 3 orders / minute


@pytest.fixture
def collected() -> list[dict[str, Any]]:
    return []


def sender_for(collected: list[dict[str, Any]], **overrides: Any) -> BatchSender:
    def handler(request: httpx.Request) -> httpx.Response:
        collected.extend(json.loads(request.content)["events"])
        return httpx.Response(202, json={"accepted": 1})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://collector")
    options: dict[str, Any] = {
        "batch_size": 10,
        "duplicate_rate": 0.0,
        "late_rate": 0.0,
        "late_delay": (0.01, 0.02),
    }
    options.update(overrides)
    return BatchSender(client, random.Random(4), **options)


async def test_sender_batches(collected: list[dict[str, Any]]) -> None:
    sender = sender_for(collected)
    for i in range(25):
        sender.submit({"event_id": str(i)})
    await sender.flush()
    assert [e["event_id"] for e in collected] == [str(i) for i in range(25)]
    assert sender.stats.sent == 25


async def test_sender_duplicates_keep_the_event_id(collected: list[dict[str, Any]]) -> None:
    sender = sender_for(collected, duplicate_rate=1.0)
    sender.submit({"event_id": "a"})
    await sender.flush()
    assert [e["event_id"] for e in collected] == ["a", "a"]


async def test_sender_delays_late_events(collected: list[dict[str, Any]]) -> None:
    sender = sender_for(collected, late_rate=1.0)
    sender.submit({"event_id": "late"})
    await sender.flush()
    assert collected == []
    await asyncio.sleep(0.05)
    await sender.flush()
    assert [e["event_id"] for e in collected] == ["late"]


async def test_sender_retries_transient_failures() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503 if calls == 1 else 202)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://collector")
    sender = BatchSender(
        client, random.Random(5), batch_size=10, duplicate_rate=0, late_rate=0, late_delay=(1, 2)
    )
    sender.submit({"event_id": "x"})
    await sender.flush()
    assert calls == 2
    assert sender.stats.sent == 1
