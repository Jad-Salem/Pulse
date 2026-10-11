"""The platform's guarantees, checked end to end through real Kafka, Flink and ClickHouse.

Run against a stack started with `docker compose up -d` (see scripts/wait-for-stack.sh).
"""

import base64
import json
import os
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
from clickhouse_connect.driver.client import Client
from confluent_kafka import Producer
from conftest import KAFKA, Shopper, eventually, select

CATALOG = {
    p["product_id"]: p
    for p in json.loads((Path(__file__).parents[2] / "data" / "products.json").read_text())
}


def test_collector_rejects_unauthenticated_and_invalid_events(
    collector: httpx.Client, shopper: Shopper
) -> None:
    assert (
        collector.post("/v1/events", json=shopper.event("page_view"), headers={"X-API-Key": "nope"})
    ).status_code == 401
    assert collector.post("/v1/events", json=shopper.event("purchase")).status_code == 422


def test_events_are_deduplicated_enriched_and_queryable(
    collector: httpx.Client, clickhouse: Client, shopper: Shopper
) -> None:
    purchase = shopper.purchase(4, order_id=f"e2e-o-{uuid4().hex[:8]}")
    batch = [
        shopper.event("page_view"),
        shopper.event("product_view", 1, product_id="P0001"),
        shopper.event("add_to_cart", 2, product_id="P0001", quantity=2, unit_price_cents=2500),
        shopper.event("checkout_started", 3),
        purchase,
    ]
    assert collector.post("/v1/batch", json={"events": batch}).status_code == 202
    # The client never saw the response and retries the purchase with the same event_id.
    assert collector.post("/v1/events", json=purchase).status_code == 202

    def rows() -> list[tuple[Any, ...]] | None:
        result = select(
            clickhouse,
            "SELECT event_type, product_category, revenue_cents FROM pulse.events "
            "WHERE session_id = {sid:String}",
            sid=shopper.session_id,
        )
        return result if len(result) >= len(batch) else None

    # No FINAL: the duplicate must have been removed by the stream processor, not by a merge.
    eventually(rows)
    time.sleep(15)  # give a wrongly forwarded duplicate a further checkpoint to show up
    found = rows() or []
    assert len(found) == len(batch)
    purchases = [r for r in found if r[0] == "purchase"]
    assert len(purchases) == 1
    assert purchases[0][1] == CATALOG["P0001"]["category"]
    assert purchases[0][2] == 5000


def test_bad_records_go_to_the_dead_letter_table(clickhouse: Client) -> None:
    marker = f"e2e-poison-{uuid4().hex}".encode()
    producer = Producer({"bootstrap.servers": KAFKA})
    producer.produce("events.raw", key=b"e2e", value=marker)
    assert producer.flush(15) == 0

    def letters() -> list[tuple[Any, ...]] | None:
        rows = select(
            clickhouse,
            "SELECT stage, reason FROM pulse.dead_letters WHERE payload_base64 = {p:String}",
            p=base64.b64encode(marker).decode(),
        )
        return rows or None

    [(stage, reason)] = eventually(letters)
    assert stage == "decode"
    assert "wire format" in reason


def test_purchase_burst_raises_an_alert(
    collector: httpx.Client, clickhouse: Client, shopper: Shopper
) -> None:
    orders = [shopper.purchase(i * 5, order_id=f"e2e-o-{uuid4().hex[:8]}") for i in range(3)]
    assert collector.post("/v1/batch", json={"events": orders}).status_code == 202

    def alerts() -> list[tuple[Any, ...]] | None:
        rows = select(
            clickhouse,
            "SELECT alert_type, observed_value FROM pulse.alerts FINAL "
            "WHERE entity_id = {u:String}",
            u=shopper.user_id,
        )
        return rows or None

    [(alert_type, observed)] = eventually(alerts)
    assert alert_type == "purchase_velocity"
    assert observed == 3


def test_sessions_close_after_the_inactivity_gap(
    collector: httpx.Client, clickhouse: Client, shopper: Shopper
) -> None:
    events = [shopper.event("page_view"), shopper.event("product_view", 2, product_id="P0002")]
    assert collector.post("/v1/batch", json={"events": events}).status_code == 202
    gap = float(os.environ.get("PULSE_SESSION_GAP_SECONDS", "120"))

    def summary() -> list[tuple[Any, ...]] | None:
        rows = select(
            clickhouse,
            "SELECT event_count, product_views, converted FROM pulse.sessions FINAL "
            "WHERE session_id = {s:String}",
            s=shopper.session_id,
        )
        return rows or None

    # Closing needs the watermark to pass the gap, which needs other traffic (the load generator).
    [(event_count, product_views, converted)] = eventually(summary, timeout=gap + 180)
    assert (event_count, product_views, converted) == (2, 1, False)


def test_query_api_serves_live_metrics(query_api: httpx.Client) -> None:
    overview = eventually(
        lambda: (
            (r := query_api.get("/v1/overview", params={"window": "15m"}).json())["events"] and r
        )
    )
    assert overview["active_users"] > 0

    for path, params in [
        ("/v1/timeseries", {"metric": "revenue_cents", "window": "15m", "step": "1m"}),
        ("/v1/top-products", {"window": "1h"}),
        ("/v1/funnel", {"window": "1h"}),
        ("/v1/breakdown", {"dimension": "device", "window": "1h"}),
        ("/v1/sessions/stats", {"window": "1h"}),
        ("/v1/alerts", {}),
    ]:
        response = query_api.get(path, params=params)
        assert response.status_code == 200, (path, response.text)

    funnel = query_api.get("/v1/funnel", params={"window": "1h"}).json()["steps"]
    assert [s["sessions"] for s in funnel] == sorted((s["sessions"] for s in funnel), reverse=True)
