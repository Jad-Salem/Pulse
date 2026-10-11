from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from collector.models import EventIn, to_epoch_millis

from .conftest import make_event, purchase


def test_valid_purchase_maps_to_avro_record() -> None:
    event = EventIn.model_validate(purchase(event_time="2026-01-02T03:04:05.678+00:00"))
    received_at = datetime(2026, 1, 2, 3, 4, 6, tzinfo=UTC)

    record = event.to_avro(received_at)

    assert record["event_time"] == 1767323045678
    assert record["received_at"] == 1767323046000
    assert record["quantity"] == 2
    assert record["unit_price_cents"] == 1999
    assert record["referrer"] is None
    assert set(record) == {
        "event_id", "event_type", "event_time", "received_at", "user_id", "session_id",
        "page_url", "referrer", "product_id", "quantity", "unit_price_cents", "order_id",
        "country", "device", "user_agent",
    }  # fmt: skip


def test_epoch_millis_is_exact_for_non_utc_offsets() -> None:
    value = datetime.fromisoformat("2026-01-02T05:04:05.001+02:00")
    assert to_epoch_millis(value) == 1767323045001


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"event_type": "product_view"}, "product_id"),
        ({"event_type": "add_to_cart", "product_id": "P1"}, "quantity, unit_price_cents"),
        (
            {"event_type": "purchase", "product_id": "P1", "quantity": 1, "unit_price_cents": 5},
            "order_id",
        ),
    ],
)
def test_type_specific_fields_are_required(overrides: dict[str, object], message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        EventIn.model_validate(make_event(**overrides))


@pytest.mark.parametrize(
    "overrides",
    [
        {"event_type": "signup"},
        {"country": "us"},
        {"country": "USA"},
        {"device": "console"},
        {"user_id": "   "},
        {"event_id": "not-a-uuid"},
        {"event_time": "2026-01-02T03:04:05"},  # naive timestamps are ambiguous
        {"unexpected_field": 1},
    ],
)
def test_contract_violations_are_rejected(overrides: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        EventIn.model_validate(make_event(**overrides))


def test_quantity_bounds() -> None:
    with pytest.raises(ValidationError):
        EventIn.model_validate(purchase(quantity=0))
    with pytest.raises(ValidationError):
        EventIn.model_validate(purchase(quantity=1001))


def test_clock_violation() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    skew, age = timedelta(minutes=5), timedelta(days=7)

    def at(delta: timedelta) -> EventIn:
        return EventIn.model_validate(make_event(event_time=(now + delta).isoformat()))

    assert at(timedelta(minutes=4)).clock_violation(now, skew, age) is None
    assert at(-timedelta(days=6)).clock_violation(now, skew, age) is None
    assert at(timedelta(minutes=6)).clock_violation(now, skew, age) == "event_time is in the future"
    assert at(-timedelta(days=8)).clock_violation(now, skew, age) == "event_time is too old"
