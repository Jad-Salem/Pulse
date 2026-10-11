"""Wire models for the collector API and their mapping to the events.raw Avro contract.

The validation rules here mirror schemas/README.md ("Validation contract"); the stream processor
re-applies the same rules to events that reach Kafka by other paths.
"""

from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints, model_validator

EventType = Literal[
    "page_view",
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "checkout_started",
    "purchase",
]
Device = Literal["desktop", "mobile", "tablet", "unknown"]

PRODUCT_EVENTS: frozenset[str] = frozenset(
    {"product_view", "add_to_cart", "remove_from_cart", "purchase"}
)
LINE_ITEM_EVENTS: frozenset[str] = frozenset({"add_to_cart", "remove_from_cart", "purchase"})
MAX_BATCH_SIZE = 500

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

Id = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)]
Url = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=2048)]


def to_epoch_millis(value: datetime) -> int:
    """Exact integer milliseconds since the epoch (no float rounding)."""
    return (value - _EPOCH) // timedelta(milliseconds=1)


class EventIn(BaseModel):
    """A single event as sent by a client."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: UUID
    event_type: EventType
    event_time: AwareDatetime
    user_id: Id
    session_id: Id
    page_url: Url
    referrer: Url | None = None
    product_id: Id | None = None
    quantity: int | None = Field(default=None, ge=1, le=1000)
    unit_price_cents: int | None = Field(default=None, ge=0)
    order_id: Id | None = None
    country: Annotated[str, StringConstraints(pattern=r"^[A-Z]{2}$")]
    device: Device = "unknown"
    user_agent: Annotated[str, StringConstraints(max_length=512)] | None = None

    @model_validator(mode="after")
    def _type_specific_fields(self) -> Self:
        missing: list[str] = []
        if self.event_type in PRODUCT_EVENTS and self.product_id is None:
            missing.append("product_id")
        if self.event_type in LINE_ITEM_EVENTS:
            if self.quantity is None:
                missing.append("quantity")
            if self.unit_price_cents is None:
                missing.append("unit_price_cents")
        if self.event_type == "purchase" and self.order_id is None:
            missing.append("order_id")
        if missing:
            raise ValueError(f"{self.event_type} events require: {', '.join(missing)}")
        return self

    def clock_violation(
        self, received_at: datetime, max_skew: timedelta, max_age: timedelta
    ) -> str | None:
        """Return a reason if event_time is implausible relative to the server clock."""
        if self.event_time > received_at + max_skew:
            return "event_time is in the future"
        if self.event_time < received_at - max_age:
            return "event_time is too old"
        return None

    def to_avro(self, received_at: datetime) -> dict[str, Any]:
        """Map to the events.raw Avro record (timestamps as epoch millis)."""
        return {
            "event_id": str(self.event_id),
            "event_type": self.event_type,
            "event_time": to_epoch_millis(self.event_time),
            "received_at": to_epoch_millis(received_at),
            "user_id": self.user_id,
            "session_id": self.session_id,
            "page_url": self.page_url,
            "referrer": self.referrer,
            "product_id": self.product_id,
            "quantity": self.quantity,
            "unit_price_cents": self.unit_price_cents,
            "order_id": self.order_id,
            "country": self.country,
            "device": self.device,
            "user_agent": self.user_agent,
        }


class EventBatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    events: list[EventIn] = Field(min_length=1, max_length=MAX_BATCH_SIZE)


class Accepted(BaseModel):
    accepted: int
