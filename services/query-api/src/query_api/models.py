"""Response models. These define the public API contract (and the OpenAPI document at /docs)."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

Metric = Literal["events", "revenue_cents", "active_users", "sessions", "orders"]
Dimension = Literal["country", "device", "category", "event_type"]
ProductRanking = Literal["revenue", "units"]


class Overview(BaseModel):
    window_seconds: int
    events: int
    active_users: int
    sessions: int
    orders: int
    revenue_cents: int
    conversion_rate: float
    average_order_value_cents: int
    generated_at: datetime


class TimeseriesPoint(BaseModel):
    bucket: datetime
    value: float


class Timeseries(BaseModel):
    metric: Metric
    window_seconds: int
    step_seconds: int
    points: list[TimeseriesPoint]


class ProductStat(BaseModel):
    product_id: str
    product_name: str
    category: str
    units: int
    orders: int
    revenue_cents: int


class FunnelStep(BaseModel):
    step: str
    sessions: int
    conversion_from_previous: float
    conversion_from_start: float


class Funnel(BaseModel):
    window_seconds: int
    conversion_window_seconds: int
    steps: list[FunnelStep]


class BreakdownRow(BaseModel):
    key: str
    events: int
    users: int
    revenue_cents: int


class Breakdown(BaseModel):
    dimension: Dimension
    window_seconds: int
    rows: list[BreakdownRow]


class SessionStats(BaseModel):
    window_seconds: int
    sessions: int
    average_duration_seconds: float
    average_events: float
    bounce_rate: float
    conversion_rate: float


class Alert(BaseModel):
    alert_id: str
    alert_type: str
    severity: str
    entity_id: str
    description: str
    observed_value: float
    threshold: float
    window_start: datetime
    window_end: datetime
    detected_at: datetime
