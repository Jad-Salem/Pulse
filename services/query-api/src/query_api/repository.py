"""Analytics queries.

Every user-supplied *value* is bound as a ClickHouse server-side parameter ({name:Type}).
User-supplied *choices* (metric, dimension, ranking) select SQL fragments from fixed allow-lists;
no request text is ever interpolated into SQL.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from query_api.models import (
    Alert,
    Breakdown,
    BreakdownRow,
    Dimension,
    Funnel,
    FunnelStep,
    Metric,
    Overview,
    ProductRanking,
    ProductStat,
    SessionStats,
    Timeseries,
    TimeseriesPoint,
)

Row = dict[str, Any]


class QueryRunner(Protocol):
    async def fetch(self, sql: str, parameters: Mapping[str, Any]) -> list[Row]: ...


@dataclass(frozen=True)
class _MetricSql:
    expression: str
    condition: str = "1"


_METRICS: dict[Metric, _MetricSql] = {
    "events": _MetricSql("sum(events)"),
    "revenue_cents": _MetricSql("sum(revenue_cents)"),
    "active_users": _MetricSql("uniqMerge(users)"),
    "sessions": _MetricSql("uniqMerge(sessions)"),
    "orders": _MetricSql("uniqMerge(orders)", "event_type = 'purchase'"),
}

_DIMENSIONS: dict[Dimension, str] = {
    "country": "country",
    "device": "device",
    "category": "product_category",
    "event_type": "event_type",
}

_PRODUCT_ORDER: dict[ProductRanking, str] = {
    "revenue": "revenue_cents DESC, units DESC",
    "units": "units DESC, revenue_cents DESC",
}

FUNNEL_STEPS: Sequence[str] = ("product_view", "add_to_cart", "checkout_started", "purchase")
FUNNEL_CONVERSION_WINDOW_SECONDS = 3600

_ROLLUP_RANGE = "minute >= toStartOfMinute(now() - toIntervalSecond({window:UInt32}))"
_EVENTS_RANGE = "event_time >= now64(3) - toIntervalSecond({window:UInt32})"


def _ratio(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


class AnalyticsRepository:
    def __init__(self, runner: QueryRunner) -> None:
        self._runner = runner

    async def overview(self, window: int) -> Overview:
        params = {"window": window}
        [totals] = await self._runner.fetch(
            f"""
            SELECT sum(events) AS events,
                   uniqMerge(users) AS active_users,
                   uniqMerge(sessions) AS sessions,
                   sum(revenue_cents) AS revenue_cents
            FROM pulse.metrics_minute
            WHERE {_ROLLUP_RANGE}
            """,
            params,
        )
        [purchases] = await self._runner.fetch(
            f"""
            SELECT uniqMerge(orders) AS orders,
                   uniqMerge(sessions) AS converted_sessions
            FROM pulse.metrics_minute
            WHERE {_ROLLUP_RANGE} AND event_type = 'purchase'
            """,
            params,
        )
        orders = int(purchases["orders"])
        revenue = int(totals["revenue_cents"])
        return Overview(
            window_seconds=window,
            events=int(totals["events"]),
            active_users=int(totals["active_users"]),
            sessions=int(totals["sessions"]),
            orders=orders,
            revenue_cents=revenue,
            conversion_rate=_ratio(purchases["converted_sessions"], totals["sessions"]),
            average_order_value_cents=round(_ratio(revenue, orders)),
            generated_at=datetime.now(UTC),
        )

    async def timeseries(self, metric: Metric, window: int, step: int) -> Timeseries:
        spec = _METRICS[metric]
        rows = await self._runner.fetch(
            f"""
            SELECT toStartOfInterval(minute, toIntervalSecond({{step:UInt32}})) AS bucket,
                   toFloat64({spec.expression}) AS value
            FROM pulse.metrics_minute
            WHERE {_ROLLUP_RANGE} AND {spec.condition}
            GROUP BY bucket
            ORDER BY bucket WITH FILL
                FROM toStartOfInterval(now() - toIntervalSecond({{window:UInt32}}),
                                       toIntervalSecond({{step:UInt32}}))
                TO now()
                STEP toIntervalSecond({{step:UInt32}})
            """,
            {"window": window, "step": step},
        )
        return Timeseries(
            metric=metric,
            window_seconds=window,
            step_seconds=step,
            points=[TimeseriesPoint(bucket=r["bucket"], value=r["value"]) for r in rows],
        )

    async def top_products(self, window: int, limit: int, by: ProductRanking) -> list[ProductStat]:
        rows = await self._runner.fetch(
            f"""
            SELECT product_id,
                   any(product_name) AS product_name,
                   any(product_category) AS category,
                   sum(quantity) AS units,
                   uniqExact(order_id) AS orders,
                   sum(revenue_cents) AS revenue_cents
            FROM pulse.events FINAL
            WHERE event_type = 'purchase' AND {_EVENTS_RANGE}
            GROUP BY product_id
            ORDER BY {_PRODUCT_ORDER[by]}
            LIMIT {{limit:UInt16}}
            """,
            {"window": window, "limit": limit},
        )
        return [ProductStat.model_validate(r) for r in rows]

    async def funnel(self, window: int) -> Funnel:
        conditions = ", ".join(f"event_type = '{step}'" for step in FUNNEL_STEPS)
        levels = ", ".join(
            f"countIf(level >= {i}) AS step_{i}" for i in range(1, len(FUNNEL_STEPS) + 1)
        )
        [row] = await self._runner.fetch(
            f"""
            SELECT {levels}
            FROM (
                SELECT session_id,
                       -- windowFunnel takes DateTime (seconds), not DateTime64.
                       windowFunnel({FUNNEL_CONVERSION_WINDOW_SECONDS})(
                           toDateTime(event_time), {conditions}
                       ) AS level
                FROM pulse.events FINAL
                WHERE {_EVENTS_RANGE}
                GROUP BY session_id
            )
            """,
            {"window": window},
        )
        counts = [int(row[f"step_{i}"]) for i in range(1, len(FUNNEL_STEPS) + 1)]
        steps = [
            FunnelStep(
                step=name,
                sessions=count,
                conversion_from_previous=_ratio(count, counts[i - 1]) if i else 1.0,
                conversion_from_start=_ratio(count, counts[0]),
            )
            for i, (name, count) in enumerate(zip(FUNNEL_STEPS, counts, strict=True))
        ]
        return Funnel(
            window_seconds=window,
            conversion_window_seconds=FUNNEL_CONVERSION_WINDOW_SECONDS,
            steps=steps,
        )

    async def breakdown(self, dimension: Dimension, window: int) -> Breakdown:
        column = _DIMENSIONS[dimension]
        rows = await self._runner.fetch(
            f"""
            SELECT if({column} = '', '(none)', {column}) AS key,
                   sum(events) AS events,
                   uniqMerge(users) AS users,
                   sum(revenue_cents) AS revenue_cents
            FROM pulse.metrics_minute
            WHERE {_ROLLUP_RANGE}
            GROUP BY key
            ORDER BY revenue_cents DESC, events DESC
            LIMIT 50
            """,
            {"window": window},
        )
        return Breakdown(
            dimension=dimension,
            window_seconds=window,
            rows=[BreakdownRow.model_validate(r) for r in rows],
        )

    async def session_stats(self, window: int) -> SessionStats:
        [row] = await self._runner.fetch(
            """
            SELECT count() AS sessions,
                   ifNotFinite(avg(duration_ms) / 1000, 0) AS average_duration_seconds,
                   ifNotFinite(avg(event_count), 0) AS average_events,
                   countIf(event_count = 1) AS bounces,
                   countIf(converted) AS converted
            FROM pulse.sessions FINAL
            WHERE session_end >= now64(3) - toIntervalSecond({window:UInt32})
            """,
            {"window": window},
        )
        sessions = int(row["sessions"])
        return SessionStats(
            window_seconds=window,
            sessions=sessions,
            average_duration_seconds=float(row["average_duration_seconds"]),
            average_events=float(row["average_events"]),
            bounce_rate=_ratio(row["bounces"], sessions),
            conversion_rate=_ratio(row["converted"], sessions),
        )

    async def alerts(self, limit: int) -> list[Alert]:
        rows = await self._runner.fetch(
            """
            SELECT alert_id, alert_type, severity, entity_id, description,
                   observed_value, threshold, window_start, window_end, detected_at
            FROM pulse.alerts FINAL
            ORDER BY detected_at DESC
            LIMIT {limit:UInt16}
            """,
            {"limit": limit},
        )
        return [Alert.model_validate(r) for r in rows]
