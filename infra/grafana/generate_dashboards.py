"""Dashboards as code. Run `python infra/grafana/generate_dashboards.py` after editing.

Standard library only. The generated JSON is committed; CI fails if it is out of date.
"""

import json
from pathlib import Path
from typing import Any

OUT = Path(__file__).parent / "dashboards"
CH = {"type": "grafana-clickhouse-datasource", "uid": "clickhouse"}
PROM = {"type": "prometheus", "uid": "prometheus"}
TABLE, TIMESERIES = 1, 0  # ClickHouse plugin result formats


class Layout:
    """Places panels left-to-right on Grafana's 24-column grid."""

    def __init__(self) -> None:
        self.x = self.y = self.row_height = 0
        self.next_id = 1

    def place(self, width: int, height: int) -> dict[str, int]:
        if self.x + width > 24:
            self.x, self.y, self.row_height = 0, self.y + self.row_height, 0
        pos = {"h": height, "w": width, "x": self.x, "y": self.y}
        self.x += width
        self.row_height = max(self.row_height, height)
        return pos

    def new_id(self) -> int:
        self.next_id += 1
        return self.next_id - 1


def ch_target(sql: str, fmt: int, ref: str = "A") -> dict[str, Any]:
    return {
        "refId": ref,
        "datasource": CH,
        "editorType": "sql",
        "rawSql": " ".join(sql.split()),
        "format": fmt,
        "queryType": "timeseries" if fmt == TIMESERIES else "table",
        "pluginVersion": "4.22.1",
    }


def prom_target(expr: str, legend: str, ref: str = "A") -> dict[str, Any]:
    return {
        "refId": ref,
        "datasource": PROM,
        "expr": " ".join(expr.split()),
        "legendFormat": legend,
    }


def panel(
    layout: Layout,
    kind: str,
    title: str,
    targets: list[dict[str, Any]],
    *,
    width: int,
    height: int,
    unit: str | None = None,
    description: str = "",
    options: dict[str, Any] | None = None,
    custom: dict[str, Any] | None = None,
    decimals: int | None = None,
) -> dict[str, Any]:
    defaults: dict[str, Any] = {"color": {"mode": "palette-classic"}, "custom": custom or {}}
    if kind == "stat":
        defaults["color"] = {"mode": "fixed", "fixedColor": "text"}
    if unit:
        defaults["unit"] = unit
    if decimals is not None:
        defaults["decimals"] = decimals
    return {
        "id": layout.new_id(),
        "type": kind,
        "title": title,
        "description": description,
        "datasource": targets[0]["datasource"],
        "gridPos": layout.place(width, height),
        "targets": targets,
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": options or {},
    }


STAT_OPTIONS = {
    "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
    "colorMode": "value",
    "graphMode": "none",
    "textMode": "value",
    "justifyMode": "auto",
    "orientation": "auto",
}
LEGEND_BOTTOM = {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                 "tooltip": {"mode": "multi", "sort": "desc"}}  # fmt: skip
LINE = {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 10, "showPoints": "never"}
STACKED_BARS = {"drawStyle": "bars", "fillOpacity": 80, "stacking": {"mode": "normal"},
                "showPoints": "never"}  # fmt: skip


def dashboard(
    uid: str, title: str, description: str, panels: list[dict[str, Any]]
) -> dict[str, Any]:
    return {
        "uid": uid,
        "title": title,
        "description": description,
        "tags": ["pulse"],
        "timezone": "browser",
        "editable": False,
        "graphTooltip": 1,
        "refresh": "10s",
        "time": {"from": "now-30m", "to": "now"},
        "schemaVersion": 41,
        "version": 1,
        "annotations": {"list": []},
        "templating": {"list": []},
        "panels": panels,
    }


def business_overview() -> dict[str, Any]:
    l = Layout()  # noqa: E741
    rollup = "FROM pulse.metrics_minute WHERE $__timeFilter(minute)"
    panels = [
        panel(
            l,
            "stat",
            "Revenue",
            [ch_target(f"SELECT sum(revenue_cents) / 100 AS revenue {rollup}", TABLE)],
            width=4,
            height=4,
            unit="currencyUSD",
            decimals=0,
            options=STAT_OPTIONS,
        ),
        panel(
            l,
            "stat",
            "Orders",
            [
                ch_target(
                    f"SELECT uniqMerge(orders) AS orders {rollup} AND event_type = 'purchase'",
                    TABLE,
                )
            ],
            width=4,
            height=4,
            options=STAT_OPTIONS,
        ),
        panel(
            l,
            "stat",
            "Average order value",
            [
                ch_target(
                    f"""SELECT if(uniqMerge(orders) = 0, 0, sum(revenue_cents) / 100 / uniqMerge(orders))
                             AS aov {rollup} AND event_type = 'purchase'""",
                    TABLE,
                )
            ],
            width=4,
            height=4,
            unit="currencyUSD",
            decimals=2,
            options=STAT_OPTIONS,
        ),
        panel(
            l,
            "stat",
            "Active users",
            [ch_target(f"SELECT uniqMerge(users) AS users {rollup}", TABLE)],
            width=4,
            height=4,
            options=STAT_OPTIONS,
        ),
        panel(
            l,
            "stat",
            "Session conversion",
            [
                ch_target(
                    f"""SELECT if(all_sessions = 0, 0, converted / all_sessions) AS conversion
                        FROM (SELECT uniqMerge(sessions) AS all_sessions {rollup}) AS a
                        CROSS JOIN (SELECT uniqMerge(sessions) AS converted {rollup}
                                    AND event_type = 'purchase') AS b""",
                    TABLE,
                )
            ],
            width=4,
            height=4,
            unit="percentunit",
            decimals=1,
            options=STAT_OPTIONS,
            description="Sessions with a purchase / all sessions with any event in the range.",
        ),
        panel(
            l,
            "stat",
            "Pipeline latency (p95)",
            [
                ch_target(
                    """SELECT quantile(0.95)(dateDiff('millisecond', received_at, ingested_at)) / 1000
                            AS p95 FROM pulse.events WHERE $__timeFilter_ms(ingested_at)""",
                    TABLE,
                )
            ],
            width=4,
            height=4,
            unit="s",
            decimals=1,
            options=STAT_OPTIONS,
            description="Collector accept -> queryable in ClickHouse, including the exactly-once "
            "checkpoint commit.",
        ),
        panel(
            l,
            "timeseries",
            "Revenue per minute",
            [
                ch_target(
                    f"SELECT minute AS time, sum(revenue_cents) / 100 AS revenue {rollup} "
                    "GROUP BY time ORDER BY time",
                    TIMESERIES,
                )
            ],
            width=12,
            height=8,
            unit="currencyUSD",
            custom=STACKED_BARS,
            options=LEGEND_BOTTOM,
        ),
        panel(
            l,
            "timeseries",
            "Events per minute by type",
            [
                ch_target(
                    f"SELECT minute AS time, event_type, sum(events) AS events {rollup} "
                    "GROUP BY time, event_type ORDER BY time",
                    TIMESERIES,
                )
            ],
            width=12,
            height=8,
            custom=LINE,
            options=LEGEND_BOTTOM,
        ),
        panel(
            l,
            "bargauge",
            "Purchase funnel (sessions)",
            [
                ch_target(
                    """SELECT countIf(level >= 1) AS `1. viewed product`,
                                   countIf(level >= 2) AS `2. added to cart`,
                                   countIf(level >= 3) AS `3. started checkout`,
                                   countIf(level >= 4) AS `4. purchased`
                            FROM (SELECT session_id,
                                         windowFunnel(3600)(toDateTime(event_time),
                                             event_type = 'product_view', event_type = 'add_to_cart',
                                             event_type = 'checkout_started', event_type = 'purchase') AS level
                                  FROM pulse.events FINAL WHERE $__timeFilter_ms(event_time)
                                  GROUP BY session_id)""",
                    TABLE,
                )
            ],
            width=8,
            height=8,
            options={
                "orientation": "horizontal",
                "displayMode": "gradient",
                "showUnfilled": True,
                "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            },
        ),
        panel(
            l,
            "barchart",
            "Revenue by category",
            [
                ch_target(
                    f"""SELECT product_category AS category, sum(revenue_cents) / 100 AS revenue
                             {rollup} AND event_type = 'purchase' GROUP BY category ORDER BY revenue DESC""",
                    TABLE,
                )
            ],
            width=8,
            height=8,
            unit="currencyUSD",
            options={
                "orientation": "horizontal",
                "xField": "category",
                "showValue": "auto",
                "legend": {"showLegend": False, "displayMode": "list", "placement": "bottom"},
            },
        ),
        panel(
            l,
            "barchart",
            "Active users by country",
            [
                ch_target(
                    f"""SELECT country, uniqMerge(users) AS users {rollup}
                             GROUP BY country ORDER BY users DESC LIMIT 10""",
                    TABLE,
                )
            ],
            width=8,
            height=8,
            options={
                "orientation": "horizontal",
                "xField": "country",
                "showValue": "auto",
                "legend": {"showLegend": False, "displayMode": "list", "placement": "bottom"},
            },
        ),
        panel(
            l,
            "table",
            "Top products",
            [
                ch_target(
                    """SELECT any(product_name) AS product, any(product_category) AS category,
                                   sum(quantity) AS units, uniqExact(order_id) AS orders,
                                   sum(revenue_cents) / 100 AS revenue
                            FROM pulse.events FINAL
                            WHERE event_type = 'purchase' AND $__timeFilter_ms(event_time)
                            GROUP BY product_id ORDER BY revenue DESC LIMIT 10""",
                    TABLE,
                )
            ],
            width=12,
            height=9,
        ),
        panel(
            l,
            "table",
            "Real-time alerts",
            [
                ch_target(
                    """SELECT detected_at, severity, alert_type, entity_id, description
                            FROM pulse.alerts FINAL WHERE $__timeFilter_ms(window_end)
                            ORDER BY detected_at DESC LIMIT 50""",
                    TABLE,
                )
            ],
            width=12,
            height=9,
        ),
        panel(
            l,
            "timeseries",
            "Closed sessions per minute",
            [
                ch_target(
                    """SELECT toStartOfMinute(session_end) AS time,
                                   countIf(converted) AS converted, countIf(NOT converted) AS browsed
                            FROM pulse.sessions FINAL WHERE $__timeFilter_ms(session_end)
                            GROUP BY time ORDER BY time""",
                    TIMESERIES,
                )
            ],
            width=12,
            height=8,
            custom=STACKED_BARS,
            options=LEGEND_BOTTOM,
        ),
        panel(
            l,
            "table",
            "Dead letters",
            [
                ch_target(
                    """SELECT failed_at, stage, reason, source_topic, source_partition, source_offset,
                                   payload_size
                            FROM pulse.dead_letters WHERE $__timeFilter_ms(failed_at)
                            ORDER BY failed_at DESC LIMIT 50""",
                    TABLE,
                )
            ],
            width=12,
            height=8,
            description="Records the stream processor could not decode or that broke the contract.",
        ),
    ]
    return dashboard(
        "pulse-business",
        "Pulse / Business overview",
        "Real-time e-commerce KPIs, served from ClickHouse.",
        panels,
    )


def platform_health() -> dict[str, Any]:
    l = Layout()  # noqa: E741
    panels = [
        panel(
            l,
            "timeseries",
            "Collector: accepted events/s",
            [
                prom_target(
                    "sum by (event_type) (rate(collector_events_accepted_total[1m]))",
                    "{{event_type}}",
                )
            ],
            width=8,
            height=8,
            unit="ops",
            custom=LINE,
            options=LEGEND_BOTTOM,
        ),
        panel(
            l,
            "timeseries",
            "Collector: rejected events/s",
            [
                prom_target(
                    "sum by (reason) (rate(collector_events_rejected_total[1m]))", "{{reason}}"
                )
            ],
            width=8,
            height=8,
            unit="ops",
            custom=LINE,
            options=LEGEND_BOTTOM,
        ),
        panel(
            l,
            "timeseries",
            "Collector: Kafka ack latency",
            [
                prom_target(
                    "histogram_quantile(0.5, sum by (le) (rate(collector_produce_latency_seconds_bucket[1m])))",
                    "p50",
                ),
                prom_target(
                    "histogram_quantile(0.99, sum by (le) (rate(collector_produce_latency_seconds_bucket[1m])))",
                    "p99",
                    "B",
                ),
            ],
            width=8,
            height=8,
            unit="s",
            custom=LINE,
            options=LEGEND_BOTTOM,
        ),
        panel(
            l,
            "timeseries",
            "Kafka: consumer lag by group",
            [prom_target("sum by (consumergroup) (kafka_consumergroup_lag)", "{{consumergroup}}")],
            width=12,
            height=8,
            custom=LINE,
            options=LEGEND_BOTTOM,
            description="stream-processor commits offsets on checkpoint; clickhouse-* groups are the "
            "Kafka table engines.",
        ),
        panel(
            l,
            "timeseries",
            "Kafka: messages in/s by topic",
            [
                prom_target(
                    'sum by (topic) (rate(kafka_topic_partition_current_offset{topic!~"__.*|_schemas"}[1m]))',
                    "{{topic}}",
                )
            ],
            width=12,
            height=8,
            unit="ops",
            custom=LINE,
            options=LEGEND_BOTTOM,
        ),
        panel(
            l,
            "timeseries",
            "Flink: records out/s by operator",
            [
                prom_target(
                    "sum by (operator_name) (flink_taskmanager_job_task_operator_numRecordsOutPerSecond)",
                    "{{operator_name}}",
                )
            ],
            width=12,
            height=8,
            unit="ops",
            custom=LINE,
            options=LEGEND_BOTTOM,
        ),
        panel(
            l,
            "timeseries",
            "Flink: data quality",
            [
                prom_target(
                    "sum(rate(flink_taskmanager_job_task_operator_duplicatesDropped[1m]))",
                    "duplicates dropped",
                ),
                prom_target(
                    "sum(rate(flink_taskmanager_job_task_operator_deadLettersDecode[1m]))",
                    "DLQ: decode",
                    "B",
                ),
                prom_target(
                    "sum(rate(flink_taskmanager_job_task_operator_deadLettersValidation[1m]))",
                    "DLQ: contract",
                    "C",
                ),
                prom_target(
                    "sum(rate(flink_taskmanager_job_task_operator_catalogMisses[1m]))",
                    "catalog misses",
                    "D",
                ),
                prom_target(
                    "sum(rate(flink_taskmanager_job_task_operator_numLateRecordsDropped[1m]))",
                    "late (sessions)",
                    "E",
                ),
            ],
            width=12,
            height=8,
            unit="ops",
            custom=LINE,
            options=LEGEND_BOTTOM,
        ),
        panel(
            l,
            "timeseries",
            "Flink: checkpoint duration",
            [
                prom_target(
                    "max(flink_jobmanager_job_lastCheckpointDuration) / 1000", "last checkpoint"
                )
            ],
            width=8,
            height=8,
            unit="s",
            custom=LINE,
            options=LEGEND_BOTTOM,
            description="Exactly-once results become visible downstream when a checkpoint commits.",
        ),
        panel(
            l,
            "stat",
            "Flink: restarts",
            [prom_target("max(flink_jobmanager_job_numRestarts)", "restarts")],
            width=4,
            height=8,
            options=STAT_OPTIONS,
        ),
        panel(
            l,
            "stat",
            "Flink: running jobs",
            [prom_target("max(flink_jobmanager_numRunningJobs)", "jobs")],
            width=4,
            height=8,
            options=STAT_OPTIONS,
        ),
        panel(
            l,
            "timeseries",
            "ClickHouse: rows inserted/s",
            [prom_target("sum(rate(ClickHouseProfileEvents_InsertedRows[1m]))", "rows")],
            width=8,
            height=8,
            unit="ops",
            custom=LINE,
            options=LEGEND_BOTTOM,
        ),
        panel(
            l,
            "timeseries",
            "Query API: p95 latency by route",
            [
                prom_target(
                    "histogram_quantile(0.95, sum by (le, route) (rate(http_request_duration_seconds_bucket"
                    '{job="query-api", route=~"/v1/.*"}[5m])))',
                    "{{route}}",
                )
            ],
            width=12,
            height=8,
            unit="s",
            custom=LINE,
            options=LEGEND_BOTTOM,
        ),
        panel(
            l,
            "timeseries",
            "ClickHouse: queries/s",
            [
                prom_target("sum(rate(ClickHouseProfileEvents_SelectQuery[1m]))", "select"),
                prom_target("sum(rate(ClickHouseProfileEvents_FailedQuery[1m]))", "failed", "B"),
            ],
            width=12,
            height=8,
            unit="ops",
            custom=LINE,
            options=LEGEND_BOTTOM,
        ),
    ]
    return dashboard(
        "pulse-platform",
        "Pulse / Platform health",
        "Throughput, lag, data quality and latency across every component.",
        panels,
    )


def main() -> None:
    OUT.mkdir(exist_ok=True)
    for name, board in {
        "business-overview": business_overview(),
        "platform-health": platform_health(),
    }.items():
        path = OUT / f"{name}.json"
        path.write_text(json.dumps(board, indent=2) + "\n", encoding="utf-8", newline="\n")
        print(f"wrote {path}")  # noqa: T201


if __name__ == "__main__":
    main()
