"""Structured logging and Prometheus metrics."""

import logging
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import structlog
from fastapi import Request, Response
from prometheus_client import CollectorRegistry, Counter, Histogram


def configure_logging(level: str, json: bool) -> None:
    renderer: structlog.types.Processor = (
        structlog.processors.JSONRenderer() if json else structlog.dev.ConsoleRenderer()
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level.upper())),
        logger_factory=structlog.PrintLoggerFactory(sys.stdout),
        cache_logger_on_first_use=True,
    )


@dataclass(frozen=True)
class Metrics:
    registry: CollectorRegistry
    events_accepted: Counter
    events_rejected: Counter
    produce_latency: Histogram
    http_requests: Histogram

    @classmethod
    def create(cls) -> "Metrics":
        registry = CollectorRegistry()
        return cls(
            registry=registry,
            events_accepted=Counter(
                "collector_events_accepted_total",
                "Events acknowledged by Kafka.",
                ["event_type"],
                registry=registry,
            ),
            events_rejected=Counter(
                "collector_events_rejected_total",
                "Events rejected before reaching Kafka.",
                ["reason"],
                registry=registry,
            ),
            produce_latency=Histogram(
                "collector_produce_latency_seconds",
                "Time from produce() to broker acknowledgement.",
                buckets=(0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
                registry=registry,
            ),
            http_requests=Histogram(
                "http_request_duration_seconds",
                "HTTP request latency.",
                ["method", "route", "status"],
                registry=registry,
            ),
        )


def http_metrics_middleware(
    metrics: Metrics,
) -> Callable[[Request, Callable[[Request], Awaitable[Response]]], Awaitable[Response]]:
    async def middleware(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        start = time.perf_counter()
        response = await call_next(request)
        route = request.scope.get("route")
        # Label by route template, never the raw path, to keep cardinality bounded.
        template = getattr(route, "path", "unmatched")
        metrics.http_requests.labels(request.method, template, str(response.status_code)).observe(
            time.perf_counter() - start
        )
        return response

    return middleware
