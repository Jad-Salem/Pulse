"""HTTP API over the analytics repository."""

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Annotated, Protocol

import structlog
from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse, Response, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Histogram, generate_latest

from query_api.config import Settings
from query_api.durations import DURATION_PATTERN, parse_duration
from query_api.models import (
    Alert,
    Breakdown,
    Dimension,
    Funnel,
    Metric,
    Overview,
    ProductRanking,
    ProductStat,
    SessionStats,
    Timeseries,
)
from query_api.repository import AnalyticsRepository, QueryRunner

log = structlog.get_logger(__name__)

MAX_WINDOW_SECONDS = 30 * 86400
MAX_POINTS = 1440


class ManagedRunner(QueryRunner, Protocol):
    async def ping(self) -> bool: ...

    async def close(self) -> None: ...


RunnerFactory = Callable[[Settings], Awaitable[ManagedRunner]]


async def _clickhouse(settings: Settings) -> ManagedRunner:
    from query_api.clickhouse import ClickHouseRunner

    return await ClickHouseRunner.connect(settings)


def window_param(
    window: Annotated[
        str, Query(pattern=DURATION_PATTERN, description="Look-back window, e.g. 15m, 6h, 7d")
    ] = "15m",
) -> int:
    seconds = parse_duration(window)
    if not 60 <= seconds <= MAX_WINDOW_SECONDS:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT, "window must be between 1m and 30d"
        )
    return seconds


Window = Annotated[int, Depends(window_param)]


async def overview_stream(
    repository: AnalyticsRepository,
    window: int,
    interval: float,
    disconnected: Callable[[], Awaitable[bool]],
) -> AsyncIterator[str]:
    """Server-sent events: one `overview` event per interval until the client goes away."""
    while not await disconnected():
        try:
            overview = await repository.overview(window)
            yield f"event: overview\ndata: {overview.model_dump_json()}\n\n"
        except Exception:
            log.warning("live overview query failed", exc_info=True)
            yield 'event: error\ndata: {"detail": "query failed"}\n\n'
        await asyncio.sleep(interval)


def create_app(
    settings: Settings | None = None, runner_factory: RunnerFactory = _clickhouse
) -> FastAPI:
    settings = settings or Settings()
    registry = CollectorRegistry()
    request_latency = Histogram(
        "http_request_duration_seconds",
        "HTTP request latency.",
        ["method", "route", "status"],
        registry=registry,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        runner = await runner_factory(settings)
        app.state.runner = runner
        app.state.repository = AnalyticsRepository(runner)
        yield
        await runner.close()

    app = FastAPI(
        title="Pulse Query API",
        version="0.1.0",
        summary="Real-time e-commerce analytics served from ClickHouse.",
        lifespan=lifespan,
    )

    @app.middleware("http")
    async def record_latency(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        start = time.perf_counter()
        response = await call_next(request)
        template = getattr(request.scope.get("route"), "path", "unmatched")
        request_latency.labels(request.method, template, str(response.status_code)).observe(
            time.perf_counter() - start
        )
        return response

    def repository(request: Request) -> AnalyticsRepository:
        repo: AnalyticsRepository = request.app.state.repository
        return repo

    Repo = Annotated[AnalyticsRepository, Depends(repository)]

    @app.get("/v1/overview", summary="Headline KPIs for a look-back window")
    async def get_overview(repo: Repo, window: Window) -> Overview:
        return await repo.overview(window)

    @app.get("/v1/timeseries", summary="One metric bucketed over time")
    async def get_timeseries(
        repo: Repo,
        window: Window,
        metric: Metric = "events",
        step: Annotated[str, Query(pattern=DURATION_PATTERN)] = "1m",
    ) -> Timeseries:
        step_seconds = parse_duration(step)
        if step_seconds < 60 or step_seconds % 60:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT, "step must be a whole number of minutes"
            )
        if window // step_seconds > MAX_POINTS:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT, f"at most {MAX_POINTS} points per series"
            )
        return await repo.timeseries(metric, window, step_seconds)

    @app.get("/v1/top-products", summary="Best-selling products")
    async def get_top_products(
        repo: Repo,
        window: Window,
        limit: Annotated[int, Query(ge=1, le=100)] = 10,
        by: ProductRanking = "revenue",
    ) -> list[ProductStat]:
        return await repo.top_products(window, limit, by)

    @app.get("/v1/funnel", summary="Session conversion funnel (view, cart, checkout, purchase)")
    async def get_funnel(repo: Repo, window: Window) -> Funnel:
        return await repo.funnel(window)

    @app.get("/v1/breakdown", summary="Events, users and revenue split by a dimension")
    async def get_breakdown(
        repo: Repo, window: Window, dimension: Dimension = "country"
    ) -> Breakdown:
        return await repo.breakdown(dimension, window)

    @app.get("/v1/sessions/stats", summary="Closed-session statistics")
    async def get_session_stats(repo: Repo, window: Window) -> SessionStats:
        return await repo.session_stats(window)

    @app.get("/v1/alerts", summary="Most recent real-time alerts")
    async def get_alerts(
        repo: Repo, limit: Annotated[int, Query(ge=1, le=500)] = 50
    ) -> list[Alert]:
        return await repo.alerts(limit)

    @app.get(
        "/v1/live",
        summary="Live KPI feed (Server-Sent Events)",
        response_class=StreamingResponse,
        responses={200: {"content": {"text/event-stream": {}}}},
    )
    async def live(request: Request, repo: Repo) -> StreamingResponse:
        stream = overview_stream(
            repo,
            settings.live_window_seconds,
            settings.live_interval_seconds,
            request.is_disconnected,
        )
        return StreamingResponse(
            stream,
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", include_in_schema=False)
    async def readyz(request: Request) -> JSONResponse:
        try:
            ready = await request.app.state.runner.ping()
        except Exception:
            log.warning("clickhouse ping failed", exc_info=True)
            ready = False
        return JSONResponse(
            {"status": "ready" if ready else "not ready"},
            status_code=status.HTTP_200_OK if ready else status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    return app
