"""HTTP API: POST /v1/events, POST /v1/batch, health and metrics."""

import asyncio
import secrets
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Annotated

import structlog
from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from collector.config import Settings
from collector.models import Accepted, EventBatch, EventIn
from collector.observability import Metrics, configure_logging, http_metrics_middleware
from collector.producer import EventSink, KafkaEventSink, ProducerUnavailableError

log = structlog.get_logger(__name__)


def create_app(settings: Settings | None = None, sink: EventSink | None = None) -> FastAPI:
    settings = settings or Settings()  # values come from the environment
    configure_logging(settings.log_level, settings.log_json)
    metrics = Metrics.create()
    event_sink = sink or KafkaEventSink(settings)
    max_skew = timedelta(seconds=settings.max_clock_skew_seconds)
    max_age = timedelta(seconds=settings.max_event_age_seconds)
    api_keys = settings.api_key_set
    if not api_keys:
        raise ValueError("COLLECTOR_API_KEYS must contain at least one key")

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        event_sink.start()
        log.info("collector started", topic=settings.topic)
        yield
        await asyncio.to_thread(event_sink.close, 10.0)
        log.info("collector stopped")

    app = FastAPI(
        title="Pulse Collector",
        version="0.1.0",
        summary="Accepts behavioural events and produces them to Kafka.",
        lifespan=lifespan,
    )
    app.middleware("http")(http_metrics_middleware(metrics))

    def require_api_key(x_api_key: Annotated[str | None, Header()] = None) -> None:
        if x_api_key is None or not any(secrets.compare_digest(x_api_key, k) for k in api_keys):
            metrics.events_rejected.labels("unauthorized").inc()
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing or invalid X-API-Key")

    @app.exception_handler(RequestValidationError)
    async def on_invalid(_: Request, exc: RequestValidationError) -> JSONResponse:
        metrics.events_rejected.labels("invalid").inc()
        return JSONResponse(
            {"detail": jsonable_errors(exc)}, status_code=status.HTTP_422_UNPROCESSABLE_CONTENT
        )

    async def accept(events: list[EventIn]) -> Accepted:
        received_at = datetime.now(UTC)
        for index, event in enumerate(events):
            if reason := event.clock_violation(received_at, max_skew, max_age):
                metrics.events_rejected.labels("clock").inc()
                raise HTTPException(
                    status.HTTP_422_UNPROCESSABLE_CONTENT,
                    [{"loc": ["body", "events", index, "event_time"], "msg": reason}],
                )

        start = time.perf_counter()
        results = await asyncio.gather(
            *(event_sink.send(e.user_id, e.to_avro(received_at)) for e in events),
            return_exceptions=True,
        )
        metrics.produce_latency.observe(time.perf_counter() - start)

        for result in results:
            # Anything other than a transient producer failure is a bug: surface it as a 500.
            if isinstance(result, BaseException) and not isinstance(
                result, ProducerUnavailableError
            ):
                raise result
        failures = [r for r in results if isinstance(r, ProducerUnavailableError)]
        for event, result in zip(events, results, strict=True):
            if not isinstance(result, BaseException):
                metrics.events_accepted.labels(event.event_type).inc()
        if failures:
            metrics.events_rejected.labels("unavailable").inc(len(failures))
            log.warning(
                "produce failed", failed=len(failures), total=len(events), error=str(failures[0])
            )
            # Events carry idempotency keys, so a client retry of the whole batch is safe:
            # duplicates are removed downstream by event_id.
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "event pipeline temporarily unavailable; retry with the same event_ids",
                headers={"Retry-After": "1"},
            )
        return Accepted(accepted=len(events))

    @app.post(
        "/v1/events",
        status_code=status.HTTP_202_ACCEPTED,
        dependencies=[Depends(require_api_key)],
        summary="Ingest one event",
    )
    async def ingest_one(event: EventIn) -> Accepted:
        return await accept([event])

    @app.post(
        "/v1/batch",
        status_code=status.HTTP_202_ACCEPTED,
        dependencies=[Depends(require_api_key)],
        summary="Ingest up to 500 events atomically (all validated before any is produced)",
    )
    async def ingest_batch(batch: EventBatch) -> Accepted:
        return await accept(batch.events)

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", include_in_schema=False)
    async def readyz() -> JSONResponse:
        ready = await asyncio.to_thread(event_sink.is_ready, 3.0)
        return JSONResponse(
            {"status": "ready" if ready else "not ready"},
            status_code=status.HTTP_200_OK if ready else status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    @app.get("/metrics", include_in_schema=False)
    async def prometheus_metrics() -> Response:
        return Response(generate_latest(metrics.registry), media_type=CONTENT_TYPE_LATEST)

    return app


def jsonable_errors(exc: RequestValidationError) -> list[dict[str, object]]:
    """Validation errors without the echoed input (which may be large or sensitive)."""
    return [
        {"loc": list(err.get("loc", ())), "msg": err.get("msg", ""), "type": err.get("type", "")}
        for err in exc.errors()
    ]
