# syntax=docker/dockerfile:1.7
# Build context: repository root (the image needs schemas/avro).
FROM python:3.13-slim-bookworm AS build
COPY --from=ghcr.io/astral-sh/uv:0.13.0 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /src
COPY services/collector/pyproject.toml services/collector/uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv uv sync --locked --no-dev --no-install-project
COPY services/collector/src ./src
RUN --mount=type=cache,target=/root/.cache/uv uv sync --locked --no-dev --no-editable

FROM python:3.13-slim-bookworm
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app
COPY --from=build /opt/venv /opt/venv
COPY schemas/avro/event.avsc /app/schemas/avro/event.avsc
WORKDIR /app
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    COLLECTOR_SCHEMA_PATH=/app/schemas/avro/event.avsc
USER app
EXPOSE 8000
# One worker per container: scale out with replicas so each process owns its producer and metrics.
CMD ["uvicorn", "collector.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
