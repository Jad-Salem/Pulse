# syntax=docker/dockerfile:1.7
# Build context: repository root (the job applies the repo's declarative definitions).
FROM python:3.13-slim-bookworm AS build
COPY --from=ghcr.io/astral-sh/uv:0.13.0 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /src
COPY tools/platform-init/pyproject.toml tools/platform-init/uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv uv sync --locked --no-dev --no-install-project
COPY tools/platform-init/src ./src
RUN --mount=type=cache,target=/root/.cache/uv uv sync --locked --no-dev --no-editable

FROM python:3.13-slim-bookworm
RUN useradd --system --uid 10001 --no-create-home app
COPY --from=build /opt/venv /opt/venv
WORKDIR /app
COPY schemas/avro schemas/avro
COPY infra/kafka/topics.yaml infra/kafka/topics.yaml
COPY infra/clickhouse/migrations infra/clickhouse/migrations
COPY data/products.json data/products.json
ENV PATH=/opt/venv/bin:$PATH PYTHONUNBUFFERED=1
USER app
CMD ["platform-init"]
