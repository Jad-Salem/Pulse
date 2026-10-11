# syntax=docker/dockerfile:1.7
# Build context: repository root (needs data/products.json and schemas/avro).
FROM python:3.13-slim-bookworm AS build
COPY --from=ghcr.io/astral-sh/uv:0.13.0 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /src
COPY tools/load-generator/pyproject.toml tools/load-generator/uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv uv sync --locked --no-dev --no-install-project
COPY tools/load-generator/src ./src
RUN --mount=type=cache,target=/root/.cache/uv uv sync --locked --no-dev --no-editable

FROM python:3.13-slim-bookworm
RUN useradd --system --uid 10001 --no-create-home app
COPY --from=build /opt/venv /opt/venv
WORKDIR /app
COPY data/products.json data/products.json
COPY schemas/avro/event.avsc schemas/avro/event.avsc
ENV PATH=/opt/venv/bin:$PATH PYTHONUNBUFFERED=1
USER app
CMD ["load-generator"]
