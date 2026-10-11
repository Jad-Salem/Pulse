"""ASGI entrypoint: `uvicorn collector.main:app`."""

from collector.app import create_app

app = create_app()
