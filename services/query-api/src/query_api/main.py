"""ASGI entrypoint: `uvicorn query_api.main:app`."""

import logging
import sys

import structlog

from query_api.app import create_app
from query_api.config import Settings

settings = Settings()
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer()
        if settings.log_json
        else structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(
        logging.getLevelName(settings.log_level.upper())
    ),
    logger_factory=structlog.PrintLoggerFactory(sys.stdout),
)
app = create_app(settings)
