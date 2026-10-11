"""Register the Avro contracts in Schema Registry before any producer starts."""

import logging
from pathlib import Path
from typing import Any, Protocol

from platform_init.topics import TopicSpec

log = logging.getLogger(__name__)

COMPATIBILITY = "BACKWARD"


class IncompatibleSchemaError(RuntimeError):
    pass


class Registry(Protocol):
    def get_subjects(self) -> list[str]: ...

    def test_compatibility(self, subject_name: str, schema: Any) -> bool: ...

    def set_compatibility(
        self, subject_name: str | None = None, level: str | None = None
    ) -> Any: ...

    def register_schema(self, subject_name: str, schema: Any) -> int: ...


def register_schemas(
    registry: Registry, specs: list[TopicSpec], schemas_dir: Path
) -> dict[str, int]:
    """Register every topic's value schema under <topic>-value. Idempotent.

    An existing subject is checked for compatibility first, so an incompatible change fails the
    deployment here instead of breaking a consumer at runtime.
    """
    from confluent_kafka.schema_registry import Schema

    existing = set(registry.get_subjects())
    registered: dict[str, int] = {}
    for spec in specs:
        if spec.value_schema is None:
            continue
        schema = Schema((schemas_dir / spec.value_schema).read_text(encoding="utf-8"), "AVRO")
        subject = spec.value_subject
        if subject in existing and not registry.test_compatibility(subject, schema):
            raise IncompatibleSchemaError(
                f"{spec.value_schema} is not {COMPATIBILITY}-compatible with {subject}"
            )
        registry.set_compatibility(subject_name=subject, level=COMPATIBILITY)
        registered[subject] = registry.register_schema(subject, schema)
        log.info("registered %s (id %d)", subject, registered[subject])
    return registered
