"""Declarative Kafka topics: create what is missing, converge config on what exists."""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

import yaml

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    replication_factor: int
    config: dict[str, str] = field(default_factory=dict)
    value_schema: str | None = None

    @property
    def value_subject(self) -> str:
        return f"{self.name}-value"


def load_topic_specs(path: Path) -> list[TopicSpec]:
    document: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
    defaults: dict[str, Any] = document.get("defaults", {})
    default_config = {k: str(v) for k, v in defaults.get("config", {}).items()}
    specs = []
    for entry in document["topics"]:
        config = default_config | {k: str(v) for k, v in entry.get("config", {}).items()}
        specs.append(
            TopicSpec(
                name=entry["name"],
                partitions=int(entry["partitions"]),
                replication_factor=int(
                    entry.get("replication_factor", defaults.get("replication_factor", 1))
                ),
                config=config,
                value_schema=entry.get("value_schema"),
            )
        )
    names = [s.name for s in specs]
    if len(names) != len(set(names)):
        raise ValueError(f"duplicate topic names in {path}")
    return specs


class Admin(Protocol):
    def list_topics(self, timeout: float) -> Any: ...

    def create_topics(self, new_topics: list[Any], **kwargs: Any) -> dict[str, Any]: ...

    def incremental_alter_configs(self, resources: list[Any], **kwargs: Any) -> dict[Any, Any]: ...


def ensure_topics(admin: Admin, specs: list[TopicSpec], timeout: float = 30.0) -> list[str]:
    """Create missing topics and apply configured settings to all of them. Returns created names."""
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import (
        AlterConfigOpType,
        ConfigEntry,
        ConfigResource,
        NewTopic,
        ResourceType,
    )

    existing = admin.list_topics(timeout=timeout).topics
    missing = [s for s in specs if s.name not in existing]
    created: list[str] = []
    if missing:
        futures = admin.create_topics(
            [
                NewTopic(
                    s.name,
                    num_partitions=s.partitions,
                    replication_factor=s.replication_factor,
                    config=s.config,
                )
                for s in missing
            ],
            operation_timeout=timeout,
        )
        for name, future in futures.items():
            try:
                future.result()
                created.append(name)
                log.info("created topic %s", name)
            except KafkaException as exc:
                if "TOPIC_ALREADY_EXISTS" not in str(exc):  # lost a race with another init run
                    raise

    for spec in specs:
        if spec.name in existing:
            current = existing[spec.name]
            if len(current.partitions) != spec.partitions:
                log.warning(
                    "topic %s has %d partitions, spec says %d (partition changes are manual)",
                    spec.name,
                    len(current.partitions),
                    spec.partitions,
                )
    resources = [
        ConfigResource(
            ResourceType.TOPIC,
            spec.name,
            incremental_configs=[
                ConfigEntry(k, v, incremental_operation=AlterConfigOpType.SET)
                for k, v in spec.config.items()
            ],
        )
        for spec in specs
        if spec.config
    ]
    if resources:
        for future in admin.incremental_alter_configs(resources).values():
            future.result()
    return created
