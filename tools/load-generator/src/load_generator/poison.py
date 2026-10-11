"""Writes bad records straight to events.raw, bypassing the collector.

This is what a misbehaving upstream producer looks like. The stream processor must route these
to the dead-letter topic and keep going.
"""

import os
import random
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class PoisonProducer:
    def __init__(self, bootstrap_servers: str, schema_registry_url: str, schema_path: Path) -> None:
        from confluent_kafka import Producer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer

        self._producer = Producer(
            {"bootstrap.servers": bootstrap_servers, "client.id": "pulse-chaos"}
        )
        self._serializer = AvroSerializer(
            SchemaRegistryClient({"url": schema_registry_url}),
            schema_path.read_text(encoding="utf-8"),
            conf={"auto.register.schemas": False},
        )
        self.topic = "events.raw"

    def send(self, rng: random.Random) -> str:
        """Send one bad record; returns which kind."""
        from confluent_kafka.serialization import MessageField, SerializationContext

        if rng.random() < 0.5:
            kind, value = "garbage", os.urandom(rng.randint(8, 64))
        else:
            # Schema-valid Avro that breaks the contract: a purchase with no order_id.
            kind = "contract-violation"
            value = self._serializer(
                contract_violation(), SerializationContext(self.topic, MessageField.VALUE)
            )
        self._producer.produce(self.topic, key=b"chaos", value=value)
        self._producer.poll(0)
        return kind

    def close(self) -> None:
        self._producer.flush(5)


def contract_violation() -> dict[str, Any]:
    now = int(datetime.now(UTC).timestamp() * 1000)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "purchase",
        "event_time": now,
        "received_at": now,
        "user_id": "u-chaos",
        "session_id": "s-chaos",
        "page_url": "https://shop.example.com/checkout",
        "referrer": None,
        "product_id": "P0001",
        "quantity": 1,
        "unit_price_cents": 100,
        "order_id": None,
        "country": "US",
        "device": "desktop",
        "user_agent": None,
    }
