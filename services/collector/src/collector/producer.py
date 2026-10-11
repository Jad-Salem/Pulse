"""Durable, asyncio-friendly Kafka producer for the events.raw topic.

librdkafka is thread-based: produce() only enqueues, and delivery reports are delivered by poll().
A dedicated poll thread services delivery reports and resolves an asyncio future per record, so a
request handler can await the broker acknowledgement (acks=all) without blocking the event loop.
"""

import asyncio
import threading
from typing import Any, Protocol

import structlog
from confluent_kafka import KafkaError, KafkaException, Message, Producer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from collector.config import Settings

log = structlog.get_logger(__name__)


class ProducerUnavailableError(Exception):
    """The record could not be accepted right now; the client should retry later."""


class EventSink(Protocol):
    """What the HTTP layer needs from a producer (lets tests substitute a fake)."""

    def start(self) -> None: ...

    async def send(self, key: str, value: dict[str, Any]) -> None: ...

    def is_ready(self, timeout: float) -> bool: ...

    def close(self, timeout: float) -> int: ...


class KafkaEventSink:
    def __init__(self, settings: Settings) -> None:
        self._topic = settings.topic
        self._subject = settings.value_subject
        self._schema_str = settings.schema_path.read_text(encoding="utf-8")
        self._registry = SchemaRegistryClient({"url": settings.schema_registry_url})
        # Contracts are registered by platform-init; a producer must never change them.
        self._serializer = AvroSerializer(
            self._registry,
            self._schema_str,
            conf={"auto.register.schemas": False, "use.latest.version": False},
        )
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "client.id": "pulse-collector",
                "enable.idempotence": True,  # implies acks=all, ordered, no duplicates on retry
                "acks": "all",
                "compression.type": "zstd",
                "linger.ms": 5,
                "delivery.timeout.ms": settings.delivery_timeout_ms,
                "queue.buffering.max.messages": 200_000,
            }
        )
        self._stop = threading.Event()
        self._poller = threading.Thread(target=self._poll_loop, name="kafka-poll", daemon=True)

    def start(self) -> None:
        self._poller.start()

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            self._producer.poll(0.1)

    async def send(self, key: str, value: dict[str, Any]) -> None:
        loop = asyncio.get_running_loop()
        delivered: asyncio.Future[None] = loop.create_future()

        def on_delivery(err: KafkaError | None, _msg: Message) -> None:
            loop.call_soon_threadsafe(_settle, delivered, err)

        payload = self._serializer(value, SerializationContext(self._topic, MessageField.VALUE))
        try:
            self._producer.produce(
                self._topic, key=key.encode(), value=payload, on_delivery=on_delivery
            )
        except BufferError as exc:  # local queue full: shed load instead of buffering unboundedly
            raise ProducerUnavailableError("producer queue is full") from exc
        await delivered

    def is_ready(self, timeout: float) -> bool:
        """Kafka metadata for the topic is reachable and our schema is registered."""
        try:
            metadata = self._producer.list_topics(self._topic, timeout=timeout)
            topic = metadata.topics.get(self._topic)
            if topic is None or topic.error is not None:
                return False
            self._registry.lookup_schema(self._subject, Schema(self._schema_str, "AVRO"))
        except Exception:
            log.warning("readiness check failed", exc_info=True)
            return False
        return True

    def close(self, timeout: float) -> int:
        self._stop.set()
        if self._poller.is_alive():
            self._poller.join(timeout)
        remaining: int = self._producer.flush(timeout)
        if remaining:
            log.error("records not delivered at shutdown", remaining=remaining)
        return remaining


def _settle(future: asyncio.Future[None], err: KafkaError | None) -> None:
    if future.done():
        return
    if err is None:
        future.set_result(None)
    else:
        future.set_exception(ProducerUnavailableError(str(KafkaException(err))))
