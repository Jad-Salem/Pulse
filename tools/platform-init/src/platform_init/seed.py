"""Publish the product catalog to its compacted topic."""

import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol


class Producer(Protocol):
    def produce(self, topic: str, value: bytes, key: bytes) -> None: ...

    def flush(self, timeout: float) -> int: ...


def load_products(path: Path) -> list[dict[str, Any]]:
    products: list[dict[str, Any]] = json.loads(path.read_text(encoding="utf-8"))
    ids = [p["product_id"] for p in products]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate product_id in catalog")
    return products


def to_record(product: dict[str, Any], updated_at: datetime) -> dict[str, Any]:
    return {
        "product_id": product["product_id"],
        "name": product["name"],
        "category": product["category"],
        "brand": product["brand"],
        "list_price_cents": int(product["list_price_cents"]),
        "updated_at": updated_at,
    }


def seed_products(
    producer: Producer,
    serialize: Callable[[dict[str, Any]], bytes],
    topic: str,
    products: list[dict[str, Any]],
) -> int:
    """Upsert every product (keyed by product_id, so compaction keeps the latest)."""
    now = datetime.now(UTC)
    for product in products:
        producer.produce(
            topic, key=product["product_id"].encode(), value=serialize(to_record(product, now))
        )
    if remaining := producer.flush(30):
        raise RuntimeError(f"{remaining} catalog records were not delivered")
    return len(products)
