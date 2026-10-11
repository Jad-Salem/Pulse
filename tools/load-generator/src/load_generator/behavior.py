"""Shopper behaviour as pure generators: (pause_seconds, event_fields) steps.

event_id and event_time are stamped when a step is actually emitted, so these generators are
deterministic for a given RNG and easy to test.
"""

import random
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

BASE_URL = "https://shop.example.com"

COUNTRIES = {
    "US": 30,
    "GB": 10,
    "DE": 9,
    "IN": 8,
    "FR": 7,
    "BR": 6,
    "CA": 6,
    "JP": 5,
    "AU": 4,
    "EG": 4,
    "NL": 4,
    "ES": 4,
    "MX": 3,
}
DEVICES = {"desktop": 45, "mobile": 45, "tablet": 10}
USER_AGENTS = {
    "desktop": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PulseLoadGen/1.0",
    "mobile": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) PulseLoadGen/1.0",
    "tablet": "Mozilla/5.0 (iPad; CPU OS 18_0 like Mac OS X) PulseLoadGen/1.0",
}
REFERRERS = [
    None,
    None,
    "https://www.google.com/",
    "https://news.example.org/",
    "https://social.example.net/",
]

Step = tuple[float, dict[str, Any]]


@dataclass(frozen=True)
class Product:
    product_id: str
    name: str
    category: str
    list_price_cents: int

    @classmethod
    def from_json(cls, raw: dict[str, Any]) -> "Product":
        return cls(raw["product_id"], raw["name"], raw["category"], int(raw["list_price_cents"]))


@dataclass(frozen=True)
class Visitor:
    user_id: str
    country: str
    device: str

    @classmethod
    def random(cls, rng: random.Random) -> "Visitor":
        return cls(
            user_id=f"u-{uuid.UUID(int=rng.getrandbits(128)).hex[:12]}",
            country=_weighted(rng, COUNTRIES),
            device=_weighted(rng, DEVICES),
        )


def _weighted(rng: random.Random, weights: dict[str, int]) -> str:
    return rng.choices(list(weights), weights=list(weights.values()))[0]


class _Session:
    def __init__(self, visitor: Visitor, rng: random.Random) -> None:
        self.visitor = visitor
        self.rng = rng
        self.session_id = f"s-{uuid.UUID(int=rng.getrandbits(128)).hex[:16]}"

    def event(self, event_type: str, page: str, **fields: Any) -> dict[str, Any]:
        return {
            "event_type": event_type,
            "user_id": self.visitor.user_id,
            "session_id": self.session_id,
            "page_url": BASE_URL + page,
            "country": self.visitor.country,
            "device": self.visitor.device,
            "user_agent": USER_AGENTS[self.visitor.device],
            **fields,
        }

    def price(self, product: Product) -> int:
        discount = self.rng.choice([0, 0, 0, 5, 10, 20])
        return product.list_price_cents * (100 - discount) // 100

    def new_order_id(self) -> str:
        return f"o-{uuid.UUID(int=self.rng.getrandbits(128)).hex[:16]}"


def shopping_session(
    visitor: Visitor, catalog: list[Product], rng: random.Random, think_time: float
) -> Iterator[Step]:
    """One visit: land, browse, maybe build a cart, maybe check out and buy."""
    session = _Session(visitor, rng)
    categories = sorted({p.category for p in catalog})

    def pause() -> float:
        return min(rng.expovariate(1 / think_time), think_time * 6)

    landing = rng.choice(["/", "/", f"/c/{rng.choice(categories)}"])
    yield 0.0, session.event("page_view", landing, referrer=rng.choice(REFERRERS))

    viewed: Product | None = None
    cart: dict[str, tuple[Product, int, int]] = {}
    for _ in range(rng.randint(1, 15)):
        roll = rng.random()
        if viewed is not None and roll < 0.22:
            quantity = rng.choice([1, 1, 1, 2, 3])
            price = session.price(viewed)
            cart[viewed.product_id] = (viewed, quantity, price)
            yield (
                pause(),
                session.event(
                    "add_to_cart",
                    f"/p/{viewed.product_id}",
                    product_id=viewed.product_id,
                    quantity=quantity,
                    unit_price_cents=price,
                ),
            )
        elif cart and roll < 0.27:
            product, quantity, price = cart.pop(rng.choice(sorted(cart)))
            yield (
                pause(),
                session.event(
                    "remove_from_cart",
                    "/cart",
                    product_id=product.product_id,
                    quantity=quantity,
                    unit_price_cents=price,
                ),
            )
        elif cart and roll < 0.40:
            yield pause(), session.event("checkout_started", "/checkout")
            if rng.random() < 0.65:
                yield from _purchase(session, cart, pause())
            return
        elif roll < 0.70:
            viewed = rng.choice(catalog)
            yield (
                pause(),
                session.event(
                    "product_view", f"/p/{viewed.product_id}", product_id=viewed.product_id
                ),
            )
        elif roll < 0.93:
            yield pause(), session.event("page_view", f"/c/{rng.choice(categories)}")
        else:
            return  # left the site


def _purchase(
    session: _Session, cart: dict[str, tuple[Product, int, int]], pause: float
) -> Iterator[Step]:
    """One purchase event per cart line, all sharing the order_id, emitted together."""
    order_id = session.new_order_id()
    for i, (product, quantity, price) in enumerate(cart.values()):
        yield (
            (pause if i == 0 else 0.0),
            session.event(
                "purchase",
                f"/order/{order_id}",
                product_id=product.product_id,
                quantity=quantity,
                unit_price_cents=price,
                order_id=order_id,
            ),
        )


def fraud_session(visitor: Visitor, catalog: list[Product], rng: random.Random) -> Iterator[Step]:
    """A card-testing bot: cheap single-item orders every few seconds, forever."""
    session = _Session(visitor, rng)
    cheapest = sorted(catalog, key=lambda p: p.list_price_cents)[:5]
    while True:
        product = rng.choice(cheapest)
        price = product.list_price_cents
        yield (
            rng.uniform(2, 6),
            session.event(
                "add_to_cart",
                f"/p/{product.product_id}",
                product_id=product.product_id,
                quantity=1,
                unit_price_cents=price,
            ),
        )
        yield rng.uniform(0.5, 1.5), session.event("checkout_started", "/checkout")
        yield from _purchase(
            session, {product.product_id: (product, 1, price)}, rng.uniform(0.5, 2)
        )
