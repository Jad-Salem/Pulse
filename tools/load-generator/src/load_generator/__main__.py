"""Run simulated shoppers against the collector until stopped (or for LOADGEN_DURATION_SECONDS)."""

import asyncio
import contextlib
import json
import logging
import random
import signal
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime

import httpx

from load_generator.behavior import Product, Step, Visitor, fraud_session, shopping_session
from load_generator.config import Settings
from load_generator.sender import BatchSender

log = logging.getLogger("load_generator")


async def _sleep(seconds: float, stop: asyncio.Event) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)


async def drive(steps: Iterator[Step], sender: BatchSender, stop: asyncio.Event) -> None:
    for pause, fields in steps:
        await _sleep(pause, stop)
        if stop.is_set():
            return
        sender.submit(
            {"event_id": str(uuid.uuid4()), "event_time": datetime.now(UTC).isoformat(), **fields}
        )


async def shopper(
    catalog: list[Product],
    rng: random.Random,
    settings: Settings,
    sender: BatchSender,
    stop: asyncio.Event,
) -> None:
    visitor = Visitor.random(rng)
    await _sleep(rng.uniform(0, settings.think_time_seconds * 3), stop)  # stagger start-up
    while not stop.is_set():
        await drive(
            shopping_session(visitor, catalog, rng, settings.think_time_seconds), sender, stop
        )
        # Time away between visits; sometimes long enough to start a brand-new visitor.
        await _sleep(rng.expovariate(1 / 20), stop)
        if rng.random() < 0.3:
            visitor = Visitor.random(rng)


async def flusher(
    sender: BatchSender, interval: float, batch_size: int, stop: asyncio.Event
) -> None:
    while not stop.is_set():
        if sender.pending() < batch_size:
            await _sleep(interval, stop)
        await sender.flush()
    await sender.flush()


async def poisoner(
    settings: Settings, rng: random.Random, sender: BatchSender, stop: asyncio.Event
) -> None:
    if (
        not (settings.kafka_bootstrap_servers and settings.schema_registry_url)
        or not settings.poison_per_minute
    ):
        return
    from load_generator.poison import PoisonProducer

    producer = PoisonProducer(
        settings.kafka_bootstrap_servers, settings.schema_registry_url, settings.event_schema_file
    )
    try:
        while not stop.is_set():
            await _sleep(rng.expovariate(settings.poison_per_minute / 60), stop)
            if not stop.is_set():
                kind = await asyncio.to_thread(producer.send, rng)
                sender.stats.poison += 1
                log.debug("sent poison record (%s)", kind)
    finally:
        await asyncio.to_thread(producer.close)


async def reporter(sender: BatchSender, stop: asyncio.Event) -> None:
    while not stop.is_set():
        await _sleep(10, stop)
        log.info("stats %s", sender.stats)


async def run(settings: Settings, install_signals: Callable[[asyncio.Event], None]) -> None:
    catalog = [Product.from_json(p) for p in json.loads(settings.products_file.read_text("utf-8"))]
    master = random.Random(settings.seed)
    stop = asyncio.Event()
    install_signals(stop)

    async with httpx.AsyncClient(
        base_url=settings.collector_url, headers={"X-API-Key": settings.api_key}, timeout=10
    ) as client:
        sender = BatchSender(
            client,
            random.Random(master.random()),
            batch_size=settings.batch_size,
            duplicate_rate=settings.duplicate_rate,
            late_rate=settings.late_rate,
            late_delay=settings.late_delay_seconds,
        )
        tasks = [
            asyncio.create_task(
                shopper(catalog, random.Random(master.random()), settings, sender, stop)
            )
            for _ in range(settings.users)
        ]
        for _ in range(settings.fraud_users):
            bot = Visitor.random(random.Random(master.random()))
            steps = fraud_session(bot, catalog, random.Random(master.random()))
            tasks.append(asyncio.create_task(drive(steps, sender, stop)))
        tasks.append(
            asyncio.create_task(
                flusher(sender, settings.flush_interval_seconds, settings.batch_size, stop)
            )
        )
        tasks.append(
            asyncio.create_task(poisoner(settings, random.Random(master.random()), sender, stop))
        )
        tasks.append(asyncio.create_task(reporter(sender, stop)))
        log.info(
            "started %d shoppers and %d bots against %s",
            settings.users,
            settings.fraud_users,
            settings.collector_url,
        )

        if settings.duration_seconds:
            await _sleep(settings.duration_seconds, stop)
            stop.set()
        await asyncio.gather(*tasks)
        sender.cancel_delayed()
        log.info("finished: %s", sender.stats)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

    def install_signals(stop: asyncio.Event) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):  # not available on Windows
                loop.add_signal_handler(sig, stop.set)

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run(Settings(), install_signals))


if __name__ == "__main__":
    main()
