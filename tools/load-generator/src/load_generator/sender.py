"""Batches events to the collector, with controlled duplication and lateness."""

import asyncio
import copy
import logging
import random
from dataclasses import dataclass
from typing import Any

import httpx

log = logging.getLogger(__name__)


@dataclass
class Stats:
    sent: int = 0
    duplicated: int = 0
    delayed: int = 0
    failed_batches: int = 0
    rejected: int = 0
    poison: int = 0


class BatchSender:
    """Buffers events and POSTs them to /v1/batch.

    Retries reuse the same event_ids, so they are safe (the pipeline de-duplicates). A small,
    configurable share of events is deliberately duplicated (a client retry after a lost response)
    or delayed (a phone that was offline), which is what the pipeline has to cope with in reality.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        rng: random.Random,
        *,
        batch_size: int,
        duplicate_rate: float,
        late_rate: float,
        late_delay: tuple[float, float],
    ) -> None:
        self._client = client
        self._rng = rng
        self._batch_size = batch_size
        self._duplicate_rate = duplicate_rate
        self._late_rate = late_rate
        self._late_delay = late_delay
        self._buffer: list[dict[str, Any]] = []
        self._delayed: set[asyncio.TimerHandle] = set()
        self.stats = Stats()

    def submit(self, event: dict[str, Any]) -> None:
        if self._rng.random() < self._late_rate:
            self.stats.delayed += 1
            delay = self._rng.uniform(*self._late_delay)
            loop = asyncio.get_running_loop()
            handle: asyncio.TimerHandle | None = None

            def release() -> None:
                self._buffer.append(event)
                if handle is not None:
                    self._delayed.discard(handle)

            handle = loop.call_later(delay, release)
            self._delayed.add(handle)
        else:
            self._buffer.append(event)
        if self._rng.random() < self._duplicate_rate:
            self.stats.duplicated += 1
            self._buffer.append(copy.deepcopy(event))

    def pending(self) -> int:
        return len(self._buffer)

    async def flush(self) -> None:
        while self._buffer:
            batch, self._buffer = self._buffer[: self._batch_size], self._buffer[self._batch_size :]
            await self._post(batch)

    async def _post(self, batch: list[dict[str, Any]], attempts: int = 5) -> None:
        delay = 0.5
        for attempt in range(1, attempts + 1):
            try:
                response = await self._client.post("/v1/batch", json={"events": batch})
            except httpx.TransportError as exc:
                log.warning("collector unreachable (attempt %d): %s", attempt, exc)
            else:
                if response.status_code == 202:
                    self.stats.sent += len(batch)
                    return
                if response.status_code == 422:
                    self.stats.rejected += len(batch)
                    log.error("collector rejected a batch: %s", response.text[:500])
                    return
                log.warning("collector returned %d (attempt %d)", response.status_code, attempt)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 8)
        self.stats.failed_batches += 1
        log.error("dropping a batch of %d events after %d attempts", len(batch), attempts)

    def cancel_delayed(self) -> None:
        for handle in self._delayed:
            handle.cancel()
        self._delayed.clear()
