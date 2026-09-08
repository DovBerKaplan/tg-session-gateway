"""Update Bus — the internal queue between Telegram and consumers (spec §6.2).

Contract:
- poller appends updates + advances the TELEGRAM offset; ack to Telegram
  happens only after the update is safely in the queue
- consumers pull with their own INTERNAL offset; `consumer_ack` drops
  everything up to it — app restarts never lose or duplicate past that
- bounded per bot; overflow policy drop_oldest (default) / reject_new
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass
from typing import Optional


@dataclass
class QueuedUpdate:
    internal_id: int
    update: dict
    enqueued_at: float


class Overflow(Exception):
    """reject_new policy hit the cap."""


class UpdateQueue:
    def __init__(self, maxsize: int = 5000, overflow: str = "drop_oldest",
                 ttl_s: float = 3600.0):
        self.maxsize = maxsize
        self.overflow = overflow
        self.ttl_s = ttl_s
        self._q: deque[QueuedUpdate] = deque()
        self._next_id = 1
        self._consumer_offset = 0  # highest internal_id acked by the consumer
        self._new_data = asyncio.Event()
        self.dropped = 0
        self.expired = 0

    # ── poller side ──────────────────────────────────────────────────

    def push_all(self, updates: list[dict]) -> None:
        self._prune_expired()
        for u in updates:
            self._push(u)

    def _push(self, update: dict) -> None:
        if len(self._q) >= self.maxsize:
            if self.overflow == "reject_new":
                raise Overflow()
            self._q.popleft()  # drop_oldest
            self.dropped += 1
        self._q.append(QueuedUpdate(self._next_id, update, time.monotonic()))
        self._next_id += 1
        self._new_data.set()

    def _prune_expired(self) -> None:
        """Spec §12: queued update content is short-lived — the queue is
        infrastructure, not a conversation store."""
        if self.ttl_s <= 0:
            return
        horizon = time.monotonic() - self.ttl_s
        while self._q and self._q[0].enqueued_at < horizon:
            self._q.popleft()
            self.expired += 1

    # ── consumer side (Bot-API-compatible getUpdates semantics) ──────

    async def pull(self, offset: Optional[int], timeout: float, limit: int = 100) -> list[dict]:
        """Long-poll: wait up to `timeout` for updates newer than `offset`.

        offset here is the INTERNAL id: consumers ack by returning the
        last internal_id they saw, exactly like the real Bot API's
        update_id+1 semantics. Every returned update carries
        `_gw_internal_id` so the ack can flow back transparently."""
        if offset is not None:
            self.ack(offset)
        deadline = time.monotonic() + timeout
        while True:
            pending = [u for u in self._q if u.internal_id > self._consumer_offset]
            if pending:
                out = []
                for q in pending[:limit]:
                    u = dict(q.update)
                    u["_gw_internal_id"] = q.internal_id
                    out.append(u)
                return out
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return []
            self._new_data.clear()
            try:
                await asyncio.wait_for(self._new_data.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                return []

    def ack(self, last_seen_internal_id: int) -> None:
        self._consumer_offset = max(self._consumer_offset, last_seen_internal_id)
        while self._q and self._q[0].internal_id <= self._consumer_offset:
            self._q.popleft()

    # ── push mode ────────────────────────────────────────────────────

    async def wait_for_new(self, timeout: float) -> list[dict]:
        return await self.pull(None, timeout)

    # ── introspection ────────────────────────────────────────────────

    def depth(self) -> int:
        return len(self._q)

    def stats(self) -> dict:
        now = time.monotonic()
        return {
            "depth": len(self._q),
            "dropped_oldest": self.dropped,
            "expired_by_ttl": self.expired,
            "max_age_s": round(max((now - q.enqueued_at for q in self._q), default=0.0), 1),
            "consumer_offset": self._consumer_offset,
        }
