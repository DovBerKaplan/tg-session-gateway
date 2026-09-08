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


@dataclass
class QueuedUpdate:
    update_id: int  # Telegram's own id — the consumer speaks this natively
    update: dict
    enqueued_at: float


class Overflow(Exception):
    """reject_new policy hit the cap."""


class UpdateQueue:
    def __init__(self, maxsize: int = 5000, overflow: str = "drop_oldest", ttl_s: float = 3600.0):
        self.maxsize = maxsize
        self.overflow = overflow
        self.ttl_s = ttl_s
        self._q: deque[QueuedUpdate] = deque()
        self._seen_ids: set[int] = set()  # Telegram redelivery dedup
        self._consumer_offset = 0  # highest TELEGRAM update_id acked
        self._new_data = asyncio.Event()
        self.dropped = 0
        self.expired = 0

    # ── poller side ──────────────────────────────────────────────────

    def push_all(self, updates: list[dict]) -> None:
        self._prune_expired()
        for u in updates:
            self._push(u)

    def _push(self, update: dict) -> None:
        uid = update.get("update_id")
        if uid is None:
            uid = (self._q[-1].update_id + 1) if self._q else self._consumer_offset + 1
            update = dict(update)
            update["update_id"] = uid
        if uid in self._seen_ids:
            return  # Telegram redelivered — already queued
        if len(self._q) >= self.maxsize:
            if self.overflow == "reject_new":
                raise Overflow()
            old = self._q.popleft()  # drop_oldest
            self._seen_ids.discard(old.update_id)
            self.dropped += 1
        self._q.append(QueuedUpdate(uid, update, time.monotonic()))
        self._seen_ids.add(uid)
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

    async def pull(self, offset: int | None, timeout: float, limit: int = 100) -> list[dict]:
        """Long-poll with REAL Bot API semantics: offset is Telegram's
        update_id (+1 style) exactly as aiogram/PTB/grammY send it.
        The updates returned are Telegram's verbatim — no extra fields."""
        if offset is not None:
            self.ack(offset)
        deadline = time.monotonic() + timeout
        while True:
            pending = [u for u in self._q if u.update_id > self._consumer_offset]
            if pending:
                return [q.update for q in pending[:limit]]
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return []
            self._new_data.clear()
            try:
                await asyncio.wait_for(self._new_data.wait(), timeout=remaining)
            except TimeoutError:
                return []

    def ack(self, last_seen_update_id: int) -> None:
        """Consumers ack exactly like against Telegram: the next getUpdates
        carries update_id+1; anything ≤ it is dropped from the queue."""
        self._consumer_offset = max(self._consumer_offset, last_seen_update_id)
        while self._q and self._q[0].update_id <= self._consumer_offset:
            old = self._q.popleft()
            self._seen_ids.discard(old.update_id)

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
