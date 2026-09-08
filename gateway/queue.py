"""Update Bus — the internal queue between Telegram and consumers (spec §6.2).

Contract:
- poller appends updates + advances the TELEGRAM offset; ack to Telegram
  happens only after the update is safely in the queue
- consumers pull with their own offset; `ack` drops everything up to it
- bounded per bot; overflow policy drop_oldest (default) / reject_new
- DUAL-LAYER: in-memory deque for hot-path performance + SQLite WAL for
  persistence across gateway restarts (review §4: "a session gateway
  whose queue dies with the process has not finished the claim")

On restart, unacked updates are reloaded from SQLite. The consumer
offset is also persisted, so after a gateway restart the consumer
resumes exactly where it left off — no duplicates, no gaps.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import time
from collections import deque
from dataclasses import dataclass

log = logging.getLogger("gateway.queue")


@dataclass
class QueuedUpdate:
    update_id: int  # Telegram's own id — the consumer speaks this natively
    update: dict
    enqueued_at: float


class Overflow(Exception):
    """reject_new policy hit the cap."""


class UpdateQueue:
    """Bounded update queue with optional SQLite persistence.

    Args:
        maxsize: Maximum in-memory queue depth before overflow policy.
        overflow: "drop_oldest" (default) or "reject_new".
        ttl_s: Updates older than this are pruned (spec §12). 0 disables.
        persist_path: SQLite file path for persistence. None = memory-only.
        bot_alias: Identifier for this queue's bot in the persistence DB.
    """

    def __init__(
        self,
        maxsize: int = 5000,
        overflow: str = "drop_oldest",
        ttl_s: float = 3600.0,
        persist_path: str | None = None,
        bot_alias: str = "",
    ):
        self.maxsize = maxsize
        self.overflow = overflow
        self.ttl_s = ttl_s
        self._q: deque[QueuedUpdate] = deque()
        self._seen_ids: set[int] = set()
        self._consumer_offset = 0
        self._new_data = asyncio.Event()
        self.dropped = 0
        self.expired = 0
        self._db: sqlite3.Connection | None = None
        self._bot_alias = bot_alias

        if persist_path:
            self._init_db(persist_path)

    # ── SQLite persistence ────────────────────────────────────────────

    def _init_db(self, path: str) -> None:
        """Open SQLite WAL and create schema. Load unacked updates."""
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._db = sqlite3.connect(path)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS update_queue (
                bot_alias   TEXT NOT NULL,
                update_id   INTEGER NOT NULL,
                update_json TEXT NOT NULL,
                enqueued_at REAL NOT NULL,
                PRIMARY KEY (bot_alias, update_id)
            );
            CREATE TABLE IF NOT EXISTS consumer_offsets (
                bot_alias       TEXT PRIMARY KEY,
                consumer_offset INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        self._db.commit()
        self._load_from_db()

    def _load_from_db(self) -> None:
        """Reload unacked updates and consumer offset after a restart."""
        if not self._db:
            return
        try:
            # Restore consumer offset
            row = self._db.execute(
                "SELECT consumer_offset FROM consumer_offsets WHERE bot_alias = ?",
                (self._bot_alias,),
            ).fetchone()
            if row:
                self._consumer_offset = row[0]

            # Reload unacked updates (id > consumer_offset, within TTL)
            cutoff = time.time() - self.ttl_s if self.ttl_s > 0 else 0
            rows = self._db.execute(
                "SELECT update_id, update_json, enqueued_at FROM update_queue "
                "WHERE bot_alias = ? AND update_id > ? AND enqueued_at > ? "
                "ORDER BY update_id LIMIT ?",
                (self._bot_alias, self._consumer_offset, cutoff, self.maxsize),
            ).fetchall()

            for uid, ujson, _ts in rows:
                update = json.loads(ujson)
                # Use wall-clock for reloaded items (monotonic resets on boot)
                enqueued = time.monotonic() - (time.time() - _ts)
                self._q.append(QueuedUpdate(uid, update, enqueued))
                self._seen_ids.add(uid)

            # Remove acked entries from DB (cleanup)
            self._db.execute(
                "DELETE FROM update_queue WHERE bot_alias = ? AND update_id <= ?",
                (self._bot_alias, self._consumer_offset),
            )
            self._db.commit()

            if rows:
                log.info(
                    "[%s] restored %d unacked updates from disk (offset=%d)",
                    self._bot_alias,
                    len(rows),
                    self._consumer_offset,
                )
        except Exception as e:
            log.warning("[%s] failed to reload from SQLite: %s", self._bot_alias, e)

    def _persist_push(self, uid: int, update: dict) -> None:
        if not self._db:
            return
        try:
            self._db.execute(
                "INSERT OR IGNORE INTO update_queue (bot_alias, update_id, update_json, enqueued_at) "
                "VALUES (?, ?, ?, ?)",
                (self._bot_alias, uid, json.dumps(update), time.time()),
            )
            self._db.commit()
        except Exception as e:
            log.warning("[%s] persist push failed for %d: %s", self._bot_alias, uid, e)

    def _persist_ack(self, last_seen: int) -> None:
        if not self._db:
            return
        try:
            self._db.execute(
                "DELETE FROM update_queue WHERE bot_alias = ? AND update_id <= ?",
                (self._bot_alias, last_seen),
            )
            self._db.execute(
                "INSERT INTO consumer_offsets (bot_alias, consumer_offset) VALUES (?, ?) "
                "ON CONFLICT(bot_alias) DO UPDATE SET consumer_offset = excluded.consumer_offset",
                (self._bot_alias, last_seen),
            )
            self._db.commit()
        except Exception as e:
            log.warning("[%s] persist ack failed at %d: %s", self._bot_alias, last_seen, e)

    def _persist_cleanup(self, cutoff_wall: float) -> None:
        """Remove TTL-expired entries from SQLite."""
        if not self._db:
            return
        try:
            self._db.execute(
                "DELETE FROM update_queue WHERE bot_alias = ? AND enqueued_at < ?",
                (self._bot_alias, cutoff_wall),
            )
            self._db.commit()
        except Exception:
            pass

    def close(self) -> None:
        """Flush and close the persistence DB."""
        if self._db:
            try:
                self._db.commit()
                self._db.close()
            except Exception:
                pass
            self._db = None

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
        self._persist_push(uid, update)
        self._new_data.set()

    def _prune_expired(self) -> None:
        """Spec §12: queued update content is short-lived."""
        if self.ttl_s <= 0:
            return
        horizon = time.monotonic() - self.ttl_s
        wall_cutoff = time.time() - self.ttl_s
        while self._q and self._q[0].enqueued_at < horizon:
            old = self._q.popleft()
            self._seen_ids.discard(old.update_id)
            self.expired += 1
        if self._db:
            self._persist_cleanup(wall_cutoff)

    # ── consumer side (Bot-API-compatible getUpdates semantics) ──────

    async def pull(self, offset: int | None, timeout: float, limit: int = 100) -> list[dict]:
        """Long-poll with REAL Bot API semantics: offset is Telegram's
        update_id (+1 style) exactly as aiogram/PTB/grammY send it."""
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
        """Consumers ack exactly like against Telegram."""
        self._consumer_offset = max(self._consumer_offset, last_seen_update_id)
        while self._q and self._q[0].update_id <= self._consumer_offset:
            old = self._q.popleft()
            self._seen_ids.discard(old.update_id)
        self._persist_ack(self._consumer_offset)

    # ── push mode ────────────────────────────────────────────────────

    async def wait_for_new(self, timeout: float) -> list[dict]:
        return await self.pull(None, timeout)

    # ── introspection ────────────────────────────────────────────────

    def depth(self) -> int:
        return len(self._q)

    @property
    def persistent(self) -> bool:
        return self._db is not None

    def stats(self) -> dict:
        now = time.monotonic()
        return {
            "depth": len(self._q),
            "dropped_oldest": self.dropped,
            "expired_by_ttl": self.expired,
            "max_age_s": round(max((now - q.enqueued_at for q in self._q), default=0.0), 1),
            "consumer_offset": self._consumer_offset,
            "persistent": self.persistent,
        }
