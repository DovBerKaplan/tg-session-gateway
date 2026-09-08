"""Session plane — one poller per token, exclusive ownership (spec §6.1).

States: connecting → live → (paused | draining) → error
On attach: deleteWebhook then long-poll getUpdates (XOR rule, spec §4.6).
The poller is the ONLY thing in the world calling getUpdates upstream.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import httpx

from .config import Config
from .queue import UpdateQueue
from .rateguard import BotRateGuard
from .store import Store

log = logging.getLogger("gateway.session")


class State(str, Enum):
    connecting = "connecting"
    live = "live"
    paused = "paused"
    draining = "draining"
    error = "error"


@dataclass
class BotSession:
    alias: str
    token: str
    state: State = State.connecting
    queue: UpdateQueue = field(default_factory=UpdateQueue)
    guard: BotRateGuard = field(default_factory=BotRateGuard)
    task: Optional[asyncio.Task] = None
    connected_since: float = 0.0
    last_error: str = ""
    real_429: int = 0
    synthetic_429: int = 0
    push_url: Optional[str] = None
    push_task: Optional[asyncio.Task] = None

    def public_status(self) -> dict:
        return {
            "alias": self.alias,
            "state": self.state.value,
            "connected_since": self.connected_since,
            "connection_age_s": round(time.time() - self.connected_since, 1)
            if self.connected_since
            else None,
            "queue": self.queue.stats(),
            "buckets": self.guard.snapshot(),
            "real_429": self.real_429,
            "synthetic_429": self.synthetic_429,
            "last_error": self.last_error[:200],
            "push_url": self.push_url,
        }


class SessionManager:
    def __init__(self, cfg: Config, store: Store, client: httpx.AsyncClient):
        self.cfg = cfg
        self.store = store
        self.client = client
        self.sessions: dict[str, BotSession] = {}

    # ── lifecycle ────────────────────────────────────────────────────

    async def attach(self, alias: str, token: str) -> BotSession:
        if alias in self.sessions:
            return self.sessions[alias]
        s = BotSession(
            alias=alias,
            token=token,
            queue=UpdateQueue(self.cfg.policy.update_queue_max),
            guard=BotRateGuard(self.cfg.rate, self.cfg.policy.on_limit),
        )
        self.sessions[alias] = s
        await self.store.register_bot(alias, token)
        s.task = asyncio.create_task(self._poller(s), name=f"poll-{alias}")
        return s

    async def pause(self, alias: str) -> bool:
        s = self.sessions.get(alias)
        if not s:
            return False
        s.state = State.paused
        await self.store.set_paused(alias, True)
        return True

    async def resume(self, alias: str) -> bool:
        s = self.sessions.get(alias)
        if not s:
            return False
        await self.store.set_paused(alias, False)
        s.state = State.live if s.task and not s.task.done() else State.connecting
        return True

    async def drain(self, alias: str) -> bool:
        """Stop polling after the current batch; offset already persisted."""
        s = self.sessions.get(alias)
        if not s:
            return False
        s.state = State.draining
        return True

    async def delete(self, alias: str) -> bool:
        s = self.sessions.pop(alias, None)
        if not s:
            return False
        s.state = State.draining
        for t in (s.task, s.push_task):
            if t and not t.done():
                t.cancel()
        await self.store.delete_bot(alias)
        return True

    async def restore_all(self) -> int:
        """Gateway restart: rebuild sessions from the store (spec §9.2)."""
        n = 0
        for row in await self.store.list_bots():
            token = await self.store.get_token(row["alias"])
            if not token:
                continue
            s = await self.attach(row["alias"], token)
            if row["paused"]:
                s.state = State.paused
            n += 1
        return n

    async def shutdown(self) -> None:
        for alias, s in list(self.sessions.items()):
            s.state = State.draining
            for t in (s.task, s.push_task):
                if t and not t.done():
                    t.cancel()
        await asyncio.gather(
            *(t for s in self.sessions.values()
              for t in (s.task, s.push_task) if t),
            return_exceptions=True,
        )

    # ── the poller — exclusive getUpdates owner ──────────────────────

    async def _poller(self, s: BotSession) -> None:
        backoff = 1.0
        offset = await self.store.get_offset(s.alias)
        try:
            await self.client.post(
                f"/bot{s.token}/deleteWebhook", json={"drop_pending_updates": False}
            )
        except Exception as e:
            log.warning("[%s] deleteWebhook failed: %s", s.alias, e)
        s.state = State.live
        s.connected_since = time.time()
        log.info("[%s] live (offset=%d)", s.alias, offset)

        while True:
            if s.state in (State.paused, State.draining):
                await asyncio.sleep(0.5)
                continue
            try:
                resp = await self.client.post(
                    f"/bot{s.token}/getUpdates",
                    json={"offset": offset + 1, "timeout": self.cfg.poll_timeout},
                    timeout=self.cfg.poll_timeout + 10,
                )
                data = resp.json()
                if not data.get("ok"):
                    desc = data.get("description", "")
                    params = data.get("parameters", {}) or {}
                    retry = params.get("retry_after")
                    if retry:
                        s.real_429 += 1
                        s.guard.report_429(None, float(retry))
                        await asyncio.sleep(float(retry) + 1)
                        continue
                    log.warning("[%s] getUpdates error: %s", s.alias, desc)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, self.cfg.poll_backoff_max)
                    continue
                backoff = 1.0
                updates = data.get("result", [])
                if updates:
                    # queue FIRST, then persist offset, then loop (spec §6.2)
                    s.queue.push_all(updates)
                    offset = max(u["update_id"] for u in updates)
                    await self.store.save_offset(s.alias, offset)
                    if s.push_url:
                        asyncio.create_task(self._push_to_consumer(s, updates))
                else:
                    s.state = State.live
            except asyncio.CancelledError:
                raise
            except Exception as e:
                s.state = State.error
                s.last_error = str(e)
                log.warning("[%s] poller error: %s — backing off", s.alias, e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.cfg.poll_backoff_max)
                s.state = State.live

    async def _push_to_consumer(self, s: BotSession, updates: list[dict]) -> None:
        """Push mode: internal webhook to the consumer (spec §10.1)."""
        try:
            await self.client.post(s.push_url, json={"updates": updates}, timeout=10)
        except Exception as e:
            log.info("[%s] push to consumer failed (queue keeps growing): %s", s.alias, e)

    def find_by_token(self, token: str) -> Optional[BotSession]:
        for s in self.sessions.values():
            if s.token == token:
                return s
        return None
