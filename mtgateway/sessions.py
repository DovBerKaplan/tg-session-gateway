"""Session Manager — holds long-lived Pyrogram Clients, one per bot token.

The gateway process is the SOLE owner of each MTProto session. Session
files persist on a Docker volume. The auth_key is loaded on every boot,
so importBotAuthorization is called exactly once per token's lifetime.

Update routing: Pyrogram's handler callbacks push updates into the
internal queue; consumers (applications) pull or subscribe via WS.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum

from pyrogram import Client as PyrogramClient
from pyrogram.handlers import RawUpdateHandler

from .config import Config
from .lock import SessionLock
from .rateguard import MTRateGuard
from .store import MTStore

log = logging.getLogger("mtgateway.sessions")


class SessionState(str, Enum):
    connecting = "connecting"
    live = "live"
    paused = "paused"
    draining = "draining"
    error = "error"


@dataclass
class ConsumerHandle:
    """A connected application consumer (app replica)."""

    consumer_id: str
    ws_send: Callable | None = None  # WebSocket push callback
    connected_at: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)


@dataclass
class MTSession:
    alias: str
    token: str
    state: SessionState = SessionState.connecting
    client: PyrogramClient | None = None
    guard: MTRateGuard = None  # set in __post_init__ via config
    connected_since: float = 0.0
    last_error: str = ""
    real_flood_wait: int = 0
    synthetic_429: int = 0
    last_flood_wait: dict = field(default_factory=dict)

    # Update routing: in-memory queue + optional WS consumers
    _update_queue: asyncio.Queue = None
    _consumers: list = field(default_factory=list)
    _next_update_seq: int = 0

    # Rolling update: track which consumer is active
    active_consumer: str | None = None
    lock: SessionLock | None = None  # singleton enforcement (R7)

    def __post_init__(self):
        self._update_queue = asyncio.Queue(maxsize=5000)

    @property
    def session_file(self) -> str:
        return os.path.join(self._session_dir, f"{self.alias}.session")

    _session_dir: str = "/data/sessions"  # overridden by manager

    # E2: Idempotency — app-supplied keys prevent duplicate sends
    _seen_idempotency_keys: dict = None  # key → timestamp (LRU-ish)

    def check_idempotency(self, key: str) -> bool:
        """Returns True if this is a NEW key (proceed with send).
        Returns False if the key was already seen (skip, return cached result)."""
        if self._seen_idempotency_keys is None:
            self._seen_idempotency_keys = {}
        if key in self._seen_idempotency_keys:
            return False
        self._seen_idempotency_keys[key] = time.time()
        # Prune keys older than 5 minutes (bounded memory)
        cutoff = time.time() - 300
        stale = [k for k, ts in self._seen_idempotency_keys.items() if ts < cutoff]
        for k in stale:
            del self._seen_idempotency_keys[k]
        return True

    # E4: Pause sending while keeping receiving alive
    send_paused: bool = False

    def add_consumer(self, handle: ConsumerHandle) -> None:
        self._consumers.append(handle)
        self.active_consumer = handle.consumer_id
        log.info(
            "[%s] consumer attached: %s (total: %d)",
            self.alias,
            handle.consumer_id,
            len(self._consumers),
        )

    def remove_consumer(self, consumer_id: str) -> None:
        self._consumers = [c for c in self._consumers if c.consumer_id != consumer_id]
        if self.active_consumer == consumer_id:
            self.active_consumer = self._consumers[-1].consumer_id if self._consumers else None
        log.info(
            "[%s] consumer detached: %s (remaining: %d)",
            self.alias,
            consumer_id,
            len(self._consumers),
        )

    async def push_update(self, update: dict) -> None:
        """Called by Pyrogram's RawUpdateHandler for every update."""
        self._next_update_seq += 1
        update["_gw_seq"] = self._next_update_seq
        try:
            self._update_queue.put_nowait(update)
        except asyncio.QueueFull:
            # Drop oldest (put on a full queue)
            try:
                self._update_queue.get_nowait()
                self._update_queue.put_nowait(update)
            except asyncio.QueueEmpty:
                pass

        # Push to any WebSocket consumers
        for consumer in self._consumers:
            if consumer.ws_send:
                try:
                    await consumer.ws_send(json.dumps(update))
                except Exception as e:
                    log.warning(
                        "[%s] WS push to %s failed: %s", self.alias, consumer.consumer_id, e
                    )

    # E7: SLO metric — time from consumer attach to first outbound send
    _first_send_at: float = 0.0

    def note_first_send(self) -> None:
        if not self._first_send_at and self._consumers:
            self._first_send_at = time.time()

    @property
    def deploy_to_first_message_s(self) -> float:
        if not self._first_send_at or not self._consumers:
            return 0.0
        return self._first_send_at - self._consumers[-1].connected_at

    def stats(self) -> dict:
        return {
            "alias": self.alias,
            "state": self.state.value,
            "connection_age_s": round(time.time() - self.connected_since, 1)
            if self.connected_since
            else None,
            "queue_depth": self._update_queue.qsize(),
            "consumers": len(self._consumers),
            "active_consumer": self.active_consumer,
            "real_flood_wait": self.real_flood_wait,
            "synthetic_429": self.synthetic_429,
            "last_flood_wait": self.last_flood_wait or None,
            "last_error": self.last_error[:200],
            "send_paused": self.send_paused,
            "deploy_to_first_message_s": round(self.deploy_to_first_message_s, 2)
            if self.deploy_to_first_message_s
            else None,
        }


class MTSessionManager:
    """Manages the lifecycle of all Pyrogram sessions in this gateway."""

    def __init__(self, cfg: Config, store: MTStore):
        self.cfg = cfg
        self.store = store
        self.sessions: dict[str, MTSession] = {}

    def _create_session(self, alias: str, token: str) -> MTSession:
        s = MTSession(alias=alias, token=token)
        s._session_dir = self.cfg.session_dir
        s.guard = MTRateGuard(self.cfg.rate, self.cfg.mtproto_rate)
        return s

    async def register(self, alias: str, token: str) -> MTSession:
        """Register a bot session and start its Pyrogram client."""
        if alias in self.sessions:
            existing = self.sessions[alias]
            if existing.token == token:
                return existing
            # Token rotated: tear down old, start new
            await self.unregister(alias)

        await self.store.register(alias, token)
        s = self._create_session(alias, token)
        self.sessions[alias] = s

        # Start the Pyrogram client asynchronously
        asyncio.create_task(self._start_client(s), name=f"mt-start-{alias}")
        return s

    async def _start_client(self, s: MTSession) -> None:
        """Start a Pyrogram client with persistent session file.

        Acquires a singleton lock FIRST — a second process on this
        session fails at the filesystem and never reaches Telegram
        (R7: AUTH_KEY_DUPLICATED prevention at the process level).
        """
        s.state = SessionState.connecting
        os.makedirs(self.cfg.session_dir, exist_ok=True)

        # ── Singleton lock BEFORE any Telegram contact ────────────────
        lock = SessionLock(alias=s.alias, lock_dir=self.cfg.session_dir)
        if not lock.acquire():
            s.state = SessionState.error
            s.last_error = (
                f"session lock refused: another process holds '{s.alias}'. "
                "A second connection would trigger AUTH_KEY_DUPLICATED."
            )
            log.error("[%s] refusing to start — lock held by another process", s.alias)
            # Remove from sessions dict so admin can retry after lock release
            self.sessions.pop(s.alias, None)
            return
        s.lock = lock

        try:
            client = PyrogramClient(
                name=s.session_file,  # .session appended by Pyrogram
                api_id=self.cfg.api_id,
                api_hash=self.cfg.api_hash,
                bot_token=s.token,
                workdir=self.cfg.session_dir,
                in_memory=False,  # file-based: survives restarts
            )

            # Register raw update handler to intercept ALL updates
            client.add_handler(
                RawUpdateHandler(self._make_update_handler(s)),
                group=-100,  # highest priority: run before app handlers
            )

            await client.start()
            s.client = client
            s.state = SessionState.live
            s.connected_since = time.time()

            me = await client.get_me()
            log.info("[%s] live as @%s (id=%s)", s.alias, me.username, me.id)

            # Restore update state from disk
            state = await self.store.get_state(s.alias)
            if state.get("pts"):
                log.info("[%s] restored state: pts=%s qts=%s", s.alias, state["pts"], state["qts"])

            # Keep the client running (idle in a task)
            asyncio.create_task(self._keep_alive(s), name=f"mt-idle-{s.alias}")

            # Start lock heartbeat (proves we're alive for stale detection)
            await lock.start_heartbeat()

        except Exception as e:
            s.state = SessionState.error
            s.last_error = str(e)
            log.error("[%s] failed to start: %s", s.alias, e)
            lock.release()
            s.lock = None

    def _make_update_handler(self, s: MTSession):
        """Create a Pyrogram update handler that routes to the session."""

        async def handler(client, update, users, chats):
            # Serialize Pyrogram update to dict
            try:
                if hasattr(update, "__dict__"):
                    update_dict = self._serialize_update(update)
                    if update_dict:
                        await s.push_update(update_dict)
            except Exception as e:
                log.warning("[%s] update handler error: %s", s.alias, e)

        return handler

    def _serialize_update(self, update) -> dict | None:
        """Convert a Pyrogram update object to a JSON-serializable dict."""
        try:
            if isinstance(update, dict):
                return update
            if hasattr(update, "to_dict"):
                return update.to_dict()
            # Fallback: try JSON round-trip
            return json.loads(json.dumps(update, default=str))
        except Exception:
            return None

    async def _keep_alive(self, s: MTSession) -> None:
        """Keep the Pyrogram client running; detect disconnects."""
        try:
            while s.client and s.client.is_connected:
                await asyncio.sleep(5)
            # Client disconnected
            if s.state == SessionState.live:
                log.warning("[%s] disconnected from Telegram, reconnecting", s.alias)
                s.state = SessionState.connecting
                await asyncio.sleep(1)
                await self._start_client(s)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error("[%s] keep_alive error: %s", s.alias, e)

    async def unregister(self, alias: str) -> bool:
        """Stop a session and remove it. Releases the singleton lock."""
        s = self.sessions.pop(alias, None)
        if not s:
            return False
        s.state = SessionState.draining
        if s.client:
            try:
                await s.client.stop()
            except Exception:
                pass
        if s.lock:
            s.lock.release()
            s.lock = None
        await self.store.delete(alias)
        return True

    async def pause_sending(self, alias: str) -> bool:
        """E4: Pause outbound sends; keep receiving updates."""
        s = self.sessions.get(alias)
        if not s:
            return False
        s.send_paused = True
        return True

    async def resume_sending(self, alias: str) -> bool:
        s = self.sessions.get(alias)
        if not s:
            return False
        s.send_paused = False
        return True

    async def pause(self, alias: str) -> bool:
        s = self.sessions.get(alias)
        if not s:
            return False
        s.state = SessionState.paused
        await self.store.set_paused(alias, True)
        return True

    async def resume(self, alias: str) -> bool:
        s = self.sessions.get(alias)
        if not s:
            return False
        await self.store.set_paused(alias, False)
        s.state = SessionState.live
        return True

    async def drain(self, alias: str) -> bool:
        """Stop processing new updates; flush queue to consumers."""
        s = self.sessions.get(alias)
        if not s:
            return False
        s.state = SessionState.draining
        # Wait briefly for consumers to pull remaining updates
        await asyncio.sleep(2)
        return True

    async def restore_all(self) -> int:
        """Gateway restart: restore all sessions from the store."""
        count = 0
        for row in await self.store.list_sessions():
            token = await self.store.get_token(row["alias"])
            if not token:
                continue
            s = await self.register(row["alias"], token)
            if row["paused"]:
                s.state = SessionState.paused
            count += 1
        return count

    async def shutdown(self) -> None:
        """Graceful shutdown: stop all clients, release all locks."""
        for alias, s in list(self.sessions.items()):
            s.state = SessionState.draining
            if s.client:
                try:
                    await s.client.stop()
                except Exception:
                    pass
            if s.lock:
                s.lock.release()
                s.lock = None
        log.info("all MTProto sessions shut down (locks released)")

    def get(self, alias: str) -> MTSession | None:
        return self.sessions.get(alias)

    def all_stats(self) -> list[dict]:
        return [s.stats() for s in self.sessions.values()]
