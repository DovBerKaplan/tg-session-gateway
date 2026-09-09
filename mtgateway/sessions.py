"""Session Manager — holds long-lived Pyrogram Clients, one per bot token.

The gateway process is the SOLE owner of each MTProto session. Session
files persist on a Docker volume. The auth_key is loaded on every boot,
so importBotAuthorization is called exactly once per token's lifetime.

Update routing: Pyrogram's handler callbacks push updates into the
internal queue; consumers (applications) pull or subscribe via WS.
"""

from __future__ import annotations

import asyncio

from typing import TYPE_CHECKING
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
    guard: MTRateGuard = field(default_factory=MTRateGuard)  # replaced by manager
    connected_since: float = 0.0
    last_error: str = ""
    real_flood_wait: int = 0
    synthetic_429: int = 0
    last_flood_wait: dict = field(default_factory=dict)

    # Update routing: in-memory queue + optional WS consumers
    _update_queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=5000))
    _consumers: list = field(default_factory=list)
    _next_update_seq: int = 0

    # Rolling update: track which consumer is active
    active_consumer: str | None = None
    lock: SessionLock | None = None  # singleton enforcement (R7)

    _session_dir: str = "/data/sessions"  # overridden by manager

    @property
    def session_file(self) -> str:
        return os.path.join(self._session_dir, f"{self.alias}.session")

    # E2: Idempotency — app-supplied keys prevent duplicate sends
    _seen_idempotency_keys: dict[str, float] = field(default_factory=dict)  # fast-path cache
    _store: "MTStore | None" = None  # injected by the manager

    if TYPE_CHECKING:
        from .store import MTStore

    async def check_idempotency(self, key: str) -> bool:
        """True = NEW key (proceed with send); False = replay (suppress).

        Hardening charter: keys are persisted (SQLite) so a kill -9 in
        the deploy window cannot double-send; the in-memory dict stays
        only as a fast-path cache in front of the store.
        """
        now = time.time()
        ts = self._seen_idempotency_keys.get(key)
        if ts is not None and now - ts < 300:
            return False
        if await self._idempotency_store().idempotency_seen(self.alias, key):
            self._seen_idempotency_keys[key] = now
            return False
        await self._idempotency_store().idempotency_record(self.alias, key)
        self._seen_idempotency_keys[key] = now
        # bounded memory: drop stale cache entries
        cutoff = now - 300
        stale = [k for k, t in self._seen_idempotency_keys.items() if t < cutoff]
        for k in stale:
            del self._seen_idempotency_keys[k]
        return True

    def _idempotency_store(self):
        """Store handle — injected by the manager at registration."""
        return self._store

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
        """Called by Pyrogram's RawUpdateHandler for every update.

        Hardening charter: write-through persistence — unacked updates
        survive kill -9 and replay after restart (chaos-soak proven).
        Pull consumers ack on delivery (queue_ack); WS push stays
        at-least-once (replayed after restart until a pull acks them).
        """
        self._next_update_seq += 1
        update["_gw_seq"] = self._next_update_seq
        try:
            if self._store is not None:
                await self._store.queue_push(self.alias, update["_gw_seq"], json.dumps(update))
        except Exception:
            pass  # memory path still works; persistence is best-effort
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
        s._store = self.store
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
        asyncio.create_task(self._start_with_retries(s), name=f"mt-start-{alias}")
        return s

    async def _start_client(self, s: MTSession) -> None:
        """One-shot start: lock, connect, or fail (used by reconnects)."""
        s.state = SessionState.connecting
        os.makedirs(self.cfg.session_dir, mode=0o700, exist_ok=True)
        if not self._acquire_lock_for(s):
            s.state = SessionState.error
            s.last_error = (
                f"session lock refused: another process holds '{s.alias}'. "
                "A second connection would trigger AUTH_KEY_DUPLICATED."
            )
            log.error("[%s] refusing to start — lock held by another process", s.alias)
            self.sessions.pop(s.alias, None)
            return
        try:
            await self._start_client_inner(s)
        except Exception as e:
            s.state = SessionState.error
            s.last_error = str(e)
            log.error("[%s] failed to start: %s", s.alias, e)

    async def _start_client_inner(self, s: MTSession) -> None:
        """Connect a Pyrogram client with persistent session file.

        Assumes the singleton lock is already held (AUTH_KEY_DUPLICATED
        prevention, R7). Raises on failure — callers decide whether to
        retry (auth floods) or give up.
        """
        s.state = SessionState.connecting
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

            # Pyrogram creates the .session file with default umask —
            # tighten it: the file IS the account (auth_key).
            sf = f"{s.session_file}.session"
            try:
                if os.path.exists(sf):
                    os.chmod(sf, 0o600)
            except OSError as e:
                log.warning("[%s] could not chmod session file: %s", s.alias, e)

            me = await client.get_me()
            log.info("[%s] live as @%s (id=%s)", s.alias, me.username, me.id)

            # Warm the peer cache: a fresh session holds no access
            # hashes, so first sends fail with "Peer id invalid" until
            # the entities have been seen. Walk the bot's chats once;
            # the file session remembers them across restarts.
            try:
                dialogs = client.get_dialogs(limit=500)
                if dialogs is not None:
                    async for _ in dialogs:
                        pass
                log.info("[%s] peer cache warmed", s.alias)
            except Exception as e:
                log.warning("[%s] peer warm-up failed (non-fatal): %s", s.alias, e)

            # Restore update state from disk
            state = await self.store.get_state(s.alias)
            if state.get("pts"):
                log.info("[%s] restored state: pts=%s qts=%s", s.alias, state["pts"], state["qts"])

            # Replay unacked updates persisted before a crash/kill:
            # the seq continues where it left off, the backlog re-enters
            # the in-memory queue, consumers see exactly-once per ack.
            backlog = await self.store.queue_pull(s.alias, offset=0, limit=5000)
            if backlog:
                max_seq = max(u["_gw_seq"] for u in backlog)
                s._next_update_seq = max(s._next_update_seq, max_seq)
                for u in backlog:
                    s._update_queue.put_nowait(u)
                log.info(
                    "[%s] replayed %d unacked updates (seq→%d)", s.alias, len(backlog), max_seq
                )

            # Keep the client running (idle in a task)
            asyncio.create_task(self._keep_alive(s), name=f"mt-idle-{s.alias}")

            # Start lock heartbeat (proves we're alive for stale detection)
            if s.lock:
                await s.lock.start_heartbeat()

        except Exception:
            # release the lock for this attempt; the retry wrapper re-acquires
            if s.lock:
                s.lock.release()
                s.lock = None
            raise

    async def _start_with_retries(self, s: MTSession, max_attempts: int = 8) -> None:
        """Start a session; on an auth FLOOD_WAIT, wait it out and retry.

        Manual poking at auth floods makes them worse (every failed
        ImportBotAuthorization can refresh the penalty). The gateway
        owns the discipline instead: parse the required wait from
        Telegram's answer, sleep it out plus a margin, try again —
        with capped attempts so a permanent failure cannot spin.
        """
        from .rateguard import parse_flood_wait

        for attempt in range(1, max_attempts + 1):
            lock_ok = s.lock is not None or self._acquire_lock_for(s)
            if not lock_ok:
                return  # refused: another owner — already logged
            try:
                await self._start_client_inner(s)
                return  # live (or refused permanently — inner logged it)
            except Exception as e:
                flood = parse_flood_wait(e)
                if not flood or attempt == max_attempts:
                    s.state = SessionState.error
                    s.last_error = str(e)
                    log.error("[%s] start failed for good (attempt %d): %s", s.alias, attempt, e)
                    return
                # margin over the quoted window + jitter (charter: never below
                # the quoted wait; stagger so instances don't retry in lockstep)
                from .rateguard import jittered

                wait = jittered(float(flood["wait"]) + 60.0)
                s.state = SessionState.connecting
                s.last_error = f"auth flood: retrying in {int(wait)}s (attempt {attempt})"
                log.warning(
                    "[%s] auth flood on start (attempt %d) — sleeping %ds before retry",
                    s.alias,
                    attempt,
                    int(wait),
                )
                await asyncio.sleep(wait)

    def _acquire_lock_for(self, s: MTSession) -> bool:
        lock = SessionLock(alias=s.alias, lock_dir=self.cfg.session_dir)
        if not lock.acquire():
            self.sessions.pop(s.alias, None)
            return False
        s.lock = lock
        return True

    def _make_update_handler(self, s: MTSession):
        """Create a Pyrogram update handler that routes to the session."""

        async def handler(client, update, users, chats):
            # Serialize Pyrogram update to dict. _serialize_update returns
            # None for anything it can't serialize — no hasattr guard
            # (plain dicts have no __dict__ and were silently dropped).
            try:
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
                from .rateguard import jittered

                await asyncio.sleep(jittered(1.0, cap_extra=1.0))
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

    def find_by_token(self, token: str) -> MTSession | None:
        """Token-routed lookup — consumers that only hold a token."""
        for s in self.sessions.values():
            if s.token == token:
                return s
        return None

    def all_stats(self) -> list[dict]:
        return [s.stats() for s in self.sessions.values()]
