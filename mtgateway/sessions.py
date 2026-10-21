"""Session Manager — holds long-lived Pyrogram Clients, one per bot token.

The gateway process is the SOLE owner of each MTProto session. Session
files persist on a Docker volume. The auth_key is loaded on every boot,
so importBotAuthorization is called exactly once per token's lifetime.

Update routing: Pyrogram's handler callbacks push updates into the
internal queue; consumers (applications) pull or subscribe via WS.

Lifecycle: one supervisor task per session owns it for life — initial
start, disconnect reconnects, retryable failures with exponential
backoff, auth-flood wait-outs, and terminal quarantine (revoked keys
never re-burn). The supervisor is the ONLY path that starts a client,
and it reuses a lock this process already holds (a reconnect never
fights its own lock — the pre-supervisor bug where a disconnect
permanently dropped the session).
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
from typing import TYPE_CHECKING

from pyrogram import Client as PyrogramClient
from pyrogram.handlers import RawUpdateHandler

from .config import Config
from .lock import FileLockAdapter, RedisSessionLease
from .rateguard import MTRateGuard
from .store import MTStore

log = logging.getLogger("mtgateway.sessions")

# Errors that mean the session itself is dead/revoked/duplicated.
# Retrying any of these only makes Telegram burn the session harder —
# quarantine and demand admin action instead.
_TERMINAL_MARKERS = (
    "AUTH_KEY_DUPLICATED",
    "AUTH_KEY_INVALID",
    "AuthKeyUnregistered",
    "AuthKeyInvalid",
    "SessionDuplicated",
    "SESSION_REVOKED",
    "SessionRevoked",
    "ACCESS_TOKEN_EXPIRED",
    "ACCESS_TOKEN_INVALID",
    "UNAUTHORIZED",
)


def classify_start_error(e: Exception) -> str:
    """Bucket a start/connect failure: 'flood' (wait it out), 'terminal'
    (quarantine — the session is revoked/duplicated), 'retry' (transient
    network/generic — exponential backoff)."""
    from .rateguard import parse_flood_wait

    if parse_flood_wait(e):
        return "flood"
    msg = str(e)
    if any(m in msg for m in _TERMINAL_MARKERS):
        return "terminal"
    return "retry"


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

    # supervisor state (the manager's _supervise task owns the lifecycle)
    start_task: asyncio.Task | None = None
    terminal_error: bool = False  # quarantined — admin action required
    next_retry_at: float = 0.0  # monotonic gate for the next attempt
    _stop_supervisor: bool = False
    reconnects: int = 0
    start_attempts: int = 0
    # active liveness + dead-man switch (metrics/alerts feed)
    _last_ping_at: float = 0.0
    last_ping_ok_at: float = 0.0
    last_update_at: float = 0.0  # wall clock of the last update received
    idempotency_claims: int = 0

    # Update routing: in-memory queue + optional WS consumers
    _update_queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=5000))
    _consumers: list = field(default_factory=list)
    _next_update_seq: int = 0

    # Rolling update: track which consumer is active
    active_consumer: str | None = None
    lock: FileLockAdapter | RedisSessionLease | None = None  # singleton enforcement (R7)

    _session_dir: str = "/data/sessions"  # overridden by manager

    @property
    def session_file(self) -> str:
        return os.path.join(self._session_dir, f"{self.alias}.session")

    # E2: Idempotency — app-supplied keys prevent duplicate sends
    _seen_idempotency_keys: dict[str, float] = field(default_factory=dict)  # fast-path cache
    _store: MTStore | None = None  # injected by the manager
    _health = None  # gateway.dbhealth.StorageHealth | None — injected by the manager
    persist_failures: int = 0  # write-through failures (readyz degrades)

    if TYPE_CHECKING:
        from .store import MTStore

    async def check_idempotency(self, key: str) -> str:
        """'claimed' / 'reclaimed' (proceed with the send) or 'replay'
        (suppress — a completed send with this key exists).

        The STORE decides atomically (INSERT ON CONFLICT): two
        concurrent requests with the same key cannot both pass, and a
        failed send releases its key instead of suppressing the retry.
        The in-memory dict stays a fast-path cache for finished keys.
        """
        now = time.time()
        ts = self._seen_idempotency_keys.get(key)
        if ts is not None and now - ts < 300:
            return "replay"
        verdict = await self._idempotency_store().idempotency_claim(self.alias, key)
        if verdict == "replay":
            self._seen_idempotency_keys[key] = now
        else:
            self.idempotency_claims += 1
        # bounded memory: drop stale cache entries
        cutoff = now - 300
        stale = [k for k, t in self._seen_idempotency_keys.items() if t < cutoff]
        for k in stale:
            del self._seen_idempotency_keys[k]
        return verdict

    def note_idempotency_done(self, key: str) -> None:
        """Cache a finished key (suppresses replays without a store hit)."""
        self._seen_idempotency_keys[key] = time.time()

    def _idempotency_store(self):
        """Store handle — injected by the manager at registration."""
        return self._store

    # E4: Pause sending while keeping receiving alive
    send_paused: bool = False

    async def prune_expired_updates(self, ttl_s: float) -> int:
        """GW_UPDATE_TTL_S, finally enforced: drain the in-memory queue,
        drop what aged out, re-put the rest in order."""
        if ttl_s <= 0:
            return 0
        cutoff = time.time() - ttl_s
        kept, dropped = [], 0
        while True:
            try:
                u = self._update_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if u.get("_gw_enqueued_at", 0) < cutoff:
                dropped += 1
            else:
                kept.append(u)
        for u in kept:
            self._update_queue.put_nowait(u)
        return dropped

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

        A persistence failure (disk full, DB locked) is COUNTED and
        degrades /readyz — it must never be a silent `except: pass`
        (the in-memory queue would die with the process).
        """
        self._next_update_seq += 1
        update["_gw_seq"] = self._next_update_seq
        update["_gw_enqueued_at"] = time.time()
        self.last_update_at = time.time()
        try:
            payload = json.dumps(update)
        except (TypeError, ValueError) as e:
            # poison update: capture for inspection instead of losing it
            if self._store is not None:
                await self._store.dead_letter(
                    self.alias, repr(update)[:10000], f"unserializable: {e}"
                )
            log.error("[%s] unserializable update captured to dead letters: %s", self.alias, e)
            return
        if self._store is not None:
            try:
                await self._store.queue_push(self.alias, update["_gw_seq"], payload)
            except Exception as e:
                self.persist_failures += 1
                log.error(
                    "[%s] update persistence FAILED (update lives in memory only, "
                    "lost on restart): %s",
                    self.alias,
                    e,
                )
                if self._health is not None:
                    self._health.note_persist_failure(f"queue.push:{self.alias}", e)
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
            "terminal_error": self.terminal_error,
            "reconnects": self.reconnects,
            "start_attempts": self.start_attempts,
            "persist_failures": self.persist_failures,
            "idempotency_claims": self.idempotency_claims,
            "seconds_since_last_update": round(time.time() - self.last_update_at, 1)
            if self.last_update_at
            else None,
            "seconds_since_ping_ok": round(time.monotonic() - self.last_ping_ok_at, 1)
            if self.last_ping_ok_at
            else None,
            "deploy_to_first_message_s": round(self.deploy_to_first_message_s, 2)
            if self.deploy_to_first_message_s
            else None,
        }


class MTSessionManager:
    """Manages the lifecycle of all Pyrogram sessions in this gateway."""

    def __init__(self, cfg: Config, store: MTStore, health=None):
        self.cfg = cfg
        self.store = store
        self.sessions: dict[str, MTSession] = {}
        self.health = health  # gateway.dbhealth.StorageHealth | None

    def _create_session(self, alias: str, token: str) -> MTSession:
        s = MTSession(alias=alias, token=token)
        s._session_dir = self.cfg.session_dir
        s._store = self.store
        s._health = getattr(self, "health", None)
        # GW_UPDATE_QUEUE_MAX is finally wired (was hardcoded 5000)
        policy = getattr(self.cfg, "policy", None)
        s._update_queue = asyncio.Queue(maxsize=getattr(policy, "update_queue_max", 5000))
        s.guard = MTRateGuard(self.cfg.rate, self.cfg.mtproto_rate)
        return s

    async def register(self, alias: str, token: str) -> MTSession:
        """Register a bot session; its supervisor starts the client."""
        if alias in self.sessions:
            existing = self.sessions[alias]
            if existing.token == token and existing.start_task and not existing.start_task.done():
                return existing
            # Token rotated (or supervisor dead): tear down old, start new
            await self.unregister(alias)

        await self.store.register(alias, token)
        s = self._create_session(alias, token)
        self.sessions[alias] = s

        s.start_task = asyncio.create_task(self._supervise(s), name=f"mt-sup-{alias}")
        s.start_task.add_done_callback(self._supervisor_done_cb(alias))
        return s

    @staticmethod
    def _supervisor_done_cb(alias: str):
        def _cb(task: asyncio.Task) -> None:
            if task.cancelled():
                return
            exc = task.exception()
            if exc is not None:
                log.error("[%s] supervisor task crashed: %s", alias, exc)

        return _cb

    # ── the supervisor — owns one session for life ───────────────────

    async def _supervise(self, s: MTSession) -> None:
        """Initial start, reconnects, retries, quarantine — the ONLY
        path that starts a client.

        - reuses a lock this process already holds (reconnects never
          fight their own lock)
        - flood → sleep out the quoted window; retryable → exponential
          backoff (capped); terminal (revoked/duplicated key) →
          quarantine and exit
        - lock refused by a foreign holder → error + cooldown retry
          (the session stays registered and visible)
        """
        from .rateguard import jittered, parse_flood_wait

        backoff = 1.0
        ping_interval = 60.0  # active liveness (half-open detection)
        try:
            while not s._stop_supervisor and not s.terminal_error:
                if s.state in (SessionState.paused, SessionState.draining):
                    await asyncio.sleep(2.0)
                    continue
                if s.client is not None and s.client.is_connected and s.state == SessionState.live:
                    # ACTIVE liveness: is_connected lies through half-open
                    # TCP (NAT/firewall silently dropped the flow) — ping
                    # Telegram for real; a stuck ping forces a reconnect.
                    if time.monotonic() - s._last_ping_at > ping_interval:
                        s._last_ping_at = time.monotonic()
                        try:
                            await asyncio.wait_for(s.client.get_me(), timeout=10.0)
                            s.last_ping_ok_at = time.monotonic()
                        except Exception as e:
                            log.warning(
                                "[%s] liveness ping stuck (%s) — half-open suspected, "
                                "forcing reconnect",
                                s.alias,
                                e,
                            )
                            try:
                                await asyncio.wait_for(s.client.stop(), timeout=10.0)
                            except Exception:
                                pass
                            continue
                    await asyncio.sleep(5.0)  # healthy — watch for disconnects
                    continue
                if time.monotonic() < s.next_retry_at:
                    await asyncio.sleep(0.5)
                    continue

                if not (s.lock is not None and s.lock.held):
                    acquired = await self._acquire_lock_for(s)
                    if not acquired:
                        s.state = SessionState.error
                        s.next_retry_at = time.monotonic() + 60.0  # don't hammer the holder
                        continue

                was_live = s.connected_since > 0.0
                s.state = SessionState.connecting
                s.start_attempts += 1
                try:
                    await self._start_client_inner(s)
                    backoff = 1.0
                    if was_live:
                        s.reconnects += 1
                        log.info("[%s] reconnected", s.alias)
                    continue
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    kind = classify_start_error(e)
                    if kind == "terminal":
                        s.state = SessionState.error
                        s.terminal_error = True
                        s.last_error = f"terminal (quarantined): {e}"
                        log.critical(
                            "[%s] session quarantined — key revoked/duplicated, "
                            "admin action required: %s",
                            s.alias,
                            e,
                        )
                        continue
                    if kind == "flood":
                        flood = parse_flood_wait(e) or {}
                        wait = jittered(float(flood.get("wait", 60.0)) + 60.0)
                        s.state = SessionState.connecting
                        s.last_error = f"auth flood: retrying in {int(wait)}s"
                        log.warning(
                            "[%s] auth flood on start — sleeping %ds before retry",
                            s.alias,
                            int(wait),
                        )
                        s.next_retry_at = time.monotonic() + wait
                        continue
                    # retryable (network/generic): exponential backoff
                    s.state = SessionState.error
                    s.last_error = f"start failed (retry in {int(backoff)}s): {e}"
                    log.warning("[%s] start failed (retry in %.0fs): %s", s.alias, backoff, e)
                    s.next_retry_at = time.monotonic() + backoff
                    backoff = min(backoff * 2, 300.0)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # supervisor bug or broken client — park visibly
            s.state = SessionState.error
            s.last_error = f"supervisor crashed: {e}"
            log.error("[%s] supervisor crashed (session parked in error state): %s", s.alias, e)

    async def _start_client_inner(self, s: MTSession) -> None:
        """Connect a Pyrogram client with persistent session file.

        Assumes the singleton lock is already held (AUTH_KEY_DUPLICATED
        prevention, R7). Raises on failure — the supervisor classifies
        and decides what happens next.
        """
        s.state = SessionState.connecting
        try:
            # Best-effort cleanup of a disconnected previous client
            old = s.client
            if old is not None:
                try:
                    await old.stop()
                except Exception:
                    pass

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

            # Start lock heartbeat (proves we're alive for stale detection)
            if s.lock:
                await s.lock.start_heartbeat()

        except Exception:
            # release the lock for this attempt; the supervisor re-acquires
            await self._release_lock(s)
            raise

    async def _acquire_lock_for(self, s: MTSession) -> bool:
        from .lock import make_lock

        lock = make_lock(
            s.alias,
            self.cfg.session_dir,
            backend=getattr(self.cfg, "lock_backend", "file"),
            redis_url=getattr(self.cfg, "redis_url", "") or None,
        )
        if not await lock.acquire():
            # The session STAYS registered — the operator must see it.
            # (Popping here used to make every lock refusal permanent.)
            s.last_error = (
                f"session lock refused: another process holds '{s.alias}'. "
                "A second connection would trigger AUTH_KEY_DUPLICATED."
            )
            log.error("[%s] refusing to start — lock held by another process", s.alias)
            return False
        s.lock = lock
        return True

    async def _release_lock(self, s: MTSession) -> None:
        """Release the session lock with the backend's async surface."""
        lock, s.lock = s.lock, None
        if lock is None:
            return
        try:
            await lock.release()
        except Exception as e:
            log.warning("[%s] lock release failed: %s", s.alias, e)

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
        """Convert a Pyrogram update object to a JSON-serializable dict.

        Non-dict payloads (Pyrogram hands raw str/bytes for updates it
        did not parse) are DROPPED — consumers need dicts, and wrapping
        them caused live 'str does not support item assignment' errors
        in push_update. None ⇒ the handler skips the update.
        """
        try:
            if isinstance(update, dict):
                return update
            if hasattr(update, "to_dict"):
                d = update.to_dict()
                return d if isinstance(d, dict) else None
            return None
        except Exception:
            return None

    async def _stop_supervisor_task(self, s: MTSession) -> None:
        s._stop_supervisor = True
        if s.start_task is not None and not s.start_task.done():
            s.start_task.cancel()
            await asyncio.gather(s.start_task, return_exceptions=True)

    async def unregister(self, alias: str) -> bool:
        """Stop a session and remove it. Releases the singleton lock."""
        s = self.sessions.pop(alias, None)
        if not s:
            return False
        s.state = SessionState.draining
        await self._stop_supervisor_task(s)
        if s.client:
            try:
                await s.client.stop()
            except Exception:
                pass
        await self._release_lock(s)
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

    async def restart_session(self, alias: str) -> bool:
        """Escalation rung 1: reconnect one session (not the process).
        Clears quarantine only for non-terminal errors; terminal ones
        still require unregister + re-auth."""
        s = self.sessions.get(alias)
        if not s:
            return False
        if s.terminal_error:
            return False  # revoked key — a restart cannot fix it
        s.next_retry_at = 0.0  # supervisor reconnects on its next beat
        return True

    async def persist_states(self) -> None:
        """Best-effort pts/qts/date/seq snapshot into the store — the
        columns existed but were never written; a restart can now
        report the real Telegram update state instead of zeros."""
        for s in self.sessions.values():
            if s.state != SessionState.live or s.client is None:
                continue
            sess = getattr(s.client, "session", None)
            pts = int(getattr(sess, "pts", 0) or 0)
            qts = int(getattr(sess, "qts", 0) or 0)
            date = int(getattr(sess, "date", 0) or 0)
            seq = int(getattr(sess, "seq", 0) or 0)
            if not (pts or qts or date or seq):
                continue  # don't zero out previously saved state
            try:
                await self.store.save_state(s.alias, pts, qts, date, seq)
            except Exception as e:
                log.warning("[%s] state snapshot failed: %s", s.alias, e)

    async def restore_all(self) -> int:
        """Gateway restart: restore all sessions from the store."""
        from .lock import sweep_lock_dir

        removed = sweep_lock_dir(self.cfg.session_dir)
        if removed:
            log.info("boot janitor: removed %d stale lock(s)", removed)
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
        """Graceful shutdown: stop supervisors and clients, release locks."""
        for alias, s in list(self.sessions.items()):
            s.state = SessionState.draining
            await self._stop_supervisor_task(s)
            if s.client:
                try:
                    await s.client.stop()
                except Exception:
                    pass
            await self._release_lock(s)
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
