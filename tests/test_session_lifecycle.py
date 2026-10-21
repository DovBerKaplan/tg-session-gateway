"""Session lifecycle hardening (Phase 0) — the supervisor, error
classification, and the lock-identity rules.

The bugs these pin (all confirmed against the pre-supervisor code):

1. A reconnect used to fight the session's OWN still-held lock →
   refused → session popped → gone until process restart.
2. A failed start was terminal (no retry loop existed at all).
3. Terminal errors (revoked/duplicated keys) were retried like network
   blurs, burning the session harder with every attempt.
4. lock.release() was called without await in two paths — file locks
   leaked until atexit; redis leases leaked completely.
5. The redis heartbeat set TTL = ttl_s×1000 SECONDS (4 hours), and
   extend/release could act on a foreign lease.
6. Lock files carried PID + timestamp only — PID liveness lies across
   containers/hosts sharing a volume.
"""

import asyncio
import os
import time
from types import SimpleNamespace
from typing import ClassVar

from mtgateway.lock import BOOT_ID, HOSTNAME, SessionLock, sweep_lock_dir
from mtgateway.sessions import MTSessionManager, SessionState, classify_start_error


class FlakyClient:
    """Fake Pyrogram client with a scriptable lifecycle."""

    live_by_alias: ClassVar[dict[str, int]] = {}
    script: ClassVar[dict[str, list]] = {}  # alias -> list of outcomes ("ok" | Exception)

    def __init__(self, **kw):
        self.name = kw.get("name", "")
        self.alias = os.path.basename(self.name).replace(".session", "")
        self.is_connected = False
        self.raw_handler = None

    def add_handler(self, handler, group=0):
        self.raw_handler = handler

    async def start(self):
        script = FlakyClient.script.get(self.alias) or ["ok"]
        outcome = script.pop(0) if script else "ok"
        if isinstance(outcome, BaseException):
            raise outcome
        FlakyClient.live_by_alias[self.alias] = FlakyClient.live_by_alias.get(self.alias, 0) + 1
        self.is_connected = True

    async def stop(self):
        self.is_connected = False
        if FlakyClient.live_by_alias.get(self.alias):
            FlakyClient.live_by_alias[self.alias] -= 1

    async def get_me(self):
        return SimpleNamespace(username="fake_bot", id=1)

    def get_dialogs(self, limit=500):
        return None


class _Cfg:
    def __init__(self, d):
        self.api_id = 1
        self.api_hash = "x"
        self.data_dir = d
        self.session_dir = os.path.join(d, "sessions")
        self.rate = None
        self.mtproto_rate = None


async def _mgr(tmp_path, monkeypatch):
    from mtgateway.store import MTStore

    FlakyClient.live_by_alias.clear()
    FlakyClient.script.clear()
    monkeypatch.setattr("mtgateway.sessions.PyrogramClient", FlakyClient)
    store = MTStore(str(tmp_path))
    await store.connect()
    return MTSessionManager(_Cfg(str(tmp_path)), store), store


async def _wait_for(predicate, timeout=5.0, what="condition"):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if await predicate() if asyncio.iscoroutinefunction(predicate) else predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


class TestClassifyStartError:
    def test_flood_detected(self):
        class FloodErr(Exception):
            def __str__(self):
                # Telegram raw format the parser knows:
                # [420 FLOOD_WAIT_30] - A wait of 30 seconds is required
                return "[420 FLOOD_WAIT_30] - A wait of 30 seconds is required"

        assert classify_start_error(FloodErr()) == "flood"

    def test_terminal_markers(self):
        for marker in (
            "AUTH_KEY_DUPLICATED",
            "AuthKeyUnregistered",
            "SessionDuplicated",
            "ACCESS_TOKEN_EXPIRED",
            "UNAUTHORIZED",
        ):
            assert classify_start_error(RuntimeError(f"boomed: {marker}")) == "terminal", marker

    def test_generic_network_is_retryable(self):
        assert classify_start_error(ConnectionError("reset by peer")) == "retry"


class TestSupervisor:
    async def test_reconnect_reuses_own_lock(self, tmp_path, monkeypatch):
        """Bug 1: disconnect → reconnect must NOT fight our own lock."""
        mgr, store = await _mgr(tmp_path, monkeypatch)
        try:
            s = await mgr.register("bot1", "t")
            await _wait_for(lambda: s.state == SessionState.live, what="initial connect")
            assert s.lock is not None and s.lock.held

            s.client.is_connected = False  # Telegram drops us
            # supervisor checks every 5s + jittered reconnect delay
            await _wait_for(
                lambda: s.state == SessionState.live and s.reconnects == 1,
                timeout=15.0,
                what="reconnect under our own lock",
            )
            assert "bot1" in mgr.sessions  # never dropped
            assert FlakyClient.live_by_alias["bot1"] == 1  # exactly one live client
        finally:
            await mgr.shutdown()
            await store.close()

    async def test_retryable_failure_backs_off_then_recovers(self, tmp_path, monkeypatch):
        """Bug 2: a failed start is no longer terminal."""
        mgr, store = await _mgr(tmp_path, monkeypatch)
        try:
            FlakyClient.script["bot1"] = [ConnectionError("reset"), ConnectionError("reset"), "ok"]
            s = await mgr.register("bot1", "t")
            await _wait_for(lambda: s.state == SessionState.live, timeout=8.0, what="recovery")
            assert s.start_attempts >= 3
            assert FlakyClient.live_by_alias["bot1"] == 1
        finally:
            await mgr.shutdown()
            await store.close()

    async def test_terminal_error_quarantines(self, tmp_path, monkeypatch):
        """Bug 3: revoked/duplicated keys stop retrying immediately."""
        mgr, store = await _mgr(tmp_path, monkeypatch)
        try:
            FlakyClient.script["bot1"] = [RuntimeError("AUTH_KEY_DUPLICATED detected")] * 10
            s = await mgr.register("bot1", "t")
            await _wait_for(lambda: s.terminal_error, what="quarantine")
            await asyncio.sleep(0.3)
            assert s.start_attempts == 1  # never retried the terminal error
            assert s.state == SessionState.error
            assert "bot1" in mgr.sessions  # visible for admin action
        finally:
            await mgr.shutdown()
            await store.close()

    async def test_lock_refusal_keeps_session_with_cooldown(self, tmp_path, monkeypatch):
        mgr, store = await _mgr(tmp_path, monkeypatch)
        try:
            foreign = SessionLock(alias="bot1", lock_dir=mgr.cfg.session_dir)
            assert foreign.acquire() is True
            s = await mgr.register("bot1", "t")
            await asyncio.sleep(0.3)
            assert mgr.get("bot1") is not None
            assert s.state == SessionState.error
            assert s.next_retry_at > time.monotonic()  # cooldown, no hammering
            assert FlakyClient.live_by_alias.get("bot1", 0) == 0  # never connected
            foreign.release()
        finally:
            await mgr.shutdown()
            await store.close()

    async def test_shutdown_releases_locks_awaited(self, tmp_path, monkeypatch):
        """Bug 4: shutdown must actually release (no un-awaited coroutines)."""
        import warnings

        mgr, store = await _mgr(tmp_path, monkeypatch)
        s = await mgr.register("bot1", "t")
        await _wait_for(lambda: s.state == SessionState.live, what="live")
        lock_path = s.lock._lock.path if hasattr(s.lock, "_lock") else None
        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)  # un-awaited coroutine = failure
            await mgr.shutdown()
        assert s.lock is None
        if lock_path:
            assert not os.path.exists(lock_path)  # released on disk
        await store.close()


class TestLockIdentity:
    def _write_lock(self, d, pid, hostname, boot_id, age):
        p = os.path.join(d, "bot1.lock")
        with open(p, "w") as f:
            f.write(f"{pid}\n{hostname}\n{boot_id}\n{time.time() - age}\n")
        # staleness reads the file's MTIME (the heartbeat), not content
        old = time.time() - age
        os.utime(p, (old, old))
        return p

    def test_new_format_records_identity(self, tmp_path):
        lock = SessionLock(alias="bot1", lock_dir=str(tmp_path))
        assert lock.acquire() is True
        with open(lock.path) as f:
            lines = [ln.strip() for ln in f]
        assert lines[0] == str(os.getpid())
        assert lines[1] == HOSTNAME
        assert lines[2] == BOOT_ID
        lock.release()

    def test_cross_host_fresh_heartbeat_not_stale(self, tmp_path):
        """Bug 6: a live PID on ANOTHER host must not be judged by our
        PID namespace — but a fresh heartbeat means genuinely alive."""
        self._write_lock(tmp_path, pid=999999999, hostname="other-host", boot_id="b", age=0)
        probe = SessionLock(alias="bot1", lock_dir=str(tmp_path))
        probe._lock_path = os.path.join(tmp_path, "bot1.lock")
        assert probe._is_stale() is False

    def test_cross_host_stale_heartbeat_is_stale(self, tmp_path):
        import mtgateway.lock as lock_mod

        self._write_lock(tmp_path, pid=os.getpid(), hostname="other-host", boot_id="b", age=9999)
        probe = SessionLock(alias="bot1", lock_dir=str(tmp_path))
        probe._lock_path = os.path.join(tmp_path, "bot1.lock")
        # our own PID is alive — cross-host must ignore that and judge
        # the heartbeat only
        assert probe._heartbeat_stale() is True
        assert probe._is_stale() is True
        assert lock_mod.HOSTNAME  # sanity: identity is populated

    def test_legacy_two_line_lock_still_works(self, tmp_path):
        p = os.path.join(tmp_path, "bot1.lock")
        with open(p, "w") as f:
            f.write("999999999\n" + str(time.time()) + "\n")  # dead pid, fresh file
        probe = SessionLock(alias="bot1", lock_dir=str(tmp_path))
        probe._lock_path = p
        assert probe._is_stale() is True  # legacy: dead PID wins

    def test_sweep_removes_only_stale(self, tmp_path):
        d = str(tmp_path)
        self._write_lock(tmp_path, 999999999, "other-host", "b", age=9999)  # stale
        fresh = SessionLock(alias="alive", lock_dir=d)
        assert fresh.acquire() is True  # held by a live PID + fresh heartbeat
        assert sweep_lock_dir(d) == 1
        assert not os.path.exists(os.path.join(d, "bot1.lock"))
        assert os.path.exists(os.path.join(d, "alive.lock"))
        fresh.release()
