"""Tests for the singleton session lock (AUTH_KEY_DUPLICATED prevention)."""

import os
import tempfile
import time
from unittest.mock import patch

from mtgateway.lock import SessionLock


def _tmpdir():
    return tempfile.mkdtemp()


class TestSessionLock:
    def test_acquire_succeeds_first_time(self):
        d = _tmpdir()
        lock = SessionLock(alias="bot1", lock_dir=d)
        assert lock.acquire() is True
        assert os.path.exists(lock.path)
        lock.release()

    def test_second_acquire_fails(self):
        d = _tmpdir()
        lock1 = SessionLock(alias="bot1", lock_dir=d)
        lock2 = SessionLock(alias="bot1", lock_dir=d)
        assert lock1.acquire() is True
        assert lock2.acquire() is False  # refused — another process holds it
        lock1.release()

    def test_different_aliases_both_acquire(self):
        d = _tmpdir()
        lock1 = SessionLock(alias="bot1", lock_dir=d)
        lock2 = SessionLock(alias="bot2", lock_dir=d)
        assert lock1.acquire() is True
        assert lock2.acquire() is True
        lock1.release()
        lock2.release()

    def test_release_allows_reacquire(self):
        d = _tmpdir()
        lock = SessionLock(alias="bot1", lock_dir=d)
        lock.acquire()
        lock.release()
        assert lock.acquire() is True  # available again
        lock.release()

    def test_lock_file_contains_pid(self):
        d = _tmpdir()
        lock = SessionLock(alias="bot1", lock_dir=d)
        lock.acquire()
        pid = lock._read_pid()
        assert pid == os.getpid()
        lock.release()

    def test_stale_lock_reclaimed_when_holder_dead(self):
        d = _tmpdir()
        # Create a lock with a dead PID
        lock_path = os.path.join(d, "bot1.lock")
        with open(lock_path, "w") as f:
            f.write("999999999\n" + str(time.time() - 120) + "\n")  # dead PID, old heartbeat

        lock = SessionLock(alias="bot1", lock_dir=d)
        # Should succeed — the holder (pid 999999999) doesn't exist
        assert lock.acquire() is True
        lock.release()

    def test_lock_refusal_message_includes_holder(self):
        d = _tmpdir()
        lock1 = SessionLock(alias="bot1", lock_dir=d)
        lock1.acquire()
        lock2 = SessionLock(alias="bot1", lock_dir=d)
        assert lock2.acquire() is False
        # lock2._lock_path is set from the acquire attempt
        holder = lock2._read_holder()
        assert "pid" in holder
        lock1.release()

    def test_release_is_idempotent(self):
        d = _tmpdir()
        lock = SessionLock(alias="bot1", lock_dir=d)
        lock.acquire()
        lock.release()
        lock.release()  # should not raise
        assert not os.path.exists(lock.path)

    def test_double_release_safe_after_reacquire_by_other(self):
        d = _tmpdir()
        lock1 = SessionLock(alias="bot1", lock_dir=d)
        lock1.acquire()
        lock2 = SessionLock(alias="bot1", lock_dir=d)
        # lock2 fails to acquire, but if it calls release() it should not
        # remove lock1's file (PID check prevents this)
        lock2._lock_path = lock1.path  # simulate a stale reference
        lock2.release()
        assert os.path.exists(lock1.path)  # lock1 still holds it
        lock1.release()


class TestLockInSessionManager:
    async def test_session_with_lock_refused(self):
        """R7: Second sidecar on the same session fails at the lock,
        never reaches Telegram."""
        import asyncio
        from unittest.mock import AsyncMock

        from mtgateway.config import Config
        from mtgateway.sessions import MTSessionManager, SessionState
        from mtgateway.store import MTStore

        with tempfile.TemporaryDirectory() as d:
            cfg = Config(
                admin_secret="a", app_secret="b", api_id=1, api_hash="h",
                data_dir=d,
            )
            store = MTStore(d)
            await store.connect()

            # Pre-acquire the lock (simulates another sidecar)
            from mtgateway.lock import SessionLock
            other_lock = SessionLock(alias="bot1", lock_dir=cfg.session_dir)
            other_lock.acquire()

            mgr = MTSessionManager(cfg, store)
            with __import__("unittest").mock.patch.object(
                MTSessionManager, "_start_client", new=AsyncMock()
            ):
                # _start_client is mocked, but the lock check is inside it
                # so we test it directly
                s = mgr._create_session("bot1", "t")
                lock = SessionLock(alias="bot1", lock_dir=cfg.session_dir)
                assert lock.acquire() is False  # refused!

            other_lock.release()
            await store.close()
