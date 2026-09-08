"""Singleton session lock — prevents AUTH_KEY_DUPLICATED at the process level.

Gap analysis §6.5: "Advisory file lock *before* connect(), so a second
process never reaches Telegram." Without this, a second sidecar (or a
stray Pyrogram process) opening the same session file can invalidate
the auth_key permanently (R7 violation).

The lock uses O_CREAT | O_EXCL (atomic on all POSIX filesystems) and
includes a PID + heartbeat for stale-lock detection.
"""

from __future__ import annotations

import asyncio
import atexit
import logging
import os
import time
from dataclasses import dataclass

log = logging.getLogger("mtgateway.lock")

# Heartbeat refresh interval (seconds)
HEARTBEAT_INTERVAL = 10
# A lock without a heartbeat for this long is considered stale
STALE_AFTER_SECONDS = 60


@dataclass
class SessionLock:
    """Advisory file lock around a Pyrogram session.

    Acquired BEFORE Client.start() so that a second process fails at
    the filesystem level — it never opens a TCP connection to Telegram
    and therefore never triggers AUTH_KEY_DUPLICATED.
    """

    alias: str
    lock_dir: str
    _fd: int | None = None
    _lock_path: str = ""
    _heartbeat_task: asyncio.Task[None] | None = None

    @property
    def path(self) -> str:
        return os.path.join(self.lock_dir, f"{self.alias}.lock")

    def acquire(self) -> bool:
        """Try to acquire exclusively. Returns False if already held.

        Uses O_CREAT|O_EXCL for atomic creation. A stale lock (holder
        PID is dead + no heartbeat) is automatically reclaimed.
        """
        os.makedirs(self.lock_dir, exist_ok=True)
        self._lock_path = self.path

        # Check for stale lock
        if os.path.exists(self._lock_path):
            if self._is_stale():
                log.warning("[%s] removing stale lock (holder dead + no heartbeat)", self.alias)
                try:
                    os.unlink(self._lock_path)
                except FileNotFoundError:
                    pass
            else:
                holder = self._read_holder()
                log.error(
                    "[%s] LOCK REFUSED: another process holds this session "
                    "(holder: %s). A second connection would trigger "
                    "AUTH_KEY_DUPLICATED and may invalidate the auth_key.",
                    self.alias,
                    holder,
                )
                return False

        try:
            self._fd = os.open(self._lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(self._fd, f"{os.getpid()}\n{time.time()}\n".encode())
            log.info("[%s] session lock acquired (pid=%d)", self.alias, os.getpid())
            atexit.register(self._cleanup)
            return True
        except FileExistsError:
            log.error("[%s] LOCK REFUSED: race lost acquiring lock", self.alias)
            return False

    def release(self) -> None:
        """Release the lock. Safe to call multiple times.
        Only releases if THIS instance acquired it (has a valid _fd)."""
        if self._fd is None:
            return  # never acquired — nothing to release
        try:
            os.close(self._fd)
        except OSError:
            pass
        self._fd = None
        if self._lock_path and os.path.exists(self._lock_path):
            try:
                # Only remove if we still own it (check PID)
                holder_pid = self._read_pid()
                if holder_pid == os.getpid():
                    os.unlink(self._lock_path)
                    log.info("[%s] session lock released", self.alias)
            except (FileNotFoundError, OSError):
                pass
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None

    def _read_pid(self) -> int | None:
        """Read the PID from the lock file. None if unreadable."""
        try:
            with open(self._lock_path) as f:
                first_line = f.readline().strip()
                return int(first_line) if first_line.isdigit() else None
        except (FileNotFoundError, ValueError, OSError):
            return None

    def _read_holder(self) -> str:
        """Human-readable lock holder info for error messages."""
        pid = self._read_pid()
        return f"pid={pid}" if pid else "unknown"

    def _is_stale(self) -> bool:
        """True if the lock holder is dead (PID no longer exists)
        AND the heartbeat is old. Both conditions must be true to
        avoid reclaiming a lock from a process that merely paused."""
        pid = self._read_pid()
        if pid is None:
            return True  # unreadable = treat as stale

        # Check if the process exists
        try:
            os.kill(pid, 0)  # signal 0 = existence check, no actual signal sent
            process_alive = True
        except ProcessLookupError:
            process_alive = False
        except PermissionError:
            process_alive = True  # exists but owned by another user

        if not process_alive:
            return True  # holder is dead

        # Check heartbeat age
        try:
            mtime = os.path.getmtime(self._lock_path)
            age = time.time() - mtime
            if age > STALE_AFTER_SECONDS:
                log.warning(
                    "[%s] lock heartbeat stale (%.0fs old, pid=%d still alive)",
                    self.alias,
                    age,
                    pid,
                )
                return True
        except OSError:
            pass

        return False

    def _cleanup(self) -> None:
        """Registered with atexit — release on process exit."""
        self.release()

    async def start_heartbeat(self) -> None:
        """Periodically touch the lock file to prove we're alive."""

        async def _beat():
            while True:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                try:
                    os.utime(self._lock_path, None)  # touch mtime
                except OSError:
                    pass

        self._heartbeat_task = asyncio.create_task(_beat(), name=f"lock-hb-{self.alias}")
