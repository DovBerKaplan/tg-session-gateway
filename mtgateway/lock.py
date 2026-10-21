"""Singleton session lock — prevents AUTH_KEY_DUPLICATED at the process level.

Gap analysis §6.5: "Advisory file lock *before* connect(), so a second
process never reaches Telegram." Without this, a second sidecar (or a
stray Pyrogram process) opening the same session file can invalidate
the auth_key permanently (R7 violation).

The lock uses O_CREAT | O_EXCL (atomic on all POSIX filesystems). The
file records the holder's identity — PID, hostname, boot_id — plus a
heartbeat (mtime) for staleness:

- same host (or legacy 2-line files): a dead PID is stale IMMEDIATELY
  (fast supervisor restarts are the normal path); a live PID is stale
  only after STALE_AFTER_SECONDS without a heartbeat.
- other host: PID liveness is meaningless across PID namespaces, so
  only the heartbeat window decides.

`sweep_lock_dir` is the boot janitor applying the same rules to every
leftover *.lock before sessions are restored.
"""

from __future__ import annotations

import asyncio
import atexit
import logging
import os
import socket
import time
from dataclasses import dataclass

log = logging.getLogger("mtgateway.lock")

# Heartbeat refresh interval (seconds)
HEARTBEAT_INTERVAL = 10
# A lock without a heartbeat for this long is considered stale
STALE_AFTER_SECONDS = 60


def _boot_id() -> str:
    try:
        with open("/proc/sys/kernel/random/boot_id") as f:
            return f.read().strip()
    except OSError:
        return ""


HOSTNAME = socket.gethostname()
BOOT_ID = _boot_id()

# metrics feed: stale locks reclaimed (acquire path + boot janitor)
LOCK_METRICS = {"reclaims": 0}


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

    @property
    def held(self) -> bool:
        """True while THIS instance owns the lock (fd open)."""
        return self._fd is not None

    def acquire(self) -> bool:
        """Try to acquire exclusively. Returns False if already held.

        Uses O_CREAT|O_EXCL for atomic creation. A stale lock (holder
        dead / heartbeat silent) is automatically reclaimed.
        """
        os.makedirs(self.lock_dir, exist_ok=True)
        self._lock_path = self.path

        # Check for stale lock
        if os.path.exists(self._lock_path):
            if self._is_stale():
                log.warning("[%s] removing stale lock (holder dead + no heartbeat)", self.alias)
                try:
                    os.unlink(self._lock_path)
                    LOCK_METRICS["reclaims"] += 1
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
            os.write(self._fd, f"{os.getpid()}\n{HOSTNAME}\n{BOOT_ID}\n{time.time()}\n".encode())
            log.info(
                "[%s] session lock acquired (pid=%d host=%s)", self.alias, os.getpid(), HOSTNAME
            )
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
                # Only remove if we still own it (check identity)
                pid, hostname, _boot = self._read_identity()
                ours = pid == os.getpid() and hostname in (None, HOSTNAME)
                if ours:
                    os.unlink(self._lock_path)
                    log.info("[%s] session lock released", self.alias)
            except (FileNotFoundError, OSError):
                pass
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None

    def _read_identity(self) -> tuple[int | None, str | None, str | None]:
        """(pid, hostname, boot_id) from the lock file.

        Legacy v1 files (pid + timestamp, 2 lines) return hostname=None
        → same-host semantics, exactly the pre-identity behavior.
        """
        try:
            with open(self._lock_path) as f:
                lines = [ln.strip() for ln in f]
        except (FileNotFoundError, ValueError, OSError):
            return None, None, None
        pid = int(lines[0]) if lines and lines[0].isdigit() else None
        hostname = lines[1] if len(lines) >= 4 else None
        boot_id = lines[2] if len(lines) >= 4 else None
        return pid, hostname, boot_id

    def _read_pid(self) -> int | None:
        """Read the PID from the lock file. None if unreadable."""
        return self._read_identity()[0]

    def _read_holder(self) -> str:
        """Human-readable lock holder info for error messages."""
        pid, hostname, _boot = self._read_identity()
        if pid is None:
            return "unknown"
        return f"pid={pid}@{hostname or HOSTNAME}"

    def _heartbeat_stale(self) -> bool:
        try:
            age = time.time() - os.path.getmtime(self._lock_path)
        except OSError:
            return True
        return age > STALE_AFTER_SECONDS

    def _is_stale(self) -> bool:
        """Staleness rules (see module docstring):

        - cross-host holder → heartbeat age only (PID check would lie
          across PID namespaces)
        - same-host/legacy holder → dead PID is stale immediately (the
          fast supervisor restart path); live PID needs a silent
          heartbeat for STALE_AFTER_SECONDS
        """
        pid, hostname, _boot = self._read_identity()
        if pid is None and hostname is None:
            return True  # unreadable = treat as stale
        if hostname is not None and hostname != HOSTNAME:
            return self._heartbeat_stale()
        if pid is not None:
            try:
                os.kill(pid, 0)  # signal 0 = existence check
            except ProcessLookupError:
                return True  # holder is dead — reclaim now
            except PermissionError:
                pass  # exists but owned by another user
        if self._heartbeat_stale():
            log.warning("[%s] lock heartbeat stale (holder pid=%s still alive)", self.alias, pid)
            return True
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


def sweep_lock_dir(lock_dir: str) -> int:
    """Boot janitor: remove every *.lock that is stale by the acquire()
    rules. Returns how many were removed."""
    removed = 0
    try:
        names = os.listdir(lock_dir)
    except OSError:
        return 0
    for name in names:
        if not name.endswith(".lock"):
            continue
        probe = SessionLock(alias=name[: -len(".lock")], lock_dir=lock_dir)
        probe._lock_path = os.path.join(lock_dir, name)
        if probe._is_stale():
            try:
                os.unlink(probe._lock_path)
                removed += 1
                LOCK_METRICS["reclaims"] += 1
                log.warning("janitor: removed stale lock %s", name)
            except OSError:
                pass
    return removed


# ── Redis lease backend (ADR-0001) ─────────────────────────────────

# Atomic ownership checks: extend/release touch the lease ONLY when its
# value still equals OUR holder string — a lapsed holder can never
# extend or delete the NEXT owner's lease (GET-then-EXPIRE/DEL races).
_EXTEND_LUA = (
    "if redis.call('get', KEYS[1]) == ARGV[1] then "
    "return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end"
)
_RELEASE_LUA = (
    "if redis.call('get', KEYS[1]) == ARGV[1] then "
    "return redis.call('del', KEYS[1]) else return 0 end"
)


class RedisSessionLease:
    """Distributed lease: SET NX PX + heartbeat extension + fencing token.

    Multi-host ownership (ADR-0001): crash release is automatic (TTL
    lapse, ≤ ttl_s) — safer than file stale-detection, and the fencing
    token (monotonic per acquisition) lets future farm mode reject a
    thawed zombie's late writes. Lazy redis import keeps the file
    backend dependency-free.
    """

    def __init__(self, alias: str, redis_url: str, ttl_s: float = 15.0):
        import redis.asyncio as aioredis

        self.alias = alias
        self.ttl_s = ttl_s
        self._r = aioredis.from_url(redis_url)
        self._key = f"tgw:lease:{alias}"
        self._ft_key = f"tgw:lease:{alias}:ft"
        self._holder = f"{socket.gethostname()}:{os.getpid()}"
        self._heartbeat_task: asyncio.Task | None = None
        self.token: int | None = None  # fencing token, None = not held
        self._scripts: dict = {}  # body -> registered script (lazy)

    @property
    def held(self) -> bool:
        return self.token is not None

    def _script(self, body: str):
        scripts = getattr(self, "_scripts", None)
        if scripts is None:
            scripts = self._scripts = {}
        s = scripts.get(body)
        if s is None:
            s = scripts[body] = self._r.register_script(body)
        return s

    async def acquire(self) -> bool:
        ok = await self._r.set(self._key, self._holder, nx=True, px=int(self.ttl_s * 1000))
        if not ok:
            holder = await self._r.get(self._key)
            log.error(
                "[%s] LEASE REFUSED: held by %s (a second connection would "
                "trigger AUTH_KEY_DUPLICATED)",
                self.alias,
                holder.decode() if isinstance(holder, bytes) else holder,
            )
            return False
        self.token = int(await self._r.incr(self._ft_key))
        log.info(
            "[%s] session lease acquired (holder=%s token=%d)", self.alias, self._holder, self.token
        )
        return True

    async def release(self) -> None:
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None
        # atomic compare-and-delete: never touches a foreign lease
        try:
            await self._script(_RELEASE_LUA)(keys=[self._key], args=[self._holder])
            log.info("[%s] session lease released", self.alias)
        except Exception as e:
            log.warning("[%s] lease release failed: %s", self.alias, e)
        self.token = None

    async def extend(self) -> None:
        """Heartbeat: extend the TTL — only if we still own it."""
        # pexpire = milliseconds; plain expire() takes SECONDS (a past
        # ttl*1000 call silently turned 15s leases into 4-hour ones)
        await self._script(_EXTEND_LUA)(
            keys=[self._key], args=[self._holder, int(self.ttl_s * 1000)]
        )

    async def start_heartbeat(self, interval: float = 5.0) -> None:

        async def _beat():
            while True:
                await asyncio.sleep(interval)
                try:
                    await self.extend()
                except Exception as e:
                    log.warning("[%s] lease heartbeat failed: %s", self.alias, e)

        if self._heartbeat_task is None or self._heartbeat_task.done():
            self._heartbeat_task = asyncio.get_event_loop().create_task(_beat())


def make_lock(alias: str, lock_dir: str, backend: str = "file", redis_url: str | None = None):
    """Lock factory (ADR-0001): file by default, Redis lease for multi-host.

    Returns an object with the SAME async surface on both backends:
    await acquire() -> bool, await release(), await start_heartbeat(),
    held (property), token (property).
    """
    if backend == "redis":
        if not redis_url:
            raise ValueError("GW_LOCK_BACKEND=redis requires GW_REDIS_URL")
        return RedisSessionLease(alias, redis_url)
    return FileLockAdapter(SessionLock(alias=alias, lock_dir=lock_dir))


class FileLockAdapter:
    """Async surface over the file lock (ADR-0001 interface parity)."""

    def __init__(self, lock: SessionLock):
        self._lock = lock

    async def acquire(self) -> bool:
        return self._lock.acquire()

    async def release(self) -> None:
        self._lock.release()

    async def start_heartbeat(self, interval: float = 5.0) -> None:
        await self._lock.start_heartbeat()

    @property
    def held(self) -> bool:
        return self._lock.held

    @property
    def token(self) -> None:
        return None  # file backend has no fencing token (single-host)
