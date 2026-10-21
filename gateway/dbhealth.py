"""SQLite durability kit — pragmas, boot integrity, backups, maintenance.

Shared by both planes (the mtgateway image ships gateway/ too). The
theme: a storage problem must be VISIBLE (metrics + degraded readiness)
and RECOVERABLE (quarantine + restore), never a silent crash-loop or a
silently-lost write.

- pragma_statements: durability knobs applied by both stores and the
  Bot API queue (sync sqlite3 and aiosqlite alike — plain SQL).
- StorageHealth: cross-plane signal object; persistence failures flip
  /readyz and feed metrics (persist_failures_total, corrupt_rows_total,
  storage_degraded).
- check_or_quarantine: boot-time quick_check. A corrupt DB is moved to
  <path>.corrupt-<ts> (forensics) and the newest backup — if any — is
  restored in its place. The service then boots degraded-but-alive
  instead of crash-looping.
- backup_db / backup_files_encrypted: rotating backups (VACUUM INTO for
  SQLite consistency; Fernet-encrypted zips for arbitrary files like
  .session files).
- run_maintenance: periodic integrity_check + WAL checkpoint(TRUNCATE)
  + audit retention + backup scheduling — call from an app lifespan
  task (maintenance_loop).
"""

from __future__ import annotations

import asyncio
import glob
import logging
import os
import shutil
import sqlite3
import time
import zipfile
from dataclasses import dataclass, field

log = logging.getLogger("gateway.dbhealth")


def pragma_statements(durability: str = "normal", foreign_keys: bool = False) -> list[str]:
    """Durability PRAGMAs every SQLite file in this project gets.

    - busy_timeout: a locked writer waits instead of failing instantly
    - synchronous: NORMAL survives process death (kill -9) but not
      machine power loss; FULL costs fsyncs and survives both. Callers
      pick via GW_DURABILITY.
    - wal_autocheckpoint: keep the WAL bounded between our own
      maintenance checkpoints
    """
    sync = "FULL" if durability.upper() == "FULL" else "NORMAL"
    stmts = [
        "PRAGMA journal_mode=WAL",
        f"PRAGMA synchronous={sync}",
        "PRAGMA busy_timeout=5000",
        "PRAGMA wal_autocheckpoint=1000",
    ]
    if foreign_keys:
        stmts.append("PRAGMA foreign_keys=ON")
    return stmts


@dataclass
class StorageHealth:
    """Visible storage state — persistence failures degrade /readyz and
    surface as metrics instead of vanishing into `except: pass`."""

    persist_failures: int = 0
    corrupt_rows: int = 0
    quarantined_files: int = 0
    degraded_reason: str | None = None
    last_persist_error: str = ""
    last_maintenance_at: float = 0.0
    _reasons: set = field(default_factory=set)

    def note_persist_failure(self, where: str, exc: Exception) -> None:
        self.persist_failures += 1
        self.last_persist_error = f"{where}: {exc}"
        self._reasons.add(f"persist:{where}")
        self.degraded_reason = ", ".join(sorted(self._reasons))
        log.error("storage persist failure (%s): %s — /readyz degraded", where, exc)

    def note_quarantine(self, path: str) -> None:
        self.quarantined_files += 1
        self._reasons.add(f"quarantined:{os.path.basename(path)}")
        self.degraded_reason = ", ".join(sorted(self._reasons))

    def note_recovery(self, reason_part: str) -> None:
        """Clear one degradation cause (e.g. after a good write)."""
        self._reasons.discard(reason_part)
        self.degraded_reason = ", ".join(sorted(self._reasons)) or None

    def snapshot(self) -> dict:
        return {
            "degraded_reason": self.degraded_reason,
            "persist_failures": self.persist_failures,
            "corrupt_rows": self.corrupt_rows,
            "quarantined_files": self.quarantined_files,
            "last_persist_error": self.last_persist_error[:200],
        }


# ── boot integrity ──────────────────────────────────────────────────


def quick_check(path: str) -> tuple[bool, str]:
    """(healthy, detail) via PRAGMA quick_check on a throwaway reader."""
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            row = conn.execute("PRAGMA quick_check").fetchone()
            ok = bool(row) and row[0] == "ok"
            return ok, (row[0] if row else "no result")
        finally:
            conn.close()
    except sqlite3.Error as e:
        return False, str(e)


def check_or_quarantine(
    path: str, backups_dir: str | None = None, health: StorageHealth | None = None
) -> str:
    """Boot gate for one SQLite file. Returns the path to use.

    healthy → path unchanged. Corrupt → renamed to
    <path>.corrupt-<ts>, the newest backup (if any) is copied in, and
    the caller starts from there (or from scratch when no backup
    exists). Either way the process LIVES — crash-looping a supervisor
    over a corrupt file helps nobody.
    """
    if not os.path.exists(path):
        return path
    ok, detail = quick_check(path)
    if ok:
        return path
    ts = time.strftime("%Y%m%d-%H%M%S")
    quarantine = f"{path}.corrupt-{ts}"
    try:
        os.rename(path, quarantine)
        log.error("DB corrupt (%s: %s) — quarantined to %s", path, detail, quarantine)
    except OSError as e:
        log.error("cannot quarantine corrupt DB %s: %s", path, e)
        return path
    if health is not None:
        health.note_quarantine(path)
    restored = _restore_newest_backup(path, backups_dir)
    if restored:
        log.warning("restored %s from backup (data since last backup is lost)", path)
    return path


def _restore_newest_backup(path: str, backups_dir: str | None) -> bool:
    if not backups_dir:
        return False
    base = os.path.basename(path)
    candidates = sorted(glob.glob(os.path.join(backups_dir, f"{base}-*.db")), reverse=True)
    # timestamp suffix sorts lexically; verify before trusting it
    for cand in candidates:
        ok, _ = quick_check(cand)
        if ok:
            shutil.copyfile(cand, path)
            return True
        log.warning("backup %s failed quick_check — skipping", cand)
    return False


# ── backups ─────────────────────────────────────────────────────────


def _rotate(pattern: str, keep: int) -> None:
    files = sorted(glob.glob(pattern), reverse=True)
    for stale in files[keep:]:
        try:
            os.unlink(stale)
        except OSError:
            pass


def backup_db(path: str, backups_dir: str, keep: int = 5) -> str | None:
    """Consistent snapshot via VACUUM INTO, rotated. None on failure."""
    if not os.path.exists(path):
        return None
    os.makedirs(backups_dir, exist_ok=True)
    base = os.path.basename(path)
    ts = time.strftime("%Y%m%d-%H%M%S")
    dest = os.path.join(
        backups_dir, f"{base}-{ts}-{os.getpid()}-{int(time.time() * 1000) % 1000}.db"
    )
    try:
        conn = sqlite3.connect(path)
        try:
            conn.execute("VACUUM INTO ?", (dest,))
        finally:
            conn.close()
        _rotate(os.path.join(backups_dir, f"{base}-*.db"), keep)
        return dest
    except sqlite3.Error as e:
        log.error("backup of %s failed: %s", path, e)
        try:
            os.unlink(dest)
        except OSError:
            pass
        return None


def backup_files_encrypted(
    files: list[str], dest_dir: str, prefix: str, fernet, keep: int = 5
) -> str | None:
    """Zip `files` in memory and Fernet-encrypt the archive — used for
    .session files (the auth_keys), so backups never sit in plaintext."""
    existing = [f for f in files if os.path.exists(f)]
    if not existing:
        return None
    os.makedirs(dest_dir, exist_ok=True)
    import io

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in existing:
            zf.write(f, arcname=os.path.basename(f))
    ts = time.strftime("%Y%m%d-%H%M%S")
    dest = os.path.join(dest_dir, f"{prefix}-{ts}.zip.enc")
    try:
        token = fernet.encrypt(buf.getvalue())
        with open(dest, "wb") as fh:
            fh.write(token)
        os.chmod(dest, 0o600)
        _rotate(os.path.join(dest_dir, f"{prefix}-*.zip.enc"), keep)
        return dest
    except Exception as e:
        log.error("encrypted backup failed: %s", e)
        try:
            os.unlink(dest)
        except OSError:
            pass
        return None


# ── periodic maintenance ────────────────────────────────────────────


def run_maintenance(
    db_paths: list[str],
    backups_dir: str,
    health: StorageHealth,
    *,
    keep: int = 5,
    backup_interval_s: float = 86400.0,
    audit_retention_sql: str | None = None,
    audit_retention_days: int = 30,
    encrypted: list[str] | None = None,
    fernet=None,
) -> None:
    """One maintenance pass (call periodically; sync — run in a thread).

    - PRAGMA integrity_check on every DB (quick_check caught corruption
      at boot; this catches it developing under our feet)
    - wal_checkpoint(TRUNCATE) — bounds WAL growth between boots
    - rotating VACUUM INTO backups (and encrypted file backups) at most
      once per backup_interval_s
    - optional audit-log retention via the caller's SQL
    """
    now = time.time()
    for path in db_paths:
        if not os.path.exists(path):
            continue
        try:
            conn = sqlite3.connect(path, timeout=5)
            try:
                row = conn.execute("PRAGMA integrity_check").fetchone()
                if not (row and row[0] == "ok"):
                    log.error("integrity_check FAILED for %s: %s", path, row)
                    health.note_persist_failure(
                        f"integrity:{os.path.basename(path)}", RuntimeError(str(row))
                    )
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                if audit_retention_sql:
                    conn.execute(
                        audit_retention_sql,
                        (now - audit_retention_days * 86400.0,),
                    )
                    conn.commit()
            finally:
                conn.close()
        except sqlite3.Error as e:
            health.note_persist_failure(f"maintenance:{os.path.basename(path)}", e)

    if now - _last_backup_at(backups_dir) >= backup_interval_s:
        for path in db_paths:
            backup_db(path, backups_dir, keep=keep)
        if encrypted and fernet is not None:
            backup_files_encrypted(encrypted, backups_dir, "sessions", fernet, keep=keep)
    health.last_maintenance_at = now


def _last_backup_at(backups_dir: str) -> float:
    files = glob.glob(os.path.join(backups_dir, "*-*.db")) + glob.glob(
        os.path.join(backups_dir, "*.zip.enc")
    )
    return max((os.path.getmtime(f) for f in files), default=0.0)


async def maintenance_loop(interval_s: float, **kwargs) -> None:
    """Lifespan task wrapper: run_maintenance every interval_s."""
    while True:
        await asyncio.sleep(interval_s)
        try:
            await asyncio.to_thread(run_maintenance, **kwargs)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # never die — maintenance is best-effort
            log.error("maintenance pass crashed: %s", e)
