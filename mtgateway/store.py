"""Persistent store for MTProto session metadata and update state.

SQLite WAL — same pattern as the Bot API gateway store. Holds:
- Registered sessions (alias, encrypted token, api credentials)
- Telegram update state (pts, qts, date, seq) per session
- Update queue (for disk persistence in Phase 3)
"""

from __future__ import annotations

import logging
import os
import time

import aiosqlite
from cryptography.fernet import Fernet, InvalidToken

from gateway.dbhealth import (
    StorageHealth,
    check_or_quarantine,
    ensure_schema_async,
    pragma_statements,
)

log = logging.getLogger("mtgateway.store")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    alias       TEXT PRIMARY KEY,
    token_enc   BLOB NOT NULL,
    phone_or_token TEXT NOT NULL DEFAULT '',
    pts         INTEGER NOT NULL DEFAULT 0,
    qts         INTEGER NOT NULL DEFAULT 0,
    date        INTEGER NOT NULL DEFAULT 0,
    seq         INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    paused      INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS update_queue (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    alias       TEXT NOT NULL,
    update_id   INTEGER NOT NULL,
    update_json TEXT NOT NULL,
    enqueued_at REAL NOT NULL,
    FOREIGN KEY (alias) REFERENCES sessions(alias) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_queue_alias ON update_queue(alias, update_id);

CREATE TABLE IF NOT EXISTS admin_audit (
    ts       TEXT NOT NULL DEFAULT (datetime('now')),
    action   TEXT NOT NULL,
    alias    TEXT NOT NULL,
    detail   TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS idempotency_keys (
    alias    TEXT NOT NULL,
    key      TEXT NOT NULL,
    state    TEXT NOT NULL DEFAULT 'pending',  -- pending | done
    seen_at  REAL NOT NULL,
    PRIMARY KEY (alias, key)
);
CREATE TABLE IF NOT EXISTS dead_letters (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    alias   TEXT NOT NULL,
    payload TEXT NOT NULL,
    reason  TEXT NOT NULL,
    at      REAL NOT NULL
);
"""


class MTStore:
    """SQLite-backed persistence for MTProto session state."""

    def __init__(
        self,
        data_dir: str,
        fernet_key: str | None = None,
        durability: str = "normal",
        backups_dir: str | None = None,
        health: StorageHealth | None = None,
    ):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        os.makedirs(os.path.join(data_dir, "sessions"), exist_ok=True)
        self.path = os.path.join(data_dir, "mtgateway.db")
        self.durability = durability
        self.backups_dir = backups_dir
        self.health = health
        self._db: aiosqlite.Connection | None = None
        self._fernet = self._load_fernet(fernet_key)

    def _load_fernet(self, key: str | None) -> Fernet:
        key_path = os.path.join(self.data_dir, ".fernet_key")
        if key:
            return Fernet(key.encode())
        if os.path.exists(key_path):
            with open(key_path, "rb") as fh:
                return Fernet(fh.read())
        generated = Fernet.generate_key()
        with open(key_path, "wb") as fh:
            os.chmod(key_path, 0o600)
            fh.write(generated)
        log.warning(
            "GW_FERNET_KEY not set — generated at %s (back it up; losing it loses stored tokens)",
            key_path,
        )
        return Fernet(generated)

    async def connect(self) -> None:
        # boot gate: quarantine a corrupt DB (restore newest backup if
        # any) — the process stays alive instead of crash-looping
        check_or_quarantine(self.path, self.backups_dir, self.health)
        self._db = await aiosqlite.connect(self.path)
        self._db.row_factory = aiosqlite.Row
        for stmt in pragma_statements(self.durability, foreign_keys=True):
            await self.db.execute(stmt)
        await self.db.executescript(_SCHEMA)
        await self._migrate_idempotency()
        await ensure_schema_async(self.db, 1, {}, self.path, self.backups_dir)
        await self.db.commit()

    async def _migrate_idempotency(self) -> None:
        """v1 had `key` as the lone PK — the same key on two bots
        collided. v2: (alias, key) PK + pending/done state. Old rows
        migrate as 'done' (the v1 read-side treated recorded = replay).
        One-time, idempotent."""
        cur = await self.db.execute("PRAGMA table_info(idempotency_keys)")
        cols = [r[1] for r in await cur.fetchall()]
        if not cols or "state" in cols:
            return
        log.warning("migrating idempotency_keys to (alias,key) PK + state")
        await self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS idempotency_keys_v2 (
                alias    TEXT NOT NULL,
                key      TEXT NOT NULL,
                state    TEXT NOT NULL DEFAULT 'pending',
                seen_at  REAL NOT NULL,
                PRIMARY KEY (alias, key)
            );
            """
        )
        try:
            await self.db.execute(
                "INSERT OR IGNORE INTO idempotency_keys_v2 (alias, key, state, seen_at) "
                "SELECT alias, key, 'done', seen_at FROM idempotency_keys"
            )
        except Exception as e:  # v1 table may be empty/absent shapes
            log.warning("idempotency migration copy skipped: %s", e)
        await self.db.execute("DROP TABLE IF EXISTS idempotency_keys")
        await self.db.execute("ALTER TABLE idempotency_keys_v2 RENAME TO idempotency_keys")

    async def close(self) -> None:
        if self._db:
            await self.db.close()

    @property
    def db(self) -> aiosqlite.Connection:
        """The live connection; every method below requires connect() first."""
        if self._db is None:
            raise RuntimeError("MTStore used before connect() — call await connect() first")
        return self._db

    # ── sessions ─────────────────────────────────────────────────────

    async def register(self, alias: str, token: str) -> None:
        enc = self._fernet.encrypt(token.encode())
        await self.db.execute(
            "INSERT INTO sessions (alias, token_enc, phone_or_token) "
            "VALUES (?, ?, ?) "
            "ON CONFLICT (alias) DO UPDATE SET token_enc = excluded.token_enc",
            (alias, enc, token[:20] + "…"),
        )
        await self.db.commit()

    async def delete(self, alias: str) -> bool:
        cur = await self.db.execute("DELETE FROM sessions WHERE alias = ?", (alias,))
        await self.db.commit()
        return cur.rowcount > 0

    async def list_sessions(self) -> list[dict]:
        async with self.db.execute(
            "SELECT alias, phone_or_token, pts, qts, date, seq, paused, created_at FROM sessions"
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]

    async def get_token(self, alias: str) -> str | None:
        async with self.db.execute(
            "SELECT token_enc FROM sessions WHERE alias = ?", (alias,)
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        try:
            return self._fernet.decrypt(row["token_enc"]).decode()
        except InvalidToken:
            log.error("cannot decrypt token for %s (wrong GW_FERNET_KEY?)", alias)
            return None

    async def set_paused(self, alias: str, paused: bool) -> None:
        await self.db.execute(
            "UPDATE sessions SET paused = ? WHERE alias = ?", (int(paused), alias)
        )
        await self.db.commit()

    async def verify_tokens(self) -> list[str]:
        """Boot probe: aliases whose stored token cannot be decrypted."""
        bad = []
        for row in await self.list_sessions():
            if await self.get_token(row["alias"]) is None:
                bad.append(row["alias"])
        return bad

    # ── update state (pts/qts/date/seq) ──────────────────────────────

    async def get_state(self, alias: str) -> dict:
        async with self.db.execute(
            "SELECT pts, qts, date, seq FROM sessions WHERE alias = ?", (alias,)
        ) as cur:
            row = await cur.fetchone()
        return dict(row) if row else {"pts": 0, "qts": 0, "date": 0, "seq": 0}

    async def save_state(self, alias: str, pts: int, qts: int, date: int, seq: int) -> None:
        await self.db.execute(
            "UPDATE sessions SET pts = ?, qts = ?, date = ?, seq = ? WHERE alias = ?",
            (pts, qts, date, seq, alias),
        )
        await self.db.commit()

    # ── update queue (Phase 3: disk persistence) ─────────────────────

    async def queue_push(self, alias: str, update_id: int, update_json: str) -> None:
        import time

        await self.db.execute(
            "INSERT INTO update_queue (alias, update_id, update_json, enqueued_at) "
            "VALUES (?, ?, ?, ?)",
            (alias, update_id, update_json, time.time()),
        )
        await self.db.commit()

    async def queue_pull(self, alias: str, offset: int, limit: int = 100) -> list[dict]:
        import json

        async with self.db.execute(
            "SELECT update_id, update_json FROM update_queue "
            "WHERE alias = ? AND update_id > ? ORDER BY update_id LIMIT ?",
            (alias, offset, limit),
        ) as cur:
            rows = await cur.fetchall()
        return [{"update_id": r["update_id"], **json.loads(r["update_json"])} for r in rows]

    async def queue_ack(self, alias: str, last_seen: int) -> None:
        await self.db.execute(
            "DELETE FROM update_queue WHERE alias = ? AND update_id <= ?",
            (alias, last_seen),
        )
        await self.db.commit()

    async def queue_depth(self, alias: str) -> int:
        async with self.db.execute(
            "SELECT COUNT(*) as n FROM update_queue WHERE alias = ?", (alias,)
        ) as cur:
            row = await cur.fetchone()
        return row["n"] if row else 0

    # ── idempotency (E2: survives kill -9 — duplicate sends must not) ──

    IDEMPOTENCY_TTL_S = 300.0
    STALE_PENDING_S = 60.0  # a claim older than this died mid-send

    async def idempotency_claim(self, alias: str, key: str) -> str:
        """Atomically claim an idempotency key.

        - 'claimed'  — new key, proceed with the send
        - 'reclaimed' — a previous attempt died mid-send (stale pending);
                       retry is ALLOWED, not suppressed
        - 'replay'   — a completed send with this key exists; suppress
        """
        now = time.time()
        # expired rows must not block a fresh claim (TTL semantics)
        await self.db.execute(
            "DELETE FROM idempotency_keys WHERE alias = ? AND key = ? AND seen_at < ?",
            (alias, key, now - self.IDEMPOTENCY_TTL_S),
        )
        cur = await self.db.execute(
            "INSERT INTO idempotency_keys (alias, key, state, seen_at) "
            "VALUES (?, ?, 'pending', ?) ON CONFLICT(alias, key) DO NOTHING",
            (alias, key, now),
        )
        await self.db.commit()
        if cur.rowcount == 1:
            return "claimed"
        # a stale pending claim (crashed attempt) is reclaimable — the
        # UPDATE's WHERE clause makes the reclaim itself atomic
        cur = await self.db.execute(
            "UPDATE idempotency_keys SET seen_at = ? "
            "WHERE alias = ? AND key = ? AND state = 'pending' AND seen_at < ?",
            (now, alias, key, now - self.STALE_PENDING_S),
        )
        await self.db.commit()
        return "reclaimed" if cur.rowcount == 1 else "replay"

    async def idempotency_finish(self, alias: str, key: str) -> None:
        """The send completed — this key now suppresses retries."""
        await self.db.execute(
            "UPDATE idempotency_keys SET state = 'done', seen_at = ? WHERE alias = ? AND key = ?",
            (time.time(), alias, key),
        )
        await self.db.commit()

    async def idempotency_fail(self, alias: str, key: str) -> None:
        """The send FAILED — release the key so a retry is not
        suppressed (the old code recorded before sending and then
        swallowed every retry)."""
        await self.db.execute(
            "DELETE FROM idempotency_keys WHERE alias = ? AND key = ? AND state = 'pending'",
            (alias, key),
        )
        await self.db.commit()

    async def idempotency_seen(self, alias: str, key: str) -> bool:
        """True when a DONE key is active (a replayed send)."""
        row = await self.db.execute_fetchall(
            "SELECT 1 FROM idempotency_keys "
            "WHERE alias = ? AND key = ? AND state = 'done' AND seen_at > ?",
            (alias, key, time.time() - self.IDEMPOTENCY_TTL_S),
        )
        return bool(row)

    async def idempotency_record(self, alias: str, key: str) -> None:
        """Legacy shim: record-and-suppress (v1 semantics)."""
        await self.db.execute(
            "INSERT INTO idempotency_keys (alias, key, state, seen_at) VALUES (?, ?, 'done', ?) "
            "ON CONFLICT(alias, key) DO UPDATE SET state = 'done', seen_at = excluded.seen_at",
            (alias, key, time.time()),
        )
        await self.db.commit()

    async def idempotency_cleanup(self) -> None:
        await self.db.execute(
            "DELETE FROM idempotency_keys WHERE seen_at < ?",
            (time.time() - self.IDEMPOTENCY_TTL_S,),
        )
        await self.db.commit()

    # ── dead letters (poison updates) ────────────────────────────────

    async def dead_letter(self, alias: str, payload: str, reason: str) -> None:
        """Capture an unprocessable update instead of dropping it into
        the void; admin can inspect (and clear) via the API."""
        try:
            await self.db.execute(
                "INSERT INTO dead_letters (alias, payload, reason, at) VALUES (?, ?, ?, ?)",
                (alias, payload, reason, time.time()),
            )
            await self.db.commit()
        except Exception as e:  # DLQ is best-effort
            log.error("dead-letter write failed: %s", e)

    async def list_dead_letters(self, alias: str, limit: int = 100) -> list[dict]:
        async with self.db.execute(
            "SELECT id, payload, reason, at FROM dead_letters WHERE alias = ? "
            "ORDER BY id DESC LIMIT ?",
            (alias, limit),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]

    async def clear_dead_letters(self, alias: str) -> int:
        cur = await self.db.execute("DELETE FROM dead_letters WHERE alias = ?", (alias,))
        await self.db.commit()
        return cur.rowcount

    # ── queue TTL ────────────────────────────────────────────────────

    async def queue_ttl_cleanup(self, alias: str, cutoff: float) -> int:
        """Drop persisted updates older than the TTL (spec §12: queued
        update content is short-lived)."""
        cur = await self.db.execute(
            "DELETE FROM update_queue WHERE alias = ? AND enqueued_at < ?", (alias, cutoff)
        )
        await self.db.commit()
        return cur.rowcount

    # ── admin audit trail (append-only) ───────────────────────────────

    async def audit(self, action: str, alias: str, detail: str = "") -> None:
        """Record an admin action. Never raises into the caller's path."""
        try:
            await self.db.execute(
                "INSERT INTO admin_audit (action, alias, detail) VALUES (?, ?, ?)",
                (action, alias, detail),
            )
            await self.db.commit()
        except Exception as e:  # audit must not break the admin action itself
            log.error("audit write failed (%s %s): %s", action, alias, e)

    async def get_audit(self, limit: int = 100) -> list[dict]:
        async with self.db.execute(
            "SELECT ts, action, alias, detail FROM admin_audit ORDER BY rowid DESC LIMIT ?",
            (limit,),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]
