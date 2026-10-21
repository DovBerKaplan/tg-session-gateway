"""Persistent state: registered bots, encrypted tokens, telegram offsets.

SQLite with WAL — the write volume is tiny (one offset per poll batch).
Tokens are encrypted at rest with Fernet; the key comes from GW_FERNET_KEY
or is generated once into the data dir (with a loud warning).
"""

from __future__ import annotations

import logging
import os
import secrets
import time

import aiosqlite
from cryptography.fernet import Fernet, InvalidToken

from .dbhealth import StorageHealth, check_or_quarantine, pragma_statements

log = logging.getLogger("gateway.store")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS bots (
    alias        TEXT PRIMARY KEY,
    token_enc    BLOB NOT NULL,
    telegram_offset INTEGER NOT NULL DEFAULT 0,
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    paused       INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS admin_audit (
    ts       TEXT NOT NULL DEFAULT (datetime('now')),
    action   TEXT NOT NULL,
    alias    TEXT NOT NULL,
    detail   TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS chat_backoff (
    alias        TEXT NOT NULL,
    chat_id      INTEGER NOT NULL,
    paused_until REAL NOT NULL,  -- wall clock
    PRIMARY KEY (alias, chat_id)
);
CREATE TABLE IF NOT EXISTS idempotency_keys (
    alias    TEXT NOT NULL,
    key      TEXT NOT NULL,
    state    TEXT NOT NULL DEFAULT 'pending',  -- pending | done
    seen_at  REAL NOT NULL,
    PRIMARY KEY (alias, key)
);
"""


class Store:
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
        self.path = os.path.join(data_dir, "gateway.db")
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
        with open(key_path, "wb") as fh:  # 0600
            os.chmod(key_path, 0o600)
            fh.write(generated)
        log.warning(
            "GW_FERNET_KEY not set — generated a key at %s "
            "(back it up; losing it loses the stored tokens)",
            key_path,
        )
        return Fernet(generated)

    async def connect(self) -> None:
        # boot gate: a corrupt DB is quarantined (and a backup restored,
        # if one exists) instead of crashing the process into a loop
        check_or_quarantine(self.path, self.backups_dir, self.health)
        self._db = await aiosqlite.connect(self.path)
        self._db.row_factory = aiosqlite.Row
        for stmt in pragma_statements(self.durability):
            await self.db.execute(stmt)
        await self.db.executescript(_SCHEMA)
        await self.db.commit()

    async def close(self) -> None:
        if self._db:
            await self.db.close()

    @property
    def db(self) -> aiosqlite.Connection:
        """The live connection; every method below requires connect() first."""
        if self._db is None:
            raise RuntimeError("Store used before connect() — call await connect() first")
        return self._db

    # ── bots ─────────────────────────────────────────────────────────

    async def register_bot(self, alias: str, token: str) -> None:
        enc = self._fernet.encrypt(token.encode())
        await self.db.execute(
            "INSERT INTO bots (alias, token_enc) VALUES (?, ?) "
            "ON CONFLICT (alias) DO UPDATE SET token_enc = excluded.token_enc",
            (alias, enc),
        )
        await self.db.commit()

    async def delete_bot(self, alias: str) -> bool:
        cur = await self.db.execute("DELETE FROM bots WHERE alias = ?", (alias,))
        await self.db.commit()
        return cur.rowcount > 0

    async def set_paused(self, alias: str, paused: bool) -> None:
        await self.db.execute("UPDATE bots SET paused = ? WHERE alias = ?", (int(paused), alias))
        await self.db.commit()

    async def list_bots(self) -> list[dict]:
        async with self.db.execute("SELECT alias, telegram_offset, paused FROM bots") as cur:
            return [dict(r) for r in await cur.fetchall()]

    async def get_token(self, alias: str) -> str | None:
        async with self.db.execute("SELECT token_enc FROM bots WHERE alias = ?", (alias,)) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        try:
            return self._fernet.decrypt(row["token_enc"]).decode()
        except InvalidToken:
            log.error("cannot decrypt token for %s (wrong GW_FERNET_KEY?)", alias)
            return None

    # ── offsets (spec §6.1: survives gateway restart) ────────────────

    async def get_offset(self, alias: str) -> int:
        async with self.db.execute(
            "SELECT telegram_offset FROM bots WHERE alias = ?", (alias,)
        ) as cur:
            row = await cur.fetchone()
        return row["telegram_offset"] if row else 0

    async def save_offset(self, alias: str, offset: int) -> None:
        await self.db.execute(
            "UPDATE bots SET telegram_offset = ? WHERE alias = ?", (offset, alias)
        )
        await self.db.commit()

    # ── FLOOD_WAIT backoff ledger ────────────────────────────────────
    # The guard's chat pause is in-memory; a restart mid-FLOOD_WAIT used
    # to forget it and immediately hammer Telegram again. The ledger
    # persists the pause so the rebooted gateway waits it out instead.

    async def save_backoff(self, alias: str, chat_id: int | None, until: float) -> None:
        if chat_id is None:  # bot-wide egress pause: single well-known row
            chat_id = 0
        try:
            await self.db.execute(
                "INSERT INTO chat_backoff (alias, chat_id, paused_until) VALUES (?, ?, ?) "
                "ON CONFLICT(alias, chat_id) DO UPDATE SET paused_until = excluded.paused_until",
                (alias, chat_id, until),
            )
            await self.db.commit()
        except Exception as e:  # ledger is best-effort, never blocks sends
            log.warning("save_backoff(%s,%s) failed: %s", alias, chat_id, e)

    async def load_backoffs(self, alias: str) -> list[tuple[int, float]]:
        """Active (chat_id, paused_until) rows; expired ones are pruned."""
        try:
            now = time.time()
            await self.db.execute(
                "DELETE FROM chat_backoff WHERE alias = ? AND paused_until <= ?", (alias, now)
            )
            await self.db.commit()
            async with self.db.execute(
                "SELECT chat_id, paused_until FROM chat_backoff WHERE alias = ?", (alias,)
            ) as cur:
                rows = await cur.fetchall()
            return [(r["chat_id"], r["paused_until"]) for r in rows]
        except Exception as e:
            log.warning("load_backoffs(%s) failed: %s", alias, e)
            return []

    # ── send idempotency (spec E2 — Idempotency-Key header) ─────────

    IDEMPOTENCY_TTL_S = 300.0
    STALE_PENDING_S = 60.0  # a claim older than this died mid-send

    async def idempotency_claim(self, alias: str, key: str) -> str:
        """'claimed'/'reclaimed' (proceed with the send) or 'replay'
        (a completed send with this key exists — suppress). Atomic."""
        now = time.time()
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
        cur = await self.db.execute(
            "UPDATE idempotency_keys SET seen_at = ? "
            "WHERE alias = ? AND key = ? AND state = 'pending' AND seen_at < ?",
            (now, alias, key, now - self.STALE_PENDING_S),
        )
        await self.db.commit()
        return "reclaimed" if cur.rowcount == 1 else "replay"

    async def idempotency_finish(self, alias: str, key: str) -> None:
        await self.db.execute(
            "UPDATE idempotency_keys SET state = 'done', seen_at = ? WHERE alias = ? AND key = ?",
            (time.time(), alias, key),
        )
        await self.db.commit()

    async def idempotency_fail(self, alias: str, key: str) -> None:
        """The send failed — release the claim so retries pass."""
        await self.db.execute(
            "DELETE FROM idempotency_keys WHERE alias = ? AND key = ? AND state = 'pending'",
            (alias, key),
        )
        await self.db.commit()

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

    @staticmethod
    def token_suffix(token: str) -> str:
        """Logging-safe token identity (spec §7: alias/suffix only)."""
        return f"…{token[-4:]}" if token else "…"


def new_alias(prefix: str = "bot") -> str:
    return f"{prefix}-{secrets.token_hex(4)}"
