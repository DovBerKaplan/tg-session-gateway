"""Persistent state: registered bots, encrypted tokens, telegram offsets.

SQLite with WAL — the write volume is tiny (one offset per poll batch).
Tokens are encrypted at rest with Fernet; the key comes from GW_FERNET_KEY
or is generated once into the data dir (with a loud warning).
"""

from __future__ import annotations

import logging
import os
import secrets

import aiosqlite
from cryptography.fernet import Fernet, InvalidToken

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
"""


class Store:
    def __init__(self, data_dir: str, fernet_key: str | None = None):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.path = os.path.join(data_dir, "gateway.db")
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
        self._db = await aiosqlite.connect(self.path)
        self._db.row_factory = aiosqlite.Row
        await self.db.execute("PRAGMA journal_mode=WAL")
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
