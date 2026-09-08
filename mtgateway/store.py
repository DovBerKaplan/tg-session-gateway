"""Persistent store for MTProto session metadata and update state.

SQLite WAL — same pattern as the Bot API gateway store. Holds:
- Registered sessions (alias, encrypted token, api credentials)
- Telegram update state (pts, qts, date, seq) per session
- Update queue (for disk persistence in Phase 3)
"""

from __future__ import annotations

import logging
import os

import aiosqlite
from cryptography.fernet import Fernet, InvalidToken

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
"""


class MTStore:
    """SQLite-backed persistence for MTProto session state."""

    def __init__(self, data_dir: str, fernet_key: str | None = None):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        os.makedirs(os.path.join(data_dir, "sessions"), exist_ok=True)
        self.path = os.path.join(data_dir, "mtgateway.db")
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
        self._db = await aiosqlite.connect(self.path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA foreign_keys=ON")
        await self._db.executescript(_SCHEMA)
        await self._db.commit()

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    # ── sessions ─────────────────────────────────────────────────────

    async def register(self, alias: str, token: str) -> None:
        enc = self._fernet.encrypt(token.encode())
        await self._db.execute(
            "INSERT INTO sessions (alias, token_enc, phone_or_token) "
            "VALUES (?, ?, ?) "
            "ON CONFLICT (alias) DO UPDATE SET token_enc = excluded.token_enc",
            (alias, enc, token[:20] + "…"),
        )
        await self._db.commit()

    async def delete(self, alias: str) -> bool:
        cur = await self._db.execute("DELETE FROM sessions WHERE alias = ?", (alias,))
        await self._db.commit()
        return cur.rowcount > 0

    async def list_sessions(self) -> list[dict]:
        async with self._db.execute(
            "SELECT alias, phone_or_token, pts, qts, date, seq, paused, created_at FROM sessions"
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]

    async def get_token(self, alias: str) -> str | None:
        async with self._db.execute(
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
        await self._db.execute(
            "UPDATE sessions SET paused = ? WHERE alias = ?", (int(paused), alias)
        )
        await self._db.commit()

    # ── update state (pts/qts/date/seq) ──────────────────────────────

    async def get_state(self, alias: str) -> dict:
        async with self._db.execute(
            "SELECT pts, qts, date, seq FROM sessions WHERE alias = ?", (alias,)
        ) as cur:
            row = await cur.fetchone()
        return dict(row) if row else {"pts": 0, "qts": 0, "date": 0, "seq": 0}

    async def save_state(self, alias: str, pts: int, qts: int, date: int, seq: int) -> None:
        await self._db.execute(
            "UPDATE sessions SET pts = ?, qts = ?, date = ?, seq = ? WHERE alias = ?",
            (pts, qts, date, seq, alias),
        )
        await self._db.commit()

    # ── update queue (Phase 3: disk persistence) ─────────────────────

    async def queue_push(self, alias: str, update_id: int, update_json: str) -> None:
        import time

        await self._db.execute(
            "INSERT INTO update_queue (alias, update_id, update_json, enqueued_at) "
            "VALUES (?, ?, ?, ?)",
            (alias, update_id, update_json, time.time()),
        )
        await self._db.commit()

    async def queue_pull(self, alias: str, offset: int, limit: int = 100) -> list[dict]:
        import json

        async with self._db.execute(
            "SELECT update_id, update_json FROM update_queue "
            "WHERE alias = ? AND update_id > ? ORDER BY update_id LIMIT ?",
            (alias, offset, limit),
        ) as cur:
            rows = await cur.fetchall()
        return [{"update_id": r["update_id"], **json.loads(r["update_json"])} for r in rows]

    async def queue_ack(self, alias: str, last_seen: int) -> None:
        await self._db.execute(
            "DELETE FROM update_queue WHERE alias = ? AND update_id <= ?",
            (alias, last_seen),
        )
        await self._db.commit()

    async def queue_depth(self, alias: str) -> int:
        async with self._db.execute(
            "SELECT COUNT(*) as n FROM update_queue WHERE alias = ?", (alias,)
        ) as cur:
            row = await cur.fetchone()
        return row["n"] if row else 0
