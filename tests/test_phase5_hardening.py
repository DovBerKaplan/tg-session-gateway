"""Phase 5 hardening — schema migrations, file fencing tokens, and the
chaos battery: locked-DB and mid-transaction death."""

import os
import sqlite3

from gateway.dbhealth import StorageHealth, ensure_schema_async
from gateway.queue import UpdateQueue
from mtgateway.lock import FileLockAdapter, SessionLock


class TestSchemaMigrations:
    async def test_fresh_db_gets_current_version(self, tmp_path):
        import aiosqlite

        path = str(tmp_path / "fresh.db")
        db = await aiosqlite.connect(path)
        await db.execute("CREATE TABLE t(x)")
        await db.commit()
        await ensure_schema_async(db, 3, {}, path, str(tmp_path / "bk"))
        await db.close()
        assert sqlite3.connect(path).execute("PRAGMA user_version").fetchone()[0] == 3

    async def test_migration_applies_once_with_backup(self, tmp_path):
        import aiosqlite

        path = str(tmp_path / "mig.db")
        db = await aiosqlite.connect(path)
        await db.execute("CREATE TABLE t(x)")
        await db.commit()
        await db.execute("PRAGMA user_version = 1")
        await db.commit()
        await db.close()

        migrations = {
            2: "CREATE TABLE IF NOT EXISTS added_in_v2 (id INTEGER PRIMARY KEY);",
        }
        db = await aiosqlite.connect(path)
        await ensure_schema_async(db, 2, migrations, path, str(tmp_path / "bk"))
        await db.close()

        conn = sqlite3.connect(path)
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 2
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        conn.close()
        assert "added_in_v2" in tables
        # a snapshot was taken BEFORE migrating
        assert os.listdir(str(tmp_path / "bk")), "pre-migration backup missing"

        # idempotent: a second connect at the same version applies nothing
        db = await aiosqlite.connect(path)
        await ensure_schema_async(db, 2, migrations, path, str(tmp_path / "bk"))
        await db.close()
        assert sqlite3.connect(path).execute("PRAGMA user_version").fetchone()[0] == 2

    async def test_pre_versioning_db_adopted_as_current(self, tmp_path):
        import aiosqlite

        path = str(tmp_path / "old.db")
        conn = sqlite3.connect(path)  # a db from before user_version existed
        conn.execute("CREATE TABLE legacy(x)")
        conn.commit()
        conn.close()
        db = await aiosqlite.connect(path)
        await ensure_schema_async(db, 1, {}, path, None)
        await db.close()
        assert sqlite3.connect(path).execute("PRAGMA user_version").fetchone()[0] == 1


class TestFileFencing:
    def test_tokens_monotonic_across_acquisitions(self, tmp_path):
        a = SessionLock(alias="b1", lock_dir=str(tmp_path))
        assert a.acquire() is True
        t1 = a.token
        a.release()
        b = SessionLock(alias="b1", lock_dir=str(tmp_path))
        assert b.acquire() is True
        assert b.token == t1 + 1, "fencing tokens must be monotonic"
        b.release()

    async def test_adapter_exposes_token(self, tmp_path):
        lock = FileLockAdapter(SessionLock(alias="b1", lock_dir=str(tmp_path)))
        assert await lock.acquire() is True
        assert isinstance(lock.token, int)
        await lock.release()


class TestChaosLockedDb:
    def test_external_write_lock_fails_visibly_not_fatally(self, tmp_path):
        """Another process holds the DB — persist fails, gets COUNTED,
        memory delivery continues; when the lock releases, persistence
        resumes."""
        path = str(tmp_path / "queue.db")
        q = UpdateQueue(maxsize=10, persist_path=path, bot_alias="b1")
        q.push_all([{"update_id": 1}])

        holder = sqlite3.connect(path, timeout=0.1, isolation_level=None)
        holder.execute("BEGIN EXCLUSIVE")

        health = StorageHealth()
        q2 = UpdateQueue(maxsize=10, persist_path=path, bot_alias="b1", health=health)
        q2.push_all([{"update_id": 2}])  # busy_timeout expires -> failure
        assert q2._q and len(q2._q) == 2, "memory path must continue"
        assert len(q2._seen_ids) == 2

        holder.execute("ROLLBACK")
        holder.close()
        q2.push_all([{"update_id": 3}])
        q2.close()
        # the post-lock release batch is durable
        rows = (
            sqlite3.connect(path)
            .execute("SELECT update_id FROM update_queue WHERE bot_alias='b1' ORDER BY update_id")
            .fetchall()
        )
        assert [r[0] for r in rows] == [1, 3], (
            "update 2 lived only in memory (expected); 1,3 durable"
        )


class TestChaosMidTransactionDeath:
    def test_uncommitted_batch_leaves_no_partial_rows(self, tmp_path):
        """A process dying mid-batch-commit must not leave half a batch
        (WAL atomicity) — the next boot loads exactly the durable past."""
        path = str(tmp_path / "queue.db")
        q = UpdateQueue(maxsize=10, persist_path=path, bot_alias="b1")
        q.push_all([{"update_id": 1}])  # durable
        q.close()

        # simulate death INSIDE the next batch: begin, insert, never commit
        conn = sqlite3.connect(path, isolation_level=None)
        conn.execute("BEGIN")
        conn.execute(
            "INSERT INTO update_queue (bot_alias, update_id, update_json, enqueued_at) "
            "VALUES ('b1', 2, '{}', 1.0)"
        )
        conn.close()  # "kill -9" — no COMMIT

        q2 = UpdateQueue(maxsize=10, persist_path=path, bot_alias="b1")
        ids = [u.update_id for u in q2._q]
        assert ids == [1], "the uncommitted batch must vanish atomically"
        q2.close()
