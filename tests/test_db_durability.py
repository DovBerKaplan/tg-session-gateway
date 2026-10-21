"""DB durability (Phase 1) — pragmas, boot quarantine + restore,
visible persistence failures, corrupt-row isolation, backups, the
FLOOD_WAIT backoff ledger, and maintenance retention."""

import asyncio
import glob
import os
import sqlite3
import time

from gateway.dbhealth import (
    StorageHealth,
    backup_db,
    backup_files_encrypted,
    check_or_quarantine,
    run_maintenance,
)
from gateway.queue import UpdateQueue
from gateway.store import Store


class TestPragmas:
    async def test_store_applies_durability_pragmas(self, tmp_path):
        store = Store(str(tmp_path), durability="full")
        await store.connect()
        cur = await store.db.execute("PRAGMA synchronous")
        sync = (await cur.fetchone())[0]
        cur = await store.db.execute("PRAGMA busy_timeout")
        timeout = (await cur.fetchone())[0]
        cur = await store.db.execute("PRAGMA wal_autocheckpoint")
        ac = (await cur.fetchone())[0]
        await store.close()
        assert sync == 2  # FULL
        assert timeout == 5000
        assert ac == 1000

    def test_queue_applies_pragmas(self, tmp_path):
        q = UpdateQueue(
            maxsize=10, persist_path=str(tmp_path / "queue.db"), bot_alias="b", durability="normal"
        )
        sync = q._db.execute("PRAGMA synchronous").fetchone()[0]
        timeout = q._db.execute("PRAGMA busy_timeout").fetchone()[0]
        assert sync == 1  # NORMAL
        assert timeout == 5000


class TestBootQuarantine:
    def test_corrupt_db_quarantined_and_fresh_started(self, tmp_path):
        path = str(tmp_path / "gateway.db")
        with open(path, "wb") as f:
            f.write(b"this is definitely not sqlite" * 100)
        health = StorageHealth()
        active = check_or_quarantine(path, str(tmp_path / "backups"), health)
        assert active == path
        assert glob.glob(path + ".corrupt-*"), "corrupt file must be preserved"
        assert health.quarantined_files == 1
        assert health.degraded_reason
        # the service can now open a FRESH database and live
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE t(x)")
        conn.close()

    def test_corrupt_db_restored_from_backup(self, tmp_path):
        path = str(tmp_path / "gateway.db")
        store = Store(str(tmp_path))

        # create a healthy db with a marker row, back it up, then corrupt
        async def seed():
            await store.connect()
            await store.db.execute("CREATE TABLE marker(v TEXT)")
            await store.db.execute("INSERT INTO marker VALUES ('pre-crash')")
            await store.db.commit()
            await store.close()

        asyncio.run(seed())
        backups = str(tmp_path / "backups")
        assert backup_db(path, backups) is not None
        with open(path, "wb") as f:
            f.write(b"garbage after a good backup")
        health = StorageHealth()
        check_or_quarantine(path, backups, health)
        conn = sqlite3.connect(path)
        row = conn.execute("SELECT v FROM marker").fetchone()
        conn.close()
        assert row == ("pre-crash",), "newest healthy backup must be restored"

    def test_healthy_db_untouched(self, tmp_path):
        path = str(tmp_path / "gateway.db")
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE t(x)")
        conn.commit()
        conn.close()
        assert check_or_quarantine(path) == path
        assert not glob.glob(path + ".corrupt-*")


class TestQueueVisibility:
    def test_corrupt_row_skipped_rest_survive(self, tmp_path):
        path = str(tmp_path / "queue.db")
        q = UpdateQueue(maxsize=10, persist_path=path, bot_alias="b1")
        q.push_all([{"update_id": 1}, {"update_id": 2}, {"update_id": 3}])
        q.close()
        # corrupt the middle row's JSON directly
        conn = sqlite3.connect(path)
        conn.execute("UPDATE update_queue SET update_json = '{not json' WHERE update_id = 2")
        conn.commit()
        conn.close()
        health = StorageHealth()
        q2 = UpdateQueue(maxsize=10, persist_path=path, bot_alias="b1", health=health)
        ids = [u.update_id for u in q2._q]
        assert ids == [1, 3], "one corrupt row must not skip the rest"
        assert health.corrupt_rows == 1

    def test_persist_failure_counts_and_degrades(self, tmp_path):
        health = StorageHealth()
        q = UpdateQueue(
            maxsize=10, persist_path=str(tmp_path / "q.db"), bot_alias="b1", health=health
        )
        # simulate a full/read-only disk: every write raises
        q._db.execute("PRAGMA query_only=1")
        q.push_all([{"update_id": 1}])
        assert health.persist_failures >= 1
        assert health.degraded_reason
        assert len(q._q) == 1  # memory path still works


class TestMtgatewayPersistVisibility:
    async def test_push_update_failure_visible(self, tmp_path):
        from mtgateway.sessions import MTSession
        from mtgateway.store import MTStore

        store = MTStore(str(tmp_path))
        await store.connect()
        s = MTSession(alias="b1", token="t")
        s._store = store
        s._health = StorageHealth()

        async def boom(*a, **kw):
            raise OSError(28, "No space left on device")

        store.queue_push = boom  # type: ignore[method-assign]
        await s.push_update({"update_id": 1})
        assert s.persist_failures == 1
        assert s._health.persist_failures == 1
        assert s._health.degraded_reason
        assert s._update_queue.qsize() == 1  # memory path still works
        assert s.stats()["persist_failures"] == 1
        await store.close()


class TestBackups:
    def test_backup_db_rotates(self, tmp_path):
        path = str(tmp_path / "gateway.db")
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE t(x)")
        conn.commit()
        conn.close()
        backups = str(tmp_path / "backups")
        for _ in range(3):
            assert backup_db(path, backups, keep=2) is not None
        files = glob.glob(os.path.join(backups, "*.db"))
        assert len(files) == 2, "rotation must keep exactly `keep`"

    def test_encrypted_file_backup(self, tmp_path):
        from cryptography.fernet import Fernet

        key = Fernet.generate_key()
        fernet = Fernet(key)
        session_file = tmp_path / "bot1.session"
        session_file.write_bytes(b"auth_key material")
        dest = backup_files_encrypted(
            [str(session_file)], str(tmp_path / "backups"), "sessions", fernet, keep=2
        )
        assert dest and os.path.exists(dest)
        with open(dest, "rb") as fh:
            data = Fernet(key).decrypt(fh.read())
        import io
        import zipfile

        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            assert zf.read("bot1.session") == b"auth_key material"


class TestMaintenance:
    def test_audit_retention_prunes_old_rows(self, tmp_path):
        path = str(tmp_path / "gateway.db")
        conn = sqlite3.connect(path)
        conn.execute(
            "CREATE TABLE admin_audit (ts TEXT NOT NULL DEFAULT (datetime('now')), "
            "action TEXT NOT NULL, alias TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '')"
        )
        old = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() - 40 * 86400))
        conn.execute("INSERT INTO admin_audit (ts, action, alias) VALUES (?, 'old', 'x')", (old,))
        conn.execute("INSERT INTO admin_audit (action, alias) VALUES ('new', 'x')")
        conn.commit()
        conn.close()
        health = StorageHealth()
        run_maintenance(
            [path],
            str(tmp_path / "backups"),
            health,
            backup_interval_s=1e12,  # skip backups in this pass
            audit_retention_sql="DELETE FROM admin_audit WHERE CAST(strftime('%s', ts) AS REAL) < ?",
            audit_retention_days=30,
        )
        conn = sqlite3.connect(path)
        actions = [r[0] for r in conn.execute("SELECT action FROM admin_audit")]
        conn.close()
        assert actions == ["new"]


class TestFloodLedger:
    async def test_backoff_survives_restart(self, tmp_path):
        store = Store(str(tmp_path))
        await store.connect()
        until = time.time() + 30
        await store.save_backoff("b1", 501, until)
        rows = await store.load_backoffs("b1")
        assert rows == [(501, until)]
        # expired entries are pruned on load
        await store.save_backoff("b1", 502, time.time() - 1)
        rows = await store.load_backoffs("b1")
        assert [r[0] for r in rows] == [501]
        # egress pause = the well-known chat 0 row
        await store.save_backoff("b1", None, time.time() + 10)
        rows = await store.load_backoffs("b1")
        assert 0 in [r[0] for r in rows]
        await store.close()

    async def test_guard_rearms_from_ledger(self, tmp_path):
        """Full loop: pause → ledger → 'restart' → new guard rearmed."""
        from gateway.rateguard import BotRateGuard

        store = Store(str(tmp_path))
        await store.connect()
        await store.save_backoff("b1", 501, time.time() + 60)
        rows = await store.load_backoffs("b1")
        g2 = BotRateGuard()  # the "restarted" guard
        now = time.time()
        for chat_id, until in rows:
            g2.report_429(chat_id if chat_id != 0 else None, until - now)
        assert g2.chat_send_state(501) == "paused", "ledger must re-arm the pause"
        await store.close()
