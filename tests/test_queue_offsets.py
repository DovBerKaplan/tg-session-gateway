"""Update queue + offset tests — acceptance criteria 3, 8, 9 (spec §14)."""

import asyncio
import os

from gateway.queue import Overflow, UpdateQueue


class TestQueueOffsets:
    def test_ptb_style_offset_contract(self):
        """THE drop-in contract: consumer acks with update_id + 1 exactly
        like against Telegram (aiogram/PTB/grammY) — real ids, big numbers."""
        q = UpdateQueue()
        q.push_all([{"update_id": 9001}, {"update_id": 9002}])
        b1 = asyncio.run(q.pull(None, timeout=0.05))
        assert [u["update_id"] for u in b1] == [9001, 9002]
        assert "_gw_internal_id" not in b1[0]  # verbatim Telegram updates

        # PTB: next getUpdates carries offset = last update_id + 1
        b2 = asyncio.run(q.pull(offset=9002, timeout=0.01))
        assert b2 == []
        assert q.depth() == 0  # 9001, 9002 dropped

        q.push_all([{"update_id": 9003}])
        b3 = asyncio.run(q.pull(None, 0.05))
        assert [u["update_id"] for u in b3] == [9003]

    def test_partial_ack_keeps_tail(self):
        q = UpdateQueue()
        q.push_all([{"update_id": 1}, {"update_id": 2}, {"update_id": 3}])
        b = asyncio.run(q.pull(None, 0.05))
        q.ack(b[1]["update_id"])  # acked through #2
        b2 = asyncio.run(q.pull(None, 0.05))
        assert [u["update_id"] for u in b2] == [3]

    def test_telegram_redelivery_deduped(self):
        q = UpdateQueue()
        q.push_all([{"update_id": 50}, {"update_id": 51}])
        q.push_all([{"update_id": 50}])  # redelivery
        b = asyncio.run(q.pull(None, 0.05))
        assert [u["update_id"] for u in b] == [50, 51]

    def test_consumer_down_queue_grows_to_cap_then_drop_oldest(self):
        """Criterion 8 logic: consumer gone → queue grows, nothing reconnects."""
        q = UpdateQueue(maxsize=5, overflow="drop_oldest")
        for i in range(8):
            q.push_all([{"update_id": i}])
        assert q.depth() == 5
        assert q.dropped == 3
        b = asyncio.run(q.pull(None, 0.05))
        assert [u["update_id"] for u in b] == [3, 4, 5, 6, 7]  # oldest dropped

    def test_reject_new_policy(self):
        q = UpdateQueue(maxsize=2, overflow="reject_new")
        q.push_all([{"update_id": 1}, {"update_id": 2}])
        try:
            q.push_all([{"update_id": 3}])
            assert False, "expected Overflow"
        except Overflow:
            pass

    def test_long_poll_blocks_until_data(self):
        q = UpdateQueue()

        async def scenario():
            asyncio.get_running_loop().call_later(0.1, lambda: q.push_all([{"update_id": 7}]))
            out = await q.pull(None, timeout=2.0)
            return out

        b = asyncio.run(scenario())
        assert [u["update_id"] for u in b] == [7]

    def test_long_poll_timeout_returns_empty(self):
        q = UpdateQueue()
        assert asyncio.run(q.pull(None, timeout=0.05)) == []


class TestOffsetPersistence:
    def test_store_roundtrip(self):
        import tempfile

        from gateway.store import Store

        with tempfile.TemporaryDirectory() as d:

            async def scenario():
                s = Store(d)
                await s.connect()
                await s.register_bot("b1", "111:AAAAxxxx")
                assert await s.get_token("b1") == "111:AAAAxxxx"
                await s.save_offset("b1", 9005)
                assert await s.get_offset("b1") == 9005
                # restart simulation: brand-new store on the same dir
                await s.close()
                s2 = Store(d)
                await s2.connect()
                assert await s2.get_offset("b1") == 9005  # survives (crit 9)
                assert await s2.get_token("b1") == "111:AAAAxxxx"
                await s2.close()

            asyncio.run(scenario())


class TestQueuePersistence:
    """Review §4: queue must survive gateway restart — not just in-memory."""

    def _make_persistent_queue(self, tmpdir, alias="bot1"):
        from gateway.queue import UpdateQueue
        path = os.path.join(tmpdir, "queue.db")
        return UpdateQueue(maxsize=100, persist_path=path, bot_alias=alias), path

    def test_push_persists_to_sqlite(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            q, _ = self._make_persistent_queue(d)
            q.push_all([{"update_id": 100}, {"update_id": 101}])
            assert q.persistent is True
            assert q.stats()["persistent"] is True
            q.close()

    def test_restart_restores_unacked(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            # First "boot": push updates, don't ack
            q1, path = self._make_persistent_queue(d)
            q1.push_all([{"update_id": 100}, {"update_id": 101}, {"update_id": 102}])
            q1.close()

            # "Restart": new queue, same persistence path
            q2 = UpdateQueue(maxsize=100, persist_path=path, bot_alias="bot1")
            batch = asyncio.run(q2.pull(None, timeout=0.05))
            assert [u["update_id"] for u in batch] == [100, 101, 102]
            q2.close()

    def test_restart_respects_consumer_offset(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            # First boot: push 3, ack 2
            q1, path = self._make_persistent_queue(d)
            q1.push_all([{"update_id": 100}, {"update_id": 101}, {"update_id": 102}])
            q1.ack(101)  # consumed through 101
            q1.close()

            # Restart: should only see 102
            q2 = UpdateQueue(maxsize=100, persist_path=path, bot_alias="bot1")
            batch = asyncio.run(q2.pull(None, timeout=0.05))
            assert [u["update_id"] for u in batch] == [102]
            q2.close()

    def test_ack_persists_and_cleans_db(self):
        import sqlite3
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            q, path = self._make_persistent_queue(d)
            q.push_all([{"update_id": 1}, {"update_id": 2}, {"update_id": 3}])
            q.ack(3)  # all consumed
            q.close()

            # Verify DB is clean
            db = sqlite3.connect(path)
            count = db.execute("SELECT COUNT(*) FROM update_queue WHERE bot_alias='bot1'").fetchone()[0]
            db.close()
            assert count == 0

    def test_memory_only_when_no_persist_path(self):
        from gateway.queue import UpdateQueue
        q = UpdateQueue(maxsize=10)
        assert q.persistent is False
        q.push_all([{"update_id": 1}])
        assert q.depth() == 1
