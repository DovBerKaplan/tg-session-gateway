"""Update queue + offset tests — acceptance criteria 3, 8, 9 (spec §14)."""

import asyncio

from gateway.queue import Overflow, UpdateQueue


class TestQueueOffsets:
    def test_consumer_offset_is_internal_and_independent(self):
        """Telegram offset advances on enqueue; consumer acks separately —
        an app that disappears and returns sees exactly its unacked tail."""
        q = UpdateQueue()
        q.push_all([{"update_id": 9001}, {"update_id": 9002}])
        batch1 = asyncio.run(q.pull(None, timeout=0.05))
        assert [u["update_id"] for u in batch1] == [9001, 9002]

        # app processes 9001, acks via offset=internal id of 9001
        q.ack(batch1[0]["_gw_internal_id"])
        assert q.depth() == 1

        # NEW updates arrive while the app is down
        q.push_all([{"update_id": 9003}])
        batch2 = asyncio.run(q.pull(None, timeout=0.05))
        assert [u["update_id"] for u in batch2] == [9002, 9003]

    def test_no_duplicates_after_ack(self):
        q = UpdateQueue()
        q.push_all([{"update_id": 1}, {"update_id": 2}, {"update_id": 3}])
        b = asyncio.run(q.pull(None, 0.05))
        q.ack(b[1]["_gw_internal_id"])  # acked through #2
        b2 = asyncio.run(q.pull(None, 0.05))
        assert [u["update_id"] for u in b2] == [3]

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
        import os
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
                assert await s2.get_offset("b1") == 9005        # survives (crit 9)
                assert await s2.get_token("b1") == "111:AAAAxxxx"
                await s2.close()

            asyncio.run(scenario())
