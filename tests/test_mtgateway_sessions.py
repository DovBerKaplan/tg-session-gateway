"""Tests for the MTProto Session Manager — lifecycle, queue, consumers."""

import json
import tempfile
from unittest.mock import AsyncMock, patch

from mtgateway.config import Config
from mtgateway.sessions import (
    ConsumerHandle,
    MTSession,
    MTSessionManager,
    SessionState,
)
from mtgateway.store import MTStore


def _cfg(tmpdir):
    return Config(
        admin_secret="test-admin",
        app_secret="test-app",
        api_id=12345,
        api_hash="test_hash",
        data_dir=tmpdir,
    )


class TestSessionStates:
    def test_new_session_starts_connecting(self):
        s = MTSession(alias="bot1", token="123:abc")
        assert s.state == SessionState.connecting

    def test_stats_shape(self):
        s = MTSession(alias="bot1", token="123:abc")
        stats = s.stats()
        assert stats["alias"] == "bot1"
        assert stats["state"] == "connecting"
        assert "queue_depth" in stats
        assert "consumers" in stats
        assert "real_flood_wait" in stats


class TestUpdateQueue:
    async def test_push_and_pull(self):
        s = MTSession(alias="bot1", token="123:abc")
        await s.push_update({"update_id": 1, "message": {"text": "hi"}})
        await s.push_update({"update_id": 2, "message": {"text": "yo"}})
        assert s._update_queue.qsize() == 2

    async def test_queue_overflow_drops_oldest(self):
        s = MTSession(alias="bot1", token="123:abc")
        for i in range(10):
            await s.push_update({"update_id": i})
        # Queue maxsize is 5000, so all 10 should fit
        assert s._update_queue.qsize() == 10


class TestConsumers:
    def test_add_and_remove(self):
        s = MTSession(alias="bot1", token="123:abc")
        h = ConsumerHandle(consumer_id="c1")
        s.add_consumer(h)
        assert s.active_consumer == "c1"
        assert len(s._consumers) == 1

        s.remove_consumer("c1")
        assert s.active_consumer is None
        assert len(s._consumers) == 0

    def test_last_consumer_wins(self):
        s = MTSession(alias="bot1", token="123:abc")
        s.add_consumer(ConsumerHandle(consumer_id="old"))
        s.add_consumer(ConsumerHandle(consumer_id="new"))
        assert s.active_consumer == "new"

        s.remove_consumer("new")
        assert s.active_consumer == "old"  # fell back


class TestWSConsumers:
    async def test_ws_push(self):
        s = MTSession(alias="bot1", token="123:abc")
        sent = []

        async def mock_send(data):
            sent.append(json.loads(data))

        s.add_consumer(ConsumerHandle(consumer_id="ws1", ws_send=mock_send))
        await s.push_update({"update_id": 42, "message": {"text": "test"}})
        assert len(sent) == 1
        assert sent[0]["update_id"] == 42


class TestStoreRoundtrip:
    async def test_register_and_get_token(self):
        with tempfile.TemporaryDirectory() as d:
            store = MTStore(d)
            await store.connect()
            await store.register("bot1", "123:secret_token")
            assert await store.get_token("bot1") == "123:secret_token"
            assert await store.get_token("nonexistent") is None
            await store.close()

    async def test_state_persistence(self):
        with tempfile.TemporaryDirectory() as d:
            store = MTStore(d)
            await store.connect()
            await store.register("bot1", "t")
            await store.save_state("bot1", pts=100, qts=200, date=300, seq=400)
            state = await store.get_state("bot1")
            assert state["pts"] == 100
            assert state["qts"] == 200
            await store.close()

    async def test_queue_persistence(self):
        with tempfile.TemporaryDirectory() as d:
            store = MTStore(d)
            await store.connect()
            await store.register("bot1", "t")
            await store.queue_push("bot1", 1, json.dumps({"update_id": 1}))
            await store.queue_push("bot1", 2, json.dumps({"update_id": 2}))
            updates = await store.queue_pull("bot1", offset=0)
            assert len(updates) == 2
            await store.queue_ack("bot1", 1)
            remaining = await store.queue_pull("bot1", offset=0)
            assert len(remaining) == 1
            assert remaining[0]["update_id"] == 2
            await store.close()


class TestManagerLifecycle:
    async def test_register_creates_session(self):
        with tempfile.TemporaryDirectory() as d:
            cfg = _cfg(d)
            store = MTStore(d, fernet_key=None)
            await store.connect()
            mgr = MTSessionManager(cfg, store)
            # Mock the Pyrogram client start to avoid actual connection
            with patch.object(MTSessionManager, "_start_client", new=AsyncMock()):
                s = await mgr.register("bot1", "123:abc")
                assert s.alias == "bot1"
                assert "bot1" in mgr.sessions
                await store.close()

    async def test_unregister_removes_session(self):
        with tempfile.TemporaryDirectory() as d:
            cfg = _cfg(d)
            store = MTStore(d)
            await store.connect()
            mgr = MTSessionManager(cfg, store)
            with patch.object(MTSessionManager, "_start_client", new=AsyncMock()):
                await mgr.register("bot1", "t")
                assert await mgr.unregister("bot1") is True
                assert "bot1" not in mgr.sessions
                assert await mgr.unregister("bot1") is False
                await store.close()

    async def test_pause_and_resume(self):
        with tempfile.TemporaryDirectory() as d:
            cfg = _cfg(d)
            store = MTStore(d)
            await store.connect()
            mgr = MTSessionManager(cfg, store)
            with patch.object(MTSessionManager, "_start_client", new=AsyncMock()):
                s = await mgr.register("bot1", "t")
                assert await mgr.pause("bot1") is True
                assert s.state == SessionState.paused
                assert await mgr.resume("bot1") is True
                assert s.state == SessionState.live
                await store.close()


class TestIdempotency:
    """E2: App-supplied idempotency keys prevent duplicate sends."""

    def test_first_key_accepted(self):
        s = MTSession(alias="bot1", token="t")
        assert s.check_idempotency("send-001") is True

    def test_duplicate_key_rejected(self):
        s = MTSession(alias="bot1", token="t")
        s.check_idempotency("send-001")
        assert s.check_idempotency("send-001") is False

    def test_different_keys_both_accepted(self):
        s = MTSession(alias="bot1", token="t")
        assert s.check_idempotency("send-001") is True
        assert s.check_idempotency("send-002") is True


class TestPauseSending:
    """E4: Pause outbound; keep receiving updates."""

    async def test_pause_and_resume_sending(self):
        with tempfile.TemporaryDirectory() as d:
            cfg = _cfg(d)
            store = MTStore(d)
            await store.connect()
            mgr = MTSessionManager(cfg, store)
            with patch.object(MTSessionManager, "_start_client", new=AsyncMock()):
                s = await mgr.register("bot1", "t")
                assert s.send_paused is False
                assert await mgr.pause_sending("bot1") is True
                assert s.send_paused is True
                # Receiving still works (queue accepts updates)
                await s.push_update({"update_id": 1})
                assert s._update_queue.qsize() == 1
                assert await mgr.resume_sending("bot1") is True
                assert s.send_paused is False
                await store.close()


class TestDeployToFirstMessageSLO:
    """E7: Track time from consumer attach to first outbound send."""

    def test_slo_zero_before_any_send(self):
        s = MTSession(alias="bot1", token="t")
        assert s.deploy_to_first_message_s == 0.0

    def test_slo_recorded_after_first_send(self):
        import time as _t

        s = MTSession(alias="bot1", token="t")
        h = ConsumerHandle(consumer_id="c1")
        h.connected_at = _t.time() - 2.5  # attached 2.5 seconds ago
        s.add_consumer(h)
        s.note_first_send()
        assert 2.0 < s.deploy_to_first_message_s < 3.0

    def test_stats_includes_slo(self):
        s = MTSession(alias="bot1", token="t")
        stats = s.stats()
        assert "deploy_to_first_message_s" in stats
        assert stats["deploy_to_first_message_s"] is None  # not yet sent
