"""Delivery & idempotency semantics (Phase 2).

Pins the consumer-facing contract changes:

- mtgateway getUpdates OFFSET MODE: ack happens on the NEXT pull's
  offset (Bot API style), delivery never acks — a consumer crash
  before processing replays the batch; old no-offset clients keep the
  delivery=ack behavior unchanged.
- idempotency is atomic (concurrent same-key claims: exactly one
  winner), keyed per (alias, key) — the v1 lone-key PK collided
  between bots — and a FAILED send releases its key.
- poison updates (unserializable) land in dead letters instead of
  vanishing.
- GW_UPDATE_QUEUE_MAX / GW_UPDATE_TTL_S are actually wired in
  mtgateway (were hardcoded/ignored).
- The Bot API gateway honors an Idempotency-Key header on writes.
"""

import asyncio
import json

import httpx
import respx

from gateway.config import Config
from mtgateway.store import MTStore


class TestOffsetAck:
    async def _seed(self, tmp_path):
        store = MTStore(str(tmp_path))
        await store.connect()
        await store.register("b1", "t")  # update_queue.alias has an FK to sessions
        for i in range(1, 4):
            await store.queue_push("b1", i, json.dumps({"_gw_seq": i, "n": i}))
        return store

    async def test_delivery_does_not_ack_replay_until_offset(self, tmp_path):
        store = await self._seed(tmp_path)
        # consumer pulls from seq 1 (offset mode) — nothing acked.
        # the route pairs Bot-API offset semantics with the store's
        # exclusive pull: queue_pull(alias, offset - 1) == "from offset"
        b1 = await store.queue_pull("b1", offset=1 - 1, limit=10)
        assert [u["_gw_seq"] for u in b1] == [1, 2, 3]
        # "crash" before processing: the same pull replays identically
        b2 = await store.queue_pull("b1", offset=1 - 1, limit=10)
        assert [u["_gw_seq"] for u in b2] == [1, 2, 3]
        # processed through 2 → next pull with offset=3 acks 1..2
        await store.queue_ack("b1", 3 - 1)
        b3 = await store.queue_pull("b1", offset=3 - 1, limit=10)
        assert [u["_gw_seq"] for u in b3] == [3]
        await store.close()

    async def test_ack_never_deletes_above_the_floor(self, tmp_path):
        """max-seq ack after drop-oldest used to delete never-delivered
        rows; offset mode acks ONLY ≤ offset-1."""
        store = await self._seed(tmp_path)
        await store.queue_ack("b1", 1)  # ack exactly seq 1
        rows = await store.queue_pull("b1", offset=0, limit=10)
        assert [u["_gw_seq"] for u in rows] == [2, 3]
        await store.close()


class TestAtomicIdempotency:
    async def test_concurrent_claims_exactly_one_winner(self, tmp_path):
        store = MTStore(str(tmp_path))
        await store.connect()
        verdicts = await asyncio.gather(*[store.idempotency_claim("b1", "k") for _ in range(8)])
        assert verdicts.count("claimed") == 1
        assert verdicts.count("replay") == 7
        await store.close()

    async def test_same_key_different_aliases_do_not_collide(self, tmp_path):
        """v1 had `key` as the lone PK — two bots sharing a key
        suppressed each other's sends."""
        store = MTStore(str(tmp_path))
        await store.connect()
        assert await store.idempotency_claim("botA", "shared") == "claimed"
        assert await store.idempotency_claim("botB", "shared") == "claimed"
        await store.close()

    async def test_stale_pending_is_reclaimed(self, tmp_path):
        """A claim whose send died mid-flight must not suppress the
        retry forever."""
        import time

        store = MTStore(str(tmp_path))
        await store.connect()
        assert await store.idempotency_claim("b1", "k") == "claimed"
        # simulate: holder died right after claiming
        await store.db.execute("UPDATE idempotency_keys SET seen_at = ?", (time.time() - 120,))
        await store.db.commit()
        assert await store.idempotency_claim("b1", "k") == "reclaimed"
        await store.close()

    async def test_v1_schema_migrates(self, tmp_path):
        import sqlite3

        # hand-build a v1 database (lone-key PK, no state column)
        db_path = str(tmp_path / "mtgateway.db")
        conn = sqlite3.connect(db_path)
        conn.executescript(
            """
            CREATE TABLE idempotency_keys (key TEXT PRIMARY KEY, alias TEXT, seen_at REAL);
            INSERT INTO idempotency_keys VALUES ('old-key', 'bot1', 12345.0);
            """
        )
        conn.commit()
        conn.close()
        store = MTStore(str(tmp_path))
        await store.connect()
        row = await store.db.execute_fetchall(
            "SELECT state FROM idempotency_keys WHERE alias='bot1' AND key='old-key'"
        )
        assert row and row[0][0] == "done", "v1 rows migrate as done"
        # and the new PK shape works
        assert await store.idempotency_claim("other", "old-key") == "claimed"
        await store.close()


class TestDeadLetters:
    async def test_unserializable_update_captured_not_lost(self, tmp_path):
        from mtgateway.sessions import MTSession

        store = MTStore(str(tmp_path))
        await store.connect()
        s = MTSession(alias="b1", token="t")
        s._store = store
        poison = {"update_id": 1, "blob": b"\x00\x01"}  # bytes: json.dumps fails
        await s.push_update(poison)
        assert s._update_queue.qsize() == 0, "poison must not enqueue"
        letters = await store.list_dead_letters("b1")
        assert len(letters) == 1
        assert "unserializable" in letters[0]["reason"]
        await store.clear_dead_letters("b1")
        assert await store.list_dead_letters("b1") == []
        await store.close()


class TestEnvWiring:
    async def test_queue_max_and_ttl_wired(self, tmp_path):
        import time
        from types import SimpleNamespace

        from mtgateway.sessions import MTSessionManager

        cfg = SimpleNamespace(
            session_dir=str(tmp_path),
            rate=None,
            mtproto_rate=None,
            policy=SimpleNamespace(update_queue_max=5, update_ttl_s=3600.0),
        )
        store = MTStore(str(tmp_path))
        await store.connect()
        await store.register("b1", "t")  # FK: queue rows need the session row
        mgr = MTSessionManager.__new__(MTSessionManager)
        mgr.cfg = cfg
        mgr.store = store
        s = mgr._create_session("b1", "t")
        assert s._update_queue.maxsize == 5

        # TTL prune: an aged-out update leaves the in-memory queue
        for i in range(3):
            await s.push_update({"n": i})
        s._update_queue._queue[-1]["_gw_enqueued_at"] = time.time() - 7200
        dropped = await s.prune_expired_updates(3600)
        assert dropped == 1
        assert s._update_queue.qsize() == 2
        await store.close()


class TestBotApiIdempotencyHeader:
    @respx.mock
    async def test_key_suppresses_duplicate_and_releases_on_failure(self, tmp_path):
        import os

        os.environ.setdefault("GW_ADMIN_SECRET", "adm-secret")  # Config() reads env
        from gateway.app import create_app

        BASE = "https://api.telegram.org"
        TOKEN = "700000:itest-idem"
        ADM = {"authorization": "Bearer adm-secret"}
        send = respx.post(f"{BASE}/bot{TOKEN}/sendMessage").mock(
            return_value=httpx.Response(200, json={"ok": True, "result": {"message_id": 1}})
        )
        cfg = Config()
        cfg.admin_secret = "adm-secret"
        cfg.app_secret = "app-secret"
        cfg.data_dir = str(tmp_path)
        cfg.upstream = BASE
        cfg.poll_timeout = 0
        cfg.poll_backoff_max = 0.1
        app = create_app(cfg)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c,
        ):
            await c.post("/admin/bots", json={"token": TOKEN, "alias": "b1"}, headers=ADM)
            r1 = await c.post(
                "/bots/b1/sendMessage",
                json={"chat_id": 1, "text": "x"},
                headers={"authorization": "Bearer app-secret", "Idempotency-Key": "op-1"},
            )
            assert r1.status_code == 200 and send.call_count == 1
            r2 = await c.post(
                "/bots/b1/sendMessage",
                json={"chat_id": 1, "text": "x"},
                headers={"authorization": "Bearer app-secret", "Idempotency-Key": "op-1"},
            )
            body = r2.json()
            assert body.get("idempotent_replay") is True
            assert send.call_count == 1, "replay must not hit Telegram"

            # a FAILED send releases the key — the retry goes through
            send.mock(return_value=httpx.Response(500, json={"ok": False}))
            r3 = await c.post(
                "/bots/b1/sendMessage",
                json={"chat_id": 1, "text": "x"},
                headers={"authorization": "Bearer app-secret", "Idempotency-Key": "op-2"},
            )
            assert r3.status_code == 500
            send.mock(
                return_value=httpx.Response(200, json={"ok": True, "result": {"message_id": 2}})
            )
            r4 = await c.post(
                "/bots/b1/sendMessage",
                json={"chat_id": 1, "text": "x"},
                headers={"authorization": "Bearer app-secret", "Idempotency-Key": "op-2"},
            )
            assert r4.status_code == 200, "failed key must be released for retry"
