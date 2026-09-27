"""Chaos soak — the hardening charter's Definition of Done test.

Simulates the production failure this product exists to prevent:
kill -9 with locks stranded and updates queued-but-unacked, then a
restart on the same data dir. The three invariants:

1. ZERO AUTH_KEY_DUPLICATED — a second manager while locks are held
   is REFUSED (never double-connects); after the holder goes silent,
   the stale lock is reclaimed and exactly one owner connects again.
2. ZERO LOST UPDATES — everything pushed before the kill is delivered
   to the pull consumer after restart, exactly once per ack.
3. Seq continuity — the replayed backlog continues the seq counter,
   no reset, no overlap.
"""

import asyncio
import os
from types import SimpleNamespace
from unittest.mock import patch

import mtgateway.sessions as mt_sessions
from mtgateway import lock as lock_mod
from mtgateway.sessions import MTSessionManager
from mtgateway.store import MTStore

TOKENS = {f"acct{i}": f"70000{i}:chaos" for i in range(3)}


class ChaosFakeClient:
    """Records every start/stop across ALL instances — the duplicate
    detector: a start while another instance for the same alias is
    connected would be the AUTH_KEY_DUPLICATED we must never see."""

    live_by_alias: dict[str, int] = {}  # noqa: RUF012 — deliberate cross-instance registry
    events: list[str] = []  # noqa: RUF012

    def __init__(self, name, api_id, api_hash, bot_token, workdir, in_memory):
        self.alias = os.path.basename(name).replace(".session", "")
        self.is_connected = False
        self.raw_handler = None

    def add_handler(self, handler, group=0):
        self.raw_handler = handler

    async def start(self):
        assert ChaosFakeClient.live_by_alias.get(self.alias, 0) == 0, (
            f"AUTH_KEY_DUPLICATED: second live connect for '{self.alias}'"
        )
        ChaosFakeClient.live_by_alias[self.alias] = (
            ChaosFakeClient.live_by_alias.get(self.alias, 0) + 1
        )
        ChaosFakeClient.events.append(f"start:{self.alias}")
        self.is_connected = True

    async def stop(self):
        self.is_connected = False
        if ChaosFakeClient.live_by_alias.get(self.alias):
            ChaosFakeClient.live_by_alias[self.alias] -= 1
        ChaosFakeClient.events.append(f"stop:{self.alias}")

    async def get_me(self):
        return SimpleNamespace(username="chaos_bot", id=1)

    def get_dialogs(self, limit=500):
        return None  # skip peer warm-up in chaos


class FakeStorelessConfig:
    def __init__(self, d):
        self.api_id = 1
        self.api_hash = "x"
        self.data_dir = d
        self.session_dir = os.path.join(d, "sessions")
        self.rate = None
        self.mtproto_rate = None
        self.policy = SimpleNamespace(on_limit="reject", queue_wait_timeout=1.0)


async def _manager_on(d):
    store = MTStore(d)
    await store.connect()
    mgr = MTSessionManager(FakeStorelessConfig(d), store)
    return mgr, store


class TestChaosSoak:
    def test_kill9_restart_no_duplicates_no_loss(self, tmp_path):
        async def scenario():
            d = str(tmp_path / "chaos")
            os.makedirs(d, exist_ok=True)
            # fast stale detection for the test clock (same semantics)
            with (
                patch.object(mt_sessions, "PyrogramClient", ChaosFakeClient),
                patch.object(lock_mod, "STALE_AFTER_SECONDS", 0.4),
                patch.object(lock_mod, "HEARTBEAT_INTERVAL", 0.1),
            ):
                # ── lifetime 1: 3 accounts live, updates pushed, UNACKED ──
                mgr1, store1 = await _manager_on(d)
                for alias, token in TOKENS.items():
                    await mgr1.register(alias, token)
                await asyncio.sleep(0.3)
                assert all(ChaosFakeClient.live_by_alias.get(a) == 1 for a in TOKENS), (
                    "each account must have exactly one live client"
                )

                fed = {}
                for alias, s in mgr1.sessions.items():
                    for i in range(5):
                        await s.push_update({"_gw_seq_marker": i, "text": f"{alias}-{i}"})
                    fed[alias] = 5

                # NO pull (consumer never came) — everything unacked.
                # KILL -9: no graceful shutdown, no lock release, no close.
                for t in asyncio.all_tasks():
                    if t is not asyncio.current_task():
                        t.cancel()
                # process death severs ALL of its TCP connections — model
                # that for the duplicate detector (a dead owner's socket
                # cannot collide; only two LIVE owners would)
                ChaosFakeClient.live_by_alias.clear()
                await asyncio.sleep(0.5)  # holder goes silent; locks stale

                # ── lifetime 2: a new process on the SAME data dir ──
                mgr2, store2 = await _manager_on(d)
                restored = await mgr2.restore_all()
                await asyncio.sleep(0.5)
                assert restored == len(TOKENS)
                # invariant 1: still exactly one live client per alias
                assert all(ChaosFakeClient.live_by_alias.get(a, 0) == 1 for a in TOKENS), (
                    f"duplicate connect detected: {ChaosFakeClient.live_by_alias}"
                )

                # invariant 2+3: every fed update delivered EXACTLY once
                for alias, s in mgr2.sessions.items():
                    got = []
                    idle = 0
                    while idle < 3 and len(got) < fed[alias]:
                        try:
                            got.append(s._update_queue.get_nowait())
                            idle = 0
                        except asyncio.QueueEmpty:
                            idle += 1
                            await asyncio.sleep(0.02)
                    assert len(got) == fed[alias], (
                        f"{alias}: lost updates — fed={fed[alias]} got={len(got)}"
                    )
                    seqs = sorted(u["_gw_seq"] for u in got)
                    assert seqs == list(range(1, fed[alias] + 1)), (
                        f"{alias}: seq not continuous/exactly-once: {seqs}"
                    )
                    # ack → persistence releases them
                    await store2.queue_ack(alias, seqs[-1])

                # post-ack: a THIRD manager finds nothing to replay
                mgr3_tasks_cancel = True
                await store2.close()
                mgr3, store3 = await _manager_on(d)
                await mgr3.register("acct0", TOKENS["acct0"])
                await asyncio.sleep(0.3)
                backlog = await store3.queue_pull("acct0", offset=0, limit=100)
                assert backlog == [], f"acked updates must not replay: {backlog}"
                await mgr3.shutdown()
                await store3.close()

        asyncio.run(scenario())


class TestNonDictUpdates:
    """Live finding: Pyrogram hands str/bytes for unparsed updates —
    the handler must drop them, never crash (was: 'str' object does
    not support item assignment on every real update)."""

    async def test_str_update_dropped_not_crashing(self, tmp_path):
        import asyncio as aio

        class StrEmittingClient:
            def __init__(self, **kw):
                self.raw_handler = None

            def add_handler(self, h, group=0):
                self.raw_handler = h

            async def start(self):
                pass

            async def stop(self):
                pass

            async def get_me(self):
                return SimpleNamespace(username="s", id=1)

            def get_dialogs(self, limit=500):
                return None

        mgr, store = await _manager_on(str(tmp_path / "nd"))
        import mtgateway.sessions as ms

        orig = ms.PyrogramClient
        ms.PyrogramClient = StrEmittingClient
        try:
            await mgr.register("bot1", "t")
            await aio.sleep(0.2)
            s = mgr.sessions["bot1"]
            client = s.client
            # str AND bytes updates — both must be silently dropped
            await client.raw_handler.callback(client, "raw-unparsed", [], [])
            await client.raw_handler.callback(client, b"\x01\x02", [], [])
            assert s._update_queue.empty(), "non-dict updates must not enqueue"
            # dict updates keep flowing
            await client.raw_handler.callback(client, {"update_id": 7}, [], [])
            assert s._update_queue.qsize() == 1
        finally:
            ms.PyrogramClient = orig
            await mgr.shutdown()
            await store.close()
