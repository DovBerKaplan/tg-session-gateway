"""Black-box chaos soak against the REAL tggw binary — the port of
tests/test_chaos_soak.py's gateway-side invariants (the MTProto-plane
duplicates invariant needs a live Telegram, so it lives as an in-process
Go test in internal/mtproto).

Chaos scenario: SIGKILL (no drain, no flush, no lock release) with
updates queued-but-unacked, then a restart on the same data dir.
Invariants:

1. ZERO LOST UPDATES — everything the poller fetched before the kill is
   delivered after restart, exactly once per ack.
2. Offset semantics hold across the crash — acked prefix stays acked.
"""

from __future__ import annotations

import httpx
import pytest
from conftest import ADM, APP, TOKEN, Gateway, wait_for


def post(gw, path: str, body: dict | None = None, headers: dict | None = None):
    return httpx.post(f"{gw.base}{path}", json=body or {}, headers=headers or {}, timeout=10)


def get(gw, path: str, headers: dict | None = None):
    return httpx.get(f"{gw.base}{path}", headers=headers or {}, timeout=10)


def queue_depth(gw) -> int:
    r = get(gw, "/admin/status", ADM)
    return r.json()["bots"][0]["queue"]["depth"]


class TestKillNineChaos:
    def test_sigkill_restart_replays_unacked_exactly_once(self, fake_telegram, data_dir):
        # ── lifetime 1: register, poller fetches the batch, NO ack ──
        g1 = Gateway(data_dir, fake_telegram.url())
        g1.wait_ready()
        r = post(g1, "/admin/bots", {"token": TOKEN, "alias": "chaos"}, ADM)
        assert r.status_code == 200, r.text
        wait_for(lambda: queue_depth(g1) >= 2, what="queue to fill")

        # KILL -9: no graceful shutdown, no flush, nothing
        g1.proc.kill()
        g1.proc.wait(timeout=10)

        # ── lifetime 2: a new process on the SAME data dir ──
        g2 = Gateway(data_dir, fake_telegram.url())
        g2.wait_ready()
        try:
            # session restored from the encrypted store — no re-register
            r = get(g2, "/admin/status", ADM)
            bots = r.json()["bots"]
            assert len(bots) == 1 and bots[0]["alias"] == "chaos"

            # invariant 1: the unacked batch survived the kill and
            # replays EXACTLY once
            r = post(g2, f"/tgapi/bot{TOKEN}/getUpdates", {"timeout": 0, "offset": 1001}, APP)
            updates = r.json()["result"]
            ids = [u["update_id"] for u in updates]
            assert ids == [1001, 1002], f"lost/duplicated updates: {ids}"
            texts = [u["message"]["text"] for u in updates]
            assert texts == ["first", "second"]

            # ack via offset → the next pull is empty
            r = post(g2, f"/tgapi/bot{TOKEN}/getUpdates", {"timeout": 0, "offset": 1003}, APP)
            assert r.json()["result"] == []
        finally:
            g2.stop()

        # ── lifetime 3 after the ack: nothing replays ──
        g3 = Gateway(data_dir, fake_telegram.url())
        g3.wait_ready()
        try:
            r = post(g3, f"/tgapi/bot{TOKEN}/getUpdates", {"timeout": 0, "offset": 1003}, APP)
            assert r.json()["result"] == [], "acked updates must not replay"
        finally:
            g3.stop()

    def test_double_sigkill_still_exactly_once(self, fake_telegram, data_dir):
        """Kill twice before any ack — the persisted queue is the only
        truth and must not double-replay."""
        g = Gateway(data_dir, fake_telegram.url())
        g.wait_ready()
        post(g, "/admin/bots", {"token": TOKEN, "alias": "chaos2"}, ADM)
        wait_for(lambda: queue_depth(g) >= 2, what="queue to fill")
        g.proc.kill()
        g.proc.wait(timeout=10)

        for round_no in (1, 2):
            g = Gateway(data_dir, fake_telegram.url())
            g.wait_ready()
            try:
                r = post(g, f"/tgapi/bot{TOKEN}/getUpdates", {"timeout": 0, "offset": 1001}, APP)
                ids = [u["update_id"] for u in r.json()["result"]]
                assert ids == [1001, 1002], f"round {round_no}: {ids}"
            finally:
                g.proc.kill()  # never acked — kill again
                g.proc.wait(timeout=10)

        # final lifetime acks and confirms no duplicates ever persisted
        g = Gateway(data_dir, fake_telegram.url())
        g.wait_ready()
        try:
            r = post(g, f"/tgapi/bot{TOKEN}/getUpdates", {"timeout": 0, "offset": 1001}, APP)
            ids = [u["update_id"] for u in r.json()["result"]]
            assert ids == [1001, 1002], ids
            assert len(ids) == len(set(ids)), "duplicates replayed across kills"
            post(g, f"/tgapi/bot{TOKEN}/getUpdates", {"timeout": 0, "offset": 1003}, APP)
            r = post(g, f"/tgapi/bot{TOKEN}/getUpdates", {"timeout": 0, "offset": 1003}, APP)
            assert r.json()["result"] == []
        finally:
            g.stop()


@pytest.mark.parametrize("count", [1])
def test_soak_rapid_send_pull_cycle(fake_telegram, data_dir, count):
    """A short soak: many send→pull cycles under the rate guard with
    zero errors; the fake upstream answers instantly."""
    g = Gateway(data_dir, fake_telegram.url())
    g.wait_ready()
    try:
        post(g, "/admin/bots", {"token": TOKEN, "alias": "soak"}, ADM)
        for i in range(30):
            r = post(g, "/bots/soak/sendMessage", {"chat_id": 20 + (i % 3), "text": f"m{i}"}, APP)
            assert r.status_code == 200, f"send {i} failed: {r.text}"
        assert len(fake_telegram.sent) == 30
        # group chats: burst 2, ~20/min — the guard queues, never drops
    finally:
        g.stop()
