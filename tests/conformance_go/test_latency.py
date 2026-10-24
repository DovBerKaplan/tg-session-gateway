"""Latency benchmarks against the real tggw binary.

Measures what the 1ms target means for a GATEWAY: the overhead the
gateway adds on top of what Telegram itself costs. Telegram's own
network RTT (tens of ms) dominates any real send; the gateway must add
as close to zero as possible. The fake upstream answers instantly, so
every number below is pure gateway cost:

- send:     HTTP in -> auth -> rate guard -> idempotency -> upstream
            forward -> response out
- pull:     consumer getUpdates when data IS available (auth + offset
            ack write + queue scan + response) — the consumer hot path
- poll:     consumer getUpdates on an EMPTY queue — includes the 10ms
            no-data debounce by design (internalGetUpdates clamps the
            wait to 10ms when timeout <= 0), so this documents the
            floor instead of racing it
- delivery: update ARRIVES at the gateway (injected at the fake) ->
            poller fetch -> queue persist -> parked consumer long-poll
            wakes -> response leaves

The rate guard is raised to absurd limits for these runs: its default
(1 msg/s per private chat) is policy, not performance, and would gate
the measurement instead of the gateway's speed.

Run: TGGW_BIN=/tmp/tggw pytest -q tests/conformance_go/test_latency.py -s
"""

from __future__ import annotations

import statistics
import threading
import time

import httpx
import pytest

from conftest import ADM, APP, TOKEN, Gateway

# production-like poll behavior + rate guard off the measurement's
# back (policy limits are load-test territory, not latency):
# - POLL_TIMEOUT=1: the poller HOLDS a long-poll against the fake (the
#   fake implements the hold + wake), instead of hot-spinning — same
#   as real Telegram, where the poller is idle between update waves
# - absurd rate limits: the default 1 msg/s per chat is policy and
#   would gate the benchmark on the limiter, not on gateway speed
FAST = {
    "GW_POLL_TIMEOUT": "1",
    "GW_PRIVATE_RATE": "100000",
    "GW_PRIVATE_BURST": "100000",
    "GW_GROUP_RATE": "100000",
    "GW_GROUP_BURST": "100000",
    "GW_GLOBAL_RATE": "100000",
    "GW_GLOBAL_BURST": "100000",
}

# pooled client: measures the gateway, not TCP handshakes
CLIENT = httpx.Client(timeout=10)


def post(gw, path, body=None, headers=None):
    return CLIENT.post(f"{gw.base}{path}", json=body or {}, headers=headers or {})


class TestLatency:
    @pytest.fixture()
    def warm(self, fake_telegram, data_dir):
        g = Gateway(data_dir, fake_telegram.url(), extra_env=FAST)
        g.wait_ready()
        post(g, "/admin/bots", {"token": TOKEN, "alias": "lat"}, ADM)
        # one warmup round (connection pools, mmap'd pages, SQLite WAL)
        for _ in range(50):
            post(g, "/bots/lat/sendMessage", {"chat_id": 10, "text": "warm"}, APP)
        # drain the fake's boot batch so delivery tests own the cursor
        r = post(g, f"/tgapi/bot{TOKEN}/getUpdates", {"timeout": 0, "offset": 1003}, APP)
        assert r.status_code == 200
        yield g
        g.stop()

    def stats(self, samples_ms):
        s = sorted(samples_ms)
        n = len(s)
        return {
            "n": n,
            "p50": statistics.median(s),
            "p95": s[int(n * 0.95)],
            "p99": s[min(n - 1, int(n * 0.99))],
            "max": s[-1],
            "mean": statistics.mean(s),
        }

    def show(self, label, st):
        return (f"{label:9s} p50={st['p50']:.3f}ms p95={st['p95']:.3f}ms "
                f"p99={st['p99']:.3f}ms max={st['max']:.3f}ms mean={st['mean']:.3f}ms")

    def test_send_overhead(self, fake_telegram, warm, capsys):
        """Full send path, 500 sequential sends to one chat. Also
        measures the direct-to-fake baseline (same Python client, no
        gateway) so the report separates client cost from gateway cost."""
        def direct():
            r = CLIENT.post(f"{fake_telegram.url()}/bot{TOKEN}/sendMessage",
                            json={"chat_id": 30, "text": "x"})
            assert r.status_code == 200

        base = []
        for _ in range(100):
            t0 = time.perf_counter_ns()
            direct()
            base.append((time.perf_counter_ns() - t0) / 1e6)
        b = self.stats(base)
        print(f"\nbaseline p50={b['p50']:.3f}ms mean={b['mean']:.3f}ms  (client+fake, no gateway)")

        def one():
            r = post(warm, "/bots/lat/sendMessage", {"chat_id": 30, "text": "x"}, APP)
            assert r.status_code == 200

        samples = []
        for _ in range(500):
            t0 = time.perf_counter_ns()
            one()
            samples.append((time.perf_counter_ns() - t0) / 1e6)
        st = self.stats(samples)
        print(self.show("send", st))
        assert st["p50"] < 10.0, f"send p50={st['p50']:.3f}ms — gateway overhead blew up"
        assert st["p99"] < 30.0, f"send p99={st['p99']:.3f}ms — latency tail blew up"

    def test_pull_hot_path(self, fake_telegram, warm, capsys):
        """Consumer getUpdates WITH data available: auth + offset ack
        (SQLite write) + queue scan + response. This is the receive
        hot path every consumer hits per batch."""
        # 300 updates for 300 measured pulls (each pull acks its own)
        for i in range(300):
            fake_telegram.deliver({"update_id": 3000 + i, "message": {"text": f"m{i}"}})
        # wait for the poller to ingest the set before timing anything
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            r = post(warm, f"/tgapi/bot{TOKEN}/getUpdates",
                     {"timeout": 0, "offset": 3000, "limit": 300}, APP)
            if len(r.json()["result"]) == 300:
                break
            time.sleep(0.05)
        else:
            raise AssertionError("poller never ingested the 300 updates")

        samples = []
        offset = 3000
        for i in range(300):
            t0 = time.perf_counter_ns()
            r = post(warm, f"/tgapi/bot{TOKEN}/getUpdates",
                     {"timeout": 0, "offset": offset, "limit": 1}, APP)
            dt = (time.perf_counter_ns() - t0) / 1e6
            body = r.json()
            assert body["result"], f"update {offset} missing on pull {i}"
            offset = body["result"][-1]["update_id"] + 1
            samples.append(dt)
        st = self.stats(samples)
        print("\n" + self.show("pull", st))
        assert st["p50"] < 10.0, f"pull p50={st['p50']:.3f}ms — ack writes too slow"
        assert st["p99"] < 25.0, f"pull p99={st['p99']:.3f}ms — latency tail blew up"

    def test_poll_empty(self, warm, capsys):
        """Consumer getUpdates on an EMPTY queue. The gateway clamps
        the no-data wait to 10ms (debounce) when timeout <= 0 — by
        design, so this measures the documented floor, not a race."""
        def one():
            r = post(warm, f"/tgapi/bot{TOKEN}/getUpdates",
                     {"timeout": 0, "offset": 999999}, APP)
            assert r.status_code == 200
            assert r.json()["result"] == []

        samples = []
        for _ in range(100):
            t0 = time.perf_counter_ns()
            one()
            samples.append((time.perf_counter_ns() - t0) / 1e6)
        st = self.stats(samples)
        print("\n" + self.show("poll", st))
        # floor is the 10ms debounce (internalGetUpdates clamps timeout<=0
        # to 0.01s) + client-side overhead; assert the floor didn't move
        assert st["mean"] < 20.0, f"empty-poll mean={st['mean']:.3f}ms — debounce floor moved"

    def test_update_delivery(self, fake_telegram, warm, capsys):
        """Injection-to-receipt: the update lands at the fake (i.e.
        Telegram produced it), the gateway poller must fetch, persist
        and wake a consumer already parked in a long-poll."""
        samples = []
        for i in range(60):
            uid = 7000 + i
            box = {}

            def consumer():
                r = CLIENT.post(f"{warm.base}/tgapi/bot{TOKEN}/getUpdates",
                                json={"timeout": 5, "offset": uid},
                                headers=APP)
                box["resp"] = r
                box["t"] = time.perf_counter_ns()

            th = threading.Thread(target=consumer)
            th.start()
            time.sleep(0.03)  # let the long-poll park in the queue wait
            t0 = time.perf_counter_ns()
            fake_telegram.deliver({"update_id": uid, "message": {"text": f"inj {uid}"}})
            th.join(timeout=8)
            assert not th.is_alive(), f"consumer never received update {uid}"
            r = box["resp"]
            assert r.status_code == 200 and r.json()["result"], f"update {uid} not delivered"
            samples.append((box["t"] - t0) / 1e6)
        st = self.stats(samples)
        print("\n" + self.show("deliver", st))
        assert st["p99"] < 50.0, f"delivery p99={st['p99']:.3f}ms — poll cycle too slow"
