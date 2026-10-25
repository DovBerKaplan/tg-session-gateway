"""Load and failure-edge tests against the real tggw binary.

Black-box, same contract as the conformance suite: only HTTP, no Go
internals. Covers what the unit suite cannot see from outside:

- load:    concurrent senders across many chats (zero errors, real
           throughput number), concurrent pulls racing long-polls
- failure: upstream outage -> 502 / 500 pass-through with the
           idempotency key RELEASED (retry passes), duplicate
           Idempotency-Key under concurrency -> exactly one upstream
           send, upstream connection refused -> 502
- crash:   SIGKILL the gateway mid-flight, restart on the same data
           dir -> bot live, consumer offset durable (no redelivery),
           sends resume
- edges:   gateway-owned methods refuse (403), unknown token (404),
           malformed JSON tolerated (Python-parity: body = {})

Run: TGGW_BIN=/tmp/tggw pytest -q tests/conformance_go/test_load.py -s
"""

from __future__ import annotations

import signal
import threading
import time

import httpx
import pytest

from conftest import ADM, APP, TOKEN, FakeTelegram, Gateway, wait_for

FAST = {
    # production-like poll hold (the fake implements long-poll + wake)
    "GW_POLL_TIMEOUT": "1",
    # rate guard off the load path's back (policy, not performance):
    # per-chat buckets AND the whole-bot global write bucket
    "GW_PRIVATE_RATE": "100000",
    "GW_PRIVATE_BURST": "100000",
    "GW_GROUP_RATE": "100000",
    "GW_GROUP_BURST": "100000",
    "GW_GLOBAL_RATE": "100000",
    "GW_GLOBAL_BURST": "100000",
}

# pooled client: measures the gateway, not TCP handshakes (thread-safe)
CLIENT = httpx.Client(timeout=15)


def post(gw, path, body=None, headers=None, timeout=10):
    return CLIENT.post(f"{gw.base}{path}", json=body or {},
                       headers=headers or {}, timeout=timeout)


class TestLoad:
    @pytest.fixture()
    def fast(self, fake_telegram, data_dir):
        g = Gateway(data_dir, fake_telegram.url(), extra_env=FAST)
        g.wait_ready()
        post(g, "/admin/bots", {"token": TOKEN, "alias": "load"}, ADM)
        yield g
        g.stop()

    def test_concurrent_senders_zero_errors(self, fake_telegram, fast, capsys):
        """8 threads x 30 sends to 8 distinct chats: zero failures,
        every send reaches upstream exactly once."""
        threads, errors, n_per, n_threads = [], [], 30, 8

        def worker(wid):
            for j in range(n_per):
                r = post(fast, "/bots/load/sendMessage",
                         {"chat_id": 100 + wid, "text": f"w{wid}-{j}"}, APP)
                if r.status_code != 200:
                    errors.append((wid, j, r.status_code, r.text[:100]))

        t0 = time.perf_counter()
        for wid in range(n_threads):
            th = threading.Thread(target=worker, args=(wid,))
            threads.append(th)
            th.start()
        for th in threads:
            th.join(timeout=60)
        wall = time.perf_counter() - t0

        total = n_threads * n_per
        assert not errors, f"{len(errors)} failed sends: {errors[:5]}"
        assert len(fake_telegram.sent) == total, \
            f"upstream saw {len(fake_telegram.sent)} sends, expected {total}"
        print(f"\nload: {total} sends / {wall:.2f}s = {total / wall:.0f} sends/s "
              f"through the full gateway path")

    def test_concurrent_pulls(self, fake_telegram, fast):
        """4 puller threads racing long-polls while updates flow in.
        Racing cursors are legitimately at-least-once under the
        Telegram contract (exactly-once comes from a single consumer
        advancing one offset — pinned in the latency suite); the
        gateway must lose NOTHING and serve every connection."""
        for i in range(120):
            fake_telegram.deliver({"update_id": 2000 + i, "message": {"text": f"c{i}"}})

        got, lock, cursor = [], threading.Lock(), [2000]

        def puller():
            while True:
                with lock:
                    offset = cursor[0]
                r = post(fast, f"/tgapi/bot{TOKEN}/getUpdates",
                         {"timeout": 1, "offset": offset, "limit": 50}, APP, timeout=15)
                batch = r.json()["result"]
                if not batch:
                    if cursor[0] >= 2120:
                        return
                    continue
                with lock:
                    got.extend(batch)
                    nxt = batch[-1]["update_id"] + 1
                    if nxt > cursor[0]:
                        cursor[0] = nxt
                    if cursor[0] >= 2120:
                        return

        threads = [threading.Thread(target=puller) for _ in range(4)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=30)

        ids = sorted({u["update_id"] for u in got})
        assert ids == list(range(2000, 2120)), \
            f"loss under concurrent pulls: got {len(ids)}/120 unique"


class TestFailureEdges:
    @pytest.fixture()
    def gw(self, fake_telegram, data_dir):
        g = Gateway(data_dir, fake_telegram.url())
        g.wait_ready()
        post(g, "/admin/bots", {"token": TOKEN, "alias": "edge"}, ADM)
        yield g
        g.stop()

    def test_upstream_outage_releases_idempotency(self, fake_telegram, gw):
        """Upstream 500s: consumer gets the failure AND the idempotency
        key is released — the retry must reach upstream again, and once
        the outage clears the same key completes for real."""
        hdr = {**APP, "Idempotency-Key": "outage-1"}
        path = "/bots/edge/sendMessage"

        fake_telegram.fails = True
        r1 = post(gw, path, {"chat_id": 1, "text": "a"}, hdr)
        assert r1.status_code >= 400, "outage must fail the send"
        assert "idempotent_replay" not in r1.json()

        r2 = post(gw, path, {"chat_id": 1, "text": "a"}, hdr)
        assert r2.status_code >= 400
        assert "idempotent_replay" not in r2.json(), \
            "failed send must RELEASE the key (retry allowed)"

        fake_telegram.fails = False
        r3 = post(gw, path, {"chat_id": 1, "text": "a"}, hdr)
        assert r3.status_code == 200, "retry after recovery must pass"
        assert "idempotent_replay" not in r3.json()
        assert len(fake_telegram.sent) == 1  # exactly one real upstream send

    def test_upstream_down_connection_refused_502(self, fake_telegram, gw):
        """Upstream server gone entirely: clean 502, not a hang."""
        path = "/bots/edge/sendMessage"
        fake_telegram.close()
        t0 = time.perf_counter()
        r = post(gw, path, {"chat_id": 1, "text": "x"}, APP, timeout=15)
        dt = time.perf_counter() - t0
        assert r.status_code == 502, f"expected 502, got {r.status_code}"
        assert r.json()["description"].startswith("upstream:")
        assert dt < 5.0, f"connection failure took {dt:.2f}s — must fail fast"

    def test_duplicate_idempotency_key_single_send(self, fake_telegram, gw):
        """6 concurrent sends with ONE Idempotency-Key: exactly one
        reaches upstream; the rest are suppressed replays."""
        hdr = {**APP, "Idempotency-Key": "dup-1"}
        results = []

        def one():
            results.append(post(gw, "/bots/edge/sendMessage",
                                {"chat_id": 9, "text": "once"}, hdr))

        threads = [threading.Thread(target=one) for _ in range(6)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=15)

        assert len(results) == 6
        assert all(r.status_code == 200 for r in results), \
            [r.status_code for r in results]
        replays = sum(1 for r in results if r.json().get("idempotent_replay"))
        assert replays == 5, f"expected 5 suppressed replays, got {replays}"
        assert len(fake_telegram.sent) == 1, \
            f"upstream saw {len(fake_telegram.sent)} sends for one key"

    def test_gateway_owned_methods_refuse(self, gw):
        """setWebhook would fight the poller — the gateway owns it."""
        r = post(gw, f"/tgapi/bot{TOKEN}/setWebhook", {"url": "https://x.example"}, APP)
        assert r.status_code == 403
        assert "gateway owns" in r.json()["description"]

    def test_unknown_token_404(self, gw):
        r = post(gw, "/tgapi/bot999999:wrong/sendMessage", {"chat_id": 1}, APP)
        assert r.status_code == 404
        assert r.json()["ok"] is False

    def test_malformed_json_tolerated(self, fake_telegram, gw):
        """Parity pin: the Python gateway's _json_body tolerated invalid
        JSON as {} and forwarded; the Go gateway must not 400."""
        r = httpx.post(f"{gw.base}/bots/edge/sendMessage",
                       content=b"{not json", headers={**APP, "Content-Type": "application/json"},
                       timeout=10)
        assert r.status_code == 200
        assert fake_telegram.sent[-1] == {}, "forwarded body must be the tolerated {}"


class TestCrashRecovery:
    def test_sigkill_restart_offset_durable_and_sends_resume(self, fake_telegram, data_dir):
        """SIGKILL (the StatefulSet crash the flock design exists for),
        restart on the same data dir: bot still live, acked offset
        survives (no redelivery), sends work again."""
        env = None  # default (parity) limits; only 2 sends below
        g1 = Gateway(data_dir, fake_telegram.url(), extra_env=env)
        g1.wait_ready()
        post(g1, "/admin/bots", {"token": TOKEN, "alias": "crash"}, ADM)

        # flow one update through and ack it, so the offset is durable
        fake_telegram.deliver({"update_id": 4100, "message": {"text": "pre-kill"}})
        r = post(g1, f"/tgapi/bot{TOKEN}/getUpdates", {"timeout": 5, "offset": 4100}, APP)
        assert [u["update_id"] for u in r.json()["result"]] == [4100]
        r = post(g1, f"/tgapi/bot{TOKEN}/getUpdates", {"timeout": 0, "offset": 4101}, APP)
        assert r.json()["result"] == []  # acked

        # another update the gateway will ingest but the consumer never
        # acks — must be redelivered after the crash
        fake_telegram.deliver({"update_id": 4101, "message": {"text": "post-kill"}})
        wait_for(lambda: post(
            g1, f"/tgapi/bot{TOKEN}/getUpdates",
            {"timeout": 0, "offset": 4101, "limit": 1}, APP).json()["result"] != [],
            what="gateway to ingest update 4101")

        g1.proc.send_signal(signal.SIGKILL)
        g1.proc.wait(timeout=10)

        g2 = Gateway(data_dir, fake_telegram.url())
        g2.wait_ready()
        try:
            status = httpx.get(f"{g2.base}/admin/status", headers=ADM, timeout=5).json()
            assert status["bots"], "bot registration lost across restart"
            assert status["bots"][0]["state"] == "live"

            # acked offset survived: 4100 must NOT come back
            r = post(g2, f"/tgapi/bot{TOKEN}/getUpdates",
                     {"timeout": 0, "offset": 4101}, APP)
            ids = [u["update_id"] for u in r.json()["result"]]
            assert 4100 not in ids, f"acked update redelivered after SIGKILL: {ids}"
            assert 4101 in ids, "unacked update lost after SIGKILL"

            # and the send path works again immediately
            r = post(g2, "/bots/crash/sendMessage", {"chat_id": 5, "text": "back"}, APP)
            assert r.status_code == 200
            assert fake_telegram.sent[-1]["text"] == "back"
        finally:
            g2.stop()

class TestChaosExtended:
    """Failure shapes beyond plain death: hangs, churn."""

    def test_upstream_hang_recovers(self, data_dir):
        """Upstream accepts and HOLDS connections (no response): sends
        fail within the client timeout budget (bounded, not a hang),
        and everything recovers the moment the upstream answers again.
        Fast version: hold 2 seconds, then serve normally — no 60s
        timeout wait in CI."""
        import socket

        hold = {"on": True}
        srv = socket.socket()
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(16)
        port = srv.getsockname()[1]
        held = []
        stop = {"on": False}

        def serve_conn(conn):
            """While holding: never answer. After release: answer the
            Telegram envelope (empty updates / generic true)."""
            try:
                conn.settimeout(75)
                if hold["on"]:
                    held.append(conn)
                    import time as _t
                    while hold["on"] and not stop["on"]:
                        _t.sleep(0.1)
                    if stop["on"]:
                        return
                    # hang lifted: reset the held conn so the gateway's
                    # pool dials a fresh (servable) one
                    try:
                        conn.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    try:
                        conn.close()
                    except OSError:
                        pass
                    return
                self._answer_bot_api(conn)
            except OSError:
                pass

        def acceptor():
            srv.settimeout(0.5)
            while not stop["on"]:
                try:
                    conn, _ = srv.accept()
                except (socket.timeout, OSError):
                    continue
                threading.Thread(target=serve_conn, args=(conn,), daemon=True).start()

        th = threading.Thread(target=acceptor, daemon=True)
        th.start()
        upstream = f"http://127.0.0.1:{port}"

        g = Gateway(data_dir, upstream)
        g.wait_ready()
        try:
            r = post(g, "/admin/bots", {"token": TOKEN, "alias": "hang"}, ADM)
            assert r.status_code == 200

            t0 = time.perf_counter()
            r = post(g, "/bots/hang/sendMessage", {"chat_id": 1, "text": "x"}, APP, timeout=70)
            dt = time.perf_counter() - t0
            assert r.status_code >= 400, f"hung upstream must fail the send: {r.status_code}"
            assert dt < 65, f"send took {dt:.1f}s — must be bounded by the client timeout"

            # lift the hang: connections get served from here on. The
            # FIRST send may land on the poisoned pooled connection
            # (RST from the reset) — Go's transport does not retry
            # POSTs, so allow a bounded retry; recovery must be quick.
            hold["on"] = False
            time.sleep(0.5)
            r = None
            for _ in range(4):
                r = post(g, "/bots/hang/sendMessage", {"chat_id": 1, "text": "x"}, APP, timeout=20)
                if r.status_code == 200:
                    break
                time.sleep(0.5)
            assert r is not None and r.status_code == 200, \
                f"recovery must be quick: {r.status_code if r else '?'} {r.text[:120] if r else ''}"
        finally:
            stop["on"] = True
            hold["on"] = False
            for c in held:
                try:
                    c.close()
                except OSError:
                    pass
            srv.close()
            g.stop()

    @staticmethod
    def _answer_bot_api(conn):
        """Minimal Bot API responder: read one request, answer the
        Telegram envelope (result [] for getUpdates, true otherwise)."""
        import json as _json

        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = conn.recv(4096)
            if not chunk:
                return
            buf += chunk
        head, _, rest = buf.partition(b"\r\n\r\n")
        length = 0
        for line in head.split(b"\r\n"):
            if line.lower().startswith(b"content-length:"):
                length = int(line.split(b":")[1].strip())
        while len(rest) < length:
            rest += conn.recv(4096)
        path = head.split(b" ")[1].decode()
        body = b'"result": []' if path.endswith("getUpdates") else b'"result": true'
        payload = ('{"ok": true, %s}' % body).encode()
        conn.sendall(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            + f"Content-Length: {len(payload)}\r\n\r\n".encode()
            + payload
        )
        _ = _json  # envelope is assembled manually above

    def test_rapid_kill_restart_loop(self, fake_telegram, data_dir):
        """Three SIGKILL/restart cycles in a row: acked offsets never
        go backwards, sends resume every time (the crash-loop the safe
        mode exists for)."""
        g = None
        try:
            acked = 4900  # last acked update_id
            fake_telegram.deliver({"update_id": acked, "message": {"text": "seed"}})
            for cycle in range(3):
                g = Gateway(data_dir, fake_telegram.url())
                g.wait_ready()
                if cycle == 0:
                    post(g, "/admin/bots", {"token": TOKEN, "alias": "churn"}, ADM)
                    # ack the seed once
                    r = post(g, f"/tgapi/bot{TOKEN}/getUpdates",
                             {"timeout": 5, "offset": acked}, APP)
                    assert [u["update_id"] for u in r.json()["result"]] == [acked]
                    r = post(g, f"/tgapi/bot{TOKEN}/getUpdates",
                             {"timeout": 0, "offset": acked + 1}, APP)
                    assert r.json()["result"] == []
                # flows still work mid-churn
                r = post(g, "/bots/churn/sendMessage",
                         {"chat_id": 3, "text": f"cycle{cycle}"}, APP)
                assert r.status_code == 200
                g.proc.send_signal(signal.SIGKILL)
                g.proc.wait(timeout=10)
                g = None
            # final restart: bot comes back, acked update stays acked
            g = Gateway(data_dir, fake_telegram.url())
            g.wait_ready()
            status = httpx.get(f"{g.base}/admin/status", headers=ADM, timeout=5).json()
            assert status["bots"], "registration lost across churn"
            r = post(g, f"/tgapi/bot{TOKEN}/getUpdates",
                     {"timeout": 0, "offset": acked + 1}, APP)
            ids = [u["update_id"] for u in r.json()["result"]]
            assert acked not in ids, "acked update redelivered after 3 kills"
        finally:
            if g is not None:
                g.stop()
