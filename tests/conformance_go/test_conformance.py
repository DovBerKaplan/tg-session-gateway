"""Conformance tests: black-box lifecycle against the real tggw binary.

The Go port must answer the SAME consumer contract as the Python
gateway (tests/test_integration_gateway.py) with only the base URL
swapped.
"""

from __future__ import annotations

import time

import httpx

ADM = {"authorization": "Bearer adm-secret"}
APP = {"authorization": "Bearer app-secret"}
TOKEN = "700000:conformance"


def post(gw, path: str, body: dict | None = None, headers: dict | None = None):
    return httpx.post(f"{gw.base}{path}", json=body or {}, headers=headers or {}, timeout=10)


def get(gw, path: str, headers: dict | None = None):
    return httpx.get(f"{gw.base}{path}", headers=headers or {}, timeout=10)


def wait_for(predicate, timeout: float = 8.0, what: str = "condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {what}")


class TestGatewayLifecycle:
    def test_register_poll_pull_send_pause_delete(self, gw, fake_telegram):
        # unknown token → 404 BEFORE any Telegram call
        r = post(gw, "/tgapi/bot999:unknown/getUpdates")
        assert r.status_code == 404

        # register
        r = post(
            gw,
            "/admin/bots",
            {"token": TOKEN, "alias": "b1"},
            {"authorization": "Bearer adm-secret"},
        )
        assert r.status_code == 200, r.text
        assert r.json()["alias"] == "b1"

        # poller picks the batch up asynchronously
        def queue_fed():
            r = get(gw, "/admin/status", ADM)
            return r.json()["bots"][0]["queue"]["depth"] >= 2

        wait_for(queue_fed, what="queue to fill")

        # consumer pulls exactly the batch (offset semantics)
        r = post(gw, f"/tgapi/bot{TOKEN}/getUpdates", {"timeout": 0, "offset": 1001})
        updates = r.json()["result"]
        assert [u["update_id"] for u in updates] == [1001, 1002]
        texts = [u["message"]["text"] for u in updates]
        assert texts == ["first", "second"]

        # offset=1003 acks everything below
        r = post(gw, f"/tgapi/bot{TOKEN}/getUpdates", {"timeout": 0, "offset": 1003})
        assert r.json()["result"] == []

        # Prometheus metrics expose the session and the queue
        body = get(gw, "/metrics").text
        assert 'tg_gateway_session_state{alias="b1"} 1' in body
        assert "tg_gateway_update_queue_depth" in body
        assert "tg_gateway_real_429_total" in body

        # send through the alias route → proxied verbatim
        r = post(gw, "/bots/b1/sendMessage", {"chat_id": 10, "text": "hi"}, APP)
        assert r.status_code == 200
        assert r.json()["result"]["message_id"] == 42
        assert fake_telegram.sent and fake_telegram.sent[-1]["text"] == "hi"

        # alias route requires the app secret
        r = post(gw, "/bots/b1/sendMessage", {"chat_id": 10, "text": "x"})
        assert r.status_code == 401

        # pause: poller stops calling upstream
        r = post(gw, "/admin/bots/b1/pause", None, ADM)
        assert r.json()["ok"] is True
        r = get(gw, "/admin/status", ADM)
        assert r.json()["bots"][0]["state"] == "paused"
        calls_at_pause = fake_telegram.getupdates_calls
        time.sleep(0.3)
        assert fake_telegram.getupdates_calls == calls_at_pause
        r = post(gw, "/admin/bots/b1/resume", None, ADM)
        assert r.json()["ok"] is True

        # delete: gone from status, token no longer routes
        r = httpx.delete(f"{gw.base}/admin/bots/b1", headers=ADM, timeout=10)
        assert r.json()["ok"] is True
        r = post(gw, f"/tgapi/bot{TOKEN}/getUpdates")
        assert r.status_code == 404

    def test_restart_restores_session_and_unacked_updates(self, fake_telegram, data_dir):
        from conftest import Gateway, wait_for

        # ── first lifetime: register, let it poll, never consume ──
        g1 = Gateway(data_dir, fake_telegram.url())
        g1.wait_ready()
        try:
            r = post(g1, "/admin/bots", {"token": TOKEN, "alias": "keep"}, ADM)
            assert r.status_code == 200

            def queue_fed():
                r = get(g1, "/admin/status", ADM)
                return r.json()["bots"][0]["queue"]["depth"] >= 2

            wait_for(queue_fed, what="queue to fill")
        finally:
            g1.stop()  # graceful SIGTERM — offsets flushed, store closed

        # ── second lifetime, same data dir ──
        g2 = Gateway(data_dir, fake_telegram.url())
        g2.wait_ready()
        try:
            # session restored from the encrypted store — no re-register
            r = get(g2, "/admin/status", ADM)
            bots = r.json()["bots"]
            assert len(bots) == 1 and bots[0]["alias"] == "keep"

            # unacked updates survived the restart in queue.db
            pulled: list = []

            def drained():
                r = post(g2, f"/tgapi/bot{TOKEN}/getUpdates", {"timeout": 0, "offset": 1001})
                pulled.extend(r.json()["result"] or [])
                return len(pulled) >= 2

            wait_for(drained, what="restart replay")
            texts = [u["message"]["text"] for u in pulled]
            assert texts == ["first", "second"]
        finally:
            g2.stop()

    def test_health_endpoints(self, gw):
        r = get(gw, "/healthz")
        assert r.status_code == 200 and r.json()["ok"] is True
        r = get(gw, "/readyz")
        assert r.status_code == 200 and r.json()["ok"] is True

    def test_migrate_cutover_gate(self, data_dir):
        """tggw migrate: pyrofork fixture → gotd blobs, zero logins."""
        import os
        import subprocess

        from conftest import make_pyrofork_session, require_binary

        os.makedirs(os.path.join(data_dir, "sessions"), exist_ok=True)
        make_pyrofork_session(os.path.join(data_dir, "sessions", "b1.session"))

        r = subprocess.run(
            [require_binary(), "migrate", "--from", data_dir],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        assert r.returncode == 0, r.stderr + r.stdout
        assert "Telegram logins required: 0" in r.stdout

        # idempotent: a second run skips
        r = subprocess.run(
            [require_binary(), "migrate", "--from", data_dir],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        assert "skipped=1" in r.stdout
        assert "Telegram logins required: 0" in r.stdout

    def test_preflight_refuses_missing_fernet_key(self, data_dir):
        """The postgres-style fail-fast survives the port."""
        import os
        import subprocess

        from conftest import free_port, require_binary

        env = {k: v for k, v in os.environ.items() if not k.startswith("GW_")}
        env.update(
            {
                "GW_HOST": "127.0.0.1",
                "GW_PORT": str(free_port()),
                "GW_DATA_DIR": data_dir,
                "GW_ADMIN_SECRET": "a",
                "GW_APP_SECRET": "b",
                "GW_ALLOW_EPHEMERAL_DATA": "1",
            }
        )
        r = subprocess.run(
            [require_binary(), "botapi"],
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        assert r.returncode != 0
        assert "GW_FERNET_KEY is required" in r.stderr
