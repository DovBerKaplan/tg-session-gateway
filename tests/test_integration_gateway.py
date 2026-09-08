"""Integration: full Bot API gateway lifecycle against a mocked Telegram.

The whole plane in one process — real FastAPI app from create_app, real
SessionManager poller task, real SQLite store and persistent queue in a
temp dir, and api.telegram.org mocked with respx. The only thing faked
is Telegram itself.

Covers (review §5 "no integration test exercises the full lifecycle"):
register → poll → pull updates → send through the rate guard →
pause/resume → restart with unacked updates still queued → delete.
"""

import asyncio
import json

import httpx
import respx

from gateway.app import create_app
from gateway.config import Config

BASE = "https://api.telegram.org"
TOKEN = "700000:itest-token"
ADM = {"authorization": "Bearer adm-secret"}
APP = {"authorization": "Bearer app-secret"}

UPDATES_BATCH = [
    {"update_id": 1001, "message": {"text": "first"}},
    {"update_id": 1002, "message": {"text": "second"}},
]


def make_cfg(data_dir: str) -> Config:
    cfg = Config()
    cfg.admin_secret = "adm-secret"
    cfg.app_secret = "app-secret"
    cfg.data_dir = data_dir
    cfg.upstream = BASE
    cfg.poll_timeout = 0  # respx answers instantly; no long-poll in tests
    cfg.poll_backoff_max = 0.1
    return cfg


def telegram_mock() -> respx.Route:
    """deleteWebhook OK; getUpdates yields one batch then sleeps+empty."""

    async def feed(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else {}
        # consumer-visible internal feed: one batch after the first beat
        if feed.calls == 0 and body.get("offset", 0) <= 1001:
            feed.calls += 1
            return httpx.Response(200, json={"ok": True, "result": UPDATES_BATCH})
        await asyncio.sleep(0.01)  # keep the empty-poll loop from hot-spinning
        return httpx.Response(200, json={"ok": True, "result": []})

    feed.calls = 0
    respx.post(f"{BASE}/bot{TOKEN}/deleteWebhook").respond(200, json={"ok": True, "result": True})
    return respx.post(f"{BASE}/bot{TOKEN}/getUpdates").mock(side_effect=feed)


async def wait_for(predicate, timeout: float = 5.0, what: str = "condition"):
    """Poll predicate until true; fail loud with context."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"timed out waiting for {what}")


class TestGatewayLifecycle:
    @respx.mock
    async def test_register_poll_pull_send_pause_delete(self, tmp_path):
        gu = telegram_mock()
        send = respx.post(f"{BASE}/bot{TOKEN}/sendMessage").respond(
            200, json={"ok": True, "result": {"message_id": 42}}
        )

        cfg = make_cfg(str(tmp_path / "d1"))
        app = create_app(cfg)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c,
        ):
            # unknown token → 404 before any Telegram call
            r = await c.post("/tgapi/bot999:unknown/getUpdates", json={})
            assert r.status_code == 404

            # register
            r = await c.post("/admin/bots", json={"token": TOKEN, "alias": "b1"}, headers=ADM)
            assert r.status_code == 200, r.text
            assert r.json()["alias"] == "b1"

            # poller picks the batch up asynchronously — poll until it lands
            deadline = asyncio.get_event_loop().time() + 5
            updates: list = []
            while len(updates) < 2 and asyncio.get_event_loop().time() < deadline:
                r = await c.post(f"/tgapi/bot{TOKEN}/getUpdates", json={"timeout": 0})
                updates.extend(r.json().get("result") or [])
            assert len(updates) == 2, f"poller never fed the queue: {updates}"
            assert gu.called

            # Prometheus metrics expose the session and the queue
            r = await c.get("/metrics")
            assert r.status_code == 200
            body = r.text
            assert 'tg_gateway_session_state{alias="b1"} 1' in body
            assert "tg_gateway_update_queue_depth" in body
            assert "tg_gateway_real_429_total" in body

            # send through the alias route → proxied verbatim
            r = await c.post(
                "/bots/b1/sendMessage",
                json={"chat_id": 10, "text": "hi"},
                headers=APP,
            )
            assert r.status_code == 200
            assert r.json()["result"]["message_id"] == 42
            assert send.called

            # alias route requires the app secret
            r = await c.post("/bots/b1/sendMessage", json={"chat_id": 10, "text": "x"})
            assert r.status_code == 401

            # pause: poller stops calling upstream; resume: continues
            r = await c.post("/admin/bots/b1/pause", headers=ADM)
            assert r.json()["ok"] is True
            r = await c.get("/admin/status", headers=ADM)
            assert r.json()["bots"][0]["state"] == "paused"
            calls_at_pause = len(gu.calls)
            await asyncio.sleep(0.2)
            assert len(gu.calls) == calls_at_pause  # no upstream polls
            r = await c.post("/admin/bots/b1/resume", headers=ADM)
            assert r.json()["ok"] is True

            # delete: gone from status, token no longer routes
            r = await c.delete("/admin/bots/b1", headers=ADM)
            assert r.json()["ok"] is True
            r = await c.post(f"/tgapi/bot{TOKEN}/getUpdates", json={})
            assert r.status_code == 404

    @respx.mock
    async def test_restart_restores_session_and_unacked_updates(self, tmp_path):
        data_dir = str(tmp_path / "d2")

        # ── first gateway lifetime: register, let it poll, never consume ──
        telegram_mock()
        cfg = make_cfg(data_dir)
        app1 = create_app(cfg)
        async with (
            app1.router.lifespan_context(app1),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app1), base_url="http://gw") as c,
        ):
            r = await c.post("/admin/bots", json={"token": TOKEN, "alias": "keep"}, headers=ADM)
            assert r.status_code == 200

            # wait until the poller fetched the batch into the queue
            async def queue_fed():
                r = await c.get("/admin/status", headers=ADM)
                return r.json()["bots"][0]["queue"]["depth"] >= 2

            await wait_for(queue_fed, what="queue to fill")
        # ── gateway dies WITHOUT the consumer acking ─────────────────────

        # ── second gateway lifetime: same data dir ───────────────────────
        telegram_mock()
        app2 = create_app(make_cfg(data_dir))
        async with (
            app2.router.lifespan_context(app2),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app2), base_url="http://gw") as c,
        ):
            # session restored from the encrypted store — no re-register
            r = await c.get("/admin/status", headers=ADM)
            bots = r.json()["bots"]
            assert len(bots) == 1 and bots[0]["alias"] == "keep"

            # unacked updates survived the restart in queue.db
            deadline = asyncio.get_event_loop().time() + 5
            pulled = []
            while len(pulled) < 2 and asyncio.get_event_loop().time() < deadline:
                r = await c.post(f"/tgapi/bot{TOKEN}/getUpdates", json={"timeout": 1})
                pulled.extend(r.json().get("result") or [])
                await asyncio.sleep(0.05)
            texts = [u["message"]["text"] for u in pulled]
            assert texts == ["first", "second"], f"queue not restored: {pulled}"
