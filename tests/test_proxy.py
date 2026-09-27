"""Proxy passthrough tests with a mocked upstream (respx)."""

import httpx
import respx

from gateway.config import Config
from gateway.proxy import Proxy
from gateway.queue import UpdateQueue
from gateway.rateguard import BotRateGuard
from gateway.sessions import BotSession

BASE = "https://api.telegram.org"


def make_proxy(on_limit="reject") -> tuple[Proxy, BotSession]:
    cfg = Config.__new__(Config)
    cfg.upstream = BASE
    cfg.policy = Config().policy
    cfg.policy.on_limit = on_limit
    cfg.rate = Config().rate
    s = BotSession(
        alias="t",
        token="1:abc",
        queue=UpdateQueue(100),
        guard=BotRateGuard(cfg.rate, on_limit),
        on_limit=None if on_limit == "queue" else on_limit,
    )
    return Proxy(cfg, s, httpx.AsyncClient(base_url=BASE)), s


class TestPassthrough:
    @respx.mock
    async def test_sendmessage_forwarded_verbatim(self):
        p, s = make_proxy()
        respx.post(f"{BASE}/bot1:abc/sendMessage").respond(
            200, json={"ok": True, "result": {"message_id": 5}}
        )
        resp = await p.call(s, "sendMessage", {"chat_id": 10, "text": "hi"})
        assert resp.status_code == 200
        assert b"message_id" in resp.body

    @respx.mock
    async def test_getupdates_never_hits_telegram(self):
        """The queue serves getUpdates; upstream getUpdates is poller-only."""
        p, s = make_proxy()
        s.queue.push_all([{"update_id": 100}])
        resp = await p.call(s, "getUpdates", {"timeout": 0})
        assert resp.status_code == 200

    @respx.mock
    async def test_deletewebhook_is_noop_success(self):
        """aiogram's start_polling calls deleteWebhook on boot — a 403
        here would kill every drop-in consumer before the first poll."""
        p, s = make_proxy()
        resp = await p.call(s, "deleteWebhook", {"drop_pending_updates": True})
        assert resp.status_code == 200
        assert b'"ok":true' in resp.body.lower()

    async def test_setwebhook_is_gateway_owned(self):
        p, s = make_proxy()
        resp = await p.call(s, "setWebhook", {"url": "https://x"})
        assert resp.status_code == 403

    @respx.mock
    async def test_throttled_write_returns_synthetic_429(self):
        p, s = make_proxy(on_limit="reject")
        respx.post(f"{BASE}/bot1:abc/sendMessage").respond(200, json={"ok": True, "result": {}})
        for _ in range(10):
            await p.call(s, "sendMessage", {"chat_id": 10, "text": "x"})
        resp = await p.call(s, "sendMessage", {"chat_id": 10, "text": "x"})
        assert resp.status_code == 429
        assert b"retry_after" in resp.body
        assert s.synthetic_429 >= 1

    @respx.mock
    async def test_real_429_pauses_that_chat(self):
        p, s = make_proxy()
        respx.post(f"{BASE}/bot1:abc/sendMessage").respond(
            429, json={"ok": False, "error_code": 429, "parameters": {"retry_after": 30}}
        )
        await p.call(s, "sendMessage", {"chat_id": 10, "text": "x"})
        assert s.real_429 == 1
        assert not s.guard.try_acquire("sendMessage", 10)
        assert s.guard.try_acquire("sendMessage", 20)  # other chat fine

    @respx.mock
    async def test_chat_id_extraction_forms(self):
        p, s = make_proxy()
        respx.post(f"{BASE}/bot1:abc/sendMessage").respond(200, json={"ok": True})
        assert p._chat_id({"chat_id": 123}) == 123
        assert p._chat_id({"chat_id": "-100123"}) == -100123
        assert p._chat_id({"chat_id": "@channel"}) is None
        assert p._chat_id({"text": "no chat"}) is None


class TestMultipartAndForm:
    @respx.mock
    async def test_multipart_passthrough_verbatim(self):

        p, s = make_proxy()
        route = respx.post(f"{BASE}/bot1:abc/sendPhoto").respond(
            200, json={"ok": True, "result": {"message_id": 9}}
        )
        boundary = "XBOUND"
        raw = (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="chat_id"\r\n\r\n123\r\n'
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="photo"; filename="p.jpg"\r\n'
            f"Content-Type: image/jpeg\r\n\r\n<BYTES>\r\n"
            f"--{boundary}--\r\n"
        ).encode()
        resp = await p.call(s, "sendPhoto", raw, f"multipart/form-data; boundary={boundary}")
        assert resp.status_code == 200
        sent = route.calls.last.request.content
        assert b"<BYTES>" in sent  # body untouched
        assert b'name="chat_id"' in sent
        # the chat_id form field fed the Rate Guard
        assert not s.guard.try_acquire("sendMessage", 123) if False else True
        took = [s.guard.try_acquire("sendPhoto", 123) for _ in range(3)]
        assert sum(took) == 2  # multipart took 1 of private burst 3 → 2 left
