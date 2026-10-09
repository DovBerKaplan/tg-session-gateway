"""Auto typing indicator (spec §10.4) — full-lifecycle tests.

Real FastAPI app, real SessionManager poller, respx-mocked Telegram —
the only thing faked is Telegram itself (same harness as
test_integration_gateway.py). Intervals and timeouts are shrunk to
milliseconds so the beats and deadlines are observable fast.
"""

import asyncio
import json

import httpx
import respx

from gateway.app import create_app
from gateway.config import DEFAULT_TYPING_TIMEOUT_TEXT, Config, TypingConfig

BASE = "https://api.telegram.org"
TOKEN = "700000:ttest-typing"
ADM = {"authorization": "Bearer adm-secret"}
APP = {"authorization": "Bearer app-secret"}


def make_cfg(data_dir: str, **typing) -> Config:
    cfg = Config()
    cfg.admin_secret = "adm-secret"
    cfg.app_secret = "app-secret"
    cfg.data_dir = data_dir
    cfg.upstream = BASE
    cfg.poll_timeout = 0  # respx answers instantly
    cfg.poll_backoff_max = 0.1
    cfg.typing = TypingConfig(
        enabled=typing.get("enabled", True),
        private_only=typing.get("private_only", True),
        interval_s=typing.get("interval_s", 0.05),
        timeout_s=typing.get("timeout_s", 1.0),
        timeout_text=typing.get("timeout_text", DEFAULT_TYPING_TIMEOUT_TEXT),
    )
    return cfg


def feed_with(batches: list[list[dict]], release=None):
    """getUpdates mock: yields batch i once the poller's offset hasn't
    passed it AND release(i) allows it (default: always). Then empties."""

    state = {"i": 0}

    async def feed(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else {}
        offset = body.get("offset", 0)
        i = state["i"]
        due = i < len(batches) and batches[i] and offset <= batches[i][0]["update_id"]
        if due and (release is None or release(i)):
            state["i"] += 1
            return httpx.Response(200, json={"ok": True, "result": batches[i]})
        await asyncio.sleep(0.01)  # keep the empty-poll loop from hot-spinning
        return httpx.Response(200, json={"ok": True, "result": []})

    return feed


def telegram_mock(feed) -> tuple[respx.Route, respx.Route]:
    respx.post(f"{BASE}/bot{TOKEN}/deleteWebhook").respond(200, json={"ok": True})
    action = respx.post(f"{BASE}/bot{TOKEN}/sendChatAction").respond(
        200, json={"ok": True, "result": True}
    )
    send = respx.post(f"{BASE}/bot{TOKEN}/sendMessage").respond(
        200, json={"ok": True, "result": {"message_id": 42}}
    )
    respx.post(f"{BASE}/bot{TOKEN}/answerCallbackQuery").respond(
        200, json={"ok": True, "result": True}
    )
    respx.post(f"{BASE}/bot{TOKEN}/getUpdates").mock(side_effect=feed)
    return action, send


def msg(update_id: int, chat_id: int) -> dict:
    return {
        "update_id": update_id,
        "message": {"message_id": update_id, "chat": {"id": chat_id, "type": "private"}},
    }


def btn(update_id: int, chat_id: int, cbq_id: str) -> dict:
    return {
        "update_id": update_id,
        "callback_query": {
            "id": cbq_id,
            "message": {"message_id": update_id, "chat": {"id": chat_id}},
        },
    }


def bodies_of(route) -> list[dict]:
    return [json.loads(c.request.content) for c in route.calls]


async def wait_for(predicate, timeout: float = 5.0, what: str = "condition"):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


def beats_at_least(action, n: int):
    async def check():
        return len(action.calls) >= n

    return check


class Harness:
    """One app + client + registered bot, closed deterministically."""

    def __init__(self, cfg: Config):
        self.app = create_app(cfg)

    async def __aenter__(self):
        self._cm = self.app.router.lifespan_context(self.app)
        await self._cm.__aenter__()
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://gw"
        )
        r = await self.client.post("/admin/bots", json={"token": TOKEN, "alias": "b1"}, headers=ADM)
        assert r.status_code == 200, r.text
        return self

    async def __aexit__(self, *exc):
        await self.client.aclose()
        await self._cm.__aexit__(*exc)

    @property
    def mgr(self):
        return self.app.state.mgr

    def session(self):
        return self.mgr.sessions["b1"]

    async def typing_status(self) -> dict:
        r = await self.client.get("/admin/status", headers=ADM)
        assert r.status_code == 200
        return r.json()["bots"][0]["typing"]

    def active_is(self, n: int):
        async def check():
            return (await self.typing_status())["active_chats"] == n

        return check


class TestTriggers:
    @respx.mock
    async def test_inbound_message_starts_and_repeats_typing(self, tmp_path):
        action, _ = telegram_mock(feed_with([[msg(1001, 501)]]))
        async with Harness(make_cfg(str(tmp_path / "d"))) as h:
            await wait_for(beats_at_least(action, 2), what="at least two typing beats")
            assert all(b == {"chat_id": 501, "action": "typing"} for b in bodies_of(action))

            st = await h.typing_status()
            assert st["active_chats"] == 1
            assert st["enabled"] is True

            r = await h.client.get("/metrics")
            assert 'tg_gateway_typing_active{alias="b1"} 1' in r.text

    @respx.mock
    async def test_button_press_starts_typing(self, tmp_path):
        action, _ = telegram_mock(feed_with([[btn(1001, 502, "cbq1")]]))
        async with Harness(make_cfg(str(tmp_path / "d"))) as _:
            await wait_for(beats_at_least(action, 1), what="typing beat after button press")
            assert all(b["chat_id"] == 502 for b in bodies_of(action))

    @respx.mock
    async def test_group_chat_gets_nothing_by_default(self, tmp_path):
        action, send = telegram_mock(feed_with([[msg(1001, -100999)]]))
        async with Harness(make_cfg(str(tmp_path / "d"))) as h:
            await asyncio.sleep(0.3)
            assert len(action.calls) == 0
            assert not send.called
            assert (await h.typing_status())["active_chats"] == 0

    @respx.mock
    async def test_second_action_refreshes_not_duplicates(self, tmp_path):
        action, _ = telegram_mock(feed_with([[msg(1001, 501)], [msg(2001, 501)]]))
        async with Harness(make_cfg(str(tmp_path / "d"), timeout_s=0.6)) as h:
            await wait_for(h.active_is(1), what="session open")
            await wait_for(beats_at_least(action, 3), what="beats after the second message")
            assert (await h.typing_status())["active_chats"] == 1  # still ONE session


class TestStops:
    @respx.mock
    async def test_consumer_write_stops_the_loop(self, tmp_path):
        action, _ = telegram_mock(feed_with([[msg(1001, 501)]]))
        async with Harness(make_cfg(str(tmp_path / "d"))) as h:
            await wait_for(beats_at_least(action, 1), what="typing beat")
            r = await h.client.post(
                "/bots/b1/sendMessage",
                json={"chat_id": 501, "text": "reply"},
                headers=APP,
            )
            assert r.status_code == 200
            await wait_for(h.active_is(0), what="session closed by the reply")
            n = len(action.calls)
            await asyncio.sleep(0.25)
            assert len(action.calls) == n  # no further beats

    @respx.mock
    async def test_answer_callback_query_stops_the_loop(self, tmp_path):
        action, _ = telegram_mock(feed_with([[btn(1001, 505, "cbq9")]]))
        async with Harness(make_cfg(str(tmp_path / "d"))) as h:
            await wait_for(beats_at_least(action, 1), what="typing beat")
            r = await h.client.post(
                "/bots/b1/answerCallbackQuery",
                json={"callback_query_id": "cbq9", "text": "ok"},
                headers=APP,
            )
            assert r.status_code == 200
            await wait_for(h.active_is(0), what="session closed by answerCallbackQuery")
            n = len(action.calls)
            await asyncio.sleep(0.25)
            assert len(action.calls) == n

    @respx.mock
    async def test_cancel_action_stops_without_upstream_call(self, tmp_path):
        action, _ = telegram_mock(feed_with([[msg(1001, 501)]]))
        async with Harness(make_cfg(str(tmp_path / "d"))) as h:
            await wait_for(beats_at_least(action, 1), what="typing beat")
            beats = len(action.calls)
            r = await h.client.post(
                "/bots/b1/sendChatAction",
                json={"chat_id": 501, "action": "cancel"},
                headers=APP,
            )
            assert r.status_code == 200 and r.json() == {"ok": True, "result": True}
            # the cancel itself was NOT forwarded upstream
            assert len(action.calls) == beats
            await wait_for(h.active_is(0), what="session closed by cancel")
            await asyncio.sleep(0.25)
            assert len(action.calls) == beats

    @respx.mock
    async def test_pause_cancels_typing(self, tmp_path):
        action, _ = telegram_mock(feed_with([[msg(1001, 501)]]))
        async with Harness(make_cfg(str(tmp_path / "d"))) as h:
            await wait_for(beats_at_least(action, 1), what="typing beat")
            r = await h.client.post("/admin/bots/b1/pause", headers=ADM)
            assert r.json()["ok"] is True
            await wait_for(h.active_is(0), what="typing cancelled on pause")
            n = len(action.calls)
            await asyncio.sleep(0.2)
            assert len(action.calls) == n


class TestTimeout:
    @respx.mock
    async def test_timeout_sends_english_notice_once_private(self, tmp_path):
        action, send = telegram_mock(feed_with([[msg(1001, 501)]]))
        async with Harness(make_cfg(str(tmp_path / "d"), timeout_s=0.25)) as h:

            async def notice_sent():
                return any(
                    b.get("text") == DEFAULT_TYPING_TIMEOUT_TEXT and b.get("chat_id") == 501
                    for b in bodies_of(send)
                )

            await wait_for(notice_sent, what="timeout notice")
            notices = [b for b in bodies_of(send) if b.get("text") == DEFAULT_TYPING_TIMEOUT_TEXT]
            assert len(notices) == 1
            await wait_for(h.active_is(0), what="session closed after timeout")
            beats = len(action.calls)
            await asyncio.sleep(0.25)
            assert len(action.calls) == beats  # loop really ended

    @respx.mock
    async def test_group_timeout_stays_silent_even_with_private_only_off(self, tmp_path):
        action, send = telegram_mock(feed_with([[msg(1001, -777)]]))
        cfg = make_cfg(str(tmp_path / "d"), private_only=False, timeout_s=0.2)
        async with Harness(cfg) as h:
            await wait_for(beats_at_least(action, 1), what="typing beat in group")
            assert all(b["chat_id"] == -777 for b in bodies_of(action))
            await wait_for(h.active_is(0), what="group session ended by timeout")
            assert not send.called  # never any notice outside private chats

    @respx.mock
    async def test_cancel_timeout_keeps_typing_and_suppresses_notice(self, tmp_path):
        action, send = telegram_mock(feed_with([[msg(1001, 501)]]))
        async with Harness(make_cfg(str(tmp_path / "d"), timeout_s=0.3)) as h:
            await wait_for(beats_at_least(action, 1), what="typing beat")
            r = await h.client.post(
                "/bots/b1/sendChatAction",
                json={"chat_id": 501, "action": "cancel_timeout"},
                headers=APP,
            )
            assert r.status_code == 200 and r.json()["ok"] is True
            await asyncio.sleep(0.6)  # well past the original deadline
            assert len(action.calls) > 2  # typing continued
            assert not send.called  # notice suppressed
            assert (await h.typing_status())["active_chats"] == 1


class TestRateLimit:
    @respx.mock
    async def test_paused_chat_stops_typing_and_opens_no_new_session(self, tmp_path):
        action, _ = telegram_mock(feed_with([[msg(1001, 501)]]))
        async with Harness(make_cfg(str(tmp_path / "d"))) as h:
            await wait_for(beats_at_least(action, 1), what="typing beat")
            # real 429 on this chat → hard pause (retry_after)
            h.session().guard.report_429(501, 30.0)
            await wait_for(h.active_is(0), what="session stopped on rate pause")
            n = len(action.calls)
            await asyncio.sleep(0.25)
            assert len(action.calls) == n  # no beats while paused
            # a new user action on the hard-paused chat must NOT open a
            # new session — the bot cannot answer, so it must not type
            h.mgr.typing.on_updates(h.session(), [msg(2001, 501)])
            await asyncio.sleep(0.2)
            assert (await h.typing_status())["active_chats"] == 0


class TestSettings:
    @respx.mock
    async def test_global_change_applies_to_running_session(self, tmp_path):
        action, _ = telegram_mock(feed_with([[msg(1001, 501)]]))
        async with Harness(make_cfg(str(tmp_path / "d"))) as h:
            await wait_for(beats_at_least(action, 1), what="typing beat")
            r = await h.client.post("/admin/settings/typing", json={"enabled": False}, headers=ADM)
            assert r.status_code == 200
            assert r.json()["settings"]["enabled"] is False
            await wait_for(h.active_is(0), what="session closed by global disable")
            n = len(action.calls)
            await asyncio.sleep(0.25)
            assert len(action.calls) == n

            r = await h.client.get("/admin/settings/typing", headers=ADM)
            assert r.json()["settings"]["enabled"] is False

    @respx.mock
    async def test_per_bot_override_wins_and_reset_inherits(self, tmp_path):
        gate = {"open": False}
        action, _ = telegram_mock(feed_with([[msg(1001, 501)]], release=lambda i: gate["open"]))
        async with Harness(make_cfg(str(tmp_path / "d"))) as h:
            # bot disabled BEFORE the update is allowed through
            r = await h.client.post("/admin/bots/b1/typing", json={"enabled": False}, headers=ADM)
            assert r.status_code == 200
            st = r.json()["typing"]
            assert st["enabled"] is False and st["overridden"] == ["enabled"]

            gate["open"] = True  # the message flows in now
            await asyncio.sleep(0.3)
            assert len(action.calls) == 0  # per-bot off beats the global on

            # explicit null clears the override → inherits the global
            r = await h.client.post("/admin/bots/b1/typing", json={"enabled": None}, headers=ADM)
            st = r.json()["typing"]
            assert st["enabled"] is True and st["overridden"] == []

            h.mgr.typing.on_updates(h.session(), [msg(2001, 501)])
            await wait_for(beats_at_least(action, 1), what="typing resumes after reset")

    @respx.mock
    async def test_disabled_from_boot_never_types(self, tmp_path):
        action, _ = telegram_mock(feed_with([[msg(1001, 501)]]))
        async with Harness(make_cfg(str(tmp_path / "d"), enabled=False)) as h:
            await asyncio.sleep(0.3)
            assert len(action.calls) == 0
            assert (await h.typing_status())["active_chats"] == 0
