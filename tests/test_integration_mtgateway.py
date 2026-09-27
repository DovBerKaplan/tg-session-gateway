"""Integration: MTProto sidecar lifecycle against a FAKE Pyrogram client.

The whole sidecar in one process — real FastAPI app, real store, real
singleton lock files, real rate guard; only the Pyrogram Client class
is swapped for a fake that records what would have gone to Telegram.

Covers the seven-point bar the reviews set for the sidecar:
register → lock BEFORE connect → updates → send → idempotency →
pause_sending → synthetic 429 scoped per chat → real FLOOD_WAIT
parsing → second sidecar refused before any Telegram contact.
"""

import asyncio
import os
import time
from types import SimpleNamespace

import httpx
import pytest

import mtgateway.sessions as mt_sessions
from mtgateway.app import create_app
from mtgateway.config import Config

TOKEN = "710000:mt-itest"
ADM = {"authorization": "Bearer mt-adm"}
APP = {"authorization": "Bearer mt-app"}


class FakeFloodWait(Exception):
    """str() matches the Pyrogram FloodWait format the sidecar parses."""


class FakeClient:
    """Stands in for pyrogram.Client; records every Telegram touch."""

    instances: list["FakeClient"] = []  # noqa: RUF012 — test registry, reset per run
    start_calls = 0

    def __init__(self, name, api_id, api_hash, bot_token, workdir, in_memory):
        self.name = name
        self.workdir = workdir
        self.is_connected = False
        self.sent: list[tuple] = []
        self.raw_handler = None
        self.flood_once_chat: int | None = None  # raise FloodWait once for this chat
        self._flooded = False
        FakeClient.instances.append(self)

    def add_handler(self, handler, group=0):
        self.raw_handler = handler  # RawUpdateHandler(callback)

    async def start(self):
        FakeClient.start_calls += 1
        self.is_connected = True

    async def stop(self):
        self.is_connected = False

    async def get_me(self):
        return SimpleNamespace(username="fake_bot", id=42)

    async def send_message(self, chat_id, text, **kwargs):
        if self.flood_once_chat == chat_id and not self._flooded:
            self._flooded = True
            raise FakeFloodWait("Telegram flood: FloodWait(42)")
        self.sent.append((chat_id, text))
        return {"message_id": len(self.sent)}

    # test helper: feed an update through the manager's raw handler
    async def fire_update(self, update: dict) -> None:
        assert self.raw_handler is not None
        await self.raw_handler.callback(self, update, [], [])


def make_cfg(data_dir: str) -> Config:
    cfg = Config()
    cfg.admin_secret = "mt-adm"
    cfg.app_secret = "mt-app"
    cfg.api_id = 12345  # never used: the Pyrogram Client is faked
    cfg.api_hash = "deadbeef" * 8
    cfg.data_dir = data_dir
    cfg.session_dir = os.path.join(data_dir, "sessions")
    cfg.policy.on_limit = "reject"  # deterministic synthetic 429s
    cfg.policy.queue_wait_timeout = 0.5
    return cfg


@pytest.fixture
def fake_pyrogram(monkeypatch):
    FakeClient.instances = []
    FakeClient.start_calls = 0
    monkeypatch.setattr(mt_sessions, "PyrogramClient", FakeClient)
    return FakeClient


async def wait_for(predicate, timeout: float = 5.0, what: str = "condition"):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


class TestSidecarLifecycle:
    async def test_register_lock_send_updates_idempotency(self, fake_pyrogram, tmp_path):
        cfg = make_cfg(str(tmp_path / "d1"))
        app = create_app(cfg)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://mt") as c,
        ):
            r = await c.post(
                "/v1/admin/sessions", json={"alias": "b1", "token": TOKEN}, headers=ADM
            )
            assert r.status_code == 200, r.text

            await wait_for(lambda: fake_pyrogram.start_calls == 1, what="client.start()")
            fake = fake_pyrogram.instances[-1]
            assert fake.start_calls == 1  # started ONCE — the sidecar owns it

            # the singleton lock exists on disk
            assert os.path.exists(os.path.join(cfg.session_dir, "b1.lock"))

            # app secret required
            r = await c.post("/v1/sessions/b1/send_message", json={"chat_id": 10, "text": "x"})
            assert r.status_code == 401

            # send → forwarded to the (fake) client verbatim
            r = await c.post(
                "/v1/sessions/b1/send_message",
                json={"chat_id": 10, "text": "hi"},
                headers=APP,
            )
            assert r.status_code == 200 and r.json()["ok"] is True
            assert fake.sent == [(10, "hi")]

            # updates from Telegram → served to the app via pull
            await fake.fire_update({"update_id": 1, "message": {"text": "hello"}})
            r = await c.post(
                "/v1/sessions/b1/getUpdates", json={"timeout": 0, "limit": 10}, headers=APP
            )
            results = r.json().get("result") or []
            assert any(u.get("message", {}).get("text") == "hello" for u in results)

            # idempotency: same key twice → one send
            r = await c.post(
                "/v1/sessions/b1/send_message",
                json={"_idempotency_key": "k1", "chat_id": 10, "text": "once"},
                headers=APP,
            )
            assert r.json()["ok"] is True
            r = await c.post(
                "/v1/sessions/b1/send_message",
                json={"_idempotency_key": "k1", "chat_id": 10, "text": "once"},
                headers=APP,
            )
            assert r.json().get("idempotent_replay") is True
            assert fake.sent == [(10, "hi"), (10, "once")]  # exactly one "once"

            # admin audit recorded the registration
            r = await c.get("/v1/admin/audit", headers=ADM)
            entries = r.json()["entries"]
            assert entries and entries[-1]["action"] == "register"

    async def test_second_sidecar_refused_before_telegram(self, fake_pyrogram, tmp_path):
        cfg = make_cfg(str(tmp_path / "d2"))
        os.makedirs(cfg.session_dir, exist_ok=True)

        # a LIVE holder: our own pid, fresh heartbeat → lock is not stale
        def _plant_lock():
            with open(os.path.join(cfg.session_dir, "b1.lock"), "w") as f:
                f.write(f"{os.getpid()}\n{time.time()}\n")

        _plant_lock()

        app = create_app(cfg)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://mt") as c,
        ):
            r = await c.post(
                "/v1/admin/sessions", json={"alias": "b1", "token": TOKEN}, headers=ADM
            )
            assert r.status_code == 200  # accepted for start...

            await asyncio.sleep(0.3)  # let _start_client run and refuse
            # ...but the client NEVER reached Telegram
            assert fake_pyrogram.start_calls == 0
            # refused sessions are removed, not left half-alive: the
            # API answers 404 and the ONLY trace is the refusal log line
            r = await c.post(
                "/v1/sessions/b1/send_message",
                json={"chat_id": 10, "text": "x"},
                headers=APP,
            )
            assert r.status_code == 404

            # holder gone → next registration succeeds
            os.unlink(os.path.join(cfg.session_dir, "b1.lock"))
            r = await c.post(
                "/v1/admin/sessions", json={"alias": "b1", "token": TOKEN}, headers=ADM
            )
            assert r.status_code == 200
            await wait_for(lambda: fake_pyrogram.start_calls == 1, what="retry start()")

    async def test_pause_sending_blocks_writes_allows_updates(self, fake_pyrogram, tmp_path):
        cfg = make_cfg(str(tmp_path / "d3"))
        app = create_app(cfg)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://mt") as c,
        ):
            await c.post("/v1/admin/sessions", json={"alias": "b1", "token": TOKEN}, headers=ADM)
            await wait_for(lambda: fake_pyrogram.start_calls == 1, what="start()")
            fake = fake_pyrogram.instances[-1]

            r = await c.post("/v1/admin/sessions/b1/pause_sending", headers=ADM)
            assert r.json()["ok"] is True

            r = await c.post(
                "/v1/sessions/b1/send_message",
                json={"chat_id": 10, "text": "x"},
                headers=APP,
            )
            assert r.status_code == 423  # locked
            assert fake.sent == []  # nothing left the building

            # updates keep flowing while sends are paused (E4)
            await fake.fire_update({"update_id": 2, "message": {"text": "still listening"}})
            r = await c.post("/v1/sessions/b1/getUpdates", json={"timeout": 0}, headers=APP)
            assert any(
                u.get("message", {}).get("text") == "still listening"
                for u in (r.json().get("result") or [])
            )

    async def test_synthetic_429_scoped_per_chat(self, fake_pyrogram, tmp_path):
        cfg = make_cfg(str(tmp_path / "d4"))
        app = create_app(cfg)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://mt") as c,
        ):
            await c.post("/v1/admin/sessions", json={"alias": "b1", "token": TOKEN}, headers=ADM)
            await wait_for(lambda: fake_pyrogram.start_calls == 1, what="start()")

            codes = []
            for i in range(6):  # private burst is 3 — the tail must 429
                r = await c.post(
                    "/v1/sessions/b1/send_message",
                    json={"chat_id": 10, "text": f"spam{i}"},
                    headers=APP,
                )
                codes.append(r.status_code)
            assert 429 in codes, f"no synthetic 429: {codes}"

            # the hot chat is throttled, a different chat is NOT muted
            r = await c.post(
                "/v1/sessions/b1/send_message",
                json={"chat_id": 20, "text": "other chat"},
                headers=APP,
            )
            assert r.status_code == 200, f"per-chat scoping broken: {r.status_code} {r.text}"

    async def test_real_flood_wait_parsed_and_reported(self, fake_pyrogram, tmp_path):
        cfg = make_cfg(str(tmp_path / "d5"))
        app = create_app(cfg)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://mt") as c,
        ):
            await c.post("/v1/admin/sessions", json={"alias": "b1", "token": TOKEN}, headers=ADM)
            await wait_for(lambda: fake_pyrogram.start_calls == 1, what="start()")
            fake = fake_pyrogram.instances[-1]
            fake.flood_once_chat = 10

            r = await c.post(
                "/v1/sessions/b1/send_message",
                json={"chat_id": 10, "text": "will flood"},
                headers=APP,
            )
            body = r.json()
            assert r.status_code == 429
            assert "FLOOD_WAIT" in body["description"]
            assert body["parameters"]["retry_after"] == 42

            # stats surface the REAL flood wait (not synthetic)
            r = await c.get("/v1/admin/status", headers=ADM)
            s = r.json()["sessions"][0]
            assert s["real_flood_wait"] == 1
            assert s["synthetic_429"] == 0
