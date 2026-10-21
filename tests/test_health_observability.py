"""Health & observability (Phase 3) — liveness vs readiness, the
active ping (half-open detection), and the metrics pack."""

import asyncio
import time
from types import SimpleNamespace
from typing import ClassVar

import httpx
import respx

from gateway.config import Config
from gateway.loophealth import LoopBeat
from mtgateway.sessions import MTSession, SessionState


class TestLoopBeat:
    async def test_beat_proves_loop_alive(self):
        beat = LoopBeat()
        beat.start()
        try:
            await asyncio.sleep(0.1)
            assert beat.alive is True
            assert beat.lag_s < 5.0
        finally:
            await beat.stop()

    def test_stale_beat_reports_dead(self):
        beat = LoopBeat()
        beat.last = time.monotonic() - 60.0  # simulate a wedged loop
        assert beat.alive is False


class TestActivePing:
    async def test_stuck_ping_forces_reconnect(self, tmp_path, monkeypatch):
        """Half-open: is_connected stays True, the ping hangs — the
        supervisor must stop the client and reconnect."""
        import os
        import sys

        sys.path.insert(0, os.path.dirname(__file__))
        from test_session_lifecycle import FlakyClient, _Cfg, _wait_for

        import mtgateway.sessions as ms
        from mtgateway.store import MTStore

        FlakyClient.live_by_alias.clear()
        FlakyClient.script.clear()

        class HalfOpenClient(FlakyClient):
            calls: ClassVar[dict] = {"stop": 0}
            stuck_used: ClassVar[bool] = False  # the episode happens ONCE

            async def get_me(self):
                self._me_calls = getattr(self, "_me_calls", 0) + 1
                if self._me_calls > 1 and not HalfOpenClient.stuck_used:
                    HalfOpenClient.stuck_used = True
                    await asyncio.sleep(3600)  # the stuck half-open ping
                return SimpleNamespace(username="bot", id=1)

            async def stop(self):
                HalfOpenClient.calls["stop"] += 1
                self._me_calls = 0
                await super().stop()

        monkeypatch.setattr(ms, "PyrogramClient", HalfOpenClient)
        store = MTStore(str(tmp_path))
        await store.connect()
        mgr = ms.MTSessionManager(_Cfg(str(tmp_path)), store)
        try:
            s = await mgr.register("b1", "t")
            await _wait_for(lambda: s.state == SessionState.live, what="live")
            # force a ping NOW (skip the 60s cadence); it sticks once
            s._last_ping_at = time.monotonic() - 120.0
            await _wait_for(
                lambda: HalfOpenClient.calls["stop"] >= 1 and s.reconnects >= 1,
                timeout=20.0,
                what="half-open disconnect + reconnect",
            )
            assert s.state == SessionState.live
            # after the reconnect, the next ping succeeds (episode over)
            s._last_ping_at = time.monotonic() - 120.0
            await _wait_for(lambda: s.last_ping_ok_at > 0, timeout=15.0, what="healthy ping")
        finally:
            await mgr.shutdown()
            await store.close()


class TestGatewayHealthEndpoints:
    @respx.mock
    async def test_healthz_readyz_shapes(self, tmp_path):
        from gateway.app import create_app

        BASE = "https://api.telegram.org"
        TOKEN = "700000:itest-h"
        ADM = {"authorization": "Bearer adm-secret"}
        respx.post(f"{BASE}/bot{TOKEN}/deleteWebhook").respond(200, json={"ok": True})

        async def feed(request):  # small sleep keeps the poller off a hot spin
            await asyncio.sleep(0.01)
            return httpx.Response(200, json={"ok": True, "result": []})

        respx.post(f"{BASE}/bot{TOKEN}/getUpdates").mock(side_effect=feed)
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
            h = await c.get("/healthz")
            assert h.status_code == 200
            assert h.json()["loop_lag_s"] < 5

            r = await c.get("/readyz")
            assert r.status_code == 200, r.text  # write probe passes

            await c.post("/admin/bots", json={"token": TOKEN, "alias": "b1"}, headers=ADM)
            await asyncio.sleep(0.2)
            r = await c.get("/readyz")
            body = r.json()
            assert body["sessions"]["b1"] == "live"
            assert body["live"] == 1

            m = await c.get("/metrics")
            for name in (
                "tg_gateway_storage_degraded",
                "tg_gateway_loop_lag_seconds",
                "tg_gateway_persist_failures_total",
                "tg_gateway_corrupt_rows_total",
            ):
                assert name in m.text, name

    async def test_readyz_fails_when_storage_degraded(self, tmp_path):
        """A degraded StorageHealth flips readyz to 503 — persist
        failures must never hide behind an ok."""
        from gateway.app import create_app

        cfg = Config()
        cfg.admin_secret = "a"
        cfg.app_secret = "b"
        cfg.data_dir = str(tmp_path)
        app = create_app(cfg)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c,
        ):
            app.state.storage_health.note_persist_failure("queue.push:b1", OSError("full"))
            r = await c.get("/readyz")
            assert r.status_code == 503
            assert "persist" in r.text


class TestMtgatewayMetrics:
    async def test_metrics_pack(self, tmp_path, monkeypatch):
        import os
        import sys

        sys.path.insert(0, os.path.dirname(__file__))
        from test_session_lifecycle import FlakyClient

        import mtgateway.sessions as ms
        from mtgateway.app import create_app
        from mtgateway.config import Config as MTConfig

        FlakyClient.live_by_alias.clear()
        FlakyClient.script.clear()
        monkeypatch.setattr(ms, "PyrogramClient", FlakyClient)

        cfg = MTConfig(
            admin_secret="a",
            app_secret="b",
            api_id=1,
            api_hash="h",
            data_dir=str(tmp_path),
        )
        app = create_app(cfg)
        ADM = {"authorization": "Bearer a"}
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://mt") as c,
        ):
            await c.post("/v1/admin/sessions", json={"alias": "b1", "token": "t"}, headers=ADM)
            await asyncio.sleep(0.2)
            m = await c.get("/metrics")
            for name in (
                "tg_gateway_session_reconnects_total",
                "tg_gateway_session_start_attempts_total",
                "tg_gateway_idempotency_claims_total",
                "tg_gateway_persist_failures_total",
                "tg_gateway_lock_reclaims_total",
                "tg_gateway_loop_lag_seconds",
                "tg_gateway_storage_degraded",
            ):
                assert name in m.text, name
            h = await c.get("/healthz")
            assert h.json()["loop_lag_s"] < 5
            r = await c.get("/readyz")
            assert r.status_code == 200
            assert r.json()["live"] == 1

    async def test_dead_man_and_ping_stats(self):
        s = MTSession(alias="b", token="t")
        assert s.stats()["seconds_since_last_update"] is None
        await s.push_update({"n": 1})
        assert s.stats()["seconds_since_last_update"] is not None
        s.last_ping_ok_at = time.monotonic()
        assert s.stats()["seconds_since_ping_ok"] is not None
