"""Restart & deployment hardening (Phase 4) — crash-loop safe mode,
extended preflight, and boot-time token verification."""

import json
import os
import stat
import time

import httpx
import pytest

from gateway.config import Config
from gateway.preflight import preflight
from gateway.safemode import RestartTracker


class TestRestartTracker:
    def test_under_threshold_no_safe_mode(self, tmp_path):
        t = RestartTracker(str(tmp_path / "boots.json"))
        for _ in range(5):
            safe, boots = t.record_boot()
        assert safe is False and boots == 5

    def test_crash_loop_triggers_safe_mode(self, tmp_path):
        t = RestartTracker(str(tmp_path / "boots.json"))
        for _ in range(5):
            t.record_boot()
        safe, boots = t.record_boot()  # 6th boot inside the window
        assert safe is True and boots == 6

    def test_old_boots_expire_out_of_the_window(self, tmp_path):
        path = str(tmp_path / "boots.json")
        seed = [time.time() - 7000] * 10  # all outside the 600s window
        with open(path, "w") as f:
            json.dump(seed, f)
        t = RestartTracker(path)
        safe, boots = t.record_boot()
        assert safe is False and boots == 1

    def test_clear_resets_history(self, tmp_path):
        path = str(tmp_path / "boots.json")
        t = RestartTracker(path)
        for _ in range(6):
            t.record_boot()
        assert t.record_boot()[0] is True
        t.clear()
        assert RestartTracker(path).record_boot() == (False, 1)


class TestPreflightExtensions:
    def test_no_freespace_refuses_to_boot(self, tmp_path, monkeypatch):
        import shutil

        monkeypatch.setattr(
            shutil, "disk_usage", lambda p: type("U", (), {"free": 100 * 1024**2})()
        )  # 100MB < 1GB default
        with pytest.raises(SystemExit) as e:
            preflight("key", str(tmp_path))
        assert "free" in e.value.code

    def test_unwritable_data_dir_refuses_to_boot(self, tmp_path):
        ro = tmp_path / "ro"
        ro.mkdir()
        ro.chmod(stat.S_IRUSR | stat.S_IXUSR)  # r-x —— no write
        try:
            with pytest.raises(SystemExit) as e:
                preflight("key", str(ro))
            assert "writable" in e.value.code
        finally:
            ro.chmod(0o755)

    def test_healthy_passes(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GW_ALLOW_EPHEMERAL_DATA", "1")  # tmp dirs aren't mounts
        preflight("key", str(tmp_path))  # no exit


def _read_json(path):
    with open(path) as f:
        return json.load(f)


def _write_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f)


class TestSafeModeEndToEnd:
    def _cfg(self, tmp_path):
        cfg = Config()
        cfg.admin_secret = "adm-secret"
        cfg.app_secret = "app-secret"
        cfg.data_dir = str(tmp_path)
        cfg.upstream = "https://api.telegram.org"
        cfg.poll_timeout = 0
        cfg.poll_backoff_max = 0.1
        return cfg

    async def test_safe_mode_blocks_restore_until_acked(self, tmp_path):
        import asyncio

        import respx

        from gateway.app import create_app

        BASE = "https://api.telegram.org"
        TOKEN = "700000:itest-safe"
        ADM = {"authorization": "Bearer adm-secret"}
        with respx.mock:
            respx.post(f"{BASE}/bot{TOKEN}/deleteWebhook").respond(200, json={"ok": True})

            async def feed(request):
                await asyncio.sleep(0.01)
                return httpx.Response(200, json={"ok": True, "result": []})

            respx.post(f"{BASE}/bot{TOKEN}/getUpdates").mock(side_effect=feed)

            # lifetime 1: register a bot, then "crash" repeatedly
            cfg = self._cfg(tmp_path)
            app = create_app(cfg)
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://gw"
                ) as c,
            ):
                await c.post("/admin/bots", json={"token": TOKEN, "alias": "b1"}, headers=ADM)

            # simulate the crash loop: 6 boots recorded in the window
            hist_path = os.path.join(cfg.data_dir, "boot_history.json")
            hist = await asyncio.to_thread(_read_json, hist_path)
            hist = (hist + [time.time()] * 6)[-10:]
            await asyncio.to_thread(_write_json, hist_path, hist)

            # lifetime 2: safe mode — bot stays on disk, nothing connects
            app2 = create_app(self._cfg(tmp_path))
            async with (
                app2.router.lifespan_context(app2),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app2), base_url="http://gw"
                ) as c,
            ):
                r = await c.get("/readyz")
                assert r.status_code == 503 and r.json().get("safe_mode") is True
                st = await c.get("/admin/status", headers=ADM)
                assert st.json()["bots"] == [], "safe mode must not auto-connect"

                # ack: boot history cleared, sessions restored, ready
                r = await c.post("/admin/safe_mode/ack", headers=ADM)
                assert r.json() == {"ok": True, "safe_mode": False, "restored": 1}
                r = await c.get("/readyz")
                assert r.status_code == 200
                assert r.json()["sessions"]["b1"] == "live"
