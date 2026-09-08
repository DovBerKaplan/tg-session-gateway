"""Admin plane (spec §10.2) — never exposed to the internet.

Bearer GW_ADMIN_SECRET. Bot CRUD, pause/drain/resume, push-mode wiring,
per-bot status with the spec §13 metrics, gateway-wide health.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .config import Config
from .sessions import SessionManager
from .store import Store, new_alias

log = logging.getLogger("gateway.admin")


class RegisterBot(BaseModel):
    alias: str | None = None
    token: str
    on_limit: str | None = None      # queue | reject — per bot (§8.3)
    paid_broadcasts: bool = False    # double opt-in arm #1 (§8.2 §8)


class SetPush(BaseModel):
    url: str | None = None  # None clears push mode → pull only


class SetPaid(BaseModel):
    enabled: bool


def build_admin_router(cfg: Config, mgr: SessionManager, store: Store) -> APIRouter:
    router = APIRouter()

    def _authed(request: Request) -> bool:
        return bool(cfg.admin_secret) and request.headers.get(
            "authorization"
        ) == f"Bearer {cfg.admin_secret}"

    @router.post("/admin/bots")
    async def register(body: RegisterBot, request: Request):
        if not _authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        alias = body.alias or new_alias()
        s = await mgr.attach(alias, body.token)
        return {"ok": True, "alias": alias, "state": s.state.value}

    @router.get("/admin/status")
    async def status(request: Request):
        if not _authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        return {
            "ok": True,
            "bots": [s.public_status() for s in mgr.sessions.values()],
        }

    @router.post("/admin/bots/{alias}/pause")
    async def pause(alias: str, request: Request):
        if not _authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        return {"ok": await mgr.pause(alias)}

    @router.post("/admin/bots/{alias}/resume")
    async def resume(alias: str, request: Request):
        if not _authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        return {"ok": await mgr.resume(alias)}

    @router.post("/admin/bots/{alias}/drain")
    async def drain(alias: str, request: Request):
        if not _authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        return {"ok": await mgr.drain(alias)}

    @router.delete("/admin/bots/{alias}")
    async def delete(alias: str, request: Request):
        if not _authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        return {"ok": await mgr.delete(alias)}

    @router.post("/admin/bots/{alias}/push")
    async def set_push(alias: str, body: SetPush, request: Request):
        if not _authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        s = mgr.sessions.get(alias)
        if not s:
            return JSONResponse({"ok": False, "error_code": 404}, status_code=404)
        s.push_url = body.url
        return {"ok": True, "push_url": s.push_url}

    return router
