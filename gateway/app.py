"""App factory — wiring: config, store, sessions, proxy, admin, health."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from . import __version__
from .admin import RegisterBot, SetPush
from .config import Config
from .sessions import SessionManager
from .store import Store

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("gateway")


def create_app(cfg: Config | None = None) -> FastAPI:
    cfg = cfg or Config()
    cfg.validate()

    store = Store(cfg.data_dir, cfg.fernet_key)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await store.connect()
        client = httpx.AsyncClient(base_url=cfg.upstream, timeout=60.0)
        mgr = SessionManager(cfg, store, client)
        app.state.store, app.state.client, app.state.mgr, app.state.cfg = (
            store, client, mgr, cfg,
        )
        restored = await mgr.restore_all()
        log.info("gateway up — restored %d bot session(s)", restored)
        yield
        await mgr.shutdown()
        await client.aclose()
        await store.close()

    app = FastAPI(title="TG Session Gateway", version=__version__, lifespan=lifespan)

    # routers resolve mgr/store/client from request.app.state (set in lifespan)
    app.include_router(build_router_from_state(cfg))
    app.include_router(build_admin_router_lazy(cfg))

    @app.get("/healthz")
    async def healthz():
        return {"ok": True, "version": __version__}

    @app.get("/readyz")
    async def readyz(request: Request):
        try:
            await request.app.state.store.list_bots()
            return {"ok": True}
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=503)

    return app


def _build_proxy_depd(cfg: Config):
    """Proxy routes that resolve mgr/client from request.state (set in lifespan)."""
    from fastapi import Request

    from .proxy import Proxy, build_router

    return build_router_from_state(cfg)


def build_router_from_state(cfg: Config):
    from .proxy import Proxy, _json_body
    from .rateguard import method_kind

    from fastapi import APIRouter
    from fastapi.responses import JSONResponse

    router = APIRouter()

    @router.post("/tgapi/bot{token}/{method}")
    async def tgapi(token: str, method: str, request: Request):
        mgr = request.app.state.mgr
        s = mgr.find_by_token(token)
        if not s:
            return JSONResponse(
                {"ok": False, "error_code": 404, "description": "unknown bot token"},
                status_code=404,
            )
        proxy = Proxy(cfg, mgr, request.app.state.client)
        return await proxy.call(s, method, await _json_body(request))

    @router.post("/bots/{alias}/{method}")
    async def by_alias(alias: str, method: str, request: Request):
        if request.headers.get("authorization") != f"Bearer {cfg.app_secret}":
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        mgr = request.app.state.mgr
        s = mgr.sessions.get(alias)
        if not s:
            return JSONResponse(
                {"ok": False, "error_code": 404, "description": "unknown alias"},
                status_code=404,
            )
        proxy = Proxy(cfg, mgr, request.app.state.client)
        return await proxy.call(s, method, await _json_body(request))

    @router.get("/file/bot{token}/{path:path}")
    async def file_proxy(token: str, path: str, request: Request):
        mgr = request.app.state.mgr
        if not mgr.find_by_token(token):
            return JSONResponse({"ok": False, "error_code": 404}, status_code=404)
        resp = await request.app.state.client.get(f"/file/bot{token}/{path}")
        return JSONResponse({"ok": False, "error_code": 503, "detail": "file passthrough disabled in factory shim"}) if False else _file_response(resp)

    def _file_response(resp):
        from fastapi import Response
        return Response(content=resp.content, status_code=resp.status_code,
                        media_type=resp.headers.get("content-type", "application/octet-stream"))

    return router


def build_admin_router_lazy(cfg: Config):
    """Admin router resolving mgr/store from request.state."""
    from fastapi import APIRouter
    from fastapi.responses import JSONResponse

    router = APIRouter()

    def _authed(request: Request) -> bool:
        return bool(cfg.admin_secret) and request.headers.get(
            "authorization") == f"Bearer {cfg.admin_secret}"

    @router.post("/admin/bots")
    async def register(body: RegisterBot, request: Request):
        if not _authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        alias = body.alias or f"bot-{__import__('secrets').token_hex(4)}"
        s = await request.app.state.mgr.attach(alias, body.token)
        return {"ok": True, "alias": alias, "state": s.state.value}

    @router.get("/admin/status")
    async def status(request: Request):
        if not _authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        return {"ok": True,
                "bots": [s.public_status() for s in request.app.state.mgr.sessions.values()]}

    @router.post("/admin/bots/{alias}/pause")
    async def pause(alias: str, request: Request):
        if not _authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        return {"ok": await request.app.state.mgr.pause(alias)}

    @router.post("/admin/bots/{alias}/resume")
    async def resume(alias: str, request: Request):
        if not _authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        return {"ok": await request.app.state.mgr.resume(alias)}

    @router.post("/admin/bots/{alias}/drain")
    async def drain(alias: str, request: Request):
        if not _authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        return {"ok": await request.app.state.mgr.drain(alias)}

    @router.delete("/admin/bots/{alias}")
    async def delete(alias: str, request: Request):
        if not _authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        return {"ok": await request.app.state.mgr.delete(alias)}

    @router.post("/admin/bots/{alias}/push")
    async def set_push(alias: str, body: SetPush, request: Request):
        if not _authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        s = request.app.state.mgr.sessions.get(alias)
        if not s:
            return JSONResponse({"ok": False, "error_code": 404}, status_code=404)
        s.push_url = body.url
        return {"ok": True, "push_url": s.push_url}

    return router
