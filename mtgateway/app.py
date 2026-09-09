"""FastAPI application for the MTProto Session Gateway.

Endpoints:
  POST /v1/sessions/{alias}/{method}   — proxy Pyrogram method calls
  WS   /v1/sessions/{alias}/updates    — push updates to consumers
  POST /v1/sessions/{alias}/getUpdates — pull updates (Bot API compat)
  GET  /v1/sessions/{alias}/health     — session health
  GET  /v1/sessions/{alias}/files/{file_id} — media download
  POST /v1/sessions/{alias}/upload     — media upload

Admin:
  POST /v1/admin/sessions             — register a bot
  GET  /v1/admin/status               — all sessions status
  POST /v1/admin/sessions/{a}/pause   — pause sending
  POST /v1/admin/sessions/{a}/resume  — resume
  POST /v1/admin/sessions/{a}/drain   — drain before shutdown
  DELETE /v1/admin/sessions/{a}       — unregister

Health:
  GET /healthz — process alive
  GET /readyz  — storage available
  GET /metrics — Prometheus (if enabled)
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from .config import Config
from .rateguard import parse_flood_wait
from .sessions import ConsumerHandle, MTSession, MTSessionManager, SessionState
from .store import MTStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("mtgateway.app")


class RegisterSession(BaseModel):
    alias: str
    token: str


class SetPaid(BaseModel):
    enabled: bool


def create_app(cfg: Config | None = None) -> FastAPI:
    cfg = cfg or Config()
    cfg.validate()
    store = MTStore(cfg.data_dir, cfg.fernet_key)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await store.connect()
        mgr = MTSessionManager(cfg, store)
        app.state.store = store
        app.state.mgr = mgr
        app.state.cfg = cfg
        restored = await mgr.restore_all()
        log.info("MTProto gateway up — restored %d session(s)", restored)
        yield
        await mgr.shutdown()
        await store.close()

    app = FastAPI(title="MTProto Session Gateway", version="0.1.0", lifespan=lifespan)

    def _app_authed(request: Request | WebSocket) -> bool:
        return request.headers.get("authorization") == f"Bearer {cfg.app_secret}"

    def _admin_authed(request: Request) -> bool:
        return request.headers.get("authorization") == f"Bearer {cfg.admin_secret}"

    def _get_session(request: Request, alias: str) -> MTSession | None:
        return request.app.state.mgr.get(alias)

    # ── Pyrogram method proxy ────────────────────────────────────────

    @app.post("/v1/sessions/{alias}/{method}")
    async def proxy_method(alias: str, method: str, request: Request):
        if not _app_authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        s = _get_session(request, alias)
        if not s:
            return JSONResponse(
                {"ok": False, "error_code": 404, "description": f"unknown session: {alias}"},
                status_code=404,
            )
        # This route SHADOWS the dedicated /getUpdates path (FastAPI matches
        # registration order) — delegate before the proxy treats it as a
        # Pyrogram method and 404s it.
        if method.lower() == "getupdates":
            return await pull_updates(alias, request)
        if not s.client or s.state != SessionState.live:
            return JSONResponse(
                {
                    "ok": False,
                    "error_code": 503,
                    "description": f"session state: {s.state.value}",
                    "retry_in": 3,
                },
                status_code=503,
            )

        body = {}
        try:
            raw = await request.body()
            if raw:
                body = json.loads(raw)
        except Exception:
            return JSONResponse(
                {"ok": False, "error_code": 400, "description": "invalid JSON body"},
                status_code=400,
            )

        chat_id = body.get("chat_id") or body.get("chat_username") or body.get("chat")
        if isinstance(chat_id, str) and chat_id.lstrip("-").isdigit():
            chat_id = int(chat_id)
        elif not isinstance(chat_id, int):
            chat_id = None

        # ── E2: Idempotency check (app-supplied key prevents duplicate sends)
        idem_key = body.pop("_idempotency_key", None)
        if idem_key and not s.check_idempotency(idem_key):
            return JSONResponse(
                {
                    "ok": True,
                    "result": None,
                    "idempotent_replay": True,
                    "description": "duplicate send suppressed (idempotency key already seen)",
                }
            )

        # ── E4: Check send-paused state
        if s.send_paused and method.lower() not in (
            "answer_callback_query",
            "answer_inline_query",
            "get_updates",
        ):
            return JSONResponse(
                {
                    "ok": False,
                    "error_code": 423,
                    "description": "sending is paused for this session (E4)",
                },
                status_code=423,
            )

        # ── Rate Guard admission ─────────────────────────────────────
        paid_requested = bool(body.get("allow_paid_broadcast"))
        s.guard.enter_paid_lane(paid_requested)
        try:
            if paid_requested and not s.guard.paid_enabled:
                return JSONResponse(
                    {
                        "ok": False,
                        "error_code": 403,
                        "description": "paid broadcast requires gateway opt-in",
                    },
                    status_code=403,
                )

            waited = 0.0
            while not s.guard.try_acquire(method, chat_id):
                if cfg.policy.on_limit == "reject" or waited > cfg.policy.queue_wait_timeout:
                    s.synthetic_429 += 1
                    retry = max(1.0, min(s.guard.wait_time(method, chat_id), 60.0))
                    return JSONResponse(
                        {
                            "ok": False,
                            "error_code": 429,
                            "description": f"Too Many Requests: retry after {int(retry)}",
                            "parameters": {"retry_after": int(retry)},
                        },
                        status_code=429,
                    )
                wait = min(s.guard.wait_time(method, chat_id), 2.0)
                await asyncio.sleep(wait)
                waited += wait

            # ── Forward to Pyrogram ──────────────────────────────────
            fn = getattr(s.client, method, None)
            if fn is None:
                return JSONResponse(
                    {
                        "ok": False,
                        "error_code": 404,
                        "description": f"unknown Pyrogram method: {method}",
                    },
                    status_code=404,
                )

            from .coerce import coerce_args

            result = await fn(**coerce_args(body))

            # Clear backoff on success (positive signal)
            if chat_id is not None:
                s.guard.clear_peer_backoff(chat_id)

            # E7: Track deploy-to-first-message SLO
            s.note_first_send()

            return JSONResponse({"ok": True, "result": _serialize(result)})
        except Exception as e:
            # Check for FLOOD_WAIT
            flood = parse_flood_wait(e)
            if flood:
                s.real_flood_wait += 1
                adaptive = s.guard.report_flood_wait(e, chat_id)
                s.last_flood_wait = {**flood, "chat_id": chat_id, "at": time.time()}
                retry = (
                    int(adaptive.get("adaptive_wait", flood["wait"])) if adaptive else flood["wait"]
                )
                return JSONResponse(
                    {
                        "ok": False,
                        "error_code": 429,
                        "description": f"FLOOD_WAIT: retry after {retry}",
                        "parameters": {"retry_after": retry},
                    },
                    status_code=429,
                )
            return JSONResponse(
                {"ok": False, "error_code": 500, "description": str(e)[:200]}, status_code=500
            )
        finally:
            s.guard.exit_paid_lane()

    @app.post("/v1/token/bot{token}/{method}")
    async def proxy_by_token(token: str, method: str, request: Request):
        """Token-routed surface — same contract as the alias route, for
        consumers (task pools) that only hold the bot token."""
        if not _app_authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        s = request.app.state.mgr.find_by_token(token)
        if not s:
            return JSONResponse(
                {"ok": False, "error_code": 404, "description": "unknown bot token"},
                status_code=404,
            )
        request.state.session_alias = s.alias
        # reuse the alias route logic by delegating with the resolved alias
        return await proxy_method(s.alias, method, request)

    # ── Pull updates (Bot API compat) ────────────────────────────────

    @app.post("/v1/sessions/{alias}/getUpdates")
    async def pull_updates(alias: str, request: Request):
        if not _app_authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        s = _get_session(request, alias)
        if not s:
            return JSONResponse({"ok": False, "error_code": 404}, status_code=404)

        body = {}
        try:
            raw = await request.body()
            if raw:
                body = json.loads(raw)
        except Exception:
            pass

        timeout = min(float(body.get("timeout", 0) or 0), 50.0)
        limit = int(body.get("limit", 100) or 100)

        if timeout == 0:
            # Non-blocking: return whatever is queued
            updates: list = []
            while not s._update_queue.empty() and len(updates) < limit:
                try:
                    updates.append(s._update_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            return JSONResponse({"ok": True, "result": updates})

        # Long-poll: wait for updates up to timeout
        deadline = time.monotonic() + timeout
        while True:
            updates = []
            while not s._update_queue.empty() and len(updates) < limit:
                try:
                    updates.append(s._update_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            if updates:
                return JSONResponse({"ok": True, "result": updates})
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return JSONResponse({"ok": True, "result": []})
            await asyncio.sleep(min(remaining, 0.5))

    # ── WebSocket push updates ────────────────────────────────────────

    @app.websocket("/v1/sessions/{alias}/updates")
    async def ws_updates(ws: WebSocket, alias: str):
        await ws.accept()
        mgr = ws.app.state.mgr
        s = mgr.get(alias)
        if not s:
            await ws.send_json({"ok": False, "error": "unknown session"})
            await ws.close(code=4404)
            return

        if not _app_authed(ws):
            await ws.send_json({"ok": False, "error": "unauthorized"})
            await ws.close(code=4401)
            return

        consumer_id = str(uuid.uuid4())[:8]

        async def ws_send(data: str):
            await ws.send_text(data)

        handle = ConsumerHandle(consumer_id=consumer_id, ws_send=ws_send)
        s.add_consumer(handle)

        try:
            await ws.send_json(
                {"ok": True, "consumer_id": consumer_id, "session": alias, "state": s.state.value}
            )
            while True:
                # Receive pings / commands from the consumer
                data = await ws.receive_text()
                handle.last_seen = time.time()
                try:
                    msg = json.loads(data)
                    if msg.get("type") == "ping":
                        await ws.send_json({"type": "pong"})
                except Exception:
                    pass
        except WebSocketDisconnect:
            pass
        except Exception as e:
            log.warning("[%s] WS error: %s", alias, e)
        finally:
            s.remove_consumer(consumer_id)

    # ── Session health ────────────────────────────────────────────────

    @app.get("/v1/sessions/{alias}/health")
    async def session_health(alias: str, request: Request):
        if not _app_authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        s = _get_session(request, alias)
        if not s:
            return JSONResponse({"ok": False, "error_code": 404}, status_code=404)
        return JSONResponse({"ok": True, **s.stats(), "rate": s.guard.snapshot()})

    # ── Media file proxy ──────────────────────────────────────────────

    @app.get("/v1/sessions/{alias}/files/{file_id}")
    async def download_file(alias: str, file_id: str, request: Request):
        if not _app_authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        s = _get_session(request, alias)
        if not s or not s.client:
            return JSONResponse({"ok": False, "error_code": 404}, status_code=404)
        try:
            media = await s.client.download_media(file_id, in_memory=True)
            if not isinstance(media, io.BytesIO):  # None or a file path, not in-memory bytes
                return JSONResponse(
                    {"ok": False, "error_code": 404, "description": "media not found"},
                    status_code=404,
                )
            return Response(content=media.getvalue(), media_type="application/octet-stream")
        except Exception as e:
            return JSONResponse(
                {"ok": False, "error_code": 500, "description": str(e)[:200]}, status_code=500
            )

    # ── Admin endpoints ──────────────────────────────────────────────

    @app.post("/v1/admin/sessions")
    async def admin_register(body: RegisterSession, request: Request):
        if not _admin_authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        s = await request.app.state.mgr.register(body.alias, body.token)
        await request.app.state.store.audit("register", body.alias, f"token=…{body.token[-4:]}")
        return {"ok": True, "alias": s.alias, "state": s.state.value}

    @app.get("/v1/admin/status")
    async def admin_status(request: Request):
        if not _admin_authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        return {"ok": True, "sessions": request.app.state.mgr.all_stats()}

    @app.post("/v1/admin/sessions/{alias}/pause")
    async def admin_pause(alias: str, request: Request):
        if not _admin_authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        ok = await request.app.state.mgr.pause(alias)
        if ok:
            await request.app.state.store.audit("pause", alias)
        return {"ok": ok}

    @app.post("/v1/admin/sessions/{alias}/resume")
    async def admin_resume(alias: str, request: Request):
        if not _admin_authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        ok = await request.app.state.mgr.resume(alias)
        if ok:
            await request.app.state.store.audit("resume", alias)
        return {"ok": ok}

    @app.post("/v1/admin/sessions/{alias}/drain")
    async def admin_drain(alias: str, request: Request):
        if not _admin_authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        ok = await request.app.state.mgr.drain(alias)
        if ok:
            await request.app.state.store.audit("drain", alias)
        return {"ok": ok}

    @app.delete("/v1/admin/sessions/{alias}")
    async def admin_delete(alias: str, request: Request):
        if not _admin_authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        ok = await request.app.state.mgr.unregister(alias)
        if ok:
            await request.app.state.store.audit("delete", alias)
        return {"ok": ok}

    @app.post("/v1/admin/sessions/{alias}/pause_sending")
    async def admin_pause_sending(alias: str, request: Request):
        """E4: Pause outbound sends; keep receiving updates."""
        if not _admin_authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        ok = await request.app.state.mgr.pause_sending(alias)
        if ok:
            await request.app.state.store.audit("pause_sending", alias)
        return {"ok": ok}

    @app.post("/v1/admin/sessions/{alias}/resume_sending")
    async def admin_resume_sending(alias: str, request: Request):
        if not _admin_authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        ok = await request.app.state.mgr.resume_sending(alias)
        if ok:
            await request.app.state.store.audit("resume_sending", alias)
        return {"ok": ok}

    @app.post("/v1/admin/sessions/{alias}/paid")
    async def admin_paid(alias: str, body: SetPaid, request: Request):
        if not _admin_authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        s = _get_session(request, alias)
        if not s:
            return JSONResponse({"ok": False, "error_code": 404}, status_code=404)
        s.guard.paid_enabled = body.enabled
        await request.app.state.store.audit("set_paid", alias, f"enabled={body.enabled}")
        return {"ok": True, "paid_broadcasts": s.guard.paid_enabled}

    @app.get("/v1/admin/audit")
    async def admin_audit_log(request: Request):
        if not _admin_authed(request):
            return JSONResponse({"ok": False, "error_code": 401}, status_code=401)
        return {"ok": True, "entries": await request.app.state.store.get_audit()}

    # ── Health endpoints ─────────────────────────────────────────────

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    @app.get("/readyz")
    async def readyz(request: Request):
        try:
            await request.app.state.store.list_sessions()
            return {"ok": True}
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=503)

    # ── Prometheus metrics ───────────────────────────────────────────

    @app.get("/metrics")
    async def metrics(request: Request):
        if not cfg.metrics_enabled:
            return Response(status_code=404)
        lines = []
        for s in request.app.state.mgr.sessions.values():
            labels = f'alias="{s.alias}"'
            lines.append(
                f"tg_gateway_session_state{{{labels}}} {1 if s.state == SessionState.live else 0}"
            )
            age = int(time.time() - s.connected_since) if s.connected_since else 0
            lines.append(f"tg_gateway_session_connection_age_seconds{{{labels}}} {age}")
            lines.append(f"tg_gateway_update_queue_depth{{{labels}}} {s._update_queue.qsize()}")
            lines.append(f"tg_gateway_real_flood_wait_total{{{labels}}} {s.real_flood_wait}")
            lines.append(f"tg_gateway_synthetic_429_total{{{labels}}} {s.synthetic_429}")
            lines.append(f"tg_gateway_consumers{{{labels}}} {len(s._consumers)}")
            slo = s.deploy_to_first_message_s
            if slo:
                lines.append(f"tg_gateway_deploy_to_first_message_seconds{{{labels}}} {slo:.2f}")
            lines.append(f"tg_gateway_send_paused{{{labels}}} {1 if s.send_paused else 0}")
        return Response(content="\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")

    return app


def _serialize(obj) -> Any:
    """Convert Pyrogram objects to JSON-serializable dicts."""
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    if isinstance(obj, list):
        return [_serialize(o) for o in obj]
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if hasattr(obj, "to_dict"):
        return _serialize(obj.to_dict())
    if hasattr(obj, "__dict__"):
        return {
            k: _serialize(v)
            for k, v in vars(obj).items()
            if not k.startswith("_") and not callable(v)
        }
    return str(obj)
