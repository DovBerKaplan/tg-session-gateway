"""Egress proxy — Bot-API-compatible passthrough with Rate Guard (spec §6.3).

Routes:
    POST /tgapi/bot<token>/<method>     (drop-in base URL replacement)
    POST /bots/<alias>/<method>         (Bearer app-secret, no token in URL)
    GET  /file/bot<token>/<path>        (transparent file proxy)

Write methods pass the guard; getUpdates is served from the internal
queue (NEVER forwarded); setWebhook/deleteWebhook are gateway-owned.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

import httpx
from fastapi import APIRouter, Response
from fastapi.responses import JSONResponse

from .config import Config
from .rateguard import method_kind
from .sessions import BotSession, SessionManager

log = logging.getLogger("gateway.proxy")

router = APIRouter()

GATEWAY_OWNED = {"getupdates", "setwebhook", "deletewebhook", "logout", "close"}


class Proxy:
    def __init__(self, cfg: Config, mgr: SessionManager, client: httpx.AsyncClient):
        self.cfg = cfg
        self.mgr = mgr
        self.client = client

    async def call(
        self,
        s: BotSession,
        method: str,
        body,
        content_type: str = "application/json",
        idem_key: str | None = None,
    ) -> Response:
        m = method.lower()
        raw = None if isinstance(body, dict) else body  # multipart/form passthrough
        if raw is not None:
            return await self._forward_raw(s, method, raw, content_type)
        body = await self._resolve_username_chat(s, body)

        if m in GATEWAY_OWNED:
            if m == "getupdates":
                return await self._internal_get_updates(s, body)
            if m == "deletewebhook":
                # Consumers call this on boot (aiogram start_polling does);
                # the gateway already deleted the webhook when it attached
                # the token. Answer success so drop-in consumers don't die.
                return JSONResponse({"ok": True, "result": True})
            return JSONResponse(
                {
                    "ok": False,
                    "error_code": 403,
                    "description": "Conflict: gateway owns this method (spec §4.6)",
                },
                status_code=403,
            )

        chat_id = self._chat_id(body)
        kind = method_kind(m)

        # ── spec E2: Idempotency-Key header (writes only) ─────────────
        # Atomic claim; a FAILED send releases the key — only a
        # completed send suppresses replays.
        idem_active = bool(idem_key) and kind == "write"
        if idem_active:
            verdict = await self.mgr.store.idempotency_claim(s.alias, idem_key or "")
            if verdict == "replay":
                return JSONResponse(
                    {
                        "ok": True,
                        "result": None,
                        "idempotent_replay": True,
                        "description": "duplicate send suppressed (Idempotency-Key already seen)",
                    }
                )

        # ── auto-typing bookkeeping (spec §10.4) ──────────────────────
        # The consumer's reply — or an explicit control — ends the
        # indicator before anything goes upstream.
        if m == "sendchataction":
            action = str(body.get("action", "")).lower()
            if action == "cancel":
                self.mgr.typing.stop(s, chat_id)
                return JSONResponse({"ok": True, "result": True})
            if action == "cancel_timeout":
                self.mgr.typing.cancel_timeout(s, chat_id)
                return JSONResponse({"ok": True, "result": True})
            # any other action: the consumer drives the indicator itself
            self.mgr.typing.stop(s, chat_id)
        elif m == "answercallbackquery":
            # answering the button press IS the reply to that action
            self.mgr.typing.stop_by_callback_id(s, body.get("callback_query_id"))
        elif kind == "write" and chat_id is not None:
            self.mgr.typing.stop(s, chat_id)

        # paid lane (spec §8.2 §8): allow_paid_broadcast rides the 1000/s
        # bucket ONLY with the gateway-side double opt-in; otherwise 403 —
        # never silently over the free ceiling.
        paid_requested = bool(body.get("allow_paid_broadcast"))
        s.guard.enter_paid_lane(paid_requested)
        try:
            if paid_requested and not s.paid_broadcasts:
                if idem_active:
                    await self.mgr.store.idempotency_fail(s.alias, idem_key or "")
                return JSONResponse(
                    {
                        "ok": False,
                        "error_code": 403,
                        "description": "allow_paid_broadcast requires the gateway-side "
                        "paid_broadcasts opt-in for this bot",
                    },
                    status_code=403,
                )
            resp = await self._admit_and_forward(s, m, method, body, chat_id, kind)
            if idem_active:
                if resp.status_code < 400:
                    await self.mgr.store.idempotency_finish(s.alias, idem_key or "")
                else:
                    await self.mgr.store.idempotency_fail(s.alias, idem_key or "")
            return resp
        finally:
            s.guard.exit_paid_lane()

    async def _admit_and_forward(self, s, m, method, body, chat_id, kind) -> Response:
        # ── admission control (spec §8.2–8.3) ─────────────────────────
        waited = 0.0
        while not s.guard.try_acquire(m, chat_id):
            if s.effective_on_limit == "reject" or kind != "write":
                s.synthetic_429 += 1
                retry = max(1.0, min(s.guard.wait_time(m, chat_id), 60.0))
                return JSONResponse(
                    {
                        "ok": False,
                        "error_code": 429,
                        "description": "Too Many Requests: retry after %d" % int(retry),
                        "parameters": {"retry_after": int(retry)},
                    },
                    status_code=429,
                )
            wait = min(s.guard.wait_time(m, chat_id), 2.0)
            if waited + wait > self.cfg.policy.queue_wait_timeout:
                s.synthetic_429 += 1
                return JSONResponse(
                    {
                        "ok": False,
                        "error_code": 429,
                        "description": "Queue wait exceeded GW_QUEUE_WAIT_TIMEOUT",
                        "parameters": {"retry_after": 1},
                    },
                    status_code=429,
                )
            await asyncio.sleep(wait)
            waited += wait

        # ── forward upstream ──────────────────────────────────────────
        try:
            resp = await self.client.post(f"/bot{s.token}/{method}", json=body)
        except httpx.HTTPError as e:
            return JSONResponse(
                {"ok": False, "error_code": 502, "description": f"upstream: {e}"},
                status_code=502,
            )
        if kind == "write":
            s.note_send()

        # learn from real 429s (highest authority, spec §8.2 §5–6)
        if resp.status_code == 429:
            s.real_429 += 1
            try:
                retry = float(resp.json().get("parameters", {}).get("retry_after", 5))
            except Exception:
                retry = 5.0
            s.guard.report_429(chat_id, retry)
            self.mgr.record_backoff(s, chat_id, retry)  # durable across restarts
            s.last_429 = {
                "method": method,
                "chat_id": chat_id,
                "retry_after": retry,
                "at": time.time(),
            }
        return Response(
            content=resp.content, status_code=resp.status_code, media_type="application/json"
        )

    async def _forward_raw(self, s, method: str, raw: bytes, content_type: str) -> Response:
        """Multipart/form-data (sendPhoto, documents...) — Rate Guard uses
        the chat_id form field when present; body passes through verbatim."""
        chat_id = None
        try:
            import re as _re

            m = _re.search(rb'name="chat_id"\r?\n\r?\n(-?\d+)\r?\n', raw)
            if m:
                chat_id = int(m.group(1))
        except Exception:
            pass
        # multipart write (sendPhoto, documents...) is the reply —
        # stop the auto-typing loop for that chat first
        self.mgr.typing.stop(s, chat_id)
        if not s.guard.try_acquire(method, chat_id):
            s.synthetic_429 += 1
            retry = max(1, min(s.guard.wait_time(method, chat_id), 60))
            return JSONResponse(
                {
                    "ok": False,
                    "error_code": 429,
                    "description": f"Too Many Requests: retry after {int(retry)}",
                    "parameters": {"retry_after": int(retry)},
                },
                status_code=429,
            )
        try:
            resp = await self.client.post(
                f"/bot{s.token}/{method}",
                content=raw,
                headers={"Content-Type": content_type},
            )
        except httpx.HTTPError as e:
            return JSONResponse(
                {"ok": False, "error_code": 502, "description": f"upstream: {e}"}, status_code=502
            )
        s.note_send()
        if resp.status_code == 429:
            s.real_429 += 1
            try:
                retry = float(resp.json().get("parameters", {}).get("retry_after", 5))
            except Exception:
                retry = 5.0
            s.guard.report_429(chat_id, retry)
            self.mgr.record_backoff(s, chat_id, retry)  # durable across restarts
            s.last_429 = {
                "method": method,
                "chat_id": chat_id,
                "retry_after": retry,
                "at": time.time(),
            }
        return Response(
            content=resp.content, status_code=resp.status_code, media_type="application/json"
        )

    async def _internal_get_updates(self, s: BotSession, body: dict) -> Response:
        """Serve from the internal queue — Bot API long-poll semantics,
        with our internal ids carried on each update as _gw_internal_id."""
        timeout = min(float(body.get("timeout", 0) or 0), 50.0)
        limit = int(body.get("limit", 100) or 100)
        offset = body.get("offset")

        # consumers pass OUR internal ids back as offset (transparent):
        # they echo the _gw_internal_id they last saw, +1 style
        updates = await s.queue.pull(offset, timeout or 0.01, limit)
        return JSONResponse({"ok": True, "result": updates})

    async def _resolve_username_chat(self, s: BotSession, body: dict) -> dict:
        """@username targets resolve to numeric ids once (getChat cache,
        spec §8.2 §4) so they land in the right per-chat bucket."""
        cid = body.get("chat_id")
        if not isinstance(cid, str) or not cid.startswith("@"):
            return body
        cached = s._chat_name_cache
        if cid in cached:
            body = dict(body)
            body["chat_id"] = cached[cid]
            return body
        try:
            resp = await self.client.post(f"/bot{s.token}/getChat", json={"chat_id": cid})
            data = resp.json()
            if data.get("ok"):
                numeric = data["result"].get("id")
                if isinstance(numeric, int):
                    cached[cid] = numeric
                    body = dict(body)
                    body["chat_id"] = numeric
        except Exception:
            pass  # resolution failed — request proceeds, global bucket only
        return body

    @staticmethod
    def _chat_id(body: dict) -> int | None:
        cid = body.get("chat_id") or body.get("chat_username") or body.get("chat")
        if cid is None:
            return None
        if isinstance(cid, bool):
            return None
        if isinstance(cid, int):
            return cid
        if isinstance(cid, str) and cid.lstrip("-").isdigit():
            return int(cid)
        if isinstance(cid, str) and cid.startswith("@"):
            return None  # username target — global bucket only
        return None


async def _json_body(request):
    try:
        raw = await request.body()
        if not raw:
            return {}
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}
