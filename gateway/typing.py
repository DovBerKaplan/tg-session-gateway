"""Auto typing indicator — "typing..." while the consumer has not
answered yet (spec §10.4).

Trigger: any user action that awaits a bot reply — `message`,
`edited_message`, `callback_query` (button press). Private chats only
by default; groups and channels get neither the indicator nor the
timeout notice.

The loop re-sends sendChatAction every interval (Telegram displays it
~5s per call) straight upstream, WITHOUT consuming rate-guard tokens —
the indicator must never steal capacity from the consumer's real reply.
Each beat first READS the guard (`chat_send_state`): a hard-paused chat
(real 429 retry_after, paused bot egress) stops the session outright —
the bot cannot answer, so it must not pretend to type — while a merely
drained bucket skips one beat and resumes when it refills.

Stop signals: any consumer write to the chat, answerCallbackQuery for
the button press that started the session, `sendChatAction` with
`action=cancel` (full stop) or `action=cancel_timeout` (suppress the
timeout notice only — typing continues), bot pause/drain/delete/shutdown,
or the timeout itself.

Settings resolve per beat — per-bot override → runtime global → env
boot defaults (three-layer model) — so admin changes apply to running
sessions immediately.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

import httpx

from .config import Config

if TYPE_CHECKING:  # runtime import would be circular (sessions → typing)
    from .sessions import BotSession

log = logging.getLogger("gateway.typing")

_SETTING_FIELDS = ("enabled", "private_only", "interval_s", "timeout_s", "timeout_text")


@dataclass
class RuntimeTypingSettings:
    """The global (gateway-wide) layer — live-mutable via the admin API."""

    enabled: bool
    private_only: bool
    interval_s: float
    timeout_s: float
    timeout_text: str

    @classmethod
    def from_config(cls, cfg) -> RuntimeTypingSettings:
        return cls(
            enabled=cfg.enabled,
            private_only=cfg.private_only,
            interval_s=cfg.interval_s,
            timeout_s=cfg.timeout_s,
            timeout_text=cfg.timeout_text,
        )

    def snapshot(self) -> dict:
        return {
            "enabled": self.enabled,
            "private_only": self.private_only,
            "interval_s": self.interval_s,
            "timeout_s": self.timeout_s,
            "timeout_text": self.timeout_text,
        }


@dataclass
class _Active:
    deadline: float  # monotonic; inf after cancel_timeout
    notify: bool = True  # False once the consumer cancelled the timeout notice
    task: asyncio.Task | None = None


class TypingManager:
    """Owns every live typing loop. One instance per SessionManager."""

    def __init__(self, cfg: Config, client: httpx.AsyncClient):
        self.cfg = cfg
        self.client = client
        self.settings = RuntimeTypingSettings.from_config(cfg.typing)
        # alias -> chat_id -> _Active
        self._active: dict[str, dict[int, _Active]] = {}
        # callback_query_id -> (alias, chat_id): lets answerCallbackQuery
        # stop the session the button press started
        self._cb_to_chat: dict[str, tuple[str, int]] = {}

    # ── settings resolution (three layers) ───────────────────────────

    def effective(self, s: BotSession) -> dict:
        g = self.settings
        out = {}
        for name in _SETTING_FIELDS:
            v = getattr(s, f"typing_{name}", None)
            out[name] = g.snapshot()[name] if v is None else v
        return out

    def update_settings(self, patch) -> dict:
        """Apply non-None fields of `patch` to the runtime global layer."""
        st = self.settings
        for name in _SETTING_FIELDS:
            v = getattr(patch, name, None)
            if v is not None:
                setattr(st, name, v)
        return st.snapshot()

    def status_for(self, s: BotSession) -> dict:
        out = self.effective(s)
        out["overridden"] = [
            name for name in _SETTING_FIELDS if getattr(s, f"typing_{name}", None) is not None
        ]
        out["active_chats"] = len(self._active.get(s.alias, {}))
        return out

    def active_count(self, alias: str) -> int:
        return len(self._active.get(alias, {}))

    # ── trigger: inbound user actions ────────────────────────────────

    def on_updates(self, s: BotSession, updates: list) -> None:
        eff = self.effective(s)
        if not eff["enabled"]:
            return
        for u in updates or []:
            chat_id = self._chat_of(u)
            if chat_id is None:
                continue
            if eff["private_only"] and chat_id <= 0:
                continue
            cbq = u.get("callback_query")
            if isinstance(cbq, dict) and cbq.get("id") is not None:
                self._cb_to_chat[str(cbq["id"])] = (s.alias, chat_id)
            # never open a session on a hard-blocked chat — the bot
            # cannot answer, so it must not pretend to type
            if s.guard.chat_send_state(chat_id) == "paused":
                continue
            self._start(s, chat_id, eff["timeout_s"])

    @staticmethod
    def _chat_of(update: dict) -> int | None:
        msg = update.get("message") or update.get("edited_message")
        if msg is None:
            cbq = update.get("callback_query")
            if isinstance(cbq, dict):
                msg = cbq.get("message")
        chat = msg.get("chat") if isinstance(msg, dict) else None
        if not isinstance(chat, dict):
            return None
        cid = chat.get("id")
        # bool is an int subclass — exclude it like the proxy's _chat_id
        if isinstance(cid, bool) or not isinstance(cid, int):
            return None
        return cid

    def _start(self, s: BotSession, chat_id: int, timeout_s: float) -> None:
        per_bot = self._active.setdefault(s.alias, {})
        rec = per_bot.get(chat_id)
        if rec is not None and rec.task is not None and not rec.task.done():
            # A new user action means new work: push the deadline out —
            # but never re-arm a timeout the consumer cancelled.
            if rec.notify:
                rec.deadline = time.monotonic() + timeout_s
            return
        rec = _Active(deadline=time.monotonic() + timeout_s)
        rec.task = asyncio.create_task(
            self._loop(s, chat_id, rec), name=f"typing-{s.alias}-{chat_id}"
        )
        per_bot[chat_id] = rec

    # ── the loop ─────────────────────────────────────────────────────

    async def _loop(self, s: BotSession, chat_id: int, rec: _Active) -> None:
        try:
            while True:
                eff = self.effective(s)  # live: admin changes apply now
                if not eff["enabled"]:
                    return
                if eff["private_only"] and chat_id <= 0:
                    return  # excluded mid-flight by a settings change
                state = s.guard.chat_send_state(chat_id)
                if state == "paused":
                    log.info(
                        "[%s] typing stopped for chat %d: chat is rate-paused",
                        s.alias,
                        chat_id,
                    )
                    return
                now = time.monotonic()
                if now >= rec.deadline:
                    await self._on_timeout(s, chat_id, rec, eff)
                    return
                if state == "ok":
                    await self._beat(s, chat_id)
                # state == "empty": skip this beat; typing resumes when
                # the bucket refills — the queued reply goes out then too
                await asyncio.sleep(eff["interval_s"])
        except asyncio.CancelledError:
            raise
        except Exception as e:  # a broken loop must never leak the session
            log.warning("[%s] typing loop for chat %d crashed: %s", s.alias, chat_id, e)
        finally:
            self._cleanup(s.alias, chat_id)

    async def _beat(self, s: BotSession, chat_id: int) -> None:
        try:
            await self.client.post(
                f"/bot{s.token}/sendChatAction",
                json={"chat_id": chat_id, "action": "typing"},
            )
        except httpx.HTTPError as e:
            log.warning("[%s] typing beat failed for chat %d: %s", s.alias, chat_id, e)

    async def _on_timeout(self, s: BotSession, chat_id: int, rec: _Active, eff: dict) -> None:
        # The notice goes to PRIVATE chats only — groups stay silent.
        if chat_id <= 0 or not rec.notify:
            return
        try:
            await self.client.post(
                f"/bot{s.token}/sendMessage",
                json={"chat_id": chat_id, "text": eff["timeout_text"]},
            )
        except httpx.HTTPError as e:
            log.warning("[%s] timeout notice failed for chat %d: %s", s.alias, chat_id, e)

    # ── stop / control ───────────────────────────────────────────────

    def stop(self, s: BotSession, chat_id: int | None) -> None:
        if chat_id is None:
            return
        rec = self._active.get(s.alias, {}).get(chat_id)
        if rec is not None and rec.task is not None and not rec.task.done():
            rec.task.cancel()

    def stop_all(self, s: BotSession) -> None:
        for rec in list(self._active.get(s.alias, {}).values()):
            if rec.task is not None and not rec.task.done():
                rec.task.cancel()
        self._active.pop(s.alias, None)
        self._purge_callbacks(s.alias)

    def stop_by_callback_id(self, s: BotSession, callback_query_id) -> None:
        """The consumer answered the button press — that IS the reply."""
        target = self._cb_to_chat.pop(str(callback_query_id), None)
        if target is not None and target[0] == s.alias:
            self.stop(s, target[1])

    def cancel_timeout(self, s: BotSession, chat_id: int | None) -> bool:
        """action=cancel_timeout: suppress the timeout notice for this
        chat's session; typing continues until a stop signal."""
        if chat_id is None:
            return False
        rec = self._active.get(s.alias, {}).get(chat_id)
        if rec is None:
            return False
        rec.notify = False
        rec.deadline = float("inf")
        return True

    # ── bookkeeping ──────────────────────────────────────────────────

    def _cleanup(self, alias: str, chat_id: int) -> None:
        per_bot = self._active.get(alias)
        if per_bot is not None:
            per_bot.pop(chat_id, None)
            if not per_bot:
                self._active.pop(alias, None)
        stale = [k for k, v in self._cb_to_chat.items() if v == (alias, chat_id)]
        for k in stale:
            self._cb_to_chat.pop(k, None)

    def _purge_callbacks(self, alias: str) -> None:
        stale = [k for k, v in self._cb_to_chat.items() if v[0] == alias]
        for k in stale:
            self._cb_to_chat.pop(k, None)
