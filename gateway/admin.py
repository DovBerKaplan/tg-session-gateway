"""Pydantic models for the admin API (routes live in app.py)."""

from pydantic import BaseModel


class RegisterBot(BaseModel):
    alias: str | None = None
    token: str
    on_limit: str | None = None  # queue | reject
    paid_broadcasts: bool = False


class SetPush(BaseModel):
    url: str | None = None


class SetPaid(BaseModel):
    enabled: bool


class SetTyping(BaseModel):
    """Auto-typing settings update (spec §10.4).

    Used by both the global endpoint (/admin/settings/typing — applies
    live to every running session) and the per-bot endpoint
    (/admin/bots/{alias}/typing — fields left null inherit the global).
    """

    enabled: bool | None = None
    private_only: bool | None = None
    interval_s: float | None = None
    timeout_s: float | None = None
    timeout_text: str | None = None
