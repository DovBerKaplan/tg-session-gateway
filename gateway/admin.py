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
