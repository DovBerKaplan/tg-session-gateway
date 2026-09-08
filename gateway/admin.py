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
