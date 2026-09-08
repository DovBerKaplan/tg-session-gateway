"""TG Session Gateway — configuration.

Environment-driven (12-factor). Every knob the spec (docs/spec.he.md)
defines has an env counterpart; sane defaults come from the spec's V1.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def _env_bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).lower() in ("1", "true", "yes")


@dataclass
class RateConfig:
    """Token buckets per the official Bots FAQ (spec §8.1).

    - private chat: 1 msg/s, short burst allowed (burst 3)
    - group:        20 msgs / 60 s (burst 2)
    - global:       ~30 msgs/s for the whole bot
    - reads (getMe/getChat/...) live in a soft global bucket
    - answerCallbackQuery / answerInlineQuery: fast path, no broadcast bucket
    """

    private_rate: float = _env_float("GW_PRIVATE_RATE", 1.0)
    private_burst: int = _env_int("GW_PRIVATE_BURST", 3)
    group_rate: float = _env_float("GW_GROUP_RATE", 20.0 / 60.0)
    group_burst: int = _env_int("GW_GROUP_BURST", 2)
    global_rate: float = _env_float("GW_GLOBAL_RATE", 30.0)
    global_burst: int = _env_int("GW_GLOBAL_BURST", 30)
    read_rate: float = _env_float("GW_READ_RATE", 20.0)
    paid_rate: float = _env_float("GW_PAID_RATE", 1000.0)


@dataclass
class BotPolicy:
    """Per-bot behavior when a bucket is empty (spec §8.3)."""

    on_limit: str = _env("GW_ON_LIMIT", "queue")  # queue | reject
    queue_wait_timeout: float = _env_float("GW_QUEUE_WAIT_TIMEOUT", 25.0)
    update_queue_max: int = _env_int("GW_UPDATE_QUEUE_MAX", 5000)
    update_ttl_s: float = _env_float("GW_UPDATE_TTL_S", 3600.0)
    paid_broadcasts_enabled: bool = _env_bool("GW_PAID_BROADCASTS", False)


@dataclass
class Config:
    # network / lifecycle
    host: str = _env("GW_HOST", "0.0.0.0")
    port: int = _env_int("GW_PORT", 8080)
    upstream: str = _env("GW_UPSTREAM", "https://api.telegram.org")

    # secrets
    admin_secret: str = _env("GW_ADMIN_SECRET", "")
    app_secret: str = _env("GW_APP_SECRET", "")  # Bearer for /bots/<alias>

    # persistence (offsets, registered bots, encrypted tokens)
    data_dir: str = _env("GW_DATA_DIR", "/data")
    fernet_key: Optional[str] = field(default_factory=lambda: os.getenv("GW_FERNET_KEY"))

    # polling
    poll_timeout: int = _env_int("GW_POLL_TIMEOUT", 25)  # upstream long-poll
    poll_backoff_max: float = _env_float("GW_POLL_BACKOFF_MAX", 30.0)

    # push mode (spec §10.1): gateway POSTs updates to the consumer
    push_health_interval: float = _env_float("GW_PUSH_HEALTH_INTERVAL", 60.0)

    rate: RateConfig = field(default_factory=RateConfig)
    policy: BotPolicy = field(default_factory=BotPolicy)

    def validate(self) -> None:
        missing = [n for n, v in (("GW_ADMIN_SECRET", self.admin_secret), ("GW_APP_SECRET", self.app_secret)) if not v]
        if missing:
            raise SystemExit(f"Missing required env: {', '.join(missing)}")
        if self.policy.on_limit not in ("queue", "reject"):
            raise SystemExit("GW_ON_LIMIT must be 'queue' or 'reject'")
