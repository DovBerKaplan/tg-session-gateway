"""Configuration for the MTProto Session Gateway."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from gateway.config import RateConfig

__all__ = ["Config", "MTProtoRateConfig", "PolicyConfig", "RateConfig"]


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def _env_bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).lower() in ("1", "true", "yes")


@dataclass
class MTProtoRateConfig:
    """MTProto-specific rate handling (beyond FAQ Bot API limits)."""

    # FLOOD_WAIT backoff: exponential multiplier per repeated offence
    flood_backoff_base: float = _env_float("GW_FLOOD_BACKOFF_BASE", 1.5)
    # SLOWMODE_WAIT: respect the group's own slow mode
    slowmode_respect: bool = _env_bool("GW_SLOWMODE_RESPECT", True)
    # PEER_FLOOD: how long to back off from a peer entirely
    peer_flood_cooldown: float = _env_float("GW_PEER_FLOOD_COOLDOWN", 300.0)
    # Silent methods (resolve, get_participants) — soft bucket
    silent_rate: float = _env_float("GW_SILENT_RATE", 5.0)


@dataclass
class PolicyConfig:
    """Per-bot behavior policies."""

    on_limit: str = _env("GW_ON_LIMIT", "queue")  # queue | reject
    queue_wait_timeout: float = _env_float("GW_QUEUE_WAIT_TIMEOUT", 25.0)
    update_queue_max: int = _env_int("GW_UPDATE_QUEUE_MAX", 5000)
    update_ttl_s: float = _env_float("GW_UPDATE_TTL_S", 3600.0)
    paid_broadcasts_enabled: bool = _env_bool("GW_PAID_BROADCASTS", False)


@dataclass
class Config:
    # network
    host: str = _env("GW_HOST", "0.0.0.0")
    port: int = _env_int("GW_PORT", 8080)
    ws_port: int = _env_int("GW_WS_PORT", 8081)

    # secrets
    admin_secret: str = _env("GW_ADMIN_SECRET", "")
    app_secret: str = _env("GW_APP_SECRET", "")

    # persistence
    data_dir: str = _env("GW_DATA_DIR", "/data")
    fernet_key: str | None = field(default_factory=lambda: os.getenv("GW_FERNET_KEY"))

    # Pyrogram
    api_id: int = _env_int("GW_API_ID", 0)
    api_hash: str = _env("GW_API_HASH", "")

    # session files directory (inside data_dir)
    session_dir: str = ""  # computed in __post_init__

    # ADR-0001: lock backend — file (default, single-host, zero-dep) or
    # redis (multi-host lease with TTL + fencing token)
    lock_backend: str = _env("GW_LOCK_BACKEND", "file")
    redis_url: str = _env("GW_REDIS_URL", "")

    # consumer management
    max_sessions: int = _env_int("GW_MAX_SESSIONS", 50)
    consumer_timeout: float = _env_float("GW_CONSUMER_TIMEOUT", 120.0)

    # Prometheus
    metrics_enabled: bool = _env_bool("GW_METRICS", True)

    rate: RateConfig = field(default_factory=RateConfig)
    mtproto_rate: MTProtoRateConfig = field(default_factory=MTProtoRateConfig)
    policy: PolicyConfig = field(default_factory=PolicyConfig)

    def __post_init__(self):
        self.session_dir = os.path.join(self.data_dir, "sessions")

    def validate(self) -> None:
        missing = [
            n
            for n, v in (
                ("GW_ADMIN_SECRET", self.admin_secret),
                ("GW_APP_SECRET", self.app_secret),
                ("GW_API_ID", self.api_id),
                ("GW_API_HASH", self.api_hash),
            )
            if not v
        ]
        if missing:
            raise SystemExit(f"Missing required env: {', '.join(missing)}")
        if self.policy.on_limit not in ("queue", "reject"):
            raise SystemExit("GW_ON_LIMIT must be 'queue' or 'reject'")
