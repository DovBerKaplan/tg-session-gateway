"""TG Session Gateway — configuration.

Environment-driven (12-factor). Every knob the spec (docs/spec.md)
defines has an env counterpart; sane defaults come from the spec's V1.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


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
class StorageConfig:
    """DB durability + maintenance knobs (dbhealth).

    - durability: NORMAL survives kill -9 (process death) but not
      machine power loss; FULL fsyncs and survives both, slower.
    - maintenance runs hourly: integrity_check, WAL checkpoint, audit
      retention; backups (rotating, encrypted for session files) at
      most once per backup_interval_s.
    """

    durability: str = _env("GW_DURABILITY", "normal")  # normal | full
    maintenance_interval_s: float = _env_float("GW_MAINTENANCE_INTERVAL_S", 3600.0)
    backup_interval_s: float = _env_float("GW_BACKUP_INTERVAL_S", 86400.0)
    backup_keep: int = _env_int("GW_BACKUP_KEEP", 5)
    audit_retention_days: int = _env_int("GW_AUDIT_RETENTION_DAYS", 30)


DEFAULT_TYPING_TIMEOUT_TEXT = (
    "⚠️ Sorry, this bot is temporarily unavailable due to a problem "
    "on the provider side. Please try again later."
)


@dataclass
class TypingConfig:
    """Auto typing indicator (spec §10.4) — boot defaults from env.

    Runtime resolution is three-layered, per beat: per-bot override →
    runtime global (mutable via /admin/settings/typing) → these values.
    """

    enabled: bool = _env_bool("GW_TYPING_ENABLED", True)
    # private chats only by default — groups/channels get neither the
    # indicator nor the timeout notice
    private_only: bool = _env_bool("GW_TYPING_PRIVATE_ONLY", True)
    interval_s: float = _env_float("GW_TYPING_INTERVAL", 4.5)  # < Telegram's ~5s display
    timeout_s: float = _env_float("GW_TYPING_TIMEOUT", 30.0)
    timeout_text: str = _env("GW_TYPING_TIMEOUT_TEXT", DEFAULT_TYPING_TIMEOUT_TEXT)


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
    fernet_key: str | None = field(default_factory=lambda: os.getenv("GW_FERNET_KEY"))

    # polling
    poll_timeout: int = _env_int("GW_POLL_TIMEOUT", 25)  # upstream long-poll
    poll_backoff_max: float = _env_float("GW_POLL_BACKOFF_MAX", 30.0)

    # push mode (spec §10.1): gateway POSTs updates to the consumer
    push_health_interval: float = _env_float("GW_PUSH_HEALTH_INTERVAL", 60.0)

    rate: RateConfig = field(default_factory=RateConfig)
    policy: BotPolicy = field(default_factory=BotPolicy)
    typing: TypingConfig = field(default_factory=TypingConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)

    @property
    def backups_dir(self) -> str:
        return os.path.join(self.data_dir, "backups")

    def validate(self) -> None:
        missing = [
            n
            for n, v in (("GW_ADMIN_SECRET", self.admin_secret), ("GW_APP_SECRET", self.app_secret))
            if not v
        ]
        if missing:
            raise SystemExit(f"Missing required env: {', '.join(missing)}")
        if self.policy.on_limit not in ("queue", "reject"):
            raise SystemExit("GW_ON_LIMIT must be 'queue' or 'reject'")
        if self.storage.durability not in ("normal", "full"):
            raise SystemExit("GW_DURABILITY must be 'normal' or 'full'")
