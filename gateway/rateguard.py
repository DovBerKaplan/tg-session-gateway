"""Rate Guard — the single source of truth for outbound pacing (spec §8).

Buckets (official FAQ numbers):
- private chat (chat_id > 0): 1 msg/s, burst 3
- group/supergroup/channel (chat_id < 0): 20 msgs / 60 s, burst 2
- whole-bot global: 30 msg/s
- read-only methods: soft global, not charged against user buckets
- answerCallbackQuery / answerInlineQuery: fast path (no broadcast bucket)

429 from Telegram is the highest authority: retry_after pauses the
offending chat bucket (chat-associated 429) or the whole bot egress.
"""

from __future__ import annotations


import time
from dataclasses import dataclass, field
from typing import Optional

from .config import RateConfig

# Methods that must never starve (spec §8.2 §3) and methods that are reads.
FAST_PATH = {"answercallbackquery", "answerinlinequery", "answershippingquery",
             "answerprecheckoutquery", "setmessageeffect"}
READ_METHODS = {
    "getme", "getchat", "getchatmember", "getchatadministrators", "getuser",
    "getmycommands", "getmyname", "getmydescription", "getmyshortdescription",
    "getwebhookinfo", "getchatmenuButton", "getmydefaultadministratorrights",
    "getfile", "getchatmembercount", "getforumtopiciconstickers", "getstickerset",
}


def method_kind(method: str) -> str:
    m = method.lower()
    if m in FAST_PATH:
        return "fast"
    if m in READ_METHODS or m.startswith("get"):
        return "read"
    return "write"


@dataclass
class Bucket:
    rate: float          # tokens per second
    capacity: int        # burst size
    tokens: float = -1.0  # -1 → start full (burst available immediately)
    updated: float = field(default_factory=time.monotonic)
    paused_until: float = 0.0

    def __post_init__(self):
        if self.tokens < 0:
            self.tokens = float(self.capacity)

    def refill(self, now: float) -> None:
        if self.paused_until and now >= self.paused_until:
            self.paused_until = 0.0
            # retry_after already waited — one send may go immediately
            self.tokens = max(self.tokens, 1.0)
        elapsed = now - self.updated
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.updated = now

    def try_take(self, now: float, n: int = 1) -> bool:
        self.refill(now)
        if self.paused_until > now:
            return False
        if self.tokens >= n:
            self.tokens -= n
            return True
        return False

    def next_available(self, now: float) -> float:
        self.refill(now)
        if self.paused_until > now:
            return self.paused_until - now
        deficit = 1.0 - self.tokens
        return max(0.0, deficit / self.rate) if deficit > 0 else 0.0

    def pause(self, seconds: float, now: float) -> None:
        self.paused_until = max(self.paused_until, now + seconds)
        self.tokens = 0.0


class BotRateGuard:
    """All pacing state for one bot token. Single asyncio loop assumed."""

    def __init__(self, cfg: RateConfig, on_limit: str = "queue"):
        self.cfg = cfg
        self.on_limit = on_limit
        self.private: dict[int, Bucket] = {}
        self.groups: dict[int, Bucket] = {}
        self.global_writes = Bucket(cfg.global_rate, cfg.global_burst)
        self.paid = Bucket(cfg.paid_rate, int(cfg.paid_rate))
        self.paid_enabled: bool = False  # double opt-in (spec §8.2 §8)
        self.reads = Bucket(cfg.read_rate, max(10, int(cfg.read_rate)))
        self.egress_paused_until: float = 0.0

    # ── bucket selection ─────────────────────────────────────────────

    def _chat_bucket(self, chat_id: int) -> Bucket:
        # spec §8.2 §4: negative chat_id is never private
        if chat_id >= 0:
            table, rate, burst = self.private, self.cfg.private_rate, self.cfg.private_burst
        else:
            table, rate, burst = self.groups, self.cfg.group_rate, self.cfg.group_burst
        b = table.get(chat_id)
        if b is None:
            b = table[chat_id] = Bucket(rate, burst, tokens=float(burst))
        return b

    # set per-request by the proxy when allow_paid_broadcast=true is
    # present AND the double opt-in allows it
    _paid_lane: bool = False

    def enter_paid_lane(self, allowed: bool) -> None:
        self._paid_lane = allowed and self.paid_enabled

    def exit_paid_lane(self) -> None:
        self._paid_lane = False

    # ── admission ────────────────────────────────────────────────────

    def try_acquire(self, method: str, chat_id: Optional[int]) -> bool:
        """Non-blocking attempt; True = go. Returns False when throttled.

        A blocked chat attempt must NOT charge the global bucket (peek
        first, charge second — single-threaded asyncio makes this atomic)."""
        now = time.monotonic()
        if now < self.egress_paused_until:
            return False
        kind = method_kind(method)
        if kind == "fast":
            return True
        if kind == "read":
            return self.reads.try_take(now)
        # paid lane: only when the bot opted in AND the request carries the
        # paid flag. It replaces the GLOBAL BROADCAST ceiling only — the
        # per-chat FAQ limits (1/s private, 20/min group) still apply.
        chat_b = self._chat_bucket(chat_id) if chat_id is not None else None
        if chat_b is not None:
            chat_b.refill(now)
            if chat_b.paused_until > now or chat_b.tokens < 1:
                return False  # chat-level block binds even on the paid lane
        if self._paid_lane:
            if not self.paid.try_take(now):
                return False
            if chat_b is not None:
                chat_b.tokens -= 1
            return True
        if chat_b is not None:
            chat_b.refill(now)
            if chat_b.paused_until > now or chat_b.tokens < 1:
                return False  # blocked at chat level — global untouched
        if not self.global_writes.try_take(now):
            return False
        if chat_b is not None:
            chat_b.refill(now)
            if chat_b.paused_until > now or chat_b.tokens < 1:
                self.global_writes.tokens += 1  # refund — race-safe in 1 thread
                return False
            chat_b.tokens -= 1
        return True

    def wait_time(self, method: str, chat_id: Optional[int]) -> float:
        """Seconds until the request may go (for queue policy / retry_after)."""
        now = time.monotonic()
        kind = method_kind(method)
        wait = max(0.0, self.egress_paused_until - now)
        if kind == "fast":
            return wait
        if kind == "read":
            return max(wait, self.reads.next_available(now))
        wait = max(wait, self.global_writes.next_available(now))
        if chat_id is not None:
            wait = max(wait, self._chat_bucket(chat_id).next_available(now))
        return wait

    # ── learning from real 429s (spec §8.2 §5–6) ─────────────────────

    def report_429(self, chat_id: Optional[int], retry_after: float) -> None:
        now = time.monotonic()
        if chat_id is not None:
            self._chat_bucket(chat_id).pause(retry_after, now)
        else:
            self.egress_paused_until = max(self.egress_paused_until, now + retry_after)
            self.global_writes.pause(retry_after, now)

    def snapshot(self) -> dict:
        now = time.monotonic()
        empty = {
            "private": sum(1 for b in self.private.values() if b.next_available(now) > 0),
            "groups": sum(1 for b in self.groups.values() if b.next_available(now) > 0),
            "egress_paused_for_s": round(max(0.0, self.egress_paused_until - now), 1),
        }
        return empty
