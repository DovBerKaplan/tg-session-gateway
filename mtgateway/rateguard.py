"""Rate Guard for MTProto — FAQ buckets + FLOOD_WAIT + adaptive backoff.

Two layers:
1. Preventive: FAQ Bot API buckets (1/s private, 20/min group, 30/s
   global) — same logic as the Bot API gateway's Rate Guard.
2. Reactive: real FLOOD_WAIT from Pyrogram exceptions — parse the wait
   time, apply per-peer adaptive backoff, respect SLOWMODE_WAIT.

The guard is per-session (per bot token), isolating bots from each other.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass

from gateway.rateguard import Bucket, RateConfig, method_kind

from .config import MTProtoRateConfig

# ── Pyrogram FLOOD_WAIT parsing ────────────────────────────────────

# Pyrogram raises FloodWait(seconds), FloodPremiumWait(seconds),
# SlowmodeWait(seconds), PeerFloodInvalid()
# The exception str() contains the wait time.
# Pyrogram exception format: FloodWait(42)
_FLOOD_WAIT_RE = re.compile(r"FloodWait\((\d+)\)", re.IGNORECASE)
_FLOOD_PREMIUM_RE = re.compile(r"FloodPremiumWait\((\d+)\)", re.IGNORECASE)
_SLOWMODE_WAIT_RE = re.compile(r"SlowmodeWait\((\d+)\)", re.IGNORECASE)
_PEER_FLOOD_RE = re.compile(r"PeerFloodInvalid", re.IGNORECASE)
# Telegram raw error format: [420 FLOOD_WAIT_X] - A wait of 42 seconds is required
_TG_FLOOD_WAIT_RE = re.compile(r"FLOOD_WAIT\S*.*?wait of (\d+) seconds", re.IGNORECASE | re.DOTALL)
_TG_SLOWMODE_RE = re.compile(r"SLOWMODE_WAIT\S*.*?wait of (\d+) seconds", re.IGNORECASE | re.DOTALL)


def parse_flood_wait(exc: Exception) -> dict | None:
    """Extract rate-limit info from a Pyrogram exception.

    Returns:
        {"type": "flood_wait", "wait": 42}
        {"type": "flood_premium", "wait": 10}
        {"type": "slowmode", "wait": 5}
        {"type": "peer_flood", "wait": 300}
        None (not a rate-limit exception)
    """
    s = str(exc)
    # Also check the exception class name
    cls_name = type(exc).__name__ if exc else ""

    m = _FLOOD_WAIT_RE.search(s) or _FLOOD_WAIT_RE.search(cls_name)
    if m:
        return {"type": "flood_wait", "wait": int(m.group(1))}
    m = _FLOOD_PREMIUM_RE.search(s) or _FLOOD_PREMIUM_RE.search(cls_name)
    if m:
        return {"type": "flood_premium", "wait": int(m.group(1))}
    m = _SLOWMODE_WAIT_RE.search(s) or _SLOWMODE_WAIT_RE.search(cls_name)
    if m:
        return {"type": "slowmode", "wait": int(m.group(1))}
    if _PEER_FLOOD_RE.search(s) or _PEER_FLOOD_RE.search(cls_name):
        return {"type": "peer_flood", "wait": 300}

    # Telegram raw error format: [420 FLOOD_WAIT_X] - A wait of 42 seconds
    m = _TG_FLOOD_WAIT_RE.search(s)
    if m:
        return {"type": "flood_wait", "wait": int(m.group(1))}
    m = _TG_SLOWMODE_RE.search(s)
    if m:
        return {"type": "slowmode", "wait": int(m.group(1))}

    # Pyrogram also embeds the class name in the exception's repr
    if "FloodWait" in cls_name:
        import re as _re

        nums = _re.findall(r"\d+", s)
        if nums:
            return {"type": "flood_wait", "wait": int(nums[-1])}
    if "SlowmodeWait" in cls_name:
        import re as _re

        nums = _re.findall(r"\d+", s)
        if nums:
            return {"type": "slowmode", "wait": int(nums[-1])}

    return None


# ── Per-peer adaptive backoff state ─────────────────────────────────


@dataclass
class PeerBackoff:
    """Exponential backoff per (peer_id, method) after real FLOOD_WAIT."""

    offences: int = 0
    last_wait: float = 0.0
    paused_until: float = 0.0


class MTRateGuard:
    """All pacing state for one MTProto session (bot token)."""

    def __init__(self, faq: RateConfig | None = None, mtproto: MTProtoRateConfig | None = None):
        self.faq = faq or RateConfig()
        self.mtproto = mtproto or MTProtoRateConfig()
        self.private: dict[int, Bucket] = {}
        self.groups: dict[int, Bucket] = {}
        self.global_writes = Bucket(self.faq.global_rate, self.faq.global_burst)
        self.reads = Bucket(self.faq.read_rate, max(10, int(self.faq.read_rate)))
        self.silent = Bucket(self.mtproto.silent_rate, max(5, int(self.mtproto.silent_rate)))
        self.paid = Bucket(self.faq.paid_rate, int(self.faq.paid_rate))
        self.paid_enabled: bool = False
        self.egress_paused_until: float = 0.0
        self._paid_lane: bool = False

        # Per-peer adaptive backoff (keyed by peer_id)
        self._peer_backoff: dict[int, PeerBackoff] = {}

    def _chat_bucket(self, chat_id: int) -> Bucket:
        if chat_id >= 0:
            table, rate, burst = self.private, self.faq.private_rate, self.faq.private_burst
        else:
            table, rate, burst = self.groups, self.faq.group_rate, self.faq.group_burst
        b = table.get(chat_id)
        if b is None:
            b = table[chat_id] = Bucket(rate, burst, tokens=float(burst))
        return b

    def _peer_backoff_state(self, peer_id: int) -> PeerBackoff:
        if peer_id not in self._peer_backoff:
            self._peer_backoff[peer_id] = PeerBackoff()
        return self._peer_backoff[peer_id]

    def try_acquire(self, method: str, chat_id: int | None) -> bool:
        """Non-blocking: True = go, False = throttled."""
        now = time.monotonic()

        # Check per-peer adaptive backoff first
        if chat_id is not None:
            pb = self._peer_backoff_state(chat_id)
            if pb.paused_until > now:
                return False

        if now < self.egress_paused_until:
            return False

        kind = method_kind(method)
        if kind == "fast":
            return True
        if kind == "read":
            return self.reads.try_take(now)
        if method.lower() in ("resolve_username", "get_participants", "join_channel", "leave_chat"):
            return self.silent.try_take(now)

        # Write: check chat bucket first (don't charge global if blocked)
        chat_b = self._chat_bucket(chat_id) if chat_id is not None else None
        if chat_b is not None:
            chat_b.refill(now)
            if chat_b.paused_until > now or chat_b.tokens < 1:
                return False

        if self._paid_lane:
            if not self.paid.try_take(now):
                return False
            if chat_b is not None:
                chat_b.tokens -= 1
            return True

        if not self.global_writes.try_take(now):
            return False
        if chat_b is not None:
            chat_b.refill(now)
            if chat_b.paused_until > now or chat_b.tokens < 1:
                self.global_writes.tokens += 1  # refund
                return False
            chat_b.tokens -= 1
        return True

    def wait_time(self, method: str, chat_id: int | None) -> float:
        """Seconds until the request may proceed."""
        now = time.monotonic()
        wait = max(0.0, self.egress_paused_until - now)

        if chat_id is not None:
            pb = self._peer_backoff_state(chat_id)
            wait = max(wait, pb.paused_until - now)

        kind = method_kind(method)
        if kind == "fast":
            return max(0.0, wait)
        if kind == "read":
            return max(wait, self.reads.next_available(now))

        wait = max(wait, self.global_writes.next_available(now))
        if chat_id is not None:
            wait = max(wait, self._chat_bucket(chat_id).next_available(now))
        return max(0.0, wait)

    def report_flood_wait(self, exc: Exception, chat_id: int | None) -> dict | None:
        """Learn from a real Pyrogram FLOOD_WAIT exception.

        Applies adaptive backoff: each repeated offence on the same peer
        increases the wait exponentially.
        """
        info = parse_flood_wait(exc)
        if not info:
            return None

        now = time.monotonic()
        wait = float(info["wait"])

        if info["type"] == "peer_flood":
            # Peer flood: back off from this peer entirely
            wait = self.mtproto.peer_flood_cooldown
            if chat_id is not None:
                pb = self._peer_backoff_state(chat_id)
                pb.offences += 1
                pb.paused_until = now + wait
            return info

        if info["type"] == "slowmode" and not self.mtproto.slowmode_respect:
            return info  # ignore slow mode

        # FLOOD_WAIT: apply adaptive backoff on this peer
        if chat_id is not None:
            pb = self._peer_backoff_state(chat_id)
            pb.offences += 1
            pb.last_wait = wait
            # Exponential: base_wait * (base ^ (offences - 1))
            adaptive = wait * (self.mtproto.flood_backoff_base ** (pb.offences - 1))
            adaptive = min(adaptive, 3600)  # cap at 1 hour
            pb.paused_until = now + adaptive
            self._chat_bucket(chat_id).pause(adaptive, now)
        else:
            # No chat association: pause entire bot egress
            self.egress_paused_until = max(self.egress_paused_until, now + wait)
            self.global_writes.pause(wait, now)

        return {
            **info,
            "adaptive_wait": wait
            if chat_id is None
            else min(
                wait
                * (
                    self.mtproto.flood_backoff_base
                    ** (self._peer_backoff_state(chat_id).offences - 1)
                ),
                3600,
            ),
        }

    def clear_peer_backoff(self, chat_id: int) -> None:
        """Reset backoff after a successful send (positive signal)."""
        if chat_id in self._peer_backoff:
            del self._peer_backoff[chat_id]

    def enter_paid_lane(self, allowed: bool) -> None:
        self._paid_lane = allowed and self.paid_enabled

    def exit_paid_lane(self) -> None:
        self._paid_lane = False

    def snapshot(self) -> dict:
        now = time.monotonic()
        peer_backoffs = {
            pid: {"offences": pb.offences, "paused_for_s": round(pb.paused_until - now, 1)}
            for pid, pb in self._peer_backoff.items()
            if pb.paused_until > now
        }
        return {
            "private_empty": sum(1 for b in self.private.values() if b.next_available(now) > 0),
            "group_empty": sum(1 for b in self.groups.values() if b.next_available(now) > 0),
            "egress_paused_for_s": round(max(0.0, self.egress_paused_until - now), 1),
            "peer_backoffs": peer_backoffs,
        }
