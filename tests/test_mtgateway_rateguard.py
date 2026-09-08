"""Tests for the MTProto Rate Guard — FAQ buckets + FLOOD_WAIT parsing."""

import time
from unittest.mock import MagicMock

from mtgateway.rateguard import (
    MTProtoRateConfig,
    MTRateGuard,
    parse_flood_wait,
)
from gateway.config import RateConfig

FAQ = RateConfig(
    private_rate=1.0, private_burst=3,
    group_rate=20.0 / 60.0, group_burst=2,
    global_rate=30.0, global_burst=30,
)
MT = MTProtoRateConfig(
    flood_backoff_base=1.5,
    slowmode_respect=True,
    peer_flood_cooldown=300.0,
    silent_rate=5.0,
)


def guard() -> MTRateGuard:
    return MTRateGuard(FAQ, MT)


class TestFloodWaitParsing:
    def test_flood_wait_basic(self):
        exc = Exception("Telegram says: [420 FLOOD_WAIT_X] - A wait of 42 seconds is required")
        info = parse_flood_wait(exc)
        assert info == {"type": "flood_wait", "wait": 42}

    def test_flood_premium(self):
        exc = Exception("FloodPremiumWait(10)")
        info = parse_flood_wait(exc)
        assert info == {"type": "flood_premium", "wait": 10}

    def test_slowmode(self):
        exc = Exception("SlowmodeWait(5)")
        info = parse_flood_wait(exc)
        assert info == {"type": "slowmode", "wait": 5}

    def test_peer_flood(self):
        exc = Exception("PeerFloodInvalid")
        info = parse_flood_wait(exc)
        assert info == {"type": "peer_flood", "wait": 300}

    def test_not_a_flood(self):
        exc = Exception("SomeOtherError(42)")
        assert parse_flood_wait(exc) is None


class TestAdaptiveBackoff:
    """Repeated FLOOD_WAIT on the same peer → exponential wait increase."""

    def _flood_exc(self, wait=10):
        return Exception(f"FloodWait({wait})")

    def test_first_offence_uses_raw_wait(self):
        g = guard()
        info = g.report_flood_wait(self._flood_exc(10), chat_id=100)
        assert info["type"] == "flood_wait"
        assert g.try_acquire("send_message", 100) is False  # paused

    def test_repeated_offence_increases_wait(self):
        g = guard()
        # First offence: wait 10s
        g.report_flood_wait(self._flood_exc(10), chat_id=100)
        pb1 = g._peer_backoff[100]

        # Simulate time passing (backoff expired)
        pb1.paused_until = 0
        pb1.offences = 1

        # Second offence: wait should be 10 * 1.5 = 15s
        g.report_flood_wait(self._flood_exc(10), chat_id=100)
        pb2 = g._peer_backoff[100]
        assert pb2.offences == 2

        # Third offence: 10 * 1.5^2 = 22.5s
        pb2.paused_until = 0
        g.report_flood_wait(self._flood_exc(10), chat_id=100)
        pb3 = g._peer_backoff[100]
        assert pb3.offences == 3

    def test_success_clears_backoff(self):
        g = guard()
        g.report_flood_wait(self._flood_exc(10), chat_id=100)
        assert 100 in g._peer_backoff
        g.clear_peer_backoff(100)
        assert 100 not in g._peer_backoff

    def test_peer_flood_uses_cooldown(self):
        g = guard()
        exc = Exception("PeerFloodInvalid")
        g.report_flood_wait(exc, chat_id=100)
        assert g._peer_backoff[100].paused_until > time.monotonic() + 290


class TestFAQBuckets:
    """FAQ limits still apply even on the MTProto lane."""

    def test_private_1_per_second(self):
        g = guard()
        ok = [g.try_acquire("send_message", 100) for _ in range(3)]
        assert all(ok)  # burst 3
        assert not g.try_acquire("send_message", 100)

    def test_group_20_per_minute(self):
        g = guard()
        ok = sum(1 for _ in range(25) if g.try_acquire("send_message", -100))
        assert ok <= 20

    def test_global_30_per_second(self):
        g = guard()
        ok = sum(1 for i in range(40) if g.try_acquire("send_message", 10_000 + i))
        assert ok <= 30

    def test_silent_methods_separate_bucket(self):
        g = guard()
        for _ in range(10):
            g.try_acquire("send_message", 100)  # saturate private
        # resolve_username should still work (different bucket)
        assert g.try_acquire("resolve_username", None)


class TestSnapshot:
    def test_snapshot_includes_peer_backoffs(self):
        g = guard()
        exc = Exception("FloodWait(30)")
        g.report_flood_wait(exc, chat_id=100)
        snap = g.snapshot()
        assert 100 in snap["peer_backoffs"]
        assert snap["peer_backoffs"][100]["offences"] == 1
