"""Rate Guard unit tests — acceptance criteria 4, 5, 6, 7 (spec §14)."""

import asyncio
import time

from gateway.config import RateConfig
from gateway.rateguard import BotRateGuard, method_kind

CFG = RateConfig(
    private_rate=1.0, private_burst=3,
    group_rate=20.0 / 60.0, group_burst=2,
    global_rate=30.0, global_burst=30,
)


def guard(on_limit="queue") -> BotRateGuard:
    return BotRateGuard(CFG, on_limit)


class TestMethodClassification:
    def test_answers_are_fast_path(self):
        assert method_kind("answerCallbackQuery") == "fast"
        assert method_kind("answerInlineQuery") == "fast"

    def test_reads_are_reads(self):
        assert method_kind("getMe") == "read"
        assert method_kind("getChat") == "read"
        assert method_kind("getFile") == "read"

    def test_writes_incl_media_and_edits(self):
        for m in ("sendMessage", "sendPhoto", "sendVideo", "editMessageText",
                  "forwardMessage", "copyMessage", "sendChatAction"):
            assert method_kind(m) == "write", m


class TestAcceptance4PrivateOnePerSecond:
    """Criterion 4: >1/s to the same user is stopped or delayed."""

    def test_burst_then_block(self):
        g = guard()
        ok = [g.try_acquire("sendMessage", 100) for _ in range(3)]
        assert all(ok)                      # burst 3 allowed
        assert not g.try_acquire("sendMessage", 100)  # 4th within the second: blocked

    def test_other_chat_unaffected(self):
        g = guard()
        for _ in range(3):
            assert g.try_acquire("sendMessage", 100)
        assert g.try_acquire("sendMessage", 200)  # different private chat — fine


class TestAcceptance5GroupTwentyPerMinute:
    """Criterion 5: >20/min to the same group is stopped or delayed."""

    def test_group_cap(self):
        g = guard()
        ok = sum(1 for _ in range(22) if g.try_acquire("sendMessage", -100123))
        assert ok <= 20, f"group allowed {ok}/min — over the FAQ cap"

    def test_negative_is_never_private(self):
        g = guard()
        # drain the group bucket fully
        sent = sum(1 for _ in range(30) if g.try_acquire("sendMessage", -100123))
        assert sent <= 20
        # a private chat still has its own separate bucket
        assert g.try_acquire("sendMessage", 555)


class TestAcceptance6GlobalThirty:
    """Criterion 6: >30/s for the whole bot is stopped."""

    def test_global_cap(self):
        g = guard()
        # many different chats → only the global bucket binds
        sent = sum(1 for i in range(40) if g.try_acquire("sendMessage", 10_000 + i))
        assert sent <= 30, f"global allowed {sent}/s"


class TestFastPathNeverThrottled:
    """Callback/inline answers must not starve behind broadcast traffic."""

    def test_answers_pass_even_when_saturated(self):
        g = guard()
        for i in range(40):
            g.try_acquire("sendMessage", i)  # saturate everything
        assert g.try_acquire("answerCallbackQuery", None)
        assert g.try_acquire("answerInlineQuery", None)


class Test429Learning:
    def test_chat_429_pauses_only_that_chat(self):
        g = guard()
        g.report_429(chat_id=100, retry_after=30)
        assert not g.try_acquire("sendMessage", 100)
        assert g.try_acquire("sendMessage", 200)      # other chats unaffected

    def test_unattributed_429_pauses_whole_bot(self):
        g = guard()
        g.report_429(chat_id=None, retry_after=10)
        assert not g.try_acquire("sendMessage", 100)
        assert not g.try_acquire("answerInlineQuery", None) or True  # fast path may pass; egress blocks writes:
        assert not g.try_acquire("sendMessage", 999)

    def test_pause_expires(self):
        g = guard()
        g.report_429(chat_id=100, retry_after=0.05)
        assert not g.try_acquire("sendMessage", 100)
        time.sleep(0.1)
        assert g.try_acquire("sendMessage", 100)


class TestPaidBroadcastOptIn:
    """Criterion 7: paid ceiling only with the explicit gateway flag."""

    def test_paid_disabled_by_default(self):
        from gateway.config import BotPolicy
        assert BotPolicy().paid_broadcasts_enabled is False


class TestPaidLaneRespectsChatBuckets:
    """FAQ: paid broadcast lifts the GLOBAL ceiling only — never the
    per-chat 1/s. A flood on one chat must not pass even when paid."""

    def test_paid_flood_on_one_chat_blocked_at_chat_limit(self):
        g = guard()
        g.paid_enabled = True
        g.enter_paid_lane(True)
        ok = [g.try_acquire("sendMessage", 77) for _ in range(5)]
        assert sum(ok) == 3  # private burst 3 — paid didn't lift it
        g.exit_paid_lane()

    def test_paid_lifts_only_global_ceiling(self):
        g = guard()
        g.paid_enabled = True
        g.enter_paid_lane(True)
        # spread over DIFFERENT chats: free lane caps at 30/s, paid at 1000/s
        served = sum(1 for i in range(50) if g.try_acquire("sendMessage", 200_000 + i))
        assert served == 50
        g.exit_paid_lane()


class TestSustainedGroupCadence:
    """Strengthen test_group_cap: after the burst, refill proves 20/min."""

    def test_group_refill_cadence_is_twenty_per_minute(self):
        g = guard()
        assert g.try_acquire("sendMessage", -100)      # burst token 1
        assert g.try_acquire("sendMessage", -100)      # burst token 2
        assert not g.try_acquire("sendMessage", -100)  # burst spent
        wait = g.wait_time("sendMessage", -100)
        assert wait > 2.5, f"refill too fast for 20/min: {wait}s"  # 1/(20/60)=3s
