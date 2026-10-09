"""Restart semantics after kill -9 mid-FloodWait (issue #19).

The exact interleaving the issue asks about: a worker is mid-FloodWait
backoff (retry scheduled at T+30s), the process takes kill -9, and a
supervisor restarts it BEFORE T+30s. What is and is not guaranteed:

1. The pending retry is NOT gateway state — the consumer owns it. On a
   real 429/FLOOD_WAIT the gateway answers with `retry_after` and keeps
   no outbound retry queue; the guard's preventive chat pause is
   in-memory and dies with the process. Recomputed from zero on boot —
   by design: Telegram is the authority and re-issues FLOOD_WAIT if
   the consumer retries too early.  (TestFloodWaitBackoffIsRecomputed)
2. Queued updates are durable and duplicate-free across the crash:
   every push commits synchronously to SQLite-WAL (safe against
   process death — not against machine power loss, synchronous=NORMAL),
   only unacked updates replay, and the persisted consumer offset
   floors delivery, so a poller redelivery racing the kill can never
   re-deliver an acked update.  (TestQueueDurabilityAcrossKillNine)
3. The advisory file lock arbitrates the old/new process overlap: a
   dead holder is reclaimed IMMEDIATELY (PID liveness), so a supervisor
   restarting before T+30s is the normal path, not a 60s wait — the
   heartbeat window only bounds a frozen holder or PID reuse.
   Already pinned by tests/test_session_lock.py
   (test_lock_reclaimable_after_holder_dies,
   test_second_process_refused_while_holder_alive) — not re-tested here.
"""

import asyncio

from gateway.queue import UpdateQueue
from gateway.rateguard import BotRateGuard


def run(coro):
    return asyncio.run(coro)


class TestFloodWaitBackoffIsRecomputed:
    """Q1: the pending retry is the consumer's; the guard pause is
    preventive, in-memory, and deliberately not durable."""

    def test_chat_pause_does_not_survive_restart(self):
        guard = BotRateGuard()
        guard.report_429(501, 30.0)  # real 429 — worker now "mid-backoff"
        assert guard.chat_send_state(501) == "paused"

        # kill -9 → the new process builds a FRESH guard; nothing about
        # the pause was persisted. If the consumer retries before the
        # window elapses, the send goes through and Telegram re-throttles.
        reborn = BotRateGuard()
        assert reborn.chat_send_state(501) == "ok"

    def test_other_chat_state_is_equally_ephemeral(self):
        guard = BotRateGuard()
        guard.report_429(None, 30.0)  # bot-wide egress pause
        assert guard.chat_send_state(123) == "paused"
        assert BotRateGuard().chat_send_state(123) == "ok"


class TestQueueDurabilityAcrossKillNine:
    """Q2: unacked updates replay exactly once across an abrupt restart,
    and a Telegram redelivery racing the kill cannot resurrect acked
    updates — the persisted consumer offset floors delivery."""

    def _q(self, tmp_path, tag):
        return UpdateQueue(maxsize=100, persist_path=str(tmp_path / "queue.db"), bot_alias=tag)

    def test_unacked_replay_once_acked_never_return(self, tmp_path):
        q = self._q(tmp_path, "b1")
        q.push_all([{"update_id": 1001}, {"update_id": 1002}, {"update_id": 1003}])

        # consumer saw 1001-1002, acked PTB-style (offset = 1003), got
        # 1003 back — then everyone takes kill -9 before 1003 is acked.
        # pull() acks offset-1 FIRST, then returns what is still pending.
        batch = run(q.pull(offset=1003, timeout=0.01))
        assert [u["update_id"] for u in batch] == [1003]

        # supervisor restart before the consumer's T+30 retry: a fresh
        # queue on the same SQLite file (no graceful close needed —
        # every push/ack already committed).
        q2 = self._q(tmp_path, "b1")

        # the kill also raced the poller's offset save → Telegram
        # redelivers the WHOLE batch into the new process
        q2.push_all([{"update_id": 1001}, {"update_id": 1002}, {"update_id": 1003}])

        replayed = run(q2.pull(None, timeout=0.01))
        # exactly the one unacked update — 1001/1002 stay below the
        # persisted consumer offset no matter how often they are re-pushed
        assert [u["update_id"] for u in replayed] == [1003]

        # and the replay itself is idempotent: re-pushing 1003 again
        # (another redelivery) yields it once, not twice
        q2.push_all([{"update_id": 1003}])
        again = run(q2.pull(None, timeout=0.01))
        assert [u["update_id"] for u in again] == [1003]

    def test_nothing_acked_full_backlog_replays(self, tmp_path):
        """The other edge of the same race: kill before ANY ack — the
        whole backlog is unacked, everything replays, exactly once."""
        q = self._q(tmp_path, "b2")
        q.push_all([{"update_id": 2001}, {"update_id": 2002}])
        run(q.pull(None, timeout=0.01))  # delivered but NOT acked

        q2 = self._q(tmp_path, "b2")
        replayed = run(q2.pull(None, timeout=0.01))
        assert [u["update_id"] for u in replayed] == [2001, 2002]  # at-least-once

        q2.push_all([{"update_id": 2001}, {"update_id": 2002}])  # redelivery
        once = run(q2.pull(None, timeout=0.01))
        assert [u["update_id"] for u in once] == [2001, 2002]  # never twice
