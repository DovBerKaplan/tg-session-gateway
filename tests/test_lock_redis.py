"""Redis lease backend (ADR-0001) — TTL, fencing, exactly-one-owner.

fakeredis implements the redis.asyncio client interface, so the lease
code runs unmodified against it.
"""

import asyncio

import pytest

pytest.importorskip("fakeredis")

from mtgateway.lock import RedisSessionLease, make_lock


def _lease(alias="bot1", ttl=0.5, r=None):
    # ONE shared fake server per test — separate instances would not
    # share state and every "exactly one owner" assertion collapses
    import fakeredis
    import fakeredis.aioredis

    if r is None:
        r = fakeredis.aioredis.FakeRedis(server=fakeredis.FakeServer())
    lease = RedisSessionLease.__new__(RedisSessionLease)
    lease.alias = alias
    lease.ttl_s = ttl
    lease._r = r
    lease._key = f"tgw:lease:{alias}"
    lease._ft_key = f"tgw:lease:{alias}:ft"
    lease._holder = "hostA:1"
    lease._heartbeat_task = None
    lease.token = None
    return lease


class TestRedisLease:
    async def test_acquire_grants_exactly_one(self):
        r = _lease()._r
        a, b = _lease(r=r), _lease(r=r)
        b._holder = "hostB:2"
        assert await a.acquire() is True
        assert await b.acquire() is False, "second holder must be refused"

    async def test_release_frees_for_next_owner(self):
        r = _lease()._r
        a, b = _lease(r=r), _lease(r=r)
        b._holder = "hostB:2"
        await a.acquire()
        await a.release()
        assert await b.acquire() is True

    async def test_ttl_expiry_releases_automatically(self):
        # crash semantics: no release call — the key lapses with the TTL
        a = _lease(ttl=0.3)
        await a.acquire()
        await asyncio.sleep(0.45)
        b = _lease(ttl=0.3, r=a._r)
        b._holder = "hostB:2"
        assert await b.acquire() is True, "expired lease must be acquirable"

    async def test_heartbeat_extends_ttl(self):
        a = _lease(ttl=0.3)
        await a.acquire()
        for _ in range(3):  # 3 heartbeats × 0.15s > original TTL
            await asyncio.sleep(0.15)
            await a.extend()
        b = _lease(ttl=0.3, r=a._r)
        b._holder = "hostB:2"
        assert await b.acquire() is False, "heartbeat must keep the lease alive"

    async def test_fencing_token_monotonic(self):
        r = _lease()._r
        a, b = _lease(r=r), _lease(r=r)
        b._holder = "hostB:2"
        await a.acquire()
        t1 = a.token
        await a.release()
        await b.acquire()
        assert b.token == t1 + 1, "fencing token must increment per acquisition"

    async def test_release_does_not_steal_foreign_lease(self):
        r = _lease()._r
        a, b = _lease(r=r), _lease(r=r)
        b._holder = "hostB:2"
        await a.acquire()
        await a.release()
        await b.acquire()
        await a.release()  # stale holder tries to release B's lease
        c = _lease(r=r)
        c._holder = "hostC:3"
        assert await c.acquire() is False, "B's lease must survive A's stale release"

    async def test_factory_selects_backend(self):
        from mtgateway.lock import FileLockAdapter

        assert isinstance(make_lock("x", "/tmp"), FileLockAdapter)
        lease = make_lock("x", "/tmp", backend="redis", redis_url="redis://localhost:6379/0")
        assert isinstance(lease, RedisSessionLease)
        with pytest.raises(ValueError):
            make_lock("x", "/tmp", backend="redis")
