"""Distributed locks: a Redis lease + fencing tokens.

Acquire:  token = INCR relay:fence:{resource}          (monotonically increasing)
          SET relay:lock:{resource} token NX PX ttl    (only if nobody holds it)
Release:  DEL only if the value is still *our* token   (Lua compare-and-delete)

Why fencing tokens? A lease can expire while its holder is paused (GC, swap, a slow network
call). Now two processes both believe they hold the lock. The lock alone can't prevent that;
the *protected resource* has to reject the stale holder. Every write carries the token and the
resource only accepts tokens newer than the last one it saw (`JobStore.fenced_write`).
This is Martin Kleppmann's argument against relying on Redlock alone for correctness.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import redis.asyncio as aioredis

from relay.keys import Keys

_RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
"""

_EXTEND_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) end
return 0
"""


class LockNotAcquiredError(TimeoutError):
    pass


@dataclass(frozen=True, slots=True)
class Lease:
    resource: str
    token: int  # the fencing token: pass it along with every write to the protected resource


class LockManager:
    def __init__(self, redis: aioredis.Redis, keys: Keys) -> None:
        self.redis = redis
        self.keys = keys
        self._release = redis.register_script(_RELEASE_LUA)
        self._extend = redis.register_script(_EXTEND_LUA)

    async def try_acquire(self, resource: str, ttl_ms: int) -> Lease | None:
        token = int(await self.redis.incr(self.keys.fence(resource)))
        ok = await self.redis.set(self.keys.lock(resource), str(token), nx=True, px=ttl_ms)
        return Lease(resource, token) if ok else None

    async def acquire(self, resource: str, ttl_ms: int = 30_000, wait_s: float = 10.0) -> Lease:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait_s
        delay = 0.01
        while True:
            lease = await self.try_acquire(resource, ttl_ms)
            if lease is not None:
                return lease
            if loop.time() >= deadline:
                raise LockNotAcquiredError(f"could not lock {resource!r} within {wait_s}s")
            await asyncio.sleep(delay * random.uniform(0.5, 1.5))  # noqa: S311
            delay = min(delay * 2, 0.5)

    async def release(self, lease: Lease) -> bool:
        """False means our lease had already expired (someone else may hold the lock now)."""
        return bool(await self._release(keys=[self.keys.lock(lease.resource)], args=[lease.token]))

    async def extend(self, lease: Lease, ttl_ms: int) -> bool:
        return bool(
            await self._extend(keys=[self.keys.lock(lease.resource)], args=[lease.token, ttl_ms])
        )

    @asynccontextmanager
    async def hold(
        self, resource: str, ttl_ms: int = 30_000, wait_s: float = 10.0
    ) -> AsyncIterator[Lease]:
        lease = await self.acquire(resource, ttl_ms, wait_s)
        try:
            yield lease
        finally:
            await self.release(lease)


# Per-job-type concurrency limit across *all* workers: a ZSET of current holders scored by the
# time they last refreshed. Stale holders (a worker that died) fall out after `ttl_ms`.
_SLOT_ACQUIRE_LUA = """
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', tonumber(ARGV[1]) - tonumber(ARGV[2]))
if redis.call('ZSCORE', KEYS[1], ARGV[4]) then return 1 end
if redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[3]) then
  redis.call('ZADD', KEYS[1], ARGV[1], ARGV[4])
  return 1
end
return 0
"""


class TypeSlots:
    """Distributed counting semaphore: at most `limit` jobs of one type run at once."""

    def __init__(self, redis: aioredis.Redis, keys: Keys, ttl_ms: int = 15_000) -> None:
        self.redis = redis
        self.keys = keys
        self.ttl_ms = ttl_ms
        self._acquire = redis.register_script(_SLOT_ACQUIRE_LUA)

    async def acquire(self, job_type: str, holder: str, limit: int, now_ms: int) -> bool:
        got = await self._acquire(
            keys=[self.keys.type_slots(job_type)], args=[now_ms, self.ttl_ms, limit, holder]
        )
        return bool(got)

    async def refresh(self, job_type: str, holder: str, now_ms: int) -> None:
        await self.redis.zadd(self.keys.type_slots(job_type), {holder: now_ms}, xx=True)

    async def release(self, job_type: str, holder: str) -> None:
        await self.redis.zrem(self.keys.type_slots(job_type), holder)

    async def holders(self, job_type: str) -> int:
        return int(await self.redis.zcard(self.keys.type_slots(job_type)))
