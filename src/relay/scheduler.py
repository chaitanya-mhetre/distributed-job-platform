"""Scheduler: the background process that keeps the queue healthy.

Run two or more for availability; a lease lock elects one leader and the others stand by.
Each tick, the leader:

  1. promotes delayed jobs whose run_at has passed (ZSET -> stream, atomically in Lua)
  2. reclaims work from dead workers (heartbeat key gone) and from stream entries that have been
     idle too long (live workers refresh their entries' idle time every heartbeat)
  3. reconciles: re-dispatches rows that were never handed to Redis (crash between INSERT and
     XADD). Runs every `reconcile_every` ticks, not every tick.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from relay.app import Relay

log = logging.getLogger("relay.scheduler")

# Acquire the lease if free, or extend it if we already hold it. One script = no race between
# "check owner" and "extend".
_LEASE_LUA = """
local owner = redis.call('GET', KEYS[1])
if owner == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
if not owner then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end
return 0
"""

_RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
"""


@dataclass
class SchedulerConfig:
    tick_s: float = 0.25
    lease_ms: int = 10_000
    # an entry idle this long is considered abandoned even if we can't prove its worker died
    stuck_idle_ms: int = 60_000
    # entries of a dead worker are reclaimed once idle this long (guards against a worker that
    # just started and hasn't been seen by XINFO yet)
    dead_idle_ms: int = 1_000
    reconcile_after_s: float = 30.0
    reconcile_every: int = 20


@dataclass
class TickResult:
    promoted: int = 0
    reclaimed: int = 0
    reconciled: int = 0


class Scheduler:
    def __init__(
        self, relay: Relay, config: SchedulerConfig | None = None, **overrides: Any
    ) -> None:
        self.relay = relay
        self.cfg = config or SchedulerConfig(**overrides)
        self.scheduler_id = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        redis = relay.broker.redis
        self._lease = redis.register_script(_LEASE_LUA)
        self._release = redis.register_script(_RELEASE_LUA)
        self._stopping = asyncio.Event()
        self._ticks = 0
        self.is_leader = False

    def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        await self.relay.setup()
        log.info("scheduler started", extra={"scheduler_id": self.scheduler_id})
        try:
            while not self._stopping.is_set():
                try:
                    if await self.hold_leadership():
                        await self.tick()
                except Exception:  # noqa: BLE001 - keep ticking through transient errors
                    log.exception("scheduler tick failed")
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stopping.wait(), self.cfg.tick_s)
        finally:
            await self._release(keys=[self.relay.broker.keys.leader], args=[self.scheduler_id])

    async def hold_leadership(self) -> bool:
        got = await self._lease(
            keys=[self.relay.broker.keys.leader], args=[self.scheduler_id, self.cfg.lease_ms]
        )
        leader = bool(got)
        if leader != self.is_leader:
            log.info(
                "leadership changed",
                extra={"scheduler_id": self.scheduler_id, "leader": leader},
            )
        self.is_leader = leader
        return leader

    async def tick(self) -> TickResult:
        self._ticks += 1
        result = TickResult()
        result.promoted = await self.promote()
        result.reclaimed = await self.reclaim()
        if self._ticks % self.cfg.reconcile_every == 0:
            result.reconciled = await self.reconcile()
        return result

    # --- 1. delayed -> stream -----------------------------------------------------------

    async def promote(self) -> int:
        # Order matters: flip the rows to 'queued' first, *then* make the entries visible in the
        # stream. The other order lets a fast worker read the entry while the row still says
        # 'scheduled'; it would skip the job and ack it, and the job would be lost.
        # A crash between the two steps is harmless: the next tick finds the same ids in the
        # ZSET, the UPDATE is a no-op, and the Lua script moves them.
        ids = await self.relay.broker.due(datetime.now(UTC))
        await self.relay.store.promote_many(ids)
        return len(await self.relay.broker.promote(ids))

    # --- 2. reclaim ---------------------------------------------------------------------

    async def reclaim(self) -> int:
        broker, store = self.relay.broker, self.relay.store
        total = 0
        for stream in broker.keys.all_queues():
            to_requeue: list[str] = []
            for consumer in await broker.consumers(stream):
                name = str(consumer["name"])
                alive = await broker.redis.exists(broker.keys.heartbeat(name))
                if alive:
                    continue
                if int(consumer["pending"]) > 0:
                    to_requeue += await broker.pending_of(stream, name)
                else:
                    await broker.delete_consumer(stream, name)
            claimed = await broker.claim(stream, to_requeue, self.cfg.dead_idle_ms)
            claimed += await broker.claim(
                stream,
                await broker.idle_pending(stream, self.cfg.stuck_idle_ms),
                self.cfg.stuck_idle_ms,
            )
            # Same ordering rule as promote(): rows back to 'queued' before the new stream
            # entries become visible to workers.
            job_ids = [UUID(fields["job_id"]) for _, fields in claimed if fields]
            for job_id in job_ids:
                job = await store.mark_queued(job_id)  # running -> queued; no-op otherwise
                if job is not None:
                    await store.close_attempt(
                        job_id, job.attempts, "lost", "worker died or stalled", 0
                    )
            await broker.republish(stream, claimed)
            if job_ids:
                log.warning("reclaimed jobs", extra={"stream": stream, "count": len(job_ids)})
            total += len(job_ids)
        return total

    # --- 3. reconcile -------------------------------------------------------------------

    async def reconcile(self) -> int:
        jobs = await self.relay.store.undispatched(self.cfg.reconcile_after_s)
        for job in jobs:
            await self.relay.dispatch(job)
        if jobs:
            log.warning("re-dispatched jobs missing from redis", extra={"count": len(jobs)})
        return len(jobs)
