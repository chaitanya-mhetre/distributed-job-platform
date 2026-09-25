"""RedisBroker: the Redis Streams side of Relay.

Only job *ids* travel through Redis. Every stream entry is `{job_id, type}`; the job's payload
and status live in Postgres.

Delivery model (at-least-once):
  XADD            producer appends an entry to relay:q:{priority}
  XREADGROUP >    a worker receives it; Redis records it in the group's Pending Entries List (PEL)
  XACK + XDEL     the worker acknowledges it once Postgres has the outcome, then deletes it so the
                  stream length equals "work not finished yet"
If a worker dies before XACK the entry stays in the PEL and the scheduler re-queues it.
"""

from __future__ import annotations

import contextlib
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast
from uuid import UUID

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from relay.keys import GROUP, Keys
from relay.models import Job, JobStatus, Priority

# Moves the given (due) jobs from the delayed ZSET into their priority stream, atomically.
# Atomic matters: two schedulers, or a crash half-way, must never drop or double-add a job.
# A job that is no longer in the ZSET (cancelled, or promoted by someone else) is skipped.
_PROMOTE_LUA = """
-- ARGV[1] = key namespace, ARGV[2..n] = job ids
local moved = {}
for i = 2, #ARGV do
  local job_id = ARGV[i]
  if redis.call('ZREM', KEYS[1], job_id) == 1 then
    local meta = redis.call('HGET', KEYS[2], job_id)
    redis.call('HDEL', KEYS[2], job_id)
    if meta then
      local sep = string.find(meta, '|', 1, true)
      local stream = ARGV[1] .. ':q:' .. string.sub(meta, 1, sep - 1)
      redis.call('XADD', stream, '*', 'job_id', job_id, 'type', string.sub(meta, sep + 1))
      table.insert(moved, job_id)
    end
  end
end
return moved
"""


@dataclass(frozen=True, slots=True)
class Message:
    """One delivered stream entry."""

    stream: str
    entry_id: str
    job_id: UUID
    job_type: str


@dataclass(frozen=True, slots=True)
class QueueStats:
    name: str
    depth: int  # entries in the stream = waiting + in-flight (acked entries are deleted)
    pending: int  # delivered to a worker, not yet acked
    oldest_age_s: float  # age of the oldest entry still in the stream
    consumers: int


@dataclass(frozen=True, slots=True)
class DeadLetter:
    entry_id: str
    job_id: UUID
    job_type: str
    error: str


class WeightedOrder:
    """Smooth weighted round-robin (the algorithm nginx uses) over priorities.

    Each call returns all priorities, most-favoured first. With weights 6:3:1, `high` comes first
    6 times in 10, `default` 3 and `low` once, and the picks are spread out rather than bunched.
    Workers read the first non-empty queue in that order, so a flood of high-priority jobs slows
    low-priority ones down but never starves them.
    """

    def __init__(self, weights: dict[Priority, int]) -> None:
        if any(w <= 0 for w in weights.values()):
            raise ValueError("weights must be positive")
        self.weights = dict(weights)
        self._current = dict.fromkeys(weights, 0)

    def next_order(self) -> list[Priority]:
        total = sum(self.weights.values())
        for p, w in self.weights.items():
            self._current[p] += w
        chosen = max(self._current, key=lambda p: self._current[p])
        self._current[chosen] -= total
        rest = [p for p in Priority if p in self.weights and p != chosen]
        return [chosen, *rest]


DEFAULT_WEIGHTS = {Priority.HIGH: 6, Priority.DEFAULT: 3, Priority.LOW: 1}


def _entry_ms(entry_id: str) -> int:
    return int(entry_id.split("-", 1)[0])


# redis-py's return types are loose unions (bytes | str | None ...). With decode_responses=True
# we know entries come back as (str id, dict[str, str] fields); normalise them in one place.
Entry = tuple[str, dict[str, str]]


def _entries(raw: Any) -> list[Entry]:
    return [(str(eid), dict(fields or {})) for eid, fields in (raw or [])]


def _dead(entry: Entry) -> DeadLetter:
    eid, f = entry
    return DeadLetter(eid, UUID(f["job_id"]), f["type"], f.get("error", ""))


class RedisBroker:
    def __init__(self, redis: aioredis.Redis, keys: Keys) -> None:
        self.redis = redis
        self.keys = keys
        self._promote = redis.register_script(_PROMOTE_LUA)

    @classmethod
    def from_url(cls, url: str, namespace: str = "relay") -> RedisBroker:
        return cls(aioredis.from_url(url, decode_responses=True), Keys(namespace))

    async def close(self) -> None:
        await self.redis.aclose()

    async def ensure_groups(self) -> None:
        for stream in self.keys.all_queues():
            try:
                await self.redis.xgroup_create(stream, GROUP, id="0", mkstream=True)
            except ResponseError as exc:
                if "BUSYGROUP" not in str(exc):
                    raise

    # --- producing ----------------------------------------------------------------------

    async def enqueue(self, job_id: UUID, job_type: str, priority: Priority) -> str:
        entry_id = await self.redis.xadd(
            self.keys.queue(priority), {"job_id": str(job_id), "type": job_type}
        )
        return str(entry_id)

    async def enqueue_many(self, jobs: Sequence[Job]) -> None:
        """Dispatch many jobs in one round trip (MULTI/EXEC pipeline).

        Queued jobs go to their priority stream, scheduled ones to the delayed ZSET, exactly as
        enqueue()/schedule() would. MULTI makes it all-or-nothing: if the call fails, none are
        in Redis and none get marked dispatched, so reconciliation re-sends the whole batch.
        """
        if not jobs:
            return
        async with self.redis.pipeline(transaction=True) as pipe:
            for job in jobs:
                if job.status is JobStatus.SCHEDULED:
                    pipe.hset(self.keys.delayed_meta, str(job.id), f"{job.priority}|{job.type}")
                    pipe.zadd(self.keys.delayed, {str(job.id): job.run_at.timestamp() * 1000})
                else:
                    pipe.xadd(
                        self.keys.queue(job.priority), {"job_id": str(job.id), "type": job.type}
                    )
            await pipe.execute()

    async def schedule(
        self, job_id: UUID, job_type: str, priority: Priority, run_at: datetime
    ) -> None:
        """Park a job in the delayed ZSET until `run_at`. Streams have no native delay."""
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(self.keys.delayed_meta, str(job_id), f"{priority}|{job_type}")
            pipe.zadd(self.keys.delayed, {str(job_id): run_at.timestamp() * 1000})
            await pipe.execute()

    async def unschedule(self, job_id: UUID) -> bool:
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.zrem(self.keys.delayed, str(job_id))
            pipe.hdel(self.keys.delayed_meta, str(job_id))
            removed, _ = await pipe.execute()
        return bool(removed)

    async def due(self, now: datetime, limit: int = 500) -> list[UUID]:
        """Delayed jobs whose run_at has passed (read-only)."""
        ids = await self.redis.zrangebyscore(
            self.keys.delayed, "-inf", now.timestamp() * 1000, start=0, num=limit
        )
        return [UUID(str(j)) for j in ids]

    async def promote(self, job_ids: Sequence[UUID]) -> list[UUID]:
        """Move these jobs from the ZSET to their streams. Call only after Postgres already
        says 'queued': the moment an entry is in a stream a worker may pick it up, and the
        worker only runs jobs whose row is 'queued'."""
        if not job_ids:
            return []
        moved = await self._promote(
            keys=[self.keys.delayed, self.keys.delayed_meta],
            args=[self.keys.ns, *(str(j) for j in job_ids)],
        )
        return [UUID(j) for j in cast(list[str], moved)]

    # --- consuming ----------------------------------------------------------------------

    async def read(
        self,
        consumer: str,
        order: Sequence[Priority],
        count: int,
        block_ms: int = 0,
    ) -> list[Message]:
        """Read up to `count` new entries, trying queues in `order`.

        First a non-blocking read of each queue in turn (this is where priority weighting
        happens). If everything is empty and `block_ms` > 0, block on all queues at once so an
        idle worker wakes up the moment any job arrives instead of polling in a loop.
        """
        for priority in order:
            msgs = await self._xreadgroup(consumer, [self.keys.queue(priority)], count, None)
            if msgs:
                return msgs
        if block_ms <= 0:
            return []
        return await self._xreadgroup(
            consumer, [self.keys.queue(p) for p in order], count, block_ms
        )

    async def _xreadgroup(
        self, consumer: str, streams: list[str], count: int, block_ms: int | None
    ) -> list[Message]:
        raw: Any = await self.redis.xreadgroup(
            GROUP, consumer, dict.fromkeys(streams, ">"), count=count, block=block_ms
        )
        out: list[Message] = []
        for stream, entries in raw or []:
            for entry_id, fields in _entries(entries):
                out.append(Message(str(stream), entry_id, UUID(fields["job_id"]), fields["type"]))
        return out

    async def ack(self, msg: Message) -> None:
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.xack(msg.stream, GROUP, msg.entry_id)
            pipe.xdel(msg.stream, msg.entry_id)
            await pipe.execute()

    # --- dead letters -------------------------------------------------------------------

    async def dead_letter(self, job_id: UUID, job_type: str, error: str) -> str:
        entry = await self.redis.xadd(
            self.keys.dlq, {"job_id": str(job_id), "type": job_type, "error": error[:2000]}
        )
        return str(entry)

    async def list_dead_letters(self, after: str = "-", count: int = 50) -> list[DeadLetter]:
        start = f"({after}" if after != "-" else "-"
        raw: Any = await self.redis.xrange(self.keys.dlq, min=start, count=count)
        return [_dead(e) for e in _entries(raw)]

    async def get_dead_letter(self, entry_id: str) -> DeadLetter | None:
        raw: Any = await self.redis.xrange(self.keys.dlq, min=entry_id, max=entry_id)
        entries = _entries(raw)
        return _dead(entries[0]) if entries else None

    async def delete_dead_letter(self, entry_id: str) -> bool:
        return bool(await self.redis.xdel(self.keys.dlq, entry_id))

    # --- recovery (used by the scheduler) -----------------------------------------------

    async def consumers(self, stream: str) -> list[dict[str, Any]]:
        try:
            rows: Any = await self.redis.xinfo_consumers(stream, GROUP)
            return list(rows)
        except ResponseError:
            return []

    async def pending_of(self, stream: str, consumer: str, count: int = 1000) -> list[str]:
        rows = await self.redis.xpending_range(
            stream, GROUP, min="-", max="+", count=count, consumername=consumer
        )
        return [str(r["message_id"]) for r in rows]

    async def idle_pending(self, stream: str, min_idle_ms: int, count: int = 1000) -> list[str]:
        rows = await self.redis.xpending_range(
            stream, GROUP, min="-", max="+", count=count, idle=min_idle_ms
        )
        return [str(r["message_id"]) for r in rows]

    async def claim(self, stream: str, entry_ids: Sequence[str], min_idle_ms: int) -> list[Entry]:
        """Take pending entries away from their (dead or stuck) consumer.

        XCLAIM with min-idle-time only claims entries that are *still* pending and idle, so if
        the original worker acks in the meantime we simply don't get that entry.
        """
        if not entry_ids:
            return []
        raw: Any = await self.redis.xclaim(
            stream, GROUP, "relay-reaper", min_idle_time=min_idle_ms, message_ids=list(entry_ids)
        )
        return _entries(raw)

    async def republish(self, stream: str, claimed: Sequence[Entry]) -> None:
        """Put a fresh copy of claimed entries at the tail of the stream (so any live worker
        gets them with a normal `>` read) and drop the old ones."""
        if not claimed:
            return
        async with self.redis.pipeline(transaction=True) as pipe:
            for entry_id, fields in claimed:
                if fields:  # empty = entry was deleted from the stream meanwhile
                    pipe.xadd(stream, {"job_id": fields["job_id"], "type": fields["type"]})
                pipe.xack(stream, GROUP, entry_id)
                pipe.xdel(stream, entry_id)
            await pipe.execute()

    async def delete_consumer(self, stream: str, consumer: str) -> None:
        with contextlib.suppress(ResponseError):
            await self.redis.xgroup_delconsumer(stream, GROUP, consumer)

    # --- stats --------------------------------------------------------------------------

    async def queue_stats(self, now_ms: int) -> list[QueueStats]:
        out: list[QueueStats] = []
        for priority in Priority:
            stream = self.keys.queue(priority)
            depth = int(await self.redis.xlen(stream))
            pending, consumers = 0, 0
            try:
                for g in await self.redis.xinfo_groups(stream):
                    if g["name"] == GROUP:
                        pending, consumers = int(g["pending"]), int(g["consumers"])
            except ResponseError:
                pass
            oldest = 0.0
            if depth:
                first = _entries(await self.redis.xrange(stream, count=1))
                if first:
                    oldest = max(0.0, (now_ms - _entry_ms(first[0][0])) / 1000)
            out.append(QueueStats(priority.value, depth, pending, oldest, consumers))
        return out

    async def delayed_count(self) -> int:
        return int(await self.redis.zcard(self.keys.delayed))

    async def dlq_size(self) -> int:
        return int(await self.redis.xlen(self.keys.dlq))
