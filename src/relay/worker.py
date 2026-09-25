"""Worker: reads job ids from Redis, runs handlers, records outcomes in Postgres.

One worker process = one asyncio event loop running up to `concurrency` jobs at once.
Background tasks inside the worker:

  heartbeat   every `heartbeat_s`: refresh relay:hb:{worker_id} (TTL `heartbeat_ttl_s`) and reset
              the idle time of in-flight stream entries (XCLAIM ... JUSTID to ourselves), so the
              scheduler can tell "slow job on a live worker" from "worker is gone".
  cancel      every 0.5 s: check relay:cancel:{job_id} for running jobs and cancel their tasks.

Outcome of one execution (always written to Postgres *before* the stream entry is acked, so a
crash in between means "run again", never "lost"):

  handler returns          -> succeeded
  PermanentError           -> failed (no retry)
  other error / timeout    -> scheduled for retry with backoff, or dead + DLQ when attempts run out
  cancel requested         -> cancelled
  worker shutting down     -> nothing written, entry left unacked -> reclaimed by the scheduler
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import socket
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

from relay import metrics
from relay.app import Relay
from relay.backoff import backoff_seconds
from relay.broker import DEFAULT_WEIGHTS, Message, WeightedOrder
from relay.context import JobContext
from relay.keys import GROUP
from relay.locks import TypeSlots
from relay.models import Job, JobStatus, PermanentError, Priority

log = logging.getLogger("relay.worker")

# How long a handler gets to react to cancellation before we give up on it.
CANCEL_GRACE_S = 5.0


def make_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"


class StopReason(StrEnum):
    NONE = ""
    CANCEL = "cancel"  # user asked to cancel this job
    SHUTDOWN = "shutdown"  # worker is stopping and the drain timeout expired


@dataclass
class InFlight:
    msg: Message
    task: asyncio.Task[Any] | None = None
    stop_reason: StopReason = StopReason.NONE
    slot_type: str | None = None  # set while holding a per-type concurrency slot


@dataclass
class WorkerConfig:
    concurrency: int = 10
    block_ms: int = 1000
    heartbeat_s: float = 5.0
    heartbeat_ttl_s: int = 15
    drain_timeout_s: float = 30.0
    cancel_poll_s: float = 0.5
    backoff_base_s: float = 1.0
    backoff_cap_s: float = 300.0
    queues: list[Priority] = field(default_factory=lambda: list(Priority))
    # weighted polling: high is tried first 6 times in 10, default 3, low 1 (no starvation)
    weights: dict[Priority, int] = field(default_factory=lambda: dict(DEFAULT_WEIGHTS))


class Worker:
    def __init__(self, relay: Relay, config: WorkerConfig | None = None, **overrides: Any) -> None:
        self.relay = relay
        self.cfg = config or WorkerConfig(**overrides)
        self.worker_id = make_worker_id()
        self._inflight: dict[uuid.UUID, InFlight] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._stopping = asyncio.Event()
        self._weighted = WeightedOrder({p: self.cfg.weights[p] for p in self.cfg.queues})
        self._slots = TypeSlots(
            relay.broker.redis, relay.broker.keys, ttl_ms=self.cfg.heartbeat_ttl_s * 1000
        )

    # --- lifecycle ----------------------------------------------------------------------

    def stop(self) -> None:
        """Begin graceful shutdown (called from the SIGTERM handler)."""
        self._stopping.set()

    async def run(self) -> None:
        await self.relay.setup()
        await self._beat()  # heartbeat exists before the first read, so we're never "dead"
        await self.relay.broker.redis.sadd(self.relay.broker.keys.workers, self.worker_id)
        background = [
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._cancel_loop()),
        ]
        log.info("worker started", extra={"worker_id": self.worker_id})
        try:
            await self._consume()
        finally:
            await self._drain()
            for t in background:
                t.cancel()
            await asyncio.gather(*background, return_exceptions=True)
            await self._deregister()
            log.info("worker stopped", extra={"worker_id": self.worker_id})

    async def _consume(self) -> None:
        broker = self.relay.broker
        while not self._stopping.is_set():
            free = self.cfg.concurrency - len(self._tasks)
            if free <= 0:
                # every slot busy: wait for one to free up (or for shutdown)
                await asyncio.wait(
                    [*self._tasks, asyncio.create_task(self._stopping.wait())],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                continue
            msgs = await broker.read(self.worker_id, self._order(), free, self.cfg.block_ms)
            for msg in msgs:
                self._inflight[msg.job_id] = InFlight(msg)
                task = asyncio.create_task(self._handle(msg))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)

    def _order(self) -> list[Priority]:
        return self._weighted.next_order()

    async def _drain(self) -> None:
        """Let in-flight jobs finish for up to drain_timeout_s, then stop them *without* acking,
        so the scheduler hands them to another worker."""
        if not self._tasks:
            return
        log.info("draining", extra={"worker_id": self.worker_id, "inflight": len(self._tasks)})
        _, pending = await asyncio.wait(self._tasks, timeout=self.cfg.drain_timeout_s)
        if pending:
            for entry in self._inflight.values():
                entry.stop_reason = StopReason.SHUTDOWN
                if entry.task:
                    entry.task.cancel()
            _, stuck = await asyncio.wait(pending, timeout=CANCEL_GRACE_S)
            for t in stuck:
                t.cancel()
            await asyncio.gather(*stuck, return_exceptions=True)

    async def _deregister(self) -> None:
        keys = self.relay.broker.keys
        redis = self.relay.broker.redis
        # Deleting the heartbeat right away lets the scheduler reclaim anything we left unacked
        # immediately, instead of waiting for the TTL to expire.
        await redis.delete(keys.heartbeat(self.worker_id))
        await redis.srem(keys.workers, self.worker_id)

    # --- background loops ---------------------------------------------------------------

    async def _beat(self) -> None:
        keys = self.relay.broker.keys
        await self.relay.broker.redis.set(
            keys.heartbeat(self.worker_id), str(time.time()), ex=self.cfg.heartbeat_ttl_s
        )

    async def _heartbeat_loop(self) -> None:
        redis = self.relay.broker.redis
        while True:
            await asyncio.sleep(self.cfg.heartbeat_s)
            try:
                await self._beat()
                by_stream: dict[str, list[Any]] = defaultdict(list)
                for entry in list(self._inflight.values()):
                    by_stream[entry.msg.stream].append(entry.msg.entry_id)
                now_ms = int(time.time() * 1000)
                for jid, entry in list(self._inflight.items()):
                    if entry.slot_type:
                        await self._slots.refresh(entry.slot_type, str(jid), now_ms)
                for stream, ids in by_stream.items():
                    # Re-claiming our own entries resets their idle time. XCLAIM only touches
                    # entries that are still pending, so it can't resurrect acked ones.
                    await redis.xclaim(
                        stream,
                        GROUP,
                        self.worker_id,
                        min_idle_time=0,
                        message_ids=ids,
                        justid=True,
                    )
            except Exception:  # noqa: BLE001 - a missed beat must not kill the worker
                log.exception("heartbeat failed", extra={"worker_id": self.worker_id})

    async def _cancel_loop(self) -> None:
        broker = self.relay.broker
        while True:
            await asyncio.sleep(self.cfg.cancel_poll_s)
            running = [(jid, e) for jid, e in self._inflight.items() if e.task is not None]
            if not running:
                continue
            try:
                flags = await broker.redis.mget([broker.keys.cancel(str(j)) for j, _ in running])
            except Exception:  # noqa: BLE001
                log.exception("cancel poll failed")
                continue
            for (job_id, entry), flag in zip(running, flags, strict=True):
                if flag and entry.task and not entry.task.done():
                    log.info("cancelling job", extra={"job_id": str(job_id)})
                    entry.stop_reason = StopReason.CANCEL
                    entry.task.cancel()

    # --- executing one job --------------------------------------------------------------

    async def _handle(self, msg: Message) -> None:
        try:
            await self._execute(msg)
        except Exception:  # noqa: BLE001
            # Infrastructure error (DB/Redis down mid-job). Leave the entry unacked: the
            # scheduler re-queues it once it has been idle long enough.
            log.exception("job handling crashed", extra={"job_id": str(msg.job_id)})
        finally:
            self._inflight.pop(msg.job_id, None)

    async def _execute(self, msg: Message) -> None:
        definition = self.relay.registry.get(msg.job_type)
        if definition is not None and definition.max_concurrent is not None:
            if not await self._slots.acquire(
                msg.job_type, str(msg.job_id), definition.max_concurrent, int(time.time() * 1000)
            ):
                await self._defer(msg)
                return
            self._inflight[msg.job_id].slot_type = msg.job_type
            try:
                await self._run(msg)
            finally:
                await self._slots.release(msg.job_type, str(msg.job_id))
        else:
            await self._run(msg)

    async def _defer(self, msg: Message) -> None:
        """Per-type limit is full: put the job back in the delayed set for a moment. This does
        not use up an attempt; the job simply waits for a free slot."""
        store, broker = self.relay.store, self.relay.broker
        run_at = datetime.now(UTC) + timedelta(seconds=random.uniform(0.1, 0.5))  # noqa: S311
        job = await store.defer(msg.job_id, run_at)
        metrics.DEFERRED.labels(msg.job_type).inc()
        if job is not None:
            await broker.schedule(job.id, job.type, job.priority, run_at)
        await broker.ack(msg)

    async def _run(self, msg: Message) -> None:
        store, broker = self.relay.store, self.relay.broker
        job = await store.start_attempt(msg.job_id, self.worker_id)
        if job is None:
            # Not 'queued': cancelled before it ran, already finished (duplicate delivery after
            # a crash-before-ack), or deferred. Either way there's nothing to run.
            await broker.ack(msg)
            return
        definition = self.relay.registry.get(job.type)
        if definition is None:
            await self._record(job, msg, "failed", f"no handler registered for {job.type!r}", 0)
            return

        entry = self._inflight[msg.job_id]
        wait_s = (datetime.now(UTC) - job.run_at).total_seconds()
        metrics.QUEUE_LATENCY.labels(job.priority.value).observe(max(0.0, wait_s))
        ctx = JobContext(job, self.worker_id, self.relay)
        started = time.monotonic()
        entry.task = asyncio.create_task(definition.fn(ctx, **job.payload))
        done, _ = await asyncio.wait({entry.task}, timeout=job.timeout_s)
        timed_out = not done
        if timed_out:
            entry.task.cancel()
            done, _ = await asyncio.wait({entry.task}, timeout=CANCEL_GRACE_S)
            if not done:
                # The handler swallowed CancelledError. We can't kill a coroutine, so we stop
                # waiting for it and record the timeout; it is logged so someone can fix it.
                log.error("handler ignored cancellation", extra={"job_id": str(job.id)})
        ms = int((time.monotonic() - started) * 1000)

        stopped = entry.task.cancelled()
        if stopped and entry.stop_reason is StopReason.SHUTDOWN:
            await store.close_attempt(job.id, job.attempts, "interrupted", "worker shutdown", ms)
            return  # no ack: the scheduler re-queues it
        if stopped and (entry.stop_reason is StopReason.CANCEL or ctx.cancel_requested):
            await self._record(job, msg, "cancelled", None, ms)
            return
        if timed_out:
            await self._record(job, msg, "timeout", f"timed out after {job.timeout_s}s", ms)
            return

        exc = entry.task.exception() if not entry.task.cancelled() else None
        if exc is None and not entry.task.cancelled():
            await self._record(job, msg, "succeeded", None, ms, result=entry.task.result())
        elif isinstance(exc, PermanentError):
            await self._record(job, msg, "failed", f"PermanentError: {exc}", ms)
        else:
            err = f"{type(exc).__name__}: {exc}" if exc else "cancelled by handler"
            await self._record(job, msg, "error", err, ms)

    async def _record(
        self,
        job: Job,
        msg: Message,
        outcome: str,
        error: str | None,
        ms: int,
        result: Any = None,
    ) -> None:
        """Write the outcome to Postgres, do any Redis follow-up, then ack."""
        store, broker = self.relay.store, self.relay.broker
        attempt_outcome = outcome
        done = "finished_at = now(), locked_by = NULL"

        async def finish(target: JobStatus, label: str, sets: str, **params: Any) -> bool:
            job_after = await store.finish(
                job.id,
                target,
                attempt=job.attempts,
                outcome=label,
                error=error,
                duration_ms=ms,
                sets=sets,
                params=params,
            )
            return job_after is not None

        if outcome == "succeeded":
            await finish(
                JobStatus.SUCCEEDED,
                outcome,
                f"result = CAST(:result AS jsonb), last_error = NULL, {done}",
                result=json.dumps(result),
            )
        elif outcome == "cancelled":
            await finish(JobStatus.CANCELLED, outcome, done)
            await broker.redis.delete(broker.keys.cancel(str(job.id)))
        elif outcome == "failed":
            await finish(JobStatus.FAILED, outcome, f"last_error = :err, {done}", err=error)
        else:  # error / timeout: retry or give up
            assert error is not None
            if job.attempts < job.max_attempts:
                delay = backoff_seconds(
                    job.attempts, base_s=self.cfg.backoff_base_s, cap_s=self.cfg.backoff_cap_s
                )
                run_at = datetime.now(UTC) + timedelta(seconds=delay)
                attempt_outcome = "retry" if outcome == "error" else "timeout"
                if await finish(
                    JobStatus.SCHEDULED,
                    attempt_outcome,
                    "run_at = :run_at, last_error = :err, locked_by = NULL",
                    run_at=run_at,
                    err=error,
                ):
                    await broker.schedule(job.id, job.type, job.priority, run_at)
            else:
                attempt_outcome = "dead"
                if await finish(JobStatus.DEAD, "dead", f"last_error = :err, {done}", err=error):
                    await broker.dead_letter(job.id, job.type, error)
        await broker.ack(msg)
        metrics.JOBS.labels(job.type, attempt_outcome).inc()
        metrics.DURATION.labels(job.type).observe(ms / 1000)
        if attempt_outcome in ("retry", "timeout") and job.attempts < job.max_attempts:
            metrics.RETRIES.labels(job.type).inc()
        log.info(
            "job finished",
            extra={
                "job_id": str(job.id),
                "job_type": job.type,
                "attempt": job.attempts,
                "outcome": attempt_outcome,
                "duration_ms": ms,
            },
        )
