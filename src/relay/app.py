"""`Relay`: the SDK object. Register handlers with `@app.job(...)`, enqueue with `app.enqueue(...)`.

    app = Relay()

    @app.job("send_email", timeout_s=30, max_attempts=5)
    async def send_email(ctx: JobContext, to: str, template: str) -> None: ...

    await app.enqueue("send_email", {"to": "a@b.c", "template": "welcome"}, priority="high")

The same object is loaded by the worker (`relay worker --app mymodule:app`) and the API, so
the set of registered job types doubles as the allow-list of what the API accepts.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from relay import metrics
from relay.broker import RedisBroker
from relay.config import Settings
from relay.db import make_engine, migrate
from relay.locks import LockManager
from relay.models import Job, JobStatus, Priority
from relay.recurring import validate_cron
from relay.store import JobStore, NewJob, NewRecurring, RecurringJob

if TYPE_CHECKING:
    from relay.producer import BatchingProducer

Handler = Callable[..., Coroutine[Any, Any, Any]]


class UnknownJobTypeError(ValueError):
    pass


class QueueFullError(RuntimeError):
    """Backpressure: the target queue is deeper than settings.max_queue_depth."""


@dataclass(frozen=True, slots=True)
class JobDef:
    name: str
    fn: Handler
    timeout_s: int = 60
    max_attempts: int = 3
    priority: Priority = Priority.DEFAULT
    # at most this many jobs of this type run at once across *all* workers (None = no limit)
    max_concurrent: int | None = None


@dataclass(frozen=True, slots=True)
class JobSpec:
    """One item for Relay.enqueue_many(): the same arguments enqueue() takes."""

    type: str
    payload: dict[str, Any] | None = None
    priority: Priority | str | None = None
    run_at: datetime | None = None
    max_attempts: int | None = None
    timeout_s: int | None = None
    idempotency_key: str | None = None


@dataclass(frozen=True, slots=True)
class EnqueueResult:
    job: Job
    created: bool  # False = idempotent hit, an existing job was returned


class Relay:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings()
        self.registry: dict[str, JobDef] = {}
        self._store: JobStore | None = None
        self._broker: RedisBroker | None = None
        self._locks: LockManager | None = None

    # --- registration -------------------------------------------------------------------

    def job(
        self,
        name: str,
        *,
        timeout_s: int = 60,
        max_attempts: int = 3,
        priority: Priority | str = Priority.DEFAULT,
        max_concurrent: int | None = None,
    ) -> Callable[[Handler], Handler]:
        def register(fn: Handler) -> Handler:
            if name in self.registry:
                raise ValueError(f"job type {name!r} registered twice")
            self.registry[name] = JobDef(
                name, fn, timeout_s, max_attempts, Priority(priority), max_concurrent
            )
            return fn

        return register

    # --- connections --------------------------------------------------------------------

    @property
    def store(self) -> JobStore:
        if self._store is None:
            self._store = JobStore(
                make_engine(
                    self.settings.database_url,
                    self.settings.db_pool_size,
                    self.settings.db_max_overflow,
                )
            )
        return self._store

    @property
    def broker(self) -> RedisBroker:
        if self._broker is None:
            self._broker = RedisBroker.from_url(self.settings.redis_url, self.settings.namespace)
        return self._broker

    @property
    def locks(self) -> LockManager:
        if self._locks is None:
            self._locks = LockManager(self.broker.redis, self.broker.keys)
        return self._locks

    async def setup(self) -> None:
        """Run DB migrations and create the consumer groups. Safe to call repeatedly."""
        await migrate(self.store.engine)
        await self.broker.ensure_groups()

    async def close(self) -> None:
        if self._broker is not None:
            await self._broker.close()
            self._broker = None
            self._locks = None
        if self._store is not None:
            await self._store.engine.dispose()
            self._store = None

    # --- producing ----------------------------------------------------------------------

    def _new_job(
        self,
        job_type: str,
        payload: dict[str, Any] | None = None,
        *,
        priority: Priority | str | None = None,
        run_at: datetime | None = None,
        max_attempts: int | None = None,
        timeout_s: int | None = None,
        idempotency_key: str | None = None,
    ) -> NewJob:
        """Validate the job type and fill defaults from its @app.job registration."""
        definition = self.registry.get(job_type)
        if self.registry and definition is None:
            raise UnknownJobTypeError(job_type)
        return NewJob(
            type=job_type,
            payload=payload or {},
            priority=Priority(priority)
            if priority
            else (definition.priority if definition else Priority.DEFAULT),
            run_at=run_at,
            max_attempts=max_attempts or (definition.max_attempts if definition else 3),
            timeout_s=timeout_s or (definition.timeout_s if definition else 60),
            idempotency_key=idempotency_key,
        )

    async def _check_backpressure(self, priorities: set[Priority]) -> None:
        if not self.settings.max_queue_depth:
            return
        for priority in priorities:
            depth = await self.broker.redis.xlen(self.broker.keys.queue(priority))
            if depth >= self.settings.max_queue_depth:
                metrics.REJECTED.labels(priority.value).inc()
                raise QueueFullError(f"queue {priority} has {depth} jobs waiting")

    async def enqueue(
        self,
        job_type: str,
        payload: dict[str, Any] | None = None,
        *,
        priority: Priority | str | None = None,
        run_at: datetime | None = None,
        max_attempts: int | None = None,
        timeout_s: int | None = None,
        idempotency_key: str | None = None,
    ) -> EnqueueResult:
        new = self._new_job(
            job_type,
            payload,
            priority=priority,
            run_at=run_at,
            max_attempts=max_attempts,
            timeout_s=timeout_s,
            idempotency_key=idempotency_key,
        )
        await self._check_backpressure({new.priority})
        job, created = await self.store.create(new)
        if created:
            await self.dispatch(job)
            metrics.ENQUEUED.labels(job.type, job.priority.value).inc()
        return EnqueueResult(job, created)

    async def enqueue_many(self, specs: Sequence[JobSpec]) -> list[EnqueueResult]:
        """Enqueue a batch with three round trips in total instead of three per job:
        one multi-row INSERT, one Redis pipeline, one UPDATE of dispatched_at.

        Same guarantees as enqueue(): Postgres first, Redis second, and rows that never got
        marked dispatched are re-sent by the scheduler's reconciliation loop. The whole batch
        is validated before anything is written, so one unknown job type rejects the batch.
        Results come back in input order.
        """
        news = [
            self._new_job(
                s.type,
                s.payload,
                priority=s.priority,
                run_at=s.run_at,
                max_attempts=s.max_attempts,
                timeout_s=s.timeout_s,
                idempotency_key=s.idempotency_key,
            )
            for s in specs
        ]
        if not news:
            return []
        await self._check_backpressure({n.priority for n in news})
        rows = await self.store.create_many(news)
        fresh = [job for job, created in rows if created]
        if fresh:
            await self.broker.enqueue_many(fresh)
            await self.store.mark_dispatched([j.id for j in fresh])
            for job in fresh:
                metrics.ENQUEUED.labels(job.type, job.priority.value).inc()
        return [EnqueueResult(job, created) for job, created in rows]

    def batching(self, max_batch: int = 200, max_delay_ms: float = 5.0) -> BatchingProducer:
        """A producer that coalesces concurrent enqueue() calls into enqueue_many() batches.

        async with app.batching() as producer:
            await producer.enqueue("send_email", {...})
        """
        from relay.producer import BatchingProducer  # local: relay.producer imports this module

        return BatchingProducer(self, max_batch=max_batch, max_delay_ms=max_delay_ms)

    async def dispatch(self, job: Job) -> None:
        """Hand a job to Redis (stream or delayed ZSET), then record that we did.

        Postgres first, Redis second: a job can never exist in Redis without a row. The crash
        window between the two is closed by the scheduler's reconciliation loop, which
        re-dispatches rows whose dispatched_at is still NULL.
        """
        if job.status is JobStatus.SCHEDULED:
            # Even if run_at has already passed: the scheduler promotes it and flips the row to
            # 'queued' in one place, so status and location never disagree.
            await self.broker.schedule(job.id, job.type, job.priority, job.run_at)
        else:
            await self.broker.enqueue(job.id, job.type, job.priority)
        await self.store.mark_dispatched([job.id])

    async def get(self, job_id: UUID) -> Job | None:
        return await self.store.get(job_id)

    # --- operator actions ---------------------------------------------------------------

    async def retry(self, job_id: UUID) -> Job | None:
        """Manually re-run a failed or dead job with a fresh attempt budget."""
        job = await self.store.mark_queued(job_id, reset_attempts=True)
        if job is not None:
            await self.dispatch(job)
        return job

    async def cancel(self, job_id: UUID) -> Job | None:
        """Cancel a job. Returns None if it doesn't exist or is already finished.

        queued/scheduled: marked cancelled right away (a stale stream entry is skipped by the
        worker, a delayed one is removed from the ZSET).
        running: a flag is set; the worker cancels the handler's task within ~cancel_poll_s,
        and the handler sees asyncio.CancelledError. The returned job still says 'running'.
        """
        job = await self.store.cancel_if_pending(job_id)
        if job is not None:
            await self.broker.unschedule(job_id)
            return job
        job = await self.store.get(job_id)
        if job is None or job.status.is_terminal:
            return None
        await self.broker.redis.set(self.broker.keys.cancel(str(job_id)), "1", ex=3600)
        return job

    async def replay_dead_letter(self, entry_id: str) -> Job | None:
        letter = await self.broker.get_dead_letter(entry_id)
        if letter is None:
            return None
        job = await self.retry(letter.job_id)
        await self.broker.delete_dead_letter(entry_id)
        return job

    async def add_recurring(
        self,
        name: str,
        cron: str,
        job_type: str,
        payload: dict[str, Any] | None = None,
        *,
        priority: Priority | str = Priority.DEFAULT,
    ) -> RecurringJob:
        validate_cron(cron)
        if self.registry and job_type not in self.registry:
            raise UnknownJobTypeError(job_type)
        return await self.store.create_recurring(
            NewRecurring(name, cron, job_type, payload or {}, Priority(priority))
        )
