"""`Relay`: the SDK object. Register handlers with `@app.job(...)`, enqueue with `app.enqueue(...)`.

    app = Relay()

    @app.job("send_email", timeout_s=30, max_attempts=5)
    async def send_email(ctx: JobContext, to: str, template: str) -> None: ...

    await app.enqueue("send_email", {"to": "a@b.c", "template": "welcome"}, priority="high")

The same object is loaded by the worker (`relay worker --app mymodule:app`) and the API, so
the set of registered job types doubles as the allow-list of what the API accepts.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from relay.broker import RedisBroker
from relay.config import Settings
from relay.db import make_engine, migrate
from relay.models import Job, JobStatus, Priority
from relay.store import JobStore, NewJob

Handler = Callable[..., Coroutine[Any, Any, Any]]


class UnknownJobTypeError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class JobDef:
    name: str
    fn: Handler
    timeout_s: int = 60
    max_attempts: int = 3
    priority: Priority = Priority.DEFAULT


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

    # --- registration -------------------------------------------------------------------

    def job(
        self,
        name: str,
        *,
        timeout_s: int = 60,
        max_attempts: int = 3,
        priority: Priority | str = Priority.DEFAULT,
    ) -> Callable[[Handler], Handler]:
        def register(fn: Handler) -> Handler:
            if name in self.registry:
                raise ValueError(f"job type {name!r} registered twice")
            self.registry[name] = JobDef(name, fn, timeout_s, max_attempts, Priority(priority))
            return fn

        return register

    # --- connections --------------------------------------------------------------------

    @property
    def store(self) -> JobStore:
        if self._store is None:
            self._store = JobStore(make_engine(self.settings.database_url))
        return self._store

    @property
    def broker(self) -> RedisBroker:
        if self._broker is None:
            self._broker = RedisBroker.from_url(self.settings.redis_url, self.settings.namespace)
        return self._broker

    async def setup(self) -> None:
        """Run DB migrations and create the consumer groups. Safe to call repeatedly."""
        await migrate(self.store.engine)
        await self.broker.ensure_groups()

    async def close(self) -> None:
        if self._broker is not None:
            await self._broker.close()
            self._broker = None
        if self._store is not None:
            await self._store.engine.dispose()
            self._store = None

    # --- producing ----------------------------------------------------------------------

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
        definition = self.registry.get(job_type)
        if self.registry and definition is None:
            raise UnknownJobTypeError(job_type)
        new = NewJob(
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
        job, created = await self.store.create(new)
        if created:
            await self.dispatch(job)
        return EnqueueResult(job, created)

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

    async def replay_dead_letter(self, entry_id: str) -> Job | None:
        letter = await self.broker.get_dead_letter(entry_id)
        if letter is None:
            return None
        job = await self.retry(letter.job_id)
        await self.broker.delete_dead_letter(entry_id)
        return job
