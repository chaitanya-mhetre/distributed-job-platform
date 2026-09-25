"""Helpers to run workers/schedulers inside a test's event loop."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

from relay import JobStatus, Relay
from relay.scheduler import Scheduler
from relay.worker import Worker
from tests.integration.conftest import wait_for

FAST_WORKER: dict[str, Any] = {
    "block_ms": 100,
    "heartbeat_s": 0.2,
    "heartbeat_ttl_s": 2,
    "cancel_poll_s": 0.05,
    "backoff_base_s": 0.05,
    "backoff_cap_s": 0.2,
    "drain_timeout_s": 5.0,
}
FAST_SCHEDULER: dict[str, Any] = {"tick_s": 0.05, "dead_idle_ms": 0, "reconcile_every": 1}


@asynccontextmanager
async def running_worker(relay: Relay, **kw: Any) -> AsyncIterator[Worker]:
    worker = Worker(relay, **(FAST_WORKER | kw))
    task = asyncio.create_task(worker.run())
    try:
        yield worker
    finally:
        worker.stop()
        await asyncio.wait_for(task, 20)


@asynccontextmanager
async def running_scheduler(relay: Relay, **kw: Any) -> AsyncIterator[Scheduler]:
    sched = Scheduler(relay, **(FAST_SCHEDULER | kw))
    task = asyncio.create_task(sched.run())
    try:
        yield sched
    finally:
        sched.stop()
        await asyncio.wait_for(task, 10)


async def wait_status(relay: Relay, job_id: UUID, *statuses: JobStatus, within: float = 20) -> None:
    async def check() -> bool:
        job = await relay.store.get(job_id)
        return job is not None and job.status in statuses

    await wait_for(check, within=within)


async def wait_all_terminal(relay: Relay, ids: list[UUID], within: float = 60) -> None:
    async def check() -> bool:
        jobs = await relay.store.get_many(ids)
        return len(jobs) == len(ids) and all(j.status.is_terminal for j in jobs.values())

    await wait_for(check, within=within)
