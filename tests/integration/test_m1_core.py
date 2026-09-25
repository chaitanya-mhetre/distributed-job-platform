"""M1 acceptance: submit 1,000 jobs and all complete, with correct status history."""

from __future__ import annotations

import asyncio
from typing import Any

from relay import JobContext, JobStatus, Relay
from relay.worker import Worker
from tests.integration.conftest import wait_for


async def run_worker(relay: Relay, **kw: Any) -> tuple[Worker, asyncio.Task[None]]:
    worker = Worker(relay, block_ms=100, **kw)
    return worker, asyncio.create_task(worker.run())


async def stop(worker: Worker, task: asyncio.Task[None]) -> None:
    worker.stop()
    await asyncio.wait_for(task, 10)


async def test_thousand_jobs_all_complete(relay: Relay) -> None:
    @relay.job("add")
    async def add(ctx: JobContext, a: int, b: int) -> int:
        return a + b

    ids = [(await relay.enqueue("add", {"a": i, "b": 1})).job.id for i in range(1000)]
    worker, task = await run_worker(relay, concurrency=50)

    async def all_done() -> bool:
        return (await relay.store.status_counts()).get("succeeded", 0) == 1000

    await wait_for(all_done, within=60)
    await stop(worker, task)

    jobs = await relay.store.get_many(ids)
    assert {j.result - j.payload["a"] for j in jobs.values()} == {1}
    assert all(j.attempts == 1 and j.status is JobStatus.SUCCEEDED for j in jobs.values())
    stats = await relay.broker.queue_stats(0)
    assert sum(s.depth + s.pending for s in stats) == 0  # acked entries are deleted


async def test_failure_is_recorded_with_attempt_history(relay: Relay) -> None:
    @relay.job("boom", max_attempts=1)
    async def boom(ctx: JobContext) -> None:
        raise ValueError("nope")

    job = (await relay.enqueue("boom")).job
    worker, task = await run_worker(relay)

    async def finished() -> bool:
        j = await relay.store.get(job.id)
        return j is not None and j.status.is_terminal

    await wait_for(finished)
    await stop(worker, task)
    history = await relay.store.attempts(job.id)
    assert len(history) == 1
    assert history[0].error is not None and "ValueError: nope" in history[0].error
    assert history[0].worker_id == worker.worker_id


async def test_unknown_type_rejected(relay: Relay) -> None:
    @relay.job("known")
    async def known(ctx: JobContext) -> None: ...

    import pytest

    from relay.app import UnknownJobTypeError

    with pytest.raises(UnknownJobTypeError):
        await relay.enqueue("unknown")
