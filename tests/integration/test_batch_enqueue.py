"""Batched producing: Relay.enqueue_many() and BatchingProducer (issue #2)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from relay import JobContext, JobSpec, JobStatus, Relay
from relay.app import UnknownJobTypeError
from relay.producer import ProducerClosedError
from relay.worker import Worker
from tests.integration.conftest import wait_for


def register(relay: Relay) -> None:
    @relay.job("add")
    async def add(ctx: JobContext, a: int, b: int) -> int:
        return a + b


async def test_enqueue_many_writes_rows_dispatches_and_keeps_order(relay: Relay) -> None:
    register(relay)
    results = await relay.enqueue_many([JobSpec("add", {"a": i, "b": 1}) for i in range(300)])

    assert [r.job.payload["a"] for r in results] == list(range(300))
    assert all(r.created and r.job.status is JobStatus.QUEUED for r in results)
    assert await relay.store.undispatched(older_than_s=0) == []  # all marked dispatched
    assert sum(s.depth for s in await relay.broker.queue_stats(0)) == 300

    worker = Worker(relay, concurrency=50, block_ms=100)
    task = asyncio.create_task(worker.run())

    async def all_done() -> bool:
        return (await relay.store.status_counts()).get("succeeded", 0) == 300

    await wait_for(all_done, within=60)
    worker.stop()
    await asyncio.wait_for(task, 10)


async def test_enqueue_many_idempotency_across_and_within_batches(relay: Relay) -> None:
    register(relay)
    first = await relay.enqueue("add", {"a": 1, "b": 1}, idempotency_key="k1")
    results = await relay.enqueue_many(
        [
            JobSpec("add", {"a": 2, "b": 2}, idempotency_key="k1"),  # hit on an existing job
            JobSpec("add", {"a": 3, "b": 3}, idempotency_key="k2"),  # new
            JobSpec("add", {"a": 4, "b": 4}, idempotency_key="k2"),  # hit within the batch
            JobSpec("add", {"a": 5, "b": 5}),  # no key: always new
        ]
    )
    assert [r.created for r in results] == [False, True, False, True]
    assert results[0].job.id == first.job.id
    assert results[2].job.id == results[1].job.id
    assert (await relay.store.status_counts()).get("queued") == 3  # k1, k2, keyless
    stats = await relay.broker.queue_stats(0)
    # k1 (from the single enqueue) + the two new jobs; the two idempotent hits sent nothing
    assert sum(s.depth for s in stats) == 3


async def test_enqueue_many_routes_future_jobs_to_the_delayed_set(relay: Relay) -> None:
    register(relay)
    later = datetime.now(UTC) + timedelta(hours=1)
    results = await relay.enqueue_many(
        [JobSpec("add", {"a": 1, "b": 1}), JobSpec("add", {"a": 2, "b": 2}, run_at=later)]
    )
    assert [r.job.status for r in results] == [JobStatus.QUEUED, JobStatus.SCHEDULED]
    assert await relay.broker.redis.zcard(relay.broker.keys.delayed) == 1


async def test_enqueue_many_rejects_the_whole_batch_on_an_unknown_type(relay: Relay) -> None:
    register(relay)
    with pytest.raises(UnknownJobTypeError):
        await relay.enqueue_many([JobSpec("add", {"a": 1, "b": 1}), JobSpec("nope")])
    assert await relay.store.status_counts() == {}  # nothing was written


async def test_batching_producer_coalesces_concurrent_calls(relay: Relay) -> None:
    register(relay)
    async with relay.batching(max_batch=100, max_delay_ms=50) as producer:
        results = await asyncio.gather(
            *(producer.enqueue("add", {"a": i, "b": 0}) for i in range(250))
        )
    assert [r.job.payload["a"] for r in results] == list(range(250))
    assert producer.batches_sent == 3  # 100 + 100 (size) + 50 (timer or close)
    assert (await relay.store.status_counts()).get("queued") == 250


async def test_batching_producer_flushes_a_lone_job_after_the_delay(relay: Relay) -> None:
    register(relay)
    async with relay.batching(max_batch=1000, max_delay_ms=20) as producer:
        result = await asyncio.wait_for(producer.enqueue("add", {"a": 1, "b": 1}), 5)
    assert result.created and producer.batches_sent == 1


async def test_batching_producer_fails_only_the_bad_caller(relay: Relay) -> None:
    register(relay)
    async with relay.batching(max_delay_ms=20) as producer:
        good = asyncio.create_task(producer.enqueue("add", {"a": 1, "b": 1}))
        with pytest.raises(UnknownJobTypeError):
            await producer.enqueue("nope")
        assert (await good).created


async def test_batching_producer_propagates_a_batch_failure_to_every_caller(
    relay: Relay, monkeypatch: pytest.MonkeyPatch
) -> None:
    register(relay)

    async def broken(specs: object) -> object:
        raise ConnectionError("postgres went away")

    monkeypatch.setattr(relay, "enqueue_many", broken)
    async with relay.batching(max_delay_ms=10) as producer:
        outcomes = await asyncio.gather(
            *(producer.enqueue("add", {"a": i, "b": 0}) for i in range(5)),
            return_exceptions=True,
        )
    assert all(isinstance(o, ConnectionError) for o in outcomes)


async def test_closed_producer_refuses_new_jobs(relay: Relay) -> None:
    register(relay)
    producer = relay.batching()
    await producer.aclose()
    with pytest.raises(ProducerClosedError):
        await producer.enqueue("add", {"a": 1, "b": 1})
