"""M4 acceptance: fencing-token locks, per-type concurrency limits, cancellation."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from relay import JobContext, JobStatus, Relay
from relay.api import create_app
from relay.locks import LockNotAcquiredError
from relay.store import fenced_write
from tests.integration.helpers import (
    running_scheduler,
    running_worker,
    wait_all_terminal,
    wait_status,
)


async def test_fencing_token_rejects_a_stale_lock_holder(relay: Relay) -> None:
    """The classic failure: A's lease expires while A is paused, B takes the lock and writes,
    then A wakes up and writes too. The lock can't stop A; the fencing token does."""
    locks, engine = relay.locks, relay.store.engine

    a = await locks.acquire("invoice:42", ttl_ms=200)
    await asyncio.sleep(0.3)  # A "pauses" (GC, network stall...) and its lease expires
    b = await locks.acquire("invoice:42", ttl_ms=5_000, wait_s=1)
    assert b.token > a.token

    assert await fenced_write(engine, "invoice:42", b.token, {"by": "B"}) is True
    assert await fenced_write(engine, "invoice:42", a.token, {"by": "A"}) is False  # rejected

    assert await locks.release(a) is False  # A can't delete B's lock either
    assert await locks.release(b) is True


async def test_lock_gives_mutual_exclusion_across_workers(relay: Relay) -> None:
    redis = relay.broker.redis
    counter_key = f"{relay.settings.namespace}:counter"

    @relay.job("increment")
    async def increment(ctx: JobContext) -> None:
        async with ctx.lock("counter", ttl_ms=5_000, wait_s=30):
            # read-modify-write with a gap: loses updates without the lock
            value = int(await redis.get(counter_key) or 0)
            await asyncio.sleep(0.01)
            await redis.set(counter_key, value + 1)

    ids = [(await relay.enqueue("increment")).job.id for _ in range(30)]
    async with running_worker(relay, concurrency=10), running_worker(relay, concurrency=10):
        await wait_all_terminal(relay, ids)
    assert int(await redis.get(counter_key) or 0) == 30


async def test_lock_wait_times_out(relay: Relay) -> None:
    held = await relay.locks.acquire("busy", ttl_ms=5_000)
    with pytest.raises(LockNotAcquiredError):
        await relay.locks.acquire("busy", ttl_ms=1_000, wait_s=0.2)
    await relay.locks.release(held)


async def test_per_type_concurrency_limit_holds_across_workers(relay: Relay) -> None:
    running = 0
    peak = 0

    @relay.job("call_llm", max_concurrent=2)
    async def call_llm(ctx: JobContext) -> None:
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.15)
        running -= 1

    ids = [(await relay.enqueue("call_llm")).job.id for _ in range(12)]
    async with (
        running_scheduler(relay),
        running_worker(relay, concurrency=10),
        running_worker(relay, concurrency=10),
    ):
        await wait_all_terminal(relay, ids, within=60)

    assert peak == 2
    jobs = await relay.store.get_many(ids)
    assert all(j.status is JobStatus.SUCCEEDED and j.attempts == 1 for j in jobs.values())


async def test_cancel_queued_job_never_runs(relay: Relay) -> None:
    ran: list[str] = []

    @relay.job("x")
    async def x(ctx: JobContext) -> None:
        ran.append("x")

    job = (await relay.enqueue("x")).job
    cancelled = await relay.cancel(job.id)
    assert cancelled is not None and cancelled.status is JobStatus.CANCELLED
    async with running_worker(relay):
        await asyncio.sleep(0.5)
    assert ran == []
    assert sum(s.depth for s in await relay.broker.queue_stats(0)) == 0  # stale entry acked


async def test_cancel_scheduled_job_removes_it_from_the_delayed_set(relay: Relay) -> None:
    from datetime import UTC, datetime, timedelta

    @relay.job("later")
    async def later(ctx: JobContext) -> None: ...

    job = (await relay.enqueue("later", run_at=datetime.now(UTC) + timedelta(hours=1))).job
    assert await relay.broker.delayed_count() == 1
    await relay.cancel(job.id)
    assert await relay.broker.delayed_count() == 0


async def test_cancel_running_job_cancels_the_handler(relay: Relay) -> None:
    started = asyncio.Event()
    saw_cancel = asyncio.Event()

    @relay.job("long", timeout_s=60)
    async def long(ctx: JobContext) -> None:
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            saw_cancel.set()
            raise

    job = (await relay.enqueue("long")).job
    async with running_worker(relay):
        await asyncio.wait_for(started.wait(), 10)
        still_running = await relay.cancel(job.id)
        assert still_running is not None and still_running.status is JobStatus.RUNNING
        await wait_status(relay, job.id, JobStatus.CANCELLED, within=5)
    assert saw_cancel.is_set()
    assert [a.outcome for a in await relay.store.attempts(job.id)] == ["cancelled"]


async def test_raise_if_cancelled_for_cpu_style_loops(relay: Relay) -> None:
    started = asyncio.Event()

    @relay.job("crunch", timeout_s=60)
    async def crunch(ctx: JobContext) -> None:
        started.set()
        for _ in range(10_000):
            await ctx.raise_if_cancelled()
            await asyncio.sleep(0.01)

    job = (await relay.enqueue("crunch")).job
    # slow cancel poll so the explicit check is what notices the flag
    async with running_worker(relay, cancel_poll_s=30):
        await asyncio.wait_for(started.wait(), 10)
        await relay.cancel(job.id)
        await wait_status(relay, job.id, JobStatus.CANCELLED, within=5)


async def test_cancel_api(relay: Relay) -> None:
    @relay.job("y")
    async def y(ctx: JobContext) -> None: ...

    job = (await relay.enqueue("y")).job
    transport = httpx.ASGITransport(app=create_app(relay))
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post(f"/v1/jobs/{job.id}:cancel")
        assert r.status_code == 202 and r.json()["status"] == "cancelled"
        assert (await c.post(f"/v1/jobs/{job.id}:cancel")).status_code == 409
        missing = "00000000-0000-0000-0000-000000000000"
        assert (await c.post(f"/v1/jobs/{missing}:cancel")).status_code == 404
