"""M3 acceptance: priorities without starvation, delayed jobs, idempotency, recurring jobs
with two schedulers (leader election)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import httpx

from relay import JobContext, JobStatus, Priority, Relay
from relay.api import create_app
from relay.scheduler import Scheduler
from tests.integration.conftest import wait_for
from tests.integration.helpers import (
    FAST_SCHEDULER,
    running_scheduler,
    running_worker,
    wait_all_terminal,
    wait_status,
)


async def test_low_priority_is_not_starved_by_a_high_priority_flood(relay: Relay) -> None:
    order: list[str] = []

    @relay.job("task")
    async def task(ctx: JobContext, tag: str) -> None:
        order.append(tag)
        await asyncio.sleep(0.002)

    high = [
        (await relay.enqueue("task", {"tag": "high"}, priority="high")).job.id for _ in range(200)
    ]
    low = [(await relay.enqueue("task", {"tag": "low"}, priority="low")).job.id for _ in range(20)]
    async with running_worker(relay, concurrency=1):
        await wait_all_terminal(relay, high + low)

    # With strict priority every low job would run after all 200 highs.
    # Weighted 6:3:1 polling (default queue empty) serves low ~1 in 7 reads.
    first_half = order[:110]
    assert first_half.count("low") >= 10
    assert order[:5].count("high") >= 4  # but high is still clearly favoured


async def test_high_priority_overtakes_a_backlog(relay: Relay) -> None:
    started: dict[str, datetime] = {}

    @relay.job("t")
    async def t(ctx: JobContext, tag: str) -> None:
        started.setdefault(tag, datetime.now(UTC))
        await asyncio.sleep(0.005)

    for _ in range(100):
        await relay.enqueue("t", {"tag": "backlog"}, priority="low")
    urgent = (await relay.enqueue("t", {"tag": "urgent"}, priority="high")).job
    async with running_worker(relay, concurrency=1):
        await wait_status(relay, urgent.id, JobStatus.SUCCEEDED)
    done = (await relay.store.status_counts()).get("succeeded", 0)
    assert done < 20  # urgent ran long before the backlog drained


async def test_run_at_delays_execution(relay: Relay) -> None:
    @relay.job("later")
    async def later(ctx: JobContext) -> str:
        return datetime.now(UTC).isoformat()

    run_at = datetime.now(UTC) + timedelta(seconds=1.5)
    job = (await relay.enqueue("later", run_at=run_at)).job
    assert job.status is JobStatus.SCHEDULED
    async with running_scheduler(relay), running_worker(relay):
        await wait_status(relay, job.id, JobStatus.SUCCEEDED)
    done = await relay.store.get(job.id)
    assert done is not None
    assert datetime.fromisoformat(done.result) >= run_at


async def test_duplicate_submission_returns_the_same_job(relay: Relay) -> None:
    @relay.job("charge")
    async def charge(ctx: JobContext, order: str) -> None: ...

    a = await relay.enqueue("charge", {"order": "o-1"}, idempotency_key="order-o-1")
    b = await relay.enqueue("charge", {"order": "o-1"}, idempotency_key="order-o-1")
    assert a.created and not b.created and a.job.id == b.job.id
    stats = await relay.broker.queue_stats(0)
    assert sum(s.depth for s in stats) == 1  # dispatched once

    transport = httpx.ASGITransport(app=create_app(relay))
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        body = {"type": "charge", "payload": {"order": "o-2"}, "idempotency_key": "k2"}
        r1 = await c.post("/v1/jobs", json=body)
        r2 = await c.post("/v1/jobs", json=body)
    assert (r1.status_code, r2.status_code) == (201, 200)
    assert r1.json()["id"] == r2.json()["id"]


async def test_ctx_once_guards_side_effects_across_redelivery(relay: Relay) -> None:
    effects: list[str] = []

    @relay.job("pay", max_attempts=3)
    async def pay(ctx: JobContext, order: str) -> None:
        if await ctx.once(f"pay:{order}"):
            effects.append(order)
        if ctx.attempt == 1:
            raise ConnectionError("crashed after the side effect")

    job = (await relay.enqueue("pay", {"order": "o-9"})).job
    async with running_scheduler(relay), running_worker(relay):
        await wait_status(relay, job.id, JobStatus.SUCCEEDED)
    assert effects == ["o-9"]


async def test_exactly_one_scheduler_is_leader_and_failover_works(relay: Relay) -> None:
    a = Scheduler(relay, **(FAST_SCHEDULER | {"lease_ms": 600}))
    b = Scheduler(relay, **(FAST_SCHEDULER | {"lease_ms": 600}))
    assert await a.hold_leadership() is True
    assert await b.hold_leadership() is False
    assert await a.hold_leadership() is True  # renew

    # a stops renewing (crashed / paused): b takes over once the lease expires
    await asyncio.sleep(0.8)
    assert await b.hold_leadership() is True
    assert await a.hold_leadership() is False


async def test_recurring_job_fires_once_per_period_with_two_schedulers(relay: Relay) -> None:
    @relay.job("tick")
    async def tick(ctx: JobContext) -> None: ...

    await relay.add_recurring("every-second", "* * * * * *", "tick")
    # lease shorter than the tick interval jitter would allow two leaders to overlap briefly;
    # the idempotency key per fire time must still prevent duplicates
    async with (
        running_scheduler(relay, lease_ms=150, tick_s=0.05),
        running_scheduler(relay, lease_ms=150, tick_s=0.05),
    ):
        await asyncio.sleep(3.3)

    jobs, _ = await relay.store.list_jobs(job_type="tick", limit=100)
    keys = [j.idempotency_key for j in jobs]
    assert 2 <= len(jobs) <= 4
    assert len(keys) == len(set(keys))


async def test_recurring_api_crud(relay: Relay) -> None:
    @relay.job("report")
    async def report(ctx: JobContext) -> None: ...

    transport = httpx.ASGITransport(app=create_app(relay))
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post(
            "/v1/recurring-jobs", json={"name": "nightly", "cron": "0 3 * * *", "type": "report"}
        )
        assert r.status_code == 201, r.text
        rid = r.json()["id"]
        dup = await c.post(
            "/v1/recurring-jobs", json={"name": "nightly", "cron": "0 3 * * *", "type": "report"}
        )
        assert dup.status_code == 409
        bad = await c.post(
            "/v1/recurring-jobs", json={"name": "x", "cron": "whenever", "type": "report"}
        )
        assert bad.status_code == 422
        r = await c.patch(f"/v1/recurring-jobs/{rid}", json={"enabled": False})
        assert r.json()["enabled"] is False
        assert len((await c.get("/v1/recurring-jobs")).json()) == 1
        assert (await c.delete(f"/v1/recurring-jobs/{rid}")).status_code == 204


async def test_scheduled_job_priority_is_kept_through_the_delay(relay: Relay) -> None:
    @relay.job("p")
    async def p(ctx: JobContext) -> None: ...

    run_at = datetime.now(UTC) + timedelta(milliseconds=200)
    job = (await relay.enqueue("p", priority=Priority.HIGH, run_at=run_at)).job
    sched = Scheduler(relay, **FAST_SCHEDULER)

    async def promoted() -> bool:
        await sched.promote()
        return await relay.broker.redis.xlen(relay.broker.keys.queue(Priority.HIGH)) == 1

    await wait_for(promoted, within=5)
    refreshed = await relay.store.get(job.id)
    assert refreshed is not None and refreshed.status is JobStatus.QUEUED
