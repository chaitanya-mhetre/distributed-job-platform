"""M2 acceptance: failure injection. No job is ever lost: every job ends succeeded, failed,
dead or cancelled, whatever the handler (or the worker) does."""

from __future__ import annotations

import asyncio
from collections import Counter

from relay import JobContext, JobStatus, PermanentError, Priority, Relay
from relay.scheduler import Scheduler
from relay.store import NewJob
from tests.integration.conftest import wait_for
from tests.integration.helpers import (
    FAST_SCHEDULER,
    running_scheduler,
    running_worker,
    wait_all_terminal,
    wait_status,
)


async def outcomes(relay: Relay, job_id: object) -> list[str | None]:
    return [a.outcome for a in await relay.store.attempts(job_id)]  # type: ignore[arg-type]


async def test_transient_errors_retry_with_backoff_then_succeed(relay: Relay) -> None:
    calls: Counter[str] = Counter()

    @relay.job("flaky", max_attempts=5)
    async def flaky(ctx: JobContext) -> str:
        calls["n"] += 1
        if ctx.attempt < 3:
            raise ConnectionError("upstream down")
        return "ok"

    job = (await relay.enqueue("flaky")).job
    async with running_scheduler(relay), running_worker(relay):
        await wait_status(relay, job.id, JobStatus.SUCCEEDED)

    done = await relay.store.get(job.id)
    assert done is not None and done.attempts == 3 and done.result == "ok"
    assert await outcomes(relay, job.id) == ["retry", "retry", "succeeded"]


async def test_exhausted_retries_go_to_dlq_and_can_be_replayed(relay: Relay) -> None:
    broken = {"yes": True}

    @relay.job("broken", max_attempts=3)
    async def broken_job(ctx: JobContext) -> str:
        if broken["yes"]:
            raise RuntimeError("bug")
        return "fixed"

    job = (await relay.enqueue("broken")).job
    async with running_scheduler(relay), running_worker(relay):
        await wait_status(relay, job.id, JobStatus.DEAD)

        async def in_dlq() -> bool:  # row is marked dead just before the DLQ XADD
            return await relay.broker.dlq_size() == 1

        await wait_for(in_dlq)
        letters = await relay.broker.list_dead_letters()
        assert [d.job_id for d in letters] == [job.id]
        assert "RuntimeError: bug" in letters[0].error
        assert await outcomes(relay, job.id) == ["retry", "retry", "dead"]

        broken["yes"] = False  # "deploy the fix", then replay
        await relay.replay_dead_letter(letters[0].entry_id)
        await wait_status(relay, job.id, JobStatus.SUCCEEDED)

    assert await relay.broker.dlq_size() == 0


async def test_permanent_error_fails_without_retry_or_dlq(relay: Relay) -> None:
    @relay.job("validate", max_attempts=5)
    async def validate(ctx: JobContext, email: str) -> str:
        raise PermanentError(f"bad email {email}")

    job = (await relay.enqueue("validate", {"email": "nope"})).job
    async with running_worker(relay):
        await wait_status(relay, job.id, JobStatus.FAILED)
    done = await relay.store.get(job.id)
    assert done is not None and done.attempts == 1
    assert await relay.broker.dlq_size() == 0


async def test_hanging_handler_times_out(relay: Relay) -> None:
    @relay.job("hang", timeout_s=1, max_attempts=2)
    async def hang(ctx: JobContext) -> None:
        await asyncio.sleep(3600)

    job = (await relay.enqueue("hang")).job
    async with running_scheduler(relay), running_worker(relay):
        await wait_status(relay, job.id, JobStatus.DEAD, within=15)
    assert await outcomes(relay, job.id) == ["timeout", "dead"]


async def test_handler_that_ignores_cancellation_is_still_timed_out(relay: Relay) -> None:
    @relay.job("stubborn", timeout_s=1, max_attempts=1)
    async def stubborn(ctx: JobContext) -> None:
        for _ in range(100):
            try:
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                continue  # bad handler: swallows cancellation

    job = (await relay.enqueue("stubborn")).job
    async with running_worker(relay):
        await wait_status(relay, job.id, JobStatus.DEAD, within=15)


async def test_jobs_of_a_crashed_worker_are_reclaimed(relay: Relay) -> None:
    """Simulated kill -9: a consumer takes 20 jobs, marks them running, then vanishes
    (no ack, no heartbeat). See experiments/kill_worker.py for the real-process version."""

    @relay.job("work")
    async def work(ctx: JobContext, i: int) -> int:
        return i

    ids = [(await relay.enqueue("work", {"i": i})).job.id for i in range(20)]
    msgs = await relay.broker.read("dead-worker-1", list(Priority), count=100)
    assert len(msgs) == 20
    for m in msgs:
        assert await relay.store.start_attempt(m.job_id, "dead-worker-1") is not None

    sched = Scheduler(relay, **FAST_SCHEDULER)
    tick = await sched.tick()
    assert tick.reclaimed == 20

    async with running_worker(relay):
        await wait_all_terminal(relay, ids)
    jobs = await relay.store.get_many(ids)
    assert all(j.status is JobStatus.SUCCEEDED and j.attempts == 2 for j in jobs.values())
    assert await outcomes(relay, ids[0]) == ["lost", "succeeded"]


async def test_live_worker_keeps_long_jobs_even_past_stuck_threshold(relay: Relay) -> None:
    """Heartbeats reset the idle time of in-flight entries, so a slow job on a healthy worker
    is not mistaken for an abandoned one."""

    @relay.job("slow", timeout_s=10)
    async def slow(ctx: JobContext) -> str:
        await asyncio.sleep(1.5)
        return "done"

    job = (await relay.enqueue("slow")).job
    async with (
        running_scheduler(relay, stuck_idle_ms=500),
        running_worker(relay, heartbeat_s=0.1),
    ):
        await wait_status(relay, job.id, JobStatus.SUCCEEDED)
    assert await outcomes(relay, job.id) == ["succeeded"]  # never reclaimed


async def test_graceful_shutdown_leaves_unfinished_job_for_another_worker(relay: Relay) -> None:
    started = asyncio.Event()

    @relay.job("long", timeout_s=30)
    async def long(ctx: JobContext) -> str:
        started.set()
        await asyncio.sleep(2 if ctx.attempt > 1 else 60)
        return "done"

    job = (await relay.enqueue("long")).job
    async with running_worker(relay, drain_timeout_s=0.3):
        await asyncio.wait_for(started.wait(), 10)
    # first worker has stopped: attempt interrupted, entry unacked, heartbeat deleted
    assert await outcomes(relay, job.id) == ["interrupted"]

    async with running_scheduler(relay), running_worker(relay):
        await wait_status(relay, job.id, JobStatus.SUCCEEDED)
    assert (await outcomes(relay, job.id))[-1] == "succeeded"


async def test_reconciliation_dispatches_rows_missing_from_redis(relay: Relay) -> None:
    @relay.job("orphan")
    async def orphan(ctx: JobContext) -> str:
        return "found"

    # Simulate a crash between INSERT and XADD: row exists, Redis never heard of it.
    job, _ = await relay.store.create(NewJob(type="orphan", payload={}))
    async with running_scheduler(relay, reconcile_after_s=0), running_worker(relay):
        await wait_status(relay, job.id, JobStatus.SUCCEEDED)


async def test_mixed_chaos_every_job_reaches_one_terminal_state(relay: Relay) -> None:
    import random

    rng = random.Random(1234)

    @relay.job("chaos", timeout_s=1, max_attempts=3)
    async def chaos(ctx: JobContext, mode: str) -> str:
        if mode == "ok":
            return "ok"
        if mode == "flaky" and rng.random() < 0.5:
            raise RuntimeError("flake")
        if mode == "permanent":
            raise PermanentError("bad input")
        if mode == "hang":
            await asyncio.sleep(10)
        if mode == "error":
            raise ValueError("always")
        return "ok"

    modes = ["ok", "flaky", "permanent", "hang", "error"]
    ids = [(await relay.enqueue("chaos", {"mode": rng.choice(modes)})).job.id for _ in range(150)]
    async with running_scheduler(relay):
        # two workers, and we restart one of them half-way through
        async with running_worker(relay, concurrency=20), running_worker(relay, concurrency=20):
            await asyncio.sleep(1.5)
        async with running_worker(relay, concurrency=40):
            await wait_all_terminal(relay, ids, within=90)

    jobs = await relay.store.get_many(ids)
    by_mode: dict[str, set[JobStatus]] = {}
    for j in jobs.values():
        by_mode.setdefault(j.payload["mode"], set()).add(j.status)
    assert by_mode.get("ok", {JobStatus.SUCCEEDED}) == {JobStatus.SUCCEEDED}
    assert by_mode.get("permanent", {JobStatus.FAILED}) == {JobStatus.FAILED}
    assert by_mode.get("hang", {JobStatus.DEAD}) == {JobStatus.DEAD}
    assert by_mode.get("error", {JobStatus.DEAD}) == {JobStatus.DEAD}
    dead = sum(j.status is JobStatus.DEAD for j in jobs.values())
    assert await relay.broker.dlq_size() == dead
