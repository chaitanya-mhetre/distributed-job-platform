"""ctx.once_atomic(): the dedupe key and the side effect commit together (issue #3).

once() has a gap: key recorded, then the process dies before the effect, so the retry skips it.
once_atomic() closes it for effects that are writes to the same Postgres database.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from relay import JobContext, Relay
from relay.worker import Worker
from tests.integration.conftest import wait_for

# Deliberately no unique constraint: if an effect ever ran twice, we'd see two rows.
LEDGER = "CREATE TABLE IF NOT EXISTS test_ledger (key text NOT NULL, job_id uuid NOT NULL)"


async def ledger_rows(relay: Relay, key: str) -> int:
    async with relay.store.engine.connect() as conn:
        n = await conn.scalar(text("SELECT count(*) FROM test_ledger WHERE key = :k"), {"k": key})
    return int(n or 0)


async def dedupe_recorded(relay: Relay, key: str) -> bool:
    async with relay.store.engine.connect() as conn:
        return bool(await conn.scalar(text("SELECT 1 FROM job_dedupe WHERE key = :k"), {"k": key}))


@pytest.fixture
async def ledger(relay: Relay) -> Relay:
    async with relay.store.engine.begin() as conn:
        await conn.execute(text(LEDGER))
        await conn.execute(text("TRUNCATE test_ledger"))
    return relay


def write_ledger(
    key: str, job_id: UUID, pause_s: float = 0.0
) -> Callable[[AsyncConnection], Awaitable[None]]:
    async def effect(conn: AsyncConnection) -> None:
        await conn.execute(
            text("INSERT INTO test_ledger (key, job_id) VALUES (:k, :j)"), {"k": key, "j": job_id}
        )
        if pause_s:
            await asyncio.sleep(pause_s)

    return effect


async def test_effect_and_key_commit_together_and_only_once(ledger: Relay) -> None:
    job_id = uuid4()
    assert await ledger.store.once_atomic("k", job_id, write_ledger("k", job_id)) is True
    assert await ledger.store.once_atomic("k", job_id, write_ledger("k", job_id)) is False
    assert await ledger_rows(ledger, "k") == 1
    assert await dedupe_recorded(ledger, "k")


async def test_a_failing_effect_records_nothing_so_the_retry_runs_it(ledger: Relay) -> None:
    job_id = uuid4()

    async def half_then_boom(conn: AsyncConnection) -> None:
        await write_ledger("k", job_id)(conn)
        raise RuntimeError("crash in the middle of the effect")

    with pytest.raises(RuntimeError):
        await ledger.store.once_atomic("k", job_id, half_then_boom)
    assert await ledger_rows(ledger, "k") == 0  # the half-written effect rolled back
    assert not await dedupe_recorded(ledger, "k")  # ...and so did the key: no gap

    assert await ledger.store.once_atomic("k", job_id, write_ledger("k", job_id)) is True
    assert await ledger_rows(ledger, "k") == 1


async def test_cancellation_mid_effect_rolls_back_both(ledger: Relay) -> None:
    """What a timeout (or the closest in-process stand-in for kill -9) does to the transaction."""
    job_id = uuid4()
    task = asyncio.create_task(
        ledger.store.once_atomic("k", job_id, write_ledger("k", job_id, pause_s=5))
    )
    await asyncio.sleep(0.3)  # the INSERTs have run, COMMIT hasn't
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await ledger_rows(ledger, "k") == 0
    assert not await dedupe_recorded(ledger, "k")


async def test_concurrent_duplicates_run_the_effect_once(ledger: Relay) -> None:
    """A reclaimed job can briefly run on two workers; the second blocks on the key's lock."""
    job_id = uuid4()
    results = await asyncio.gather(
        *(
            ledger.store.once_atomic("k", job_id, write_ledger("k", job_id, pause_s=0.2))
            for _ in range(5)
        )
    )
    assert sorted(results) == [False, False, False, False, True]
    assert await ledger_rows(ledger, "k") == 1


async def test_handlers_use_it_through_the_job_context(ledger: Relay) -> None:
    @ledger.job("credit")
    async def credit(ctx: JobContext, order: str) -> bool:
        return await ctx.once_atomic(f"credit:{order}", write_ledger(f"credit:{order}", ctx.job.id))

    first = (await ledger.enqueue("credit", {"order": "o1"})).job
    again = (await ledger.enqueue("credit", {"order": "o1"})).job  # a duplicate submission
    worker = Worker(ledger, concurrency=4, block_ms=100)
    task = asyncio.create_task(worker.run())

    async def both_done() -> bool:
        return (await ledger.store.status_counts()).get("succeeded", 0) == 2

    await wait_for(both_done)
    worker.stop()
    await asyncio.wait_for(task, 10)
    jobs = await ledger.store.get_many([first.id, again.id])
    assert sorted(j.result for j in jobs.values()) == [False, True]
    assert await ledger_rows(ledger, "credit:o1") == 1
