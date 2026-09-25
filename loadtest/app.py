"""Job handlers used by the load tests and experiments. Settings come from RELAY_* env vars."""

from __future__ import annotations

import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from relay import JobContext, Relay

app = Relay()


@app.job("noop", timeout_s=30)
async def noop(ctx: JobContext) -> None:
    """Measures Relay's own overhead: no work at all."""


@app.job("hold", timeout_s=120)
async def hold(ctx: JobContext, seconds: float) -> None:
    """Sleeps on the first attempt only, so a retry after a crash finishes quickly."""
    await asyncio.sleep(seconds if ctx.attempt == 1 else 0.05)


@app.job("count", timeout_s=30)
async def count(ctx: JobContext, key: str, effect: str = "once") -> None:
    """Counts executions (to measure duplicates), then does one side effect per job key.

    effect="once":     once() guard + a Redis write. Has the gap (key recorded, then crash).
    effect="atomic":   once_atomic() + a Postgres write in the same transaction. No gap.
    effect="receiver": no guard at all; the "downstream system" (a Redis hash) deduplicates by
                       key with HSETNX, the way a payment API honours an idempotency key.
    """
    redis = ctx.relay.broker.redis
    ns = ctx.relay.settings.namespace
    await redis.hincrby(f"{ns}:executions", key, 1)
    await asyncio.sleep(0.05)  # widen the crash window a little
    if effect == "once":
        if await ctx.once(f"effect:{key}"):
            await redis.hincrby(f"{ns}:effects", key, 1)
    elif effect == "atomic":

        async def write(conn: AsyncConnection) -> None:
            await conn.execute(
                text("INSERT INTO bench_effects (key, job_id) VALUES (:k, :j)"),
                {"k": key, "j": ctx.job.id},
            )

        await ctx.once_atomic(f"effect:{key}", write)
    elif effect == "receiver":
        await redis.hincrby(f"{ns}:deliveries", key, 1)  # what the receiver was sent
        await redis.hsetnx(f"{ns}:effects", key, str(ctx.job.id))  # what it applied
    else:
        raise ValueError(f"unknown effect mode {effect!r}")
