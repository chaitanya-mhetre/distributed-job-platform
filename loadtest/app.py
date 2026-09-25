"""Job handlers used by the load tests and experiments. Settings come from RELAY_* env vars."""

from __future__ import annotations

import asyncio

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
async def count(ctx: JobContext, key: str) -> None:
    """Counts executions (to measure duplicates) and a once()-guarded side effect."""
    redis = ctx.relay.broker.redis
    ns = ctx.relay.settings.namespace
    await redis.hincrby(f"{ns}:executions", key, 1)
    await asyncio.sleep(0.05)  # widen the crash window a little
    if await ctx.once(f"effect:{key}"):
        await redis.hincrby(f"{ns}:effects", key, 1)
