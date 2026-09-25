"""A Relay app for the real-process crash test. Settings come from RELAY_* env vars."""

import asyncio

from relay import JobContext, Relay

app = Relay()


@app.job("sleepy", timeout_s=60)
async def sleepy(ctx: JobContext, seconds: float) -> float:
    await asyncio.sleep(seconds if ctx.attempt == 1 else 0.1)
    return seconds
