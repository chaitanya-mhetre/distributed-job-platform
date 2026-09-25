"""Example job handlers. Run a worker with: `relay worker --app examples.jobs:app`."""

from __future__ import annotations

import asyncio
import random
from typing import Any

from relay import JobContext, PermanentError, Relay

app = Relay()


@app.job("noop", timeout_s=10)
async def noop(ctx: JobContext) -> None:
    """Does nothing. Used by the load tests to measure the queue's own overhead."""


@app.job("echo")
async def echo(ctx: JobContext, **payload: Any) -> dict[str, Any]:
    return payload


@app.job("sleep", timeout_s=120)
async def sleep(ctx: JobContext, seconds: float = 1.0) -> float:
    await asyncio.sleep(seconds)
    return seconds


@app.job("flaky", max_attempts=5)
async def flaky(ctx: JobContext, fail_rate: float = 0.5) -> str:
    """Fails randomly; shows retries with backoff in the dashboard."""
    if random.random() < fail_rate:  # noqa: S311 - not crypto
        raise RuntimeError("simulated transient failure")
    return "ok"


@app.job("validate", max_attempts=5)
async def validate(ctx: JobContext, email: str) -> str:
    if "@" not in email:
        raise PermanentError(f"invalid email {email!r}")  # retrying can't fix bad input
    return email
