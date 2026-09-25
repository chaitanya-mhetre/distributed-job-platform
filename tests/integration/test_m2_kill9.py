"""Real `kill -9` of a worker process holding jobs. Marked slow (spawns a subprocess)."""

from __future__ import annotations

import asyncio
import os
import signal
import sys

import pytest

from relay import JobContext, JobStatus, Relay
from tests.integration.conftest import DB_URL, REDIS_URL, wait_for
from tests.integration.helpers import running_scheduler, running_worker, wait_all_terminal

pytestmark = pytest.mark.slow


async def test_kill_9_worker_jobs_are_recovered(relay: Relay) -> None:
    @relay.job("sleepy", timeout_s=60)
    async def sleepy(ctx: JobContext, seconds: float) -> float:
        await asyncio.sleep(0.1)
        return seconds

    ids = [(await relay.enqueue("sleepy", {"seconds": 30})).job.id for _ in range(20)]
    env = os.environ | {
        "RELAY_REDIS_URL": REDIS_URL,
        "RELAY_DATABASE_URL": DB_URL,
        "RELAY_NAMESPACE": relay.settings.namespace,
        "RELAY_LOG_LEVEL": "WARNING",
    }
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "relay.cli",
        "worker",
        "--app",
        "tests.integration.crash_app:app",
        "--concurrency",
        "50",
        "--heartbeat-s",
        "0.5",
        "--heartbeat-ttl-s",
        "2",
        env=env,
    )

    async def all_running() -> bool:
        return (await relay.store.status_counts()).get("running", 0) == 20

    await wait_for(all_running, within=30)
    os.kill(proc.pid, signal.SIGKILL)  # no drain, no ack, no heartbeat cleanup
    await proc.wait()

    loop = asyncio.get_running_loop()
    killed_at = loop.time()
    async with running_scheduler(relay), running_worker(relay, concurrency=50):
        await wait_all_terminal(relay, ids, within=60)
    recovery_s = loop.time() - killed_at

    jobs = await relay.store.get_many(ids)
    assert all(j.status is JobStatus.SUCCEEDED for j in jobs.values())
    for job_id in ids:
        assert [a.outcome for a in await relay.store.attempts(job_id)] == ["lost", "succeeded"]
    # heartbeat TTL is 2 s, so recovery must take at least that long and not much more
    assert recovery_s < 15
