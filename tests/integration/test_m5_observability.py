"""M5: metrics, dashboard, backpressure."""

from __future__ import annotations

import httpx
from prometheus_client import REGISTRY

from relay import JobContext, JobStatus, Relay
from relay.api import create_app, create_dashboard_app
from tests.integration.helpers import running_worker, wait_all_terminal


def sample(name: str, labels: dict[str, str] | None = None) -> float:
    return REGISTRY.get_sample_value(name, labels or {}) or 0.0


async def test_metrics_reflect_work_done(relay: Relay) -> None:
    @relay.job("metered", max_attempts=1)
    async def metered(ctx: JobContext, fail: bool) -> None:
        if fail:
            raise RuntimeError("x")

    ok_before = sample("relay_jobs_total", {"type": "metered", "outcome": "succeeded"})
    dead_before = sample("relay_jobs_total", {"type": "metered", "outcome": "dead"})
    ids = [(await relay.enqueue("metered", {"fail": i % 4 == 0})).job.id for i in range(8)]
    async with running_worker(relay):
        await wait_all_terminal(relay, ids)

    assert sample("relay_jobs_total", {"type": "metered", "outcome": "succeeded"}) - ok_before == 6
    assert sample("relay_jobs_total", {"type": "metered", "outcome": "dead"}) - dead_before == 2

    transport = httpx.ASGITransport(app=create_app(relay))
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        body = (await c.get("/metrics")).text
    assert "relay_dlq_size 2.0" in body
    assert 'relay_queue_depth{queue="default"} 0.0' in body
    assert "relay_job_duration_seconds_bucket" in body


async def test_dashboard_renders(relay: Relay) -> None:
    @relay.job("shown", max_attempts=1)
    async def shown(ctx: JobContext) -> None:
        raise RuntimeError("visible on the dashboard")

    job = (await relay.enqueue("shown")).job
    async with running_worker(relay):
        await wait_all_terminal(relay, [job.id])

    for app in (create_app(relay), create_dashboard_app(relay)):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            page = await c.get("/dashboard")
            assert page.status_code == 200
            assert 'hx-get="/dashboard/partial"' in page.text
            partial = (await c.get("/dashboard/partial")).text
            assert "visible on the dashboard" in partial
            assert "<code>high</code>" in partial


async def test_backpressure_rejects_when_queue_is_full(relay: Relay) -> None:
    relay.settings.max_queue_depth = 5

    @relay.job("fill")
    async def fill(ctx: JobContext) -> None: ...

    for _ in range(5):
        await relay.enqueue("fill")
    transport = httpx.ASGITransport(app=create_app(relay))
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post("/v1/jobs", json={"type": "fill"})
    assert r.status_code == 503
    assert r.headers["retry-after"] == "5"
    counts = await relay.store.status_counts()
    assert counts == {JobStatus.QUEUED.value: 5}  # the rejected job was never written
