"""Read-only dashboard: server-rendered with Jinja2, refreshed by HTMX every 2 seconds.

HTMX keeps this a backend project: no JS build, the server renders HTML fragments and the page
swaps them in. Operator actions (replay, cancel, retry) go through the API or CLI, which are
authenticated; the dashboard only reads.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from relay.app import Relay
from relay.metrics import live_workers
from relay.models import JobStatus

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def dashboard_router(relay: Relay) -> APIRouter:
    router = APIRouter()

    async def snapshot() -> dict[str, object]:
        broker, store = relay.broker, relay.store
        running, _ = await store.list_jobs(status=JobStatus.RUNNING, limit=20)
        dead, _ = await store.list_jobs(status=JobStatus.DEAD, limit=10)
        failed, _ = await store.list_jobs(status=JobStatus.FAILED, limit=10)
        return {
            "now": datetime.now(UTC),
            "queues": await broker.queue_stats(int(time.time() * 1000)),
            "delayed": await broker.delayed_count(),
            "dlq_size": await broker.dlq_size(),
            "dlq": await broker.list_dead_letters(count=10),
            "counts": await store.status_counts(),
            "workers": await live_workers(relay),
            "running": running,
            "problems": sorted(
                dead + failed, key=lambda j: j.finished_at or j.run_at, reverse=True
            )[:10],
        }

    @router.get("/dashboard", response_class=HTMLResponse)
    async def page(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(request, "dashboard.html", await snapshot())

    @router.get("/dashboard/partial", response_class=HTMLResponse)
    async def partial(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(request, "_panels.html", await snapshot())

    return router
