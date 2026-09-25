"""`relay` command line: migrate, api, worker, scheduler, dlq."""

from __future__ import annotations

import asyncio
import importlib
import os
import signal
import sys
from typing import Protocol

import typer
import uvicorn

from relay.app import Relay
from relay.logs import setup_logging

app = typer.Typer(help="Relay: a distributed job queue on Redis Streams", no_args_is_help=True)

APP_OPT = typer.Option("examples.jobs:app", "--app", help="module:attribute of your Relay app")


def load_app(path: str) -> Relay:
    if os.getcwd() not in sys.path:  # let `--app examples.jobs:app` work from the repo root
        sys.path.insert(0, os.getcwd())
    module_name, _, attr = path.partition(":")
    obj = getattr(importlib.import_module(module_name), attr or "app")
    if not isinstance(obj, Relay):
        raise typer.BadParameter(f"{path} is not a Relay instance")
    return obj


@app.command()
def migrate(relay_app: str = APP_OPT) -> None:
    """Apply database migrations and create consumer groups."""
    relay = load_app(relay_app)

    async def run() -> None:
        await relay.setup()
        await relay.close()

    asyncio.run(run())
    typer.echo("migrations applied")


@app.command()
def api(relay_app: str = APP_OPT, host: str = "0.0.0.0", port: int = 18082) -> None:  # noqa: S104
    """Run the HTTP API."""
    from relay.api import create_app

    relay = load_app(relay_app)
    setup_logging(relay.settings.log_level)
    uvicorn.run(create_app(relay), host=host, port=port, log_config=None)


@app.command()
def dashboard(relay_app: str = APP_OPT, host: str = "0.0.0.0", port: int = 18083) -> None:  # noqa: S104
    """Run the read-only dashboard (+ /metrics)."""
    from relay.api import create_dashboard_app

    relay = load_app(relay_app)
    setup_logging(relay.settings.log_level)
    uvicorn.run(create_dashboard_app(relay), host=host, port=port, log_config=None)


@app.command()
def worker(
    relay_app: str = APP_OPT,
    concurrency: int = 10,
    heartbeat_s: float = 5.0,
    heartbeat_ttl_s: int = 15,
    drain_timeout_s: float = 30.0,
    metrics_port: int = typer.Option(0, help="serve Prometheus metrics on this port (0 = off)"),
) -> None:
    """Run a worker process."""
    from relay.worker import Worker, WorkerConfig

    relay = load_app(relay_app)
    setup_logging(relay.settings.log_level)
    cfg = WorkerConfig(
        concurrency=concurrency,
        heartbeat_s=heartbeat_s,
        heartbeat_ttl_s=heartbeat_ttl_s,
        drain_timeout_s=drain_timeout_s,
    )
    _serve_metrics(metrics_port)
    _run_process(relay, Worker(relay, cfg))


@app.command()
def scheduler(
    relay_app: str = APP_OPT,
    metrics_port: int = typer.Option(0, help="serve Prometheus metrics on this port (0 = off)"),
) -> None:
    """Run a scheduler (run 2+ for failover; one is elected leader)."""
    from relay.scheduler import Scheduler

    relay = load_app(relay_app)
    setup_logging(relay.settings.log_level)
    _serve_metrics(metrics_port)
    _run_process(relay, Scheduler(relay))


def _serve_metrics(port: int) -> None:
    if port:
        from prometheus_client import start_http_server

        start_http_server(port)


class _Runnable(Protocol):
    def stop(self) -> None: ...
    async def run(self) -> None: ...


def _run_process(relay: Relay, proc: _Runnable) -> None:
    """Run a long-lived process; SIGINT/SIGTERM trigger its graceful stop()."""

    async def main() -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, proc.stop)
        try:
            await proc.run()
        finally:
            await relay.close()

    asyncio.run(main())


dlq = typer.Typer(help="Inspect and replay dead-lettered jobs")
app.add_typer(dlq, name="dlq")


@dlq.command("ls")
def dlq_ls(relay_app: str = APP_OPT, limit: int = 50) -> None:
    relay = load_app(relay_app)

    async def run() -> None:
        for d in await relay.broker.list_dead_letters(count=limit):
            typer.echo(f"{d.entry_id}  {d.job_id}  {d.job_type:<20} {d.error[:80]}")
        await relay.close()

    asyncio.run(run())


@dlq.command("replay")
def dlq_replay(entry_id: str, relay_app: str = APP_OPT) -> None:
    relay = load_app(relay_app)

    async def run() -> None:
        job = await relay.replay_dead_letter(entry_id)
        typer.echo(f"re-queued job {job.id}" if job else "not found")
        await relay.close()

    asyncio.run(run())


if __name__ == "__main__":
    app()
