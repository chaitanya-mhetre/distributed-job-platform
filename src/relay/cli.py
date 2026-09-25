"""`relay` command line: migrate, api, worker, scheduler, dlq."""

from __future__ import annotations

import asyncio
import importlib
import os
import signal
import sys

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
def worker(relay_app: str = APP_OPT, concurrency: int = 10) -> None:
    """Run a worker process."""
    from relay.worker import Worker

    relay = load_app(relay_app)
    setup_logging(relay.settings.log_level)
    w = Worker(relay, concurrency=concurrency)

    async def run() -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, w.stop)
        try:
            await w.run()
        finally:
            await relay.close()

    asyncio.run(run())


if __name__ == "__main__":
    app()
