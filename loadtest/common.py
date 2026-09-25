"""Shared plumbing for load tests and experiments.

Everything runs against a dedicated `relay_bench` database and a fresh Redis namespace per run,
so results never mix with test or dev data.
"""

from __future__ import annotations

import asyncio
import json
import os
import platform
import subprocess
import sys
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from loadtest.app import app as bench_app
from relay import Relay, Settings

REDIS_URL = os.environ.get("RELAY_BENCH_REDIS_URL", "redis://:relaydev@localhost:56384/0")
DB_URL = os.environ.get(
    "RELAY_BENCH_DATABASE_URL", "postgresql+asyncpg://relay:relay@localhost:55437/relay_bench"
)
OUT = Path(__file__).parent / "out"


def git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def machine() -> dict[str, Any]:
    cpu = "unknown"
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    mem_gb = None
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal"):
                mem_gb = round(int(line.split()[1]) / 1024 / 1024, 1)
    except OSError:
        pass
    return {
        "cpu": cpu,
        "logical_cpus": os.cpu_count(),
        "mem_gb": mem_gb,
        "os": f"{platform.system()} {platform.release()}",
        "python": platform.python_version(),
        "note": "laptop; Redis + Postgres in Docker on the same machine; other projects' "
        "containers were running, so treat numbers as indicative, not a benchmark",
    }


async def ensure_database() -> None:
    base, _, name = DB_URL.rpartition("/")
    admin = create_async_engine(f"{base}/postgres", isolation_level="AUTOCOMMIT")
    try:
        async with admin.connect() as conn:
            if not await conn.scalar(
                text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": name}
            ):
                await conn.execute(text(f'CREATE DATABASE "{name}"'))
    finally:
        await admin.dispose()


async def fresh_relay(redis_url: str = REDIS_URL, **settings: Any) -> Relay:
    """A Relay bound to the bench DB, a new namespace, and the bench job handlers."""
    await ensure_database()
    relay = Relay(
        Settings(
            redis_url=redis_url,
            database_url=DB_URL,
            namespace=f"bench-{uuid.uuid4().hex[:8]}",
            max_queue_depth=0,
            # The producer (this process) submits every job; with a worker-sized pool of 8 it
            # was the bottleneck of the first latency run, not Relay's workers.
            db_pool_size=settings.pop("db_pool_size", 24),
            **settings,
        )
    )
    relay.registry.update(bench_app.registry)
    await relay.setup()
    async with relay.store.engine.begin() as conn:
        await conn.execute(text("TRUNCATE jobs, job_dedupe CASCADE"))
    return relay


@dataclass
class Proc:
    proc: asyncio.subprocess.Process
    kind: str


async def spawn(kind: str, relay: Relay, *args: str, redis_url: str | None = None) -> Proc:
    """Start `relay worker|scheduler` as a real OS process sharing this run's namespace."""
    env = os.environ | {
        "RELAY_REDIS_URL": redis_url or relay.settings.redis_url,
        "RELAY_DATABASE_URL": relay.settings.database_url,
        "RELAY_NAMESPACE": relay.settings.namespace,
        "RELAY_MAX_QUEUE_DEPTH": "0",
        "RELAY_LOG_LEVEL": "WARNING",
    }
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "relay.cli",
        kind,
        "--app",
        "loadtest.app:app",
        *args,
        env=env,
        stdout=asyncio.subprocess.DEVNULL,
    )
    return Proc(proc, kind)


async def stop_all(procs: list[Proc]) -> None:
    for p in procs:
        if p.proc.returncode is None:
            p.proc.terminate()
    await asyncio.gather(*(p.proc.wait() for p in procs))


async def cleanup(relay: Relay) -> None:
    keys = [k async for k in relay.broker.redis.scan_iter(f"{relay.settings.namespace}:*")]
    for i in range(0, len(keys), 500):
        await relay.broker.redis.delete(*keys[i : i + 500])
    await relay.close()


def percentile(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    k = (len(s) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


@dataclass
class Report:
    name: str
    params: dict[str, Any]
    results: list[dict[str, Any]] = field(default_factory=list)

    def save(self) -> Path:
        OUT.mkdir(exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        path = OUT / f"{self.name}-{stamp}.json"
        path.write_text(
            json.dumps(
                {
                    "name": self.name,
                    "date_utc": stamp,
                    "commit": git_commit(),
                    "machine": machine(),
                    "params": self.params,
                    "results": self.results,
                },
                indent=2,
                default=str,
            )
        )
        return path
