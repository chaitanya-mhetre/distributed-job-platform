"""End-to-end load test for Relay (SDK -> Postgres + Redis -> worker processes -> Postgres).

Two modes:

  drain    Pre-load N no-op jobs, then start W worker processes and time how fast they empty
           the backlog. Repeated for each W in --workers to get a scaling curve.
           Throughput = N / (last finished_at - first started_at), timestamps from Postgres.

  latency  Start W workers, then submit jobs open-loop at a fixed rate R for D seconds and
           measure submit -> complete latency (finished_at - created_at, same DB clock).
           Open-loop means we keep submitting on schedule even if Relay falls behind, which is
           what real producers do; closed-loop tests hide queueing delay.

    uv run python -m loadtest.throughput drain --jobs 20000 --workers 1 2 4 8
    uv run python -m loadtest.throughput latency --rate 500 --duration 20 --workers 4
"""

from __future__ import annotations

import argparse
import asyncio
import time
from typing import Any

from sqlalchemy import text

from loadtest.common import (
    Report,
    cleanup,
    fresh_relay,
    percentile,
    spawn,
    stop_all,
)
from relay import Relay


async def enqueue_many(relay: Relay, n: int, concurrency: int = 64) -> float:
    """Submit n no-op jobs as fast as possible; returns submit rate (jobs/s)."""
    sem = asyncio.Semaphore(concurrency)

    async def one() -> None:
        async with sem:
            await relay.enqueue("noop")

    t0 = time.perf_counter()
    await asyncio.gather(*(one() for _ in range(n)))
    return n / (time.perf_counter() - t0)


async def wait_terminal(relay: Relay, n: int, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        counts = await relay.store.status_counts()
        done = sum(counts.get(s, 0) for s in ("succeeded", "failed", "dead", "cancelled"))
        if done >= n:
            return
        if time.monotonic() > deadline:
            raise TimeoutError(f"only {done}/{n} jobs finished in {timeout_s}s: {counts}")
        await asyncio.sleep(0.2)


async def db_timings(relay: Relay) -> dict[str, Any]:
    async with relay.store.engine.connect() as conn:
        row = (
            (
                await conn.execute(
                    text(
                        "SELECT count(*) AS n, min(started_at) AS first_start,"
                        " max(finished_at) AS last_finish,"
                        " count(*) FILTER (WHERE status <> 'succeeded') AS not_ok"
                        " FROM jobs"
                    )
                )
            )
            .mappings()
            .one()
        )
        latencies: list[Any] = list(
            (
                await conn.execute(
                    text("SELECT extract(epoch FROM finished_at - created_at) FROM jobs")
                )
            )
            .scalars()
            .all()
        )
    return {"row": dict(row), "latencies_ms": [float(x) * 1000 for x in latencies]}


async def drain(args: argparse.Namespace) -> Report:
    report = Report("drain", vars(args) | {"job": "noop"})
    for workers in args.workers:
        relay = await fresh_relay()
        submit_rate = await enqueue_many(relay, args.jobs)
        procs = [
            await spawn("worker", relay, "--concurrency", str(args.concurrency))
            for _ in range(workers)
        ]
        try:
            await wait_terminal(relay, args.jobs, timeout_s=600)
        finally:
            await stop_all(procs)
        t = await db_timings(relay)
        span = (t["row"]["last_finish"] - t["row"]["first_start"]).total_seconds()
        result = {
            "workers": workers,
            "concurrency_per_worker": args.concurrency,
            "jobs": args.jobs,
            "not_succeeded": t["row"]["not_ok"],
            "drain_seconds": round(span, 2),
            "throughput_jobs_per_s": round(args.jobs / span, 1),
            "sdk_submit_rate_jobs_per_s": round(submit_rate, 1),
        }
        print(result, flush=True)
        report.results.append(result)
        await cleanup(relay)
    return report


async def latency(args: argparse.Namespace) -> Report:
    report = Report("latency", vars(args) | {"job": "noop"})
    for workers in args.workers:
        relay = await fresh_relay()
        procs = [
            await spawn("worker", relay, "--concurrency", str(args.concurrency))
            for _ in range(workers)
        ]
        await asyncio.sleep(3)  # let worker processes import and connect
        n = int(args.rate * args.duration)
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        tasks: list[asyncio.Task[Any]] = []
        submit_lat: list[float] = []

        async def submit(relay: Relay = relay, out: list[float] = submit_lat) -> None:
            s = time.perf_counter()
            await relay.enqueue("noop")
            out.append((time.perf_counter() - s) * 1000)

        for i in range(n):
            delay = t0 + i / args.rate - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            tasks.append(asyncio.create_task(submit()))
        await asyncio.gather(*tasks)
        achieved = n / (loop.time() - t0)
        try:
            await wait_terminal(relay, n, timeout_s=300)
        finally:
            await stop_all(procs)
        t = await db_timings(relay)
        lat = t["latencies_ms"]
        result = {
            "workers": workers,
            "concurrency_per_worker": args.concurrency,
            "target_rate": args.rate,
            "achieved_submit_rate": round(achieved, 1),
            "jobs": n,
            "not_succeeded": t["row"]["not_ok"],
            "e2e_latency_ms": {p: round(percentile(lat, p), 1) for p in (50, 95, 99)},
            "e2e_latency_max_ms": round(max(lat), 1),
            "sdk_enqueue_latency_ms": {p: round(percentile(submit_lat, p), 1) for p in (50, 99)},
        }
        print(result, flush=True)
        report.results.append(result)
        await cleanup(relay)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    sub = parser.add_subparsers(dest="mode", required=True)
    d = sub.add_parser("drain")
    d.add_argument("--jobs", type=int, default=20_000)
    d.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8])
    d.add_argument("--concurrency", type=int, default=50)
    lt = sub.add_parser("latency")
    lt.add_argument("--rate", type=float, default=300)
    lt.add_argument("--duration", type=float, default=20)
    lt.add_argument("--workers", type=int, nargs="+", default=[4])
    lt.add_argument("--concurrency", type=int, default=50)
    args = parser.parse_args()
    report = asyncio.run(drain(args) if args.mode == "drain" else latency(args))
    print(f"saved {report.save()}")


if __name__ == "__main__":
    main()
