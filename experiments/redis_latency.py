"""Experiment: effect of network latency between workers and Redis (via toxiproxy).

Requires the toxiproxy container: `docker compose --profile chaos up -d toxiproxy`.
Workers and the producer connect to Redis *through* toxiproxy (localhost:58475 -> redis:6379).
We drain the same backlog twice: with no toxic, then with +N ms latency on every Redis reply.

    uv run python -m experiments.redis_latency --jobs 3000 --latency-ms 100
"""

from __future__ import annotations

import argparse
import asyncio
import os

import httpx

from loadtest.common import Report, cleanup, fresh_relay, spawn, stop_all
from loadtest.throughput import db_timings, enqueue_many, wait_terminal

TOXI = os.environ.get("TOXIPROXY_URL", "http://localhost:58474")
PROXIED_REDIS = "redis://:relaydev@localhost:58475/0"


async def setup_proxy(client: httpx.AsyncClient) -> None:
    await client.delete(f"{TOXI}/proxies/relay_redis")
    r = await client.post(
        f"{TOXI}/proxies",
        json={"name": "relay_redis", "listen": "0.0.0.0:58475", "upstream": "redis:6379"},
    )
    r.raise_for_status()


async def set_latency(client: httpx.AsyncClient, ms: int) -> None:
    await client.delete(f"{TOXI}/proxies/relay_redis/toxics/latency")
    if ms:
        r = await client.post(
            f"{TOXI}/proxies/relay_redis/toxics",
            json={
                "name": "latency",
                "type": "latency",
                "stream": "downstream",
                "attributes": {"latency": ms, "jitter": 0},
            },
        )
        r.raise_for_status()


async def drain_once(n: int, workers: int, concurrency: int) -> dict[str, object]:
    relay = await fresh_relay(redis_url=PROXIED_REDIS)
    await enqueue_many(relay, n)
    procs = [
        await spawn("worker", relay, "--concurrency", str(concurrency)) for _ in range(workers)
    ]
    try:
        await wait_terminal(relay, n, timeout_s=900)
    finally:
        await stop_all(procs)
    t = await db_timings(relay)
    span = (t["row"]["last_finish"] - t["row"]["first_start"]).total_seconds()
    await cleanup(relay)
    return {"drain_seconds": round(span, 2), "throughput_jobs_per_s": round(n / span, 1)}


async def run(args: argparse.Namespace) -> Report:
    report = Report("redis_latency", vars(args))
    async with httpx.AsyncClient() as client:
        await setup_proxy(client)
        for ms in (0, args.latency_ms):
            await set_latency(client, ms)
            res = {"added_latency_ms": ms} | await drain_once(
                args.jobs, args.workers, args.concurrency
            )
            print(res, flush=True)
            report.results.append(res)
        await set_latency(client, 0)
    return report


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--jobs", type=int, default=3000)
    p.add_argument("--latency-ms", type=int, default=100)
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--concurrency", type=int, default=50)
    print(f"saved {asyncio.run(run(p.parse_args())).save()}")


if __name__ == "__main__":
    main()
