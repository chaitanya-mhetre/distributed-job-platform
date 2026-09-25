"""Experiment: at-least-once in practice. How often does a job run twice when workers crash,
and does ctx.once() keep the side effect to exactly one?

Setup: N short "count" jobs (each increments an executions counter, sleeps 50 ms, then does a
once()-guarded side effect). Three worker processes; every --kill-every seconds we SIGKILL a
random worker and start a replacement, until all jobs are done.

Reported: jobs executed more than once (expected > 0: that's what at-least-once means), and for
the side effect, how many keys got it more than once and how many never got it.

--effect picks the side-effect strategy (see loadtest/app.py `count`):
  once      once() + Redis write           -> never duplicated, but can be LOST (the gap)
  atomic    once_atomic() + Postgres write -> key and effect commit together: neither
  receiver  receiver dedupes by key        -> duplicates are delivered but absorbed

    uv run python -m experiments.duplicates --jobs 1500 --kill-every 1.0 --effect atomic
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import signal

from sqlalchemy import text

from loadtest.common import Report, cleanup, fresh_relay, spawn, stop_all


async def run(args: argparse.Namespace) -> Report:
    report = Report("duplicates", vars(args))
    relay = await fresh_relay()
    async with relay.store.engine.begin() as conn:
        # No unique constraint on purpose: a duplicated effect would show up as two rows.
        await conn.execute(
            text(
                "CREATE TABLE IF NOT EXISTS bench_effects (key text NOT NULL, job_id uuid NOT NULL)"
            )
        )
        await conn.execute(text("TRUNCATE bench_effects"))
    for i in range(args.jobs):
        await relay.enqueue("count", {"key": str(i), "effect": args.effect})
    hb = ["--heartbeat-s", "0.5", "--heartbeat-ttl-s", "2"]
    workers = [await spawn("worker", relay, "--concurrency", "50", *hb) for _ in range(3)]
    sched = await spawn("scheduler", relay)
    kills = 0
    rng = random.Random(args.seed)
    try:
        while True:
            counts = await relay.store.status_counts()
            if counts.get("succeeded", 0) >= args.jobs:
                break
            await asyncio.sleep(args.kill_every)
            if counts.get("succeeded", 0) >= args.jobs * 0.9:
                continue  # stop killing near the end so the run can finish
            victim = rng.choice(workers)
            os.kill(victim.proc.pid, signal.SIGKILL)
            await victim.proc.wait()
            workers.remove(victim)
            workers.append(await spawn("worker", relay, "--concurrency", "50", *hb))
            kills += 1
    finally:
        await stop_all([*workers, sched])

    redis, ns = relay.broker.redis, relay.settings.namespace
    executions = {k: int(v) for k, v in (await redis.hgetall(f"{ns}:executions")).items()}
    if args.effect == "atomic":
        async with relay.store.engine.connect() as conn:
            rows = await conn.execute(text("SELECT key, count(*) FROM bench_effects GROUP BY key"))
            effects = {str(k): int(c) for k, c in rows}
    elif args.effect == "receiver":
        # decode_responses=True, so keys are str
        effects = {str(k): 1 for k in await redis.hkeys(f"{ns}:effects")}  # HSETNX: one per key
    else:
        effects = {str(k): int(v) for k, v in (await redis.hgetall(f"{ns}:effects")).items()}
    deliveries = {k: int(v) for k, v in (await redis.hgetall(f"{ns}:deliveries")).items()}
    result = {
        "effect": args.effect,
        "jobs": args.jobs,
        "workers_killed": kills,
        "jobs_executed_more_than_once": sum(v > 1 for v in executions.values()),
        "total_executions": sum(executions.values()),
        "side_effects_more_than_once": sum(v > 1 for v in effects.values()),
        "side_effects_missing": args.jobs - len(effects),
        # receiver mode only: duplicate deliveries the receiver absorbed
        "duplicate_deliveries_absorbed": sum(v - 1 for v in deliveries.values() if v > 1),
    }
    print(result, flush=True)
    report.results.append(result)
    await cleanup(relay)
    return report


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--jobs", type=int, default=3000)
    p.add_argument("--kill-every", type=float, default=1.0)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--effect", choices=["once", "atomic", "receiver"], default="once")
    print(f"saved {asyncio.run(run(p.parse_args())).save()}")


if __name__ == "__main__":
    main()
