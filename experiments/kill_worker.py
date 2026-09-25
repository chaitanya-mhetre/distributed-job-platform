"""Experiment: `kill -9` a worker holding in-flight jobs; how long until they complete elsewhere?

Setup: 1 scheduler + 2 worker processes with production defaults (heartbeat every 5 s,
TTL 15 s). 50 "hold" jobs sleep 60 s on their first attempt (0.05 s on retries). Once all 50
are running we SIGKILL whichever worker holds the most, and measure:

    recovery_s = time from kill until every job the dead worker held has succeeded

Expected: roughly heartbeat TTL (the scheduler waits for the heartbeat key to expire) plus a
scheduler tick plus a retry run. Also checks no job was lost or left non-terminal.

    uv run python -m experiments.kill_worker [--heartbeat-ttl 15]
"""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import time

from loadtest.common import Report, cleanup, fresh_relay, spawn, stop_all
from relay import JobStatus


async def run(args: argparse.Namespace) -> Report:
    report = Report("kill_worker", vars(args))
    for trial in range(args.trials):
        relay = await fresh_relay()
        ids = [(await relay.enqueue("hold", {"seconds": 60})).job.id for _ in range(args.jobs)]
        hb = [
            "--heartbeat-s",
            str(args.heartbeat_ttl / 3),
            "--heartbeat-ttl-s",
            str(args.heartbeat_ttl),
        ]
        workers = [await spawn("worker", relay, "--concurrency", "100", *hb) for _ in range(2)]
        sched = await spawn("scheduler", relay)
        try:
            # polling the database is the point here: we wait on state owned by other processes
            while (await relay.store.status_counts()).get("running", 0) < args.jobs:  # noqa: ASYNC110
                await asyncio.sleep(0.1)
            jobs = await relay.store.get_many(ids)
            holders: dict[str, list[object]] = {}
            for j in jobs.values():
                holders.setdefault(j.locked_by or "?", []).append(j.id)
            victim_id = max(holders, key=lambda k: len(holders[k]))
            victim_pid = int(victim_id.split("-")[-2])
            victim = next(w for w in workers if w.proc.pid == victim_pid)
            held = holders[victim_id]

            killed = time.monotonic()
            os.kill(victim_pid, signal.SIGKILL)
            await victim.proc.wait()
            while True:
                states = await relay.store.get_many(held)  # type: ignore[arg-type]
                if all(s.status is JobStatus.SUCCEEDED for s in states.values()):
                    break
                await asyncio.sleep(0.05)
            recovery = time.monotonic() - killed
        finally:
            await stop_all([*workers, sched])
        counts = await relay.store.status_counts()
        result = {
            "trial": trial + 1,
            "jobs_held_by_killed_worker": len(held),
            "recovery_s": round(recovery, 2),
            "final_status_counts": counts,
        }
        print(result, flush=True)
        report.results.append(result)
        await cleanup(relay)
    return report


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--jobs", type=int, default=50)
    p.add_argument("--heartbeat-ttl", type=int, default=15)
    p.add_argument("--trials", type=int, default=3)
    print(f"saved {asyncio.run(run(p.parse_args())).save()}")


if __name__ == "__main__":
    main()
