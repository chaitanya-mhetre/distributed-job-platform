# Benchmarks and failure experiments

Every number here came from a committed script. The raw output is in `loadtest/results/`: JSON
files record the commit, the machine and the parameters. **Read the environment section before
quoting anything.**

## Environment (all runs: 2026-09-25)

| | |
|---|---|
| Machine | Laptop, 12th Gen Intel Core i5-1240P (16 logical CPUs), 14.8 GB RAM, Linux 7.0 |
| Services | Redis 7 and Postgres 16 in Docker on the **same** laptop (no network hop) |
| Python | 3.12, one asyncio event loop per worker process, `--concurrency 50` unless noted |
| Noise | **Heavy.** Other projects' containers and builds were running at the same time. During the latency runs the load average was about 22 on 16 threads, and at one point less than 4 GB of RAM was free. |

So these are **indicative numbers from a busy laptop, not a benchmark**. Runs were done once each
because RAM was limited. Differences smaller than about 30% are within the noise.

---

## 1. Drain throughput (no-op jobs, backlog of 20,000)

`uv run python -m loadtest.throughput drain --jobs 20000 --workers 1 2 4 8`

Throughput = 20,000 / (last `finished_at` − first `started_at`), both taken from Postgres.

| Worker processes | Baseline (`6af6bc6`) | After the round-trip fix (`9dd4586`) |
|---|---|---|
| 1 | 327 jobs/s | 440 jobs/s |
| 2 | 571 jobs/s | 420 jobs/s |
| 4 | 623 jobs/s | 574 jobs/s |
| 8 | **stalled**: Postgres `too many clients` | **1,269 jobs/s** |

What I learned:

- **The 8-worker baseline failed, and that was the most useful result.** Each process had a pool of
  10 connections plus 10 overflow. 8 × 20 = 160 connections, more than Postgres' default of 100.
  The workers got `too many clients`. The jobs they had already read stayed unacked. No scheduler was
  running in that harness, so nothing reclaimed them, and the run timed out at 9,058 of 20,000 jobs.
  - Fix: a fixed per-process budget (`db_pool_size=8`, `db_max_overflow=0`), so the total is predictable.
  - The load harness now also runs a scheduler, as production would.
  - In production the next step would be PgBouncer in transaction mode.
- **Every job cost about 9 Postgres round trips.** Starting a job was BEGIN, UPDATE, INSERT, COMMIT;
  finishing it was two more transactions. Both are now single statements: a data-modifying CTE run
  in autocommit mode.
  - The 1-worker and 8-worker numbers went up.
  - The 2- and 4-worker numbers went down. The machine was too noisy to call that a regression or
    an improvement.
  - **Honest conclusion:** the fix is correct and removes round trips, but on this machine I couldn't
    measure a clean speedup for small worker counts.
- The ceiling is **Postgres writes per job**, not Redis. Each job writes the job row twice and the
  attempt row twice. Batching status updates, or making the attempt history optional, would be
  the next lever.

## 2. Submit → complete latency (open loop)

`uv run python -m loadtest.throughput latency --rate 100 --duration 20 --workers 2`

Jobs are submitted on a fixed schedule whether or not Relay keeps up. Latency =
`finished_at − created_at` for each job, all from the same Postgres clock.

| Target rate | Achieved submit rate | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| 100 jobs/s | 97.6 jobs/s | 11.9 ms | 398 ms | 688 ms | 1,362 ms |

- **200 jobs/s: not achieved in the first session** (load average about 22). Three attempts reached
  115, 170 and 129 jobs/s (`latency-200rps-producer-bound*.log`). The single Python **producer** was
  the bottleneck, and those runs' latency numbers are not meaningful, because jobs queued inside the
  producer before they even had a row. See §2b for the follow-up.
- The long p95/p99 tail at 100/s is consistent with CPU contention: load average about 22.

## 2b. Producer batching (issue #2, measured 2026-09-25)

**Profile first** (`producer-profile.log`): 64 concurrent `enqueue()` calls, 2,000 jobs, two runs.

| Step | p50 | p95 |
|---|---|---|
| INSERT (Postgres) | 28.4–28.6 ms | 82–94 ms |
| XADD (Redis) | 2.1 ms | 3.4–4.5 ms |
| UPDATE `dispatched_at` (Postgres) | 29.5–31.6 ms | 68–72 ms |

Each job costs **two Postgres round trips**, and most of that time is spent waiting for a pooled
connection, not in the SQL itself. Redis is cheap. So the fix is to pay those round trips once per
*batch* instead of once per job:

- `JobStore.create_many`: one `INSERT ... SELECT FROM ROWS FROM (jsonb_to_recordset(...))` for N jobs,
  with the same `ON CONFLICT` idempotency as `create()`.
- `RedisBroker.enqueue_many`: all XADDs (and delayed-set writes) in one MULTI/EXEC pipeline.
- One `UPDATE ... WHERE id = ANY(...)` for `dispatched_at`.
- `BatchingProducer` (`app.batching()`): coalesces concurrent `enqueue()` calls; flushes at
  `max_batch` (200) or after `max_delay_ms` (5 ms), up to 4 batches in flight.

**Same busy laptop, same session, back to back** (load average 5–8; `latency-producer-single-vs-batch.log`;
single runs, so treat small differences as noise):

| Target | Producer | Achieved | e2e p50 | e2e p95 | e2e p99 | `enqueue()` p99 |
|---|---|---|---|---|---|---|
| 200/s, 2 workers | single | 199.0/s | 7.0 ms | 86 ms | 197 ms | 249 ms |
| 200/s, 2 workers | **batch** | 199.9/s | 7.7 ms | 23.5 ms | 46 ms | 36 ms |
| 500/s, 4 workers | single | **360/s** (fell behind) | 20 ms | 99 ms | 183 ms | **8,361 ms** |
| 500/s, 4 workers | **batch** | 499.6/s | 10.0 ms | 14.2 ms | 141 ms | 22 ms |
| 1,000/s, 4 workers | single | **crashed**: `QueuePool ... timed out` after 30 s (twice) | – | – | – | – |
| 1,000/s, 4 workers | **batch** | 998/s | 17.2 ms | 120 ms | 250 ms | 43 ms |

- **200 jobs/s is achieved with both producers today.** The earlier failure was mostly the machine
  (load about 22 then, 2–8 now). Being honest about that matters more than claiming a fix.
- The real gain is **headroom**. The single producer saturates somewhere between 200 and 500 jobs/s
  on this machine; at 1,000/s its backlog waits more than 30 s for a connection and the run dies.
  The batching producer held 1,000/s with `enqueue()` p99 of 43 ms.
- Cost of batching: up to `max_delay_ms` (5 ms) of extra latency for a lone job, which you can see in
  the 200/s p50 (7.7 vs 7.0 ms). At higher rates, batches fill before the timer fires.
- Not measured yet: where the batching producer tops out, and repeat runs with error bars (issue #1).

## 3. HTTP submit endpoint (k6)

`docker run --rm -i --network host -e RATE=100 -e DURATION=20s grafana/k6 run - < loadtest/k6/submit.js`

This used k6 v2.3.0 with a constant arrival rate of 100 requests/s for 20 s. One API process ran
against the bench database, and no workers were running.

| Requests | Failed | median | p90 | p95 | max |
|---|---|---|---|---|---|
| 2,001 | 0 % | 7.4 ms | 12.3 ms | 14.1 ms | 289 ms |

This measures API + Postgres insert + XADD only. Job execution isn't included.

## 4. Worker `kill -9` recovery

`uv run python -m experiments.kill_worker --trials 3`

The setup was 2 workers and 1 scheduler with production heartbeat settings (beat every 5 s, TTL 15 s).
50 jobs sleep 60 s on their first attempt. Once all 50 were running, the worker holding them got SIGKILL.

| Trial | Jobs held by killed worker | Time until all of them succeeded elsewhere | Final state |
|---|---|---|---|
| 1 | 50 | 15.3 s | 50 succeeded |
| 2 | 50 | 17.3 s | 50 succeeded |
| 3 | 50 | 15.3 s | 50 succeeded |

Recovery is dominated by the **heartbeat TTL** (15 s): the scheduler waits for the dead worker's
heartbeat key to expire. A lower TTL recovers faster but risks reclaiming work from a worker that
is only briefly paused (GC, CPU starvation). No job was lost in any trial.

## 5. Duplicate execution under crashes (at-least-once, measured)

`uv run python -m experiments.duplicates --jobs 1500 --kill-every 1.0`

There were 3 workers, and one was killed with `kill -9` every second (and replaced). Each job counts
its executions, then runs a side effect guarded by `ctx.once()`.

| Jobs | Workers killed | Jobs executed > 1 time | Total executions | Side effect > 1 time | Side effect **missing** |
|---|---|---|---|---|---|
| 1,500 | 24 | 264 (17.6 %) | 1,792 | **0** | **4** |

- **Duplicates are real.** A worker killed after running a job but before acking it means the job
  runs again. That's what at-least-once delivery means, and why handlers must be idempotent.
- **`once()` prevented every duplicate side effect.**
- **But 4 side effects never happened.** The process died after `once()` had recorded the key and
  before the side effect ran, so the retry skipped it. This is exactly the gap described in
  `ctx.once()`'s docstring, now measured.
  - For effects that must happen exactly once, "record" and "do" must be one atomic step: the same
    database transaction, or an idempotency key on the downstream API call.

## 6. Redis latency (toxiproxy)

`uv run python -m experiments.redis_latency ...`. Workers talked to Redis through toxiproxy,
with +100 ms added on every reply.

| Workers × concurrency | +0 ms | +100 ms |
|---|---|---|
| 2 × 50 (100 in flight) | 208 jobs/s | 214 jobs/s (no measurable effect) |
| 1 × 4 (4 in flight) | 382 jobs/s | **9.8 jobs/s** |

This is Little's law in action: throughput ≈ jobs in flight ÷ time per job.

- With 4 jobs in flight, about 4 Redis round trips per job × 100 ms ≈ 0.4 s per job, which caps
  throughput near 10 jobs/s, as measured.
- With 100 in flight, the same latency is hidden. The Redis ceiling of about 250 jobs/s is above
  what Postgres and the contended CPU were delivering anyway.
- Takeaway: concurrency hides latency until you run out of concurrency, so a latency spike shows up
  first on low-concurrency workers.
- Before trusting the first, flat result, I checked that the toxic really applied: a direct ping
  took 163 ms.

---

## Not measured yet (TBD)

- A clean, quiet-machine scaling curve with repeated runs and error bars. Needs a dedicated VM.
- Throughput with Postgres and Redis on separate hosts, where network round trips become the cost.
- A comparison against the same workload on Celery or RQ.
- Recovery time with a lower heartbeat TTL (5 s), and how often it causes false reclaims.
