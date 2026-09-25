# Relay learning guide

This guide is for studying the codebase until you can explain every design decision in an interview
without notes. Line numbers point at `src/relay/` as of v0.1.0 and may drift a little as code changes;
the function names won't.

---

## 1. The whole system in five sentences

1. A producer calls `enqueue()`. Relay **inserts a row in Postgres** (the source of truth), then
   **XADDs the job id** to a Redis stream for its priority.
2. Workers in one **consumer group** read ids with `XREADGROUP`. Redis records each delivered entry in
   the group's **Pending Entries List (PEL)** until the worker `XACK`s it.
3. The worker flips the row `queued → running` (only if it is still `queued`), runs the handler with a
   timeout, writes the outcome in **one SQL statement**, and only then acks.
4. Failures become retries (delayed ZSET with backoff), dead letters (DLQ stream), or permanent failures.
   A dead worker's unacked entries are reclaimed by the **scheduler** once its heartbeat key expires.
5. Because "run the handler" and "ack" can't be atomic, delivery is **at-least-once**. Handlers must be
   idempotent (`ctx.once`), and shared resources are protected with **fencing tokens**.

## 2. Reading order (file tour)

| # | File | Read it for |
|---|---|---|
| 1 | `models.py` | The job **state machine as data** (`TRANSITIONS`, `allowed_from`, l.17–64). Everything else enforces these rules. |
| 2 | `keys.py` | Every Redis key in one place. Sketch the key space on paper. |
| 3 | `migrations/001_jobs.sql` | The `jobs` table: the partial unique index for idempotency, and `dispatched_at` for outbox-lite. |
| 4 | `store.py` | All SQL. Start with `create` (l.87, `ON CONFLICT DO NOTHING` idempotency), then `_transition` (l.239), `start_attempt` (l.267) and `finish` (l.300): data-modifying CTEs, one round trip, `locked_by` ownership check. |
| 5 | `broker.py` | Redis Streams: `read` (l.191, weighted then blocking), `ack` (l.226, XACK + XDEL), `_PROMOTE_LUA` (l.32), `claim`/`republish` (l.274–298), `WeightedOrder` (l.79). |
| 6 | `app.py` | The SDK: `enqueue` (l.130, backpressure + idempotency), `dispatch` (l.166, **Postgres before Redis**), `cancel` (l.193). |
| 7 | `worker.py` | The heart. `_consume` (l.123), `_run` (l.261, timeouts, handlers that swallow cancellation), `_record` (l.311, outcome → retry/dead/DLQ), `_heartbeat_loop` (l.177, idle refresh), `_drain` (l.144). |
| 8 | `scheduler.py` | `hold_leadership` (l.107, lease Lua), `promote` (l.137), `reclaim` (l.149), `enqueue_recurring` (l.186), `reconcile` (l.208). |
| 9 | `locks.py` | `LockManager` (fencing tokens) and `TypeSlots` (distributed counting semaphore in Lua). |
| 10 | `context.py` | What handlers get: `once`, `lock`, `raise_if_cancelled`. |
| 11 | `backoff.py`, `recurring.py` | Small pure functions; good warm-ups. |
| 12 | `metrics.py`, `api/` | Observability and the HTTP surface. |
| 13 | `tests/integration/test_m2_failures.py` | **The best documentation of behaviour.** Each test is one failure mode. |
| 14 | `docs/benchmarks.md`, `docs/delivery-guarantees.md` | What was measured and what it means. |

Exercise: after reading 1–8, trace one job end to end on paper. Write down every SQL statement and
every Redis command, in order, for (a) success, (b) one retry then success, (c) worker `kill -9`.

## 3. Concepts, with pointers to the code

### At-least-once delivery
The worker acks **after** writing the outcome (`worker._record`, the final `broker.ack`). If it
crashes before the ack, the entry stays in the PEL and is redelivered.
- Duplicate *runs* can happen (measured: 17.6% of jobs under repeated `kill -9`).
- Duplicate *outcome writes* can't: `start_attempt` only succeeds from `queued`, and `finish` only
  succeeds for the current owner.
- Contrast with at-most-once (ack first, lose on crash) and "exactly-once" (only real inside one
  transactional system).

### Idempotency, at three levels
1. **Submission**: a unique partial index on `(type, idempotency_key)` plus `INSERT ... ON CONFLICT DO NOTHING`
   (`store.create`). The API returns 200 with the existing job instead of 201.
2. **Redelivery**: the worker checks the row's status before running (`start_attempt` returns None,
   then it just acks).
3. **Side effects**: `ctx.once(key)` → `job_dedupe` table. Know its gap: it can *skip* an effect if
   the crash lands between recording the key and doing the effect (4 of 1,500 in the experiment).

### Consumer groups and the PEL
- `XGROUP CREATE ... MKSTREAM` (`broker.ensure_groups`) creates one group, `relay-workers`, per stream.
- `XREADGROUP GROUP relay-workers <worker-id> STREAMS q >` delivers *new* entries, and each entry goes
  to exactly one consumer.
- `XPENDING` shows what each consumer holds and for how long (its idle time).
- Relay deletes entries after acking (`XDEL`), so `XLEN` = work not finished yet. That makes queue
  depth a single O(1) call.

### XCLAIM / XAUTOCLAIM and reclaiming work
- `XAUTOCLAIM stream group consumer min-idle start` scans the PEL and claims entries idle longer than
  `min-idle`. It's the one-liner way to steal abandoned work.
- Relay uses `XPENDING` + `XCLAIM` instead (`scheduler.reclaim`, `broker.claim`), because it wants two
  different policies:
  - entries of a **dead** consumer (heartbeat key gone) are reclaimed almost immediately;
  - any entry idle longer than `stuck_idle_ms` is reclaimed as a safety net.
- **The idle-refresh trick**: live workers `XCLAIM` their own in-flight entries to themselves every
  heartbeat (`worker._heartbeat_loop`, `justid=True`), which resets their idle time. So "idle for a
  long time" really means "nobody is looking after it", even for a 10-minute job.
- Reclaimed entries are **re-published** as fresh entries (`broker.republish`), after the rows are
  flipped back to `queued`. Why re-publish instead of leaving them claimed? Because claimed entries only
  come back via XPENDING/XCLAIM, while a new entry is picked up by any worker's normal `>` read.

### "Postgres before Redis" (the bug that taught it)
If a stream entry becomes visible before its row says `queued`, a fast worker reads it, sees
"not runnable", acks it, and the job is lost. The first `promote` did exactly that
(commit message of `02211a4`). Every path now changes Postgres first: `scheduler.promote`,
`scheduler.reclaim`, `app.dispatch`, `app.retry`. A crash in between leaves Postgres *ahead* of
Redis, which the next tick or `reconcile` repairs.

### Outbox-lite
Crash after `INSERT` but before `XADD` → the row exists but Redis never heard of it. `dispatched_at`
stays NULL, and `scheduler.reconcile` re-dispatches such rows after 30 s. A full transactional outbox
would write the "message" in the same transaction as the business data and have a relay process
publish it. Here the row *is* the message.

### Timeouts and cancellation
- `asyncio.wait({task}, timeout)` (not `wait_for`), then `task.cancel()`, then a 5 s grace period
  (`worker._run`). If the handler swallows `CancelledError`, the worker stops waiting and records a
  timeout anyway. `test_handler_that_ignores_cancellation_is_still_timed_out` proves it.
- Cooperative cancel: the API sets `relay:cancel:{id}`; `_cancel_loop` polls those flags for running
  jobs and cancels their tasks. `StopReason` tells "user cancel" apart from "worker shutting down".

### Heartbeats, leases and leader election
- A heartbeat is a key with a TTL (`SET relay:hb:{id} ... EX 15`). If it's missing, the worker is dead,
  or paused longer than the TTL, which is indistinguishable and why fencing exists.
- The scheduler lease (`_LEASE_LUA`) is acquire-or-renew in one script, so "check owner" and "extend"
  can't race.
- Two schedulers can briefly both be leader, for example when the old leader pauses past the lease.
  The recurring-job idempotency key (`recurring:{name}:{fire_time}`) makes that harmless.

### Fencing tokens
`LockManager.try_acquire`: `INCR relay:fence:{r}` produces a monotonically increasing token, then
`SET relay:lock:{r} token NX PX ttl`. The protected resource (`fenced_write`, `store.py` l.617) only
accepts a token greater than the last one applied. A paused holder whose lease expired is rejected even
though it still *thinks* it holds the lock. See `test_fencing_token_rejects_a_stale_lock_holder`, and
read Kleppmann's 2016 post "How to do distributed locking".

### Backpressure
- **Producer side**: `enqueue` rejects with `QueueFullError` (HTTP 503 + `Retry-After`) when the target
  stream is over `max_queue_depth`. Fail fast instead of letting latency grow without bound.
- **Consumer side**: a worker only reads as many entries as it has free slots (`_consume`), so it
  never takes more than it can run.
- **Per-type limits**: `TypeSlots` defers extra jobs back to the delayed set *without* using up an
  attempt.
- **Database side**: a fixed connection budget (`db_pool_size`, no overflow), learned from the
  8-worker load test.

### Priority without starvation
`WeightedOrder` is smooth weighted round-robin (nginx's algorithm): each priority accumulates its
weight, the largest wins and pays the total back. With 6:3:1, `high` is tried first 6 times in 10 and
the picks are spread out. `test_low_priority_is_not_starved_by_a_high_priority_flood`.

### Little's law (from the Redis-latency experiment)
Throughput ≈ jobs in flight ÷ time per job. With 4 in flight and about 0.4 s per job (4 Redis round
trips × 100 ms), throughput ≈ 10 jobs/s, as measured. With 100 in flight, the same latency disappears
behind the Postgres ceiling.

## 4. Interview questions (with short answers)

1. **At-least-once, at-most-once, exactly-once: which does Relay give, and why?**
   At-least-once. Running a handler with outside effects and acking can't be atomic. Ack-after-work
   means a crash causes a re-run, never a loss. Exactly-once only exists inside a single transactional
   boundary.
2. **What happens if a worker dies mid-job?**
   Its heartbeat key expires (15 s). The scheduler finds its pending entries (XINFO CONSUMERS + XPENDING),
   claims them, flips the rows `running → queued`, closes the attempt as `lost`, and re-publishes the
   entries. Another worker runs them. Measured recovery: 15–17 s.
3. **How do you tell a slow job from a dead worker?**
   Two signals: the worker's heartbeat key, and the idle time of each in-flight entry, which live
   workers keep resetting with XCLAIM-to-self.
4. **Why is Postgres the source of truth instead of Redis?**
   Durability and queryability: history, dashboards, idempotency via unique indexes. Redis with async
   replication can lose recent writes on failover. Here that only loses an id, which reconciliation
   restores.
5. **Walk me through the bug you found in promotion.**
   Entries were made visible before rows said `queued`. A worker skipped and acked them, so jobs were
   lost. Fix: Postgres first, then Redis, on every path. A crash in between is repaired by the next tick.
6. **Why XDEL after XACK?**
   So `XLEN` equals unfinished work (an O(1) depth metric) and the stream doesn't grow forever. The
   trade-off: no replay of history, which Postgres provides anyway.
7. **How does the delayed queue work?**
   A ZSET scored by `run_at` in ms, plus a hash of `priority|type`. The scheduler reads due ids, flips
   the rows to `queued`, then a Lua script atomically ZREMs each and XADDs it to its stream.
8. **Why a Lua script there?**
   Atomicity: "remove from ZSET" and "add to stream" happen together or not at all, even with two
   schedulers or a crash in the middle.
9. **Explain exponential backoff with jitter.**
   `min(cap, base·2^(n−1)) · U(0.5, 1.5)`. Exponential growth gives a dependency time to recover.
   Jitter spreads out retries that would otherwise all fire at once (thundering herd).
10. **Why are distributed locks dangerous? What's a fencing token?**
    A lease can expire while its holder is paused, so two holders coexist. A fencing token is a
    monotonically increasing number issued with each lease. The resource rejects writes with a token
    older than the last one it applied.
11. **Redlock?**
    Multi-node Redis locking. Kleppmann's critique: it relies on timing assumptions (bounded pauses and
    clock drift) and doesn't provide fencing, so it isn't safe for correctness. Use fencing, or a
    consensus-based store (etcd/ZooKeeper) when correctness matters.
12. **How do you prevent priority starvation?**
    Weighted polling (6:3:1) instead of strict priority. There's a test with 200 high and 20 low jobs.
13. **How is backpressure handled?**
    Workers read only up to their free slots. Producers get 503 when a queue exceeds a threshold.
    Per-type limits defer work. The DB connection budget is fixed.
14. **When would you pick Kafka over Redis Streams? RabbitMQ?**
    Kafka: event streaming, many independent consumers, replay, high sustained throughput with per-key
    ordering. RabbitMQ: classic task queues with routing, built-in DLX, priorities and redelivery.
    Redis Streams: simple ops, per-message acks, a visible PEL, and you already run Redis.
15. **How do recurring jobs avoid firing twice with two schedulers?**
    The idempotency key includes the scheduled fire time. Even if both schedulers enqueue, the unique
    index keeps one job per period.
16. **Why "no backfill" for missed cron runs?**
    After an outage, running "send daily digest" five times is worse than running it once. So only the
    latest missed period fires.
17. **How does cancellation of a running job work?**
    A Redis flag is set, the worker's cancel loop polls flags for running jobs and calls `task.cancel()`,
    and the handler sees `CancelledError`. CPU-style loops can call `ctx.raise_if_cancelled()`.
18. **What if the handler ignores cancellation?**
    After the timeout plus a 5 s grace period the worker stops waiting and records a timeout. The stray
    coroutine keeps running (Python can't kill it); it's logged so the handler can be fixed. With a
    process pool you *could* kill it.
19. **What did the load test teach you?**
    Postgres writes per job are the ceiling, and connection budgets matter: 8 processes × 20 connections
    exceeded `max_connections`. I reduced each job transition to one round trip (data-modifying CTEs in
    autocommit) and capped pools.
20. **How did you measure latency, and why open-loop?**
    `finished_at − created_at` from the same Postgres clock. Open-loop, meaning submit on a fixed
    schedule, because closed-loop tests slow the producer down when the system slows and hide queueing
    delay (coordinated omission).
21. **Your first 200 jobs/s latency run: what went wrong?**
    The producer was the bottleneck, reaching only 115–170 jobs/s, so its latency numbers were
    meaningless. I reported that and measured at a sustainable 100/s instead.
22. **Why did adding 100 ms of Redis latency not change throughput at first?**
    Little's law: with 100 jobs in flight the extra latency was hidden behind the Postgres ceiling. At 4
    in flight, throughput dropped from 382 to 9.8 jobs/s.
23. **How do you stop a stalled worker from overwriting the new owner's result?**
    `finish()` requires `locked_by = :worker`. After a reclaim the row belongs to someone else, so the
    stale UPDATE matches nothing.
24. **Why a CHECK constraint rather than a Postgres ENUM for status?**
    Adding an ENUM value needs `ALTER TYPE`, which is awkward in migrations. A CHECK change is one line.
25. **Why keyset pagination?**
    `WHERE (created_at, id) < (:ts, :id) ORDER BY ... LIMIT` seeks via an index. OFFSET scans and throws
    rows away, so it gets slower on every page and can skip or duplicate rows under inserts.
26. **What would you change for production?**
    PgBouncer; Redis with persistence plus monitoring of failovers; batching of status writes; KEDA
    autoscaling on queue depth; repeated benchmarks on dedicated hardware; per-tenant quotas if it
    became multi-tenant.

## 5. Things to try yourself

- Lower `heartbeat_ttl_s` to 3 in `experiments/kill_worker.py` and see how recovery time and false
  reclaims change. Pause a worker with `kill -STOP` to simulate a GC pause.
- Remove the `locked_by` condition in `store.finish`, run
  `test_stalled_worker_cannot_overwrite_the_new_owners_attempt`, and read the failure.
- Swap the order in `scheduler.promote` (Redis first) and run `test_m2_failures.py` a few times.
- Implement `XAUTOCLAIM`-based reclaim as an alternative and compare the code.
- Add a RabbitMQ `Broker` behind the same interface and repeat the drain benchmark.
