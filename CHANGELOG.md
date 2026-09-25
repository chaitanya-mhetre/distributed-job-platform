# Changelog

## 0.1.0 (2026-09-25)

### M1: single-queue core
- Job state machine as data; guarded status transitions in SQL; versioned SQL migrations.
- Redis Streams broker (consumer groups, XACK + XDEL), SDK (`Relay`, `@app.job`), worker, HTTP API, CLI.

### M2: failure handling
- Timeouts (including handlers that swallow cancellation), retries with capped exponential backoff + jitter.
- Delayed ZSET + Lua promotion, dead-letter stream + replay, manual retry.
- Heartbeats with idle-time refresh, dead-worker reclaim, stuck-entry safety net, graceful drain.
- Outbox-lite reconciliation of rows never handed to Redis.
- Fixed: promotion made entries visible before the row said `queued`, so jobs could be lost.

### M3: priorities, scheduling, idempotency
- Smooth weighted round-robin priority polling (6:3:1).
- Idempotent submission, `ctx.once()`, cron recurring jobs (no backfill, per-fire idempotency key).
- Scheduler leader election with a lease.

### M4: locks, limits, cancellation
- Lease locks with fencing tokens + a fenced-write demo; distributed per-type concurrency limits.
- Cooperative cancellation (API, flag polling, `ctx.raise_if_cancelled()`).

### M5: observability
- Prometheus metrics, sampled queue gauges, alert rules, Grafana dashboard, HTMX dashboard.
- Backpressure: 503 + Retry-After when a queue is over `max_queue_depth`.

### M6: load tests and experiments
- Drain/latency harness, k6 script, kill -9 recovery, duplicate-rate and Redis-latency experiments.
- perf: single-statement job transitions (CTEs, autocommit) and a fixed connection budget, after the
  8-worker run exhausted Postgres connections.
- fix: a stalled worker can no longer overwrite the new owner's attempt.

### M7: write-ups and packaging
- Redis vs Kafka vs RabbitMQ, delivery guarantees, benchmarks, learning guide; CI; wheel build.
