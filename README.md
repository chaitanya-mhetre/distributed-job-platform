# Relay

A distributed job queue in Python, built on Redis Streams and PostgreSQL. It covers priorities,
delayed and recurring jobs, retries with backoff, dead letters, idempotency, heartbeats, fencing-token
locks, cancellation, and load tests that actually ran.

> **Status:** v0.1.0, a learning/portfolio project. All 7 spec milestones are done (tags `m1`–`m7`,
> `v0.1.0`). It hasn't run in production. Numbers below come from a busy laptop; see
> [docs/benchmarks.md](docs/benchmarks.md) before quoting them.

## Problem

Every backend eventually needs "run this later, reliably": emails, reports, flaky third-party calls,
LLM batch jobs. Libraries like Celery hide how that works. Relay builds the core of a job system on
Redis Streams, so the delivery guarantees, worker failure handling, duplicates and backpressure are
visible in about 3,000 lines of typed Python and backed by tests and measurements.

## Why it exists

It's a vehicle for learning and explaining distributed-systems basics from first principles:
at-least-once delivery, idempotency, leases and heartbeats, fencing tokens, backpressure,
and honest load testing. It isn't meant to replace Celery, RQ, arq or Dramatiq.

## Architecture

```
 client / SDK ──▶ FastAPI API ──▶ PostgreSQL  jobs (source of truth), job_attempts, recurring_jobs
                      │  INSERT row, then XADD id
                      ▼
          Redis ┌──────────────────────────────────────────────────────────────┐
                │ streams  relay:q:high | relay:q:default | relay:q:low          │
                │ zset     relay:delayed (run_at)        stream relay:dlq       │
                │ keys     relay:hb:{worker}  relay:cancel:{job}  relay:lock:*   │
                └───────────┬─────────────────────────────────▲────────────────┘
                            │ XREADGROUP (weighted 6:3:1)      │ promote (Lua), XCLAIM, re-publish
                  ┌─────────▼──────────┐              ┌────────┴─────────┐
                  │ worker × N          │              │ scheduler × 2     │
                  │ asyncio, concurrency│              │ lease-elected     │
                  │ heartbeat + cancel  │              │ leader            │
                  └─────────┬──────────┘              └──────────────────┘
                            │ outcome (one SQL statement), then XACK + XDEL
                            ▼
                       PostgreSQL        dashboard (HTMX) + /metrics ──▶ Prometheus ──▶ Grafana
```

- **Postgres is the truth, Redis carries ids.** A worker runs a job only if its row says `queued`.
  Every status change is a guarded `UPDATE ... WHERE status = ANY(<allowed from>)`.
- **Postgres before Redis.** Rows change before work becomes visible in a stream. The first version
  got this backwards and could lose jobs; the retry test caught it.
- **Workers** refresh a heartbeat key and the idle time of their in-flight entries, so the scheduler
  can tell "slow job on a live worker" from "dead worker".
- **The scheduler** (one leader, via a lease) promotes delayed jobs, reclaims dead workers' jobs,
  enqueues cron jobs, and re-dispatches rows that never reached Redis ("outbox-lite").

More detail: [docs/delivery-guarantees.md](docs/delivery-guarantees.md) ·
[docs/redis-vs-kafka-vs-rabbitmq.md](docs/redis-vs-kafka-vs-rabbitmq.md) ·
[docs/LEARNING_GUIDE.md](docs/LEARNING_GUIDE.md)

## Features

- Priorities (`high` / `default` / `low`) with smooth weighted round-robin, so there's no starvation
- `run_at` scheduling, cron recurring jobs (no backfill; exactly one job per fire time, even with two schedulers)
- Retries with capped exponential backoff + jitter; `PermanentError` for "don't retry"
- Per-job timeouts, including handlers that swallow `CancelledError`
- Dead-letter stream, with replay from the API/CLI
- Idempotent submission (`idempotency_key`) and `ctx.once()` for side effects
- Heartbeats, dead-worker reclaim, graceful drain on SIGTERM
- Distributed locks with **fencing tokens**; distributed per-type concurrency limits (`max_concurrent`)
- Cooperative cancellation of queued, delayed and running jobs
- Backpressure: `503 + Retry-After` when a queue is over `max_queue_depth`
- Prometheus metrics, alert rules, Grafana dashboard, read-only HTMX dashboard, JSON logs

## Tech stack

Python 3.12 · asyncio · Redis 7 Streams + Lua · PostgreSQL 16 (SQLAlchemy 2 async + asyncpg, plain SQL
migrations) · FastAPI · Typer · Jinja2 + HTMX · prometheus-client · pytest + Hypothesis · k6 · toxiproxy ·
Docker Compose · GitHub Actions

## Quick start

```bash
uv sync
make up                                   # Redis :56384, Postgres :55437
uv run relay migrate
uv run relay worker &                     # uses examples/jobs.py
uv run relay scheduler &
uv run relay api --port 18082 &           # RELAY_API_KEY unset = no auth (dev only)
uv run relay dashboard --port 18083 &     # http://localhost:18083/dashboard
```

Or run the whole stack (API, 3 workers, 2 schedulers, dashboard, Prometheus :59091, Grafana :53001):

```bash
make stack
```

## Usage

```python
from relay import JobContext, PermanentError, Relay

app = Relay()  # settings from RELAY_* env vars (see .env.example)


@app.job("send_email", timeout_s=30, max_attempts=5)
async def send_email(ctx: JobContext, to: str, template: str) -> None:
    if "@" not in to:
        raise PermanentError("bad address")  # fail now, no retries
    async with ctx.lock(f"user:{to}") as lease:  # lease.token is the fencing token
        if await ctx.once(f"welcome:{to}"):  # side effect at most once
            ...


@app.job("call_llm", max_concurrent=5)  # at most 5 running across all workers
async def call_llm(ctx: JobContext, prompt: str) -> str: ...


await app.enqueue(
    "send_email",
    {"to": "a@b.c", "template": "welcome"},
    priority="high",
    idempotency_key="welcome-a@b.c",
)
await app.add_recurring("nightly-report", "0 3 * * *", "report")
```

### HTTP API

```
POST   /v1/jobs                     201 new job | 200 idempotent hit | 422 unknown type | 503 queue full
GET    /v1/jobs/{id}                status, result, attempt history
GET    /v1/jobs?status=&type=&cursor=&limit=      keyset pagination
POST   /v1/jobs/{id}:cancel         202 | 409 already finished
POST   /v1/jobs/{id}:retry          failed/dead jobs only
GET    /v1/dlq    POST /v1/dlq/{entry}:replay    DELETE /v1/dlq/{entry}
CRUD   /v1/recurring-jobs
GET    /metrics  /healthz  /readyz  /dashboard
```

Mutating and read endpoints require `X-API-Key` when `RELAY_API_KEY` is set.

### CLI

```
relay migrate | api | worker | scheduler | dashboard
relay dlq ls | relay dlq replay <entry-id>
```

## Testing

```bash
make check      # ruff, mypy --strict, 68 tests (unit + integration against real Redis/Postgres)
```

- **Unit tests**: state machine (including Hypothesis property tests), backoff, weighted order, cron, cursors.
- **Integration tests**: 1,000-job acceptance; retries → DLQ → replay; permanent errors; hangs and
  handlers that ignore cancellation; simulated and **real `kill -9`** of a worker; graceful drain;
  reconciliation; a 150-job chaos mix; starvation; leader failover; recurring jobs with two schedulers;
  fencing; locks; per-type limits; cancellation; metrics; dashboard; backpressure.
- **Load and failure experiments**: `loadtest/` and `experiments/`, with results in `docs/benchmarks.md`.

## Deployment

`docker-compose.yml` (profile `stack`) is a complete single-host deployment. Scaling out means more
`worker` replicas. Keep `processes × db_pool_size` under Postgres `max_connections`, or put PgBouncer
in front. Workers need `stop_grace_period` greater than the drain timeout so SIGTERM drains cleanly.
A Kubernetes version (HPA/KEDA on queue depth) is planned in `cloud-infra-lab`.

## Security

- API key on all `/v1` endpoints; request body size limit; job types are an allow-list (the
  registry), payloads are JSON only (no pickle, no arbitrary code).
- Redis and Postgres require passwords and are only published on 127.0.0.1 in Compose. Use TLS on real networks.
- The dashboard is read-only; all operator actions go through the authenticated API or CLI.

## Performance (measured on a busy laptop; see caveats)

From [docs/benchmarks.md](docs/benchmarks.md) (i5-1240P laptop, everything on one machine, heavy
background load, single runs):

- Drain of 20,000 no-op jobs: **~1,270 jobs/s with 8 worker processes**. 1 worker: ~330–440 jobs/s.
- Open-loop 100 jobs/s: submit → complete **p50 12 ms, p95 398 ms, p99 688 ms**.
- `kill -9` of a worker holding 50 jobs: all recovered in **15–17 s** (heartbeat TTL 15 s), none lost.
- Under repeated `kill -9`: **17.6%** of jobs ran more than once; `once()` stopped duplicate side
  effects, but **4 of 1,500** side effects were lost in its documented gap.

## Engineering trade-offs

- **Redis Streams over Kafka/RabbitMQ**: per-message acks and a visible pending list fit a job queue.
  Delays, DLQ and priorities had to be built by hand ([comparison](docs/redis-vs-kafka-vs-rabbitmq.md)).
- **Postgres as the source of truth**: durable history and simple dashboards, at the cost of about
  4 writes per job. That's the throughput ceiling today.
- **At-least-once, not exactly-once**: honest and measurable. Idempotency is the handler's job.
- **Heartbeat TTL (15 s)**: lower means faster recovery, but more false reclaims of paused workers.
- **Weighted, not strict, priority**: low-priority work always makes progress, so high-priority work is
  only *mostly* first.

## Limitations

- One Redis. Stream data and locks aren't safe across a Redis failover with async replication.
  Postgres plus reconciliation limits the damage for jobs, but not for locks.
- Throughput is bounded by Postgres writes per job. There's no batching yet.
- The per-type limit defers jobs by re-scheduling them, which adds latency under heavy contention.
- The benchmarks come from a noisy laptop with single runs. The CI workflow is written but has
  **not yet run on GitHub**.
- No web UI actions, no multi-tenancy, no job DAGs.
- Not published to PyPI (the name `relay` is taken; it would need another distribution name).

## Roadmap

- Batch status writes; optional attempt history, to raise the Postgres ceiling
- A second `Broker` backend (RabbitMQ) so the comparison is empirical
- Kubernetes manifests with KEDA autoscaling on queue depth (`cloud-infra-lab`)
- Repeat the benchmarks on a dedicated VM, with error bars

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). MIT licensed.
