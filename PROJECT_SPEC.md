# Distributed Job Platform — "Relay"
> A distributed job queue in Python, built on Redis Streams: priorities, scheduling, retries with backoff, dead letters, idempotency, heartbeats, distributed locks, cancellation, and real load tests.

## 1. Problem & why it exists
Every backend eventually needs "run this later, reliably": sending emails, generating reports, calling flaky APIs, running LLM batch jobs.
Libraries like Celery hide how that actually works. **Relay** builds the core of a job system myself, on top of Redis Streams,
so I can explain delivery guarantees, worker failure, duplicates, and backpressure from first principles, and back those explanations with measurements.

## 2. What this proves to an employer
| Skill | Target requirement |
|---|---|
| Distributed systems fundamentals (at-least-once, idempotency, leases, heartbeats) | Amazon/Microsoft/Atlassian "distributed systems"; Razorpay platform |
| Concurrency in Python (asyncio, process pools) | Zeko "async/await"; general Python depth |
| Queue design trade-offs (Redis vs Kafka vs RabbitMQ) | system-design interviews at every target |
| Load testing + honest measurements | Razorpay "observability/infra"; senior-signal engineering habit |
| FastAPI + Postgres result store | Zeko/EaseOps backend basics |

## 3. Scope
### In scope (v1)
- Job submission API (FastAPI) with a payload, a job type, priority (`high|default|low`), `run_at` for scheduling, `max_attempts`, `timeout_s`, `idempotency_key`
- Redis Streams: one stream per priority; consumer groups; `XREADGROUP`, `XACK`, `XAUTOCLAIM` for stuck jobs
- Workers: an async worker process with a concurrency limit, a per-job timeout, heartbeats, graceful shutdown
- Retries with exponential backoff + jitter, through a delayed-job sorted set (ZSET) moved back into the stream by the scheduler
- Dead-letter stream + an inspection/replay API
- Idempotency: submitting the same key returns the existing job; handler-side dedupe helper
- Distributed locks (a Redis `SET NX PX` lease with fencing tokens) for "only one of this job per resource at a time"
- Cancellation (cooperative: the worker checks a cancel flag; the running handler gets an `asyncio.CancelledError`)
- Job status tracking in PostgreSQL (the result store / history)
- Scheduler process: promotes delayed jobs, runs cron-style recurring jobs, reclaims jobs from dead workers
- Minimal dashboard (server-rendered with FastAPI + HTMX or Jinja): queue depths, running jobs, failures, DLQ
- Python client SDK: `relay.enqueue("send_email", {...}, priority="high")`
- Load tests (k6 for the API, a custom Python harness for end-to-end throughput) + failure-injection experiments
- Written comparison of Redis Streams vs Kafka vs RabbitMQ

### Out of scope (explicitly)
- Exactly-once processing (impossible in general; documented instead, see §8)
- Multi-region replication
- Implementing a Kafka backend in v1 (a possible M-extra behind the same `Broker` interface)
- Workflow DAGs (that's `python-backend-lab`)

## 4. Architecture
```
 client/SDK ──▶ FastAPI API ──▶ PostgreSQL (jobs table: status, attempts, result, history)
                    │
                    │ XADD
                    ▼
        Redis ┌───────────────────────────────────────────────┐
              │ streams: relay:q:high  relay:q:default  relay:q:low │
              │ zset:    relay:delayed   (score = run_at)            │
              │ stream:  relay:dlq                                   │
              │ keys:    relay:hb:{worker}, relay:cancel:{job}, locks │
              └──────────────┬───────────────────────▲────────────┘
                             │ XREADGROUP (weighted)  │ XADD (promote), XAUTOCLAIM
                     ┌───────▼────────┐        ┌──────┴─────────┐
                     │ worker × N     │        │ scheduler      │
                     │ asyncio pool   │        │ (single leader │
                     │ heartbeat task │        │  via lock)     │
                     └───────┬────────┘        └────────────────┘
                             │ status updates
                             ▼
                        PostgreSQL          dashboard ◀── reads Redis + Postgres
                                            Prometheus ◀── /metrics from api, worker, scheduler
```
- **API**: validates, writes the job row (source of truth for status), then `XADD`s. Writing Postgres first means a job never exists in Redis without a record.
  The inverse crash (row written, `XADD` fails) is repaired by the scheduler's reconciliation loop (§8.4).
- **Streams per priority**: workers read `high` first with weighted polling (for example 6:3:1), so low-priority work still makes progress. That avoids starvation.
- **Delayed ZSET**: streams have no native delay. The scheduler moves due jobs (`ZRANGEBYSCORE ... LIMIT`) into the stream atomically with a Lua script.
- **Consumer groups + pending entries list (PEL)**: Redis tracks which consumer holds each message. `XAUTOCLAIM` recovers messages whose consumer died.
- **Heartbeats**: workers refresh `relay:hb:{id}` (TTL 15 s). The scheduler treats a missing heartbeat as a dead worker and reclaims its PEL.
- **Scheduler leader election**: a lease lock, so only one scheduler promotes jobs (other replicas stay on standby).
- **PostgreSQL**: the durable history and the dashboard queries. Redis alone isn't treated as durable storage.

## 5. Tech stack & justification
| Choice | Why | Alternatives considered |
|---|---|---|
| Redis 7 Streams | consumer groups + PEL give at-least-once delivery with visible internals; I already know Redis | Kafka (better for high-throughput logs and replay), RabbitMQ (classic broker, rich routing) |
| Python asyncio + `redis.asyncio` | I/O-bound handlers; teaches cancellation and timeouts | threads; multiprocessing (used for CPU-bound handler mode) |
| FastAPI + SQLAlchemy async + Postgres | the same stack as other projects | — |
| k6 + custom asyncio harness | k6 for HTTP load; the harness measures submit→complete latency | locust (fine too; k6 chosen for scripting + thresholds) |
| Prometheus + Grafana | queue depth, lag, and throughput dashboards | — |
| `toxiproxy` | injects network latency/partitions into Redis for failure experiments | manual `tc` |

## 6. Data model
**PostgreSQL**
```
jobs(id uuid pk, type text, queue text, priority smallint, payload jsonb, idempotency_key text null,
     status enum(queued,scheduled,running,succeeded,failed,dead,cancelled),
     attempts int, max_attempts int, timeout_s int, run_at timestamptz,
     locked_by text null, lease_expires_at timestamptz null,
     result jsonb null, last_error text null, created_at, started_at, finished_at)
  unique (type, idempotency_key) where idempotency_key is not null
  index (status, run_at), index (type, created_at desc)
job_attempts(id bigserial, job_id fk, attempt int, worker_id, started_at, finished_at, outcome, error, duration_ms)
recurring_jobs(id, name unique, cron text, type, payload jsonb, enabled, last_enqueued_at)
```
**Redis keys**
```
relay:q:{priority}         stream entries {job_id, type, attempt}
relay:delayed              zset job_id → run_at epoch ms
relay:dlq                  stream
relay:hb:{worker_id}       string, TTL 15s
relay:cancel:{job_id}      string, TTL 1h
relay:lock:{resource}      string {token}, PX lease
relay:fence:{resource}     incr counter (fencing token)
relay:leader:scheduler     lease
```

## 7. API / interface design
```
POST   /v1/jobs                {type, payload, priority?, run_at?, max_attempts?, timeout_s?, idempotency_key?}
                               → 201 {id, status}  | 200 existing job (idempotent hit)
GET    /v1/jobs/{id}           → status, attempts[], result/error
GET    /v1/jobs?status=failed&type=&cursor=
POST   /v1/jobs/{id}:cancel    → 202
POST   /v1/jobs/{id}:retry     (from failed/dead)
GET    /v1/queues              → [{name, depth, pending, oldest_age_s, consumers}]
GET    /v1/dlq?cursor=         POST /v1/dlq/{entry}:replay   DELETE /v1/dlq/{entry}
CRUD   /v1/recurring-jobs
GET    /metrics  /healthz  /readyz
```
**SDK / worker**
```python
from relay import Relay, job

app = Relay(redis_url=..., db_url=...)

@app.job("send_email", timeout_s=30, max_attempts=5)
async def send_email(ctx: JobContext, to: str, template: str) -> None:
    async with ctx.lock(f"user:{to}"):       # distributed lock with fencing
        ...
    ctx.raise_if_cancelled()

app.enqueue("send_email", {"to": "a@b.c", "template": "welcome"}, priority="high")
# CLI
relay worker --queues high,default,low --concurrency 50
relay scheduler
relay dlq ls | replay <id>
```

## 8. Key engineering problems
1. **At-least-once delivery and duplicates.** A worker can finish a job and crash before `XACK`, so the job runs again.
   The design accepts duplicates. Handlers must be idempotent, and the SDK provides `ctx.once(key)` (Redis `SET NX` + a Postgres unique row) as a helper.
   The docs explain why exactly-once is only possible *within* a transactional boundary.
2. **Worker failure.** Heartbeat TTL expires → the scheduler `XAUTOCLAIM`s messages idle > `timeout_s + grace` → they're redelivered and `attempts` incremented.
   Experiment: `kill -9` a worker holding 50 jobs and measure the time to recovery (`TBD`).
3. **Retries and backoff.** `delay = min(cap, base * 2^attempt) * random(0.5, 1.5)` (jitter avoids thundering herds). After `max_attempts`, the job goes to the DLQ.
4. **Crash between the Postgres write and XADD.** Reconciliation: the scheduler scans `jobs where status='queued' and created_at < now()-30s`,
   checks whether the job is in a stream or the PEL, and re-XADDs if it's missing. Documented as the "outbox-lite" approach.
5. **Distributed locks are subtle.** A lease can expire while the holder is paused by GC or a network stall, and then two holders exist at once.
   Mitigation: fencing tokens checked by the protected resource (demonstrated with a Postgres `UPDATE ... WHERE fence < :token`).
   The docs discuss Redlock criticism (Kleppmann) honestly.
6. **Backpressure.** If producers outpace workers, stream length grows. Mitigations: `MAXLEN ~` trimming only for acked history,
   the API returns 429/503 when queue depth > threshold (configurable), and a metric plus an alert on consumer lag.
7. **Priority starvation.** Weighted polling instead of strict priority; tested under a sustained high-priority flood.
8. **Cancellation semantics.** Queued → removed/skipped when it's dequeued. Running → cooperative cancel, and the handler gets `CancelledError` within `TBD` s.
   Handlers that ignore cancellation are force-timed-out.
9. **Graceful shutdown.** On SIGTERM, stop reading, let in-flight jobs finish up to a drain timeout, then leave them unacked so they get reclaimed.

## 9. Milestones
**M1: Single-queue core (1–2 wks).** API + Postgres job row + one stream, a worker with a consumer group, ack, status updates.
*Accept:* submit 1,000 jobs and all complete; the status history is correct; integration tests with testcontainers Redis/Postgres.

**M2: Failure handling (2 wks).** Timeouts, retries with backoff + jitter, the delayed ZSET + scheduler, DLQ + replay, heartbeats + XAUTOCLAIM, graceful shutdown.
*Accept:* failure-injection tests: a handler that raises, a handler that hangs, `kill -9` of a worker. No job lost (every job ends succeeded, dead, or cancelled).

**M3: Priorities, scheduling, recurring, idempotency (1 wk).**
*Accept:* the starvation test passes; a duplicate submission returns the same job; cron jobs fire once per period with 2 scheduler replicas (leader election works).

**M4: Locks, cancellation, concurrency limits (1 wk).** Fencing-token locks; per-type concurrency limits (`max_concurrent=5` for `call_llm`); cancel.
*Accept:* a lock test with an artificial pause proves the fencing token rejects a stale holder.

**M5: Observability + dashboard (1 wk).** Prometheus metrics (queue depth, oldest age, throughput, latency histogram, retries, DLQ size), Grafana JSON, HTMX dashboard.

**M6: Load testing + experiments (1–2 wks).** k6 API tests; the end-to-end throughput harness; toxiproxy latency/partition experiments; results in `docs/benchmarks.md`.
*Accept:* every number has a script, a commit hash, and hardware specs; there are graphs.

**M7: Trade-off write-up + packaging (1 wk).** `docs/redis-vs-kafka-vs-rabbitmq.md`, the delivery-guarantees doc, the SDK published to TestPyPI (PyPI optional), Docker Compose, CI.

## 10. Testing strategy
- Unit: the backoff calculator, priority weighting, state transitions (a job state machine with illegal-transition tests).
- Integration: real Redis/Postgres; the scheduler promotion Lua script; XAUTOCLAIM recovery.
- Chaos/failure tests (marked `@pytest.mark.slow`, run nightly in CI): kill workers, pause Redis with toxiproxy, duplicate delivery.
- Property-based: random interleavings of submit/cancel/fail. Invariant: every job reaches exactly one terminal state.

## 11. Observability
Metrics: `relay_queue_depth{queue}`, `relay_pending{queue}`, `relay_oldest_job_age_seconds{queue}`, `relay_jobs_total{type,outcome}`,
`relay_job_duration_seconds{type}` (histogram), `relay_retries_total{type}`, `relay_dlq_size`, `relay_worker_heartbeats`, `relay_reclaimed_total`.
Logs: JSON with `job_id`, `attempt`, `worker_id`. Alert examples: DLQ growth, oldest age > SLO, no live workers.

## 12. Security
- API auth with a static API key (simple; this isn't a multi-tenant product) and payload size limits.
- Job types are allow-listed (no arbitrary code execution from payloads, no pickle; JSON only).
- Redis with a password + no public exposure (Compose network only); TLS noted for the cloud.

## 13. Deployment
Docker Compose: api, worker ×3, scheduler ×2, redis, postgres, prometheus, grafana, toxiproxy.
Optional: a Kubernetes manifest set reused in `cloud-infra-lab` M-K8s, with an HPA scaling workers on queue depth via KEDA (stretch goal).

## 14. Evaluation / measurements to collect
All `TBD — measure with loadtest/ and experiments/`:
- Max sustained throughput (jobs/s) for no-op jobs with 1/2/4/8 workers (a scaling curve)
- Submit → complete latency p50/p95/p99 at [N] jobs/s
- Recovery time after `kill -9` of a worker (time until reclaimed jobs finish)
- Duplicate execution rate under forced crash-before-ack (expected > 0; showing that dedupe works)
- Effect of 100 ms injected Redis latency on throughput

## 15. Prerequisite learning
`learning/python/async-concurrency`, `learning/backend/redis` (streams, Lua, transactions),
`learning/distributed-systems/` (delivery semantics, idempotency, locks & fencing, backpressure),
`learning/cloud/observability`, `system-design/designs/distributed-job-queue.md`.

## 16. Interview talking points
- "At-least-once vs at-most-once vs exactly-once: which does your system give and why?"
- "What happens if a worker dies mid-job?"
- "Why are distributed locks dangerous? What are fencing tokens?"
- "When would you pick Kafka over Redis Streams? RabbitMQ?"
- "How do you handle backpressure and priority starvation?"
- "Show me your load test methodology."

## 17. Resume bullet templates
- Built a distributed job queue in Python on Redis Streams (consumer groups, visibility-timeout reclaim, exponential backoff, dead-letter queue, fencing-token locks), with at-least-once delivery verified by failure-injection tests.
- Load-tested to [MEASURED_VALUE] jobs/s with p99 submit→complete latency of [MEASURED_VALUE] ms on [HARDWARE]; recovery after worker crash in [MEASURED_VALUE] s.

## 18. Open questions / uncertainties
- Overlap with Ponticare's Redis Streams event bus (Go): this Python version must be written by me, and the comparison doc can reference Ponticare only once the IP question is resolved.
- The dashboard stack (HTMX vs a tiny React app): HTMX keeps the focus on the backend. Revisit if it limits the demo.
- Whether to implement a second `Broker` backend (RabbitMQ or Kafka) as a stretch goal to make the comparison empirical rather than theoretical.
