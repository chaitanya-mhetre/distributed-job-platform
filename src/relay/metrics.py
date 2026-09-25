"""Prometheus metrics.

Counters/histograms are updated where things happen (worker, scheduler, enqueue).
Queue gauges (depth, pending, oldest age, DLQ size, live workers) are *sampled* from Redis by
`sample_queue_gauges`, called by the API before each /metrics scrape and by the scheduler
every few ticks. Alert rules live in deploy/prometheus/alerts.yml.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from prometheus_client import Counter, Gauge, Histogram

if TYPE_CHECKING:
    from relay.app import Relay

JOBS = Counter("relay_jobs_total", "Finished executions by outcome", ["type", "outcome"])
ENQUEUED = Counter("relay_enqueued_total", "Jobs submitted", ["type", "priority"])
REJECTED = Counter("relay_rejected_total", "Submissions rejected by backpressure", ["queue"])
RETRIES = Counter("relay_retries_total", "Executions scheduled for retry", ["type"])
RECLAIMED = Counter("relay_reclaimed_total", "Jobs reclaimed from dead or stalled workers")
PROMOTED = Counter("relay_promoted_total", "Delayed jobs moved to their queue")
DEFERRED = Counter("relay_deferred_total", "Jobs deferred by a per-type limit", ["type"])
DURATION = Histogram(
    "relay_job_duration_seconds",
    "Handler run time",
    ["type"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300),
)
QUEUE_LATENCY = Histogram(
    "relay_queue_wait_seconds",
    "Time from run_at until a worker started the job",
    ["priority"],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300, 1800),
)

QUEUE_DEPTH = Gauge("relay_queue_depth", "Entries in the stream (waiting + in flight)", ["queue"])
PENDING = Gauge("relay_pending", "Delivered but not yet acked", ["queue"])
OLDEST_AGE = Gauge("relay_oldest_job_age_seconds", "Age of the oldest entry", ["queue"])
DLQ_SIZE = Gauge("relay_dlq_size", "Entries in the dead-letter stream")
DELAYED = Gauge("relay_delayed_jobs", "Jobs waiting in the delayed set")
LIVE_WORKERS = Gauge("relay_live_workers", "Workers with a live heartbeat")
LEADER = Gauge("relay_scheduler_is_leader", "1 if this scheduler holds the lease")


async def live_workers(relay: Relay) -> list[str]:
    broker = relay.broker
    members = sorted(str(m) for m in await broker.redis.smembers(broker.keys.workers))
    alive: list[str] = []
    for wid in members:
        if await broker.redis.exists(broker.keys.heartbeat(wid)):
            alive.append(wid)
        else:
            await broker.redis.srem(broker.keys.workers, wid)  # prune crashed workers
    return alive


async def sample_queue_gauges(relay: Relay) -> None:
    broker = relay.broker
    for q in await broker.queue_stats(int(time.time() * 1000)):
        QUEUE_DEPTH.labels(q.name).set(q.depth)
        PENDING.labels(q.name).set(q.pending)
        OLDEST_AGE.labels(q.name).set(q.oldest_age_s)
    DLQ_SIZE.set(await broker.dlq_size())
    DELAYED.set(await broker.delayed_count())
    LIVE_WORKERS.set(len(await live_workers(relay)))
