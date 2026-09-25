# Redis Streams vs Kafka vs RabbitMQ (for a job queue)

Relay is built on Redis Streams. This page explains that choice and when I'd pick something else.
It's a design comparison. I did **not** benchmark Kafka or RabbitMQ in this project, so there are
no throughput claims for them here.

## The short version

| | Redis Streams | Kafka | RabbitMQ |
|---|---|---|---|
| Mental model | an append-only log in memory, with consumer groups | a partitioned, replicated, durable log on disk | a broker that routes messages into queues and deletes them once acked |
| Unit of parallelism | consumers in a group; any consumer can take any entry | **partitions**: one consumer per partition per group | consumers on a queue (competing consumers) |
| Per-message ack | yes (XACK); unacked entries sit in the PEL | no, just a committed **offset** per partition | yes (basic.ack / nack / requeue) |
| Redelivery of a stuck message | manual: XPENDING + XCLAIM/XAUTOCLAIM | none per message; you re-read from an offset | automatic when the consumer's channel dies |
| Delayed / scheduled messages | not built in (Relay uses a ZSET) | not built in | via a plugin, or TTL + dead-letter exchange tricks |
| Dead-lettering | DIY (Relay uses a DLQ stream) | DIY (a DLQ topic) | built in (dead-letter exchanges) |
| Priorities | DIY (Relay: one stream per priority + weighted polling) | DIY (separate topics) | built in (priority queues, 1–255) |
| Durability | memory first; AOF/RDB persistence; async replication can lose recent writes on failover | disk + replication with `acks=all`: strongest | disk + quorum queues (Raft) |
| Replay history | possible, if you don't delete acked entries | **the core feature**: retention by time or size | no; acked messages are gone |
| Ordering | per stream | per partition | per queue (weakens with redelivery and multiple consumers) |
| Operational weight | lowest (you probably already run Redis) | highest (brokers, partitions, rebalancing, and KRaft or ZooKeeper) | medium |

## Why Redis Streams for Relay

1. **Per-message acknowledgement and a visible pending list.** A job queue needs "this job is being
   worked on by X since T" and "give it to someone else if X died". The PEL plus XCLAIM is that,
   directly. In Kafka you only commit an offset per partition. One slow job blocks every job behind
   it in that partition (head-of-line blocking), unless you build your own per-message tracking.
2. **Any worker can take any job.** Kafka caps parallelism at the partition count, and consumers that
   join or leave trigger rebalances. For jobs with very different durations (a 5 ms email vs a
   5-minute report), competing consumers are a better fit than partitions.
3. **Small operational surface, and I can read everything.** Every Relay behaviour is a handful of
   Redis commands I can show in `redis-cli`. That's the learning goal of this project.
4. **Postgres is the durable record.** Relay doesn't need Redis to be durable: job state lives in
   Postgres and Redis only carries ids. A lost Redis entry is repaired by the reconciliation loop,
   because a row with `dispatched_at IS NULL` or a stuck `queued` row can be found again.

## What Redis Streams costs me (things I had to build)

- **Delays**: a ZSET plus a Lua script to move due jobs atomically.
- **Dead letters**: a separate stream, and a replay API.
- **Priorities**: three streams plus weighted round-robin polling.
- **Redelivery**: heartbeats, idle-time refresh (XCLAIM to self), and a reaper in the scheduler.
- **Durability caveats**: with async replication, a Redis failover can drop the last few writes. Postgres
  plus reconciliation covers that for Relay. A Redis-only design would not be safe.

RabbitMQ gives several of these out of the box: dead-letter exchanges, priorities, and redelivery on
connection loss.

## When I'd choose Kafka instead

- **Event streaming, not jobs**: many independent consumers reading the same events (analytics, search
  indexing, audit), each at its own pace, with **replay** after a bug fix.
- **Very high sustained throughput** with ordering per key (per user or account).
- **Long retention** (days or weeks) and log compaction.
- The cost: partitions limit parallelism, rebalancing, and heavy operations (or pay for a managed service).

## When I'd choose RabbitMQ instead

- A **classic task queue** with routing needs (topic or headers exchanges, fan-out), built-in
  dead-lettering, priorities, per-message TTLs.
- Teams that want redelivery on consumer death without writing a reaper.
- The cost: no replay, and throughput and ordering get harder at the very high end.

## Delivery semantics are the same everywhere

All three give **at-least-once** in practice for a worker that acks after doing the work. None of them
can make "run the handler" and "ack the message" one atomic step when the handler talks to the outside
world. See `delivery-guarantees.md` and the duplicate experiment in `benchmarks.md` §5: 17.6% of jobs
ran twice under repeated `kill -9`.
