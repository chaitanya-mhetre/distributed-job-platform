# Delivery guarantees in Relay

## What Relay promises

- **At-least-once execution.** Every job that is accepted (201 from the API, or a successful
  `enqueue()`) eventually reaches exactly **one terminal state**: `succeeded`, `failed`, `dead` or
  `cancelled`. It is never silently lost, and it may run more than once.
- **Status in Postgres is authoritative.** Redis entries are hints ("job X is ready"). A worker only
  runs a job whose row says `queued`, and every status change is a guarded
  `UPDATE ... WHERE status = ANY(<allowed from>)`.

## Why not exactly-once?

A worker has to do two things: run the handler (which may call the outside world), then ack. No
system can make those two steps atomic:

- **Ack before running**: a crash in between loses the job. That's at-most-once.
- **Ack after running**: a crash in between runs the job again. That's at-least-once, and Relay's choice.

"Exactly-once" systems (Kafka transactions, for example) give exactly-once **effects** only inside
their own transactional boundary: read from Kafka, write to Kafka, commit offsets, all in one
transaction. As soon as the handler sends an email or charges a card, you're back to at-least-once
plus idempotency.

Measured: under repeated `kill -9`, 264 of 1,500 jobs (17.6%) executed more than once
(`benchmarks.md` §5).

## Where duplicates come from, and what closes each gap

| Crash window | What happens | Mitigation in Relay |
|---|---|---|
| after `INSERT` into jobs, before `XADD` | row exists, Redis never heard of it | scheduler **reconciliation** re-dispatches rows with `dispatched_at IS NULL` (outbox-lite) |
| after `XREADGROUP`, before `start_attempt` | entry pending, row still `queued` | the reaper re-publishes the entry; the row is still `queued`, so it runs normally |
| handler finished, before the outcome is written | job runs again | **idempotent handlers**; `ctx.once_atomic()` for DB effects, receiver idempotency keys for external ones |
| outcome written, before `XACK` | entry redelivered | the worker sees the row isn't `queued` anymore and just acks: **no second run** |
| worker paused (GC, CPU starvation) past its heartbeat TTL | the reaper gives the job to someone else while the original may still finish | the late worker's outcome is rejected (`finish()` requires `locked_by` = that worker); its side effects still happened, so they need idempotency or **fencing tokens** |

## Ordering rule that prevents lost jobs

**Postgres changes before Redis makes work visible.** The first version of the scheduler moved delayed
jobs into the stream and *then* flipped the rows to `queued`. A fast worker could read the entry while
the row still said `scheduled`. It would skip the job as "not runnable" and ack it, and the job was gone.
The retry integration test caught it. Now:

- promotion: rows → `queued`, then the Lua script XADDs;
- reclaim: rows → `queued`, then the entries are re-published;
- retry: row → `scheduled`, then ZADD;
- manual retry: row → `queued`, then XADD.

A crash between the two steps leaves Postgres "ahead" of Redis, which the next scheduler tick or the
reconciliation loop repairs.

## Side effects: `ctx.once()`, `ctx.once_atomic()`, and idempotent receivers

The job may run more than once. What matters is whether its **side effect** happens exactly once.
Three tools, measured under repeated `kill -9` (`benchmarks.md` §5b, 4,000 jobs each):

| Tool | How | Duplicated? | Lost? | Use for |
|---|---|---|---|---|
| `ctx.once(key)` | INSERT key, *then* do the effect | no | **yes** (17 of 4,000) | effects where skipping is acceptable |
| `ctx.once_atomic(key, fn)` | key + effect in **one DB transaction** | no | **no**, by construction (measured: 0 of 4,000, twice) | writes to Relay's Postgres database |
| idempotent receiver | send an idempotency key; the receiver dedupes | no (duplicates absorbed) | no (the job is retried until it succeeds) | external systems: payment APIs, email providers, other services |

- **`once()`** has a gap. If the process dies after recording the key and before the effect, the
  retry skips it.
- **`once_atomic()`** closes the gap by making "record" and "do" one atomic step. If it crashes before
  COMMIT, both roll back and the retry runs the effect. After COMMIT, both are durable.
  - Concurrent duplicates (a reclaimed job running twice) serialise on the key's primary-key lock.
  - The catch: `fn` must only write through the given connection, to the same database.
- **Why not exactly-once for everything?** Doing an external call and recording that you did it are
  two different systems. No local trick makes them atomic; the process can always die between them.
  That's the Two Generals problem in practice. The receiver has to deduplicate, which is why Stripe,
  Razorpay and friends take an `Idempotency-Key` header. Use the job id or a business key (`order:123`)
  as the key.

## Locks and fencing

`ctx.lock(resource)` is a Redis lease (`SET NX PX`). A lease can expire while its holder is paused,
so two holders can briefly coexist. Relay hands out a **fencing token** (a Redis `INCR` counter) with
each lease, and the protected resource must reject writes carrying an older token than it has already
seen. `fenced_write()` shows this in SQL, and `test_fencing_token_rejects_a_stale_lock_holder` proves it.
Without fencing, a Redis lock is a performance optimisation (fewer duplicate runs), not a correctness
guarantee. See Kleppmann, "How to do distributed locking" (2016).
