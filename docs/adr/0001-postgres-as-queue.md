# ADR 0001: PostgreSQL as the source of truth and the work queue

**Status:** accepted

## Context

Notifications need durable state (status, attempts, history, filters, pagination) and a queue
that supports priorities, delayed delivery (schedules and retry backoff), cancellation of
queued items, and many concurrent workers. The expected load is "millions per day" with bursts,
capped at 100 msg/s per channel by the provider, so at most about 300 deliveries per second
system-wide.

## Options

| Option | Pros | Cons for this system |
|---|---|---|
| **PostgreSQL table + `FOR UPDATE SKIP LOCKED`** | One store, transactional with the state; priorities are `ORDER BY`; delays are a `next_attempt_at` column; cancel is an `UPDATE`; no dual write | Throughput ceiling of a single primary (thousands/s), polling |
| Kafka | Massive throughput, replay, partition ordering | No priorities (need a topic per priority), no per-message delay, no cancel of an enqueued message, needs an outbox for consistency with the state store |
| RabbitMQ | Native priority queues, acks | Delays need plugins or TTL + DLX chains; cancelling a queued message is not possible; still a dual write with the database |
| Redis lists / sorted sets / streams | Fast | Second source of truth, so DB and queue can diverge on a crash between the two writes; persistence semantics weaker |

## Decision

Use PostgreSQL for both. Every requirement (priority, schedule, backoff, cancel, crash
recovery) becomes a column and an `UPDATE`, inside the same transaction as the state change, so
there is no window in which the queue and the database disagree.

- Claim: `WITH due AS (SELECT ... WHERE channel=$1 AND status IN ('pending','retrying') AND
  next_attempt_at <= now() ORDER BY priority, next_attempt_at LIMIT $2 FOR UPDATE SKIP LOCKED)
  UPDATE ... SET status='processing', attempts=attempts+1, locked_until=now()+lease`.
- A partial index on `(channel, priority, next_attempt_at) WHERE status IN ('pending','retrying')`
  only contains waiting rows, so it stays small no matter how much history accumulates.
- Leases (`locked_until`) plus a reaper recover rows from crashed workers.
- Workers poll every 200 ms when idle; under load they loop without sleeping.

## Consequences

- One fewer moving part to run, back up and reason about.
- The ceiling is a single Postgres primary. At ~300 msg/s that is far away. The scale-out path
  is to keep Postgres for state and move dispatch to Kafka fed by a transactional outbox;
  `delivery.Queue` is the interface to swap.
- "Event-driven" is still how the system behaves: the API only records intent and returns
  202, workers react to due rows, and status changes are published as events (Redis Pub/Sub →
  WebSocket).
