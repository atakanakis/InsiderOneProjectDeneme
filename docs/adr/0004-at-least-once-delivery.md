# ADR 0004: At-least-once delivery with idempotency keys

**Status:** accepted

## Context

"Idempotency support to prevent duplicate sends" covers two different duplicates:

1. **Duplicate requests**: a client retries `POST /v1/notifications` after a timeout, or a user
   double-clicks.
2. **Duplicate deliveries**: our worker sends, the provider accepts, but we never learn it
   (timeout, crash before recording), so we send again.

## Decision

**Requests.** Optional `Idempotency-Key` header / `idempotencyKey` field with a `UNIQUE`
constraint and `INSERT ... ON CONFLICT DO NOTHING`. A replay returns the original with `200`
and `Idempotent-Replayed: true`. The same key with a different payload is a `409`: returning
the old notification would hide a client bug. Batches are all-or-nothing in one transaction and
reject duplicate keys inside the same batch.

**Deliveries.** Exactly-once delivery to an external HTTP API is not achievable: after a
timeout we cannot distinguish "lost before the provider" from "accepted, response lost".
Dropping would lose messages, so the system is **at-least-once** and narrows the duplicate
window:

- `FOR UPDATE SKIP LOCKED` makes sure only one worker owns a notification at a time.
- The completion update is guarded by the lease, so a slow worker cannot resurrect a row.
  Exception: a late *success* still wins while nobody has claimed the row again (its
  `attempts` is unchanged), even if the reaper already put it back in the queue. The message
  was delivered; discarding that result would only cause a duplicate send.
- A batch-level `Idempotency-Key` header derives `batch:<key>:<index>` for items without their own
  key, so a client retrying a timed-out batch does not double-send it.
- Every provider call carries `Idempotency-Key: <notification id>`, stable across retries, so
  a provider that supports it de-duplicates. The mock provider does, and `/stats` counts the
  suppressed duplicates.

## Consequences

Clients get a simple contract, and the rare duplicate at the provider boundary is explicitly
handed to the provider's idempotency support instead of being hidden. Keys are global here; in
a multi-tenant deployment they would be scoped per tenant.
