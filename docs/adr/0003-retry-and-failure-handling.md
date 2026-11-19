# ADR 0003: Retry policy and failure handling

**Status:** accepted

## Context

The assessment leaves delivery and retry logic to the candidate. Business context: millions of
messages a day, bursts during flash sales and breaking news, and a provider that can be slow,
throttle us, or go down.

## Decision

1. **Classification.** 2xx delivered. Network errors, timeouts, 408, 425, 429 and 5xx are
   retryable. Every other 4xx is permanent: the same request will fail the same way, and
   retrying a bad phone number 5 times only wastes quota.
2. **Exponential backoff with equal jitter.** `ceiling = min(max, base × 2^(n-1))`,
   `delay = ceiling/2 + rand(0, ceiling/2)`; base 2s, max 5m, 5 attempts. Equal jitter (not full
   jitter) keeps a floor so a retry never fires almost immediately by bad luck, while still
   spreading a burst of simultaneous failures. `Retry-After` is honoured when longer, capped at
   the max delay.
3. **Dead letter.** Exhausted or permanent failures end in `failed` with `last_error`; every
   attempt is stored in `notification_attempts` (outcome, HTTP status, error, duration).
   `POST /v1/notifications/{id}/retry` re-queues with a fresh attempt budget, keeping history.
4. **Circuit breaker per channel and worker.** 5 consecutive transient failures → open for
   15s → a single half-open probe → closed on success, open again on failure. While open the
   dispatcher does not claim rows, so an outage does not consume retries. Permanent 4xx counts
   as healthy (the provider answered).
5. **Leases and reaper.** Claimed rows carry `locked_until`; expired leases go back to
   `retrying` (or `failed` if the budget is used up). Completion is guarded by
   `status='processing' AND attempts=$n`, so a worker that lost its lease cannot overwrite the
   newer state; its attempt is still recorded for audit.
6. **Graceful shutdown.** On SIGTERM the dispatcher stops claiming; in-flight deliveries finish
   on a context detached from the shutdown signal and record their result.

## Alternatives considered

- **Fixed-interval retries:** simple, but synchronised retry storms after a provider blip.
- **Retry in-process with `time.Sleep`:** loses retries on crash and ties up worker slots;
  persisting `next_attempt_at` makes retries durable and visible.
- **Shared breaker state in Redis:** faster global reaction, but another hot key and more
  failure modes. Each worker detects an outage within 5 calls, which is good enough here.

## Consequences

Retries are durable, observable (status `retrying`, `nextAttemptAt`, attempt history,
metrics by outcome) and bounded. Worst case for a message is about 30 seconds of retries with
defaults before it is dead-lettered, unless the breaker holds it back during an outage.
