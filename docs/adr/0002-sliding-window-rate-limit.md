# ADR 0002: Strict sliding-window rate limit in Redis

**Status:** accepted (replaces an earlier token-bucket version)

## Context

"Maximum 100 messages per second per channel" must hold across every worker replica, so the
limiter has to be shared, atomic, and immune to worker clock skew.

## Decision

A Lua script in Redis per channel key:

1. read the Redis clock (`TIME`),
2. drop permits older than the window from a sorted set (`ZREMRANGEBYSCORE`),
3. grant `min(requested, limit - ZCARD)` permits, adding one member per permit,
4. if short, return how long until the oldest permit expires.

Redis runs the script atomically, so concurrent workers cannot both see free capacity.
Unused permits (the queue turned out to be empty) are removed again.

## Why not a token bucket

The first implementation was a token bucket (rate 100, burst 10). The two-worker integration
test measured 55 sends inside one second with a 50/s limit, which is correct token-bucket
behaviour: a window can contain `rate + burst`. "Maximum" means a hard cap, so the sliding
window log replaced it. Memory is O(limit) per channel (100 members), which is negligible.

## Safety margin

A permit is taken a few milliseconds before the HTTP request leaves the worker (the row still
has to be claimed). If one second's requests leave late and the next second's leave early, the
provider can observe more than the limit. The test showed 60 at a limit of 50 before the fix.
The window is therefore `1s + RATE_LIMIT_MARGIN` (50 ms): at most ~5% lower peak throughput,
but the limit now holds as the provider measures it (max 50/50 and 100/100 in tests and load
runs).

**Amendment (after independent review).** The margin only covers the *usual* gap between
permit and send. A reviewer paused the Postgres container for 0.6s during a burst and the
provider saw 150 requests in one second: the claim returned late, and the permits it carried
had been stamped before the pause. The dispatcher now measures how long Take plus the claim
took; if longer than the margin (`PermitMaxAge`), it refunds those permits and sends the
claimed rows only as fresh permits arrive. `TestRateLimitHoldsWhenClaimsStall` reproduces the
overshoot without this (75 at 50) and measures exactly the limit with it, as did a live load
run with five 0.6s pauses (100 at 100). The alternative, taking a permit per message right
before each HTTP call, would also work but costs one Redis round trip per message instead of
per claim.

**Second amendment (review 3).** The check above ended when the delivery goroutine started,
so connection setup was still uncovered: through a proxy that delayed new connections by
120 ms, a burst from idle produced 200 at a limit of 100. The permit's age is now checked
again inside the HTTP client when a connection is ready (`httptrace.GotConn`), immediately
before the request is written, and a fresh single permit is taken if it is stale. The stale
permit is not refunded, so a cold start costs up to one window of throughput; that is the
price of not exceeding the limit.

## Consequences

- Strict and simple to reason about; one Redis round trip per dispatcher loop.
- Redis becomes a dependency for sending. If it is down, dispatch pauses (logged, retried
  every second) instead of sending without a limit. Failing closed protects the provider
  contract.
- If Redis *loses its state* while up (a failover to a replica that had not received the last
  writes, or a flush), the permits of the last second are forgotten and up to twice the limit
  can be sent in the window that straddles the loss. A reviewer measured 200 at a limit of
  100 by flushing Redis four times mid-burst. A plain restart keeps the limit, because the
  window is rebuilt within a second. Guarding against state loss would need either a local
  per-process ceiling (which only works with a known, fixed number of workers) or a warm-up
  period after a key goes missing, which also slows every normal start from idle. Neither seems
  worth it for an event that needs operator action anyway. The provider's own 429 with
  `Retry-After`, which the retry policy honours, is the backstop.
- The key is versioned (`ratelimit:v2:<channel>`) so a rolling deploy from the token-bucket
  build cannot feed the new script a hash (`WRONGTYPE`), which really happened during
  development.
