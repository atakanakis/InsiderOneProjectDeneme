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

## Consequences

- Strict and simple to reason about; one Redis round trip per dispatcher loop.
- Redis becomes a dependency for sending. If it is down, dispatch pauses (logged, retried
  every second) instead of sending without a limit. Failing closed protects the provider
  contract.
- The key is versioned (`ratelimit:v2:<channel>`) so a rolling deploy from the token-bucket
  build cannot feed the new script a hash (`WRONGTYPE`), which really happened during
  development.
