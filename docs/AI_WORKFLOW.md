# How this project was built with an AI coding agent

This assessment was built with Claude Code, in the same way I build production software: the
agent writes much of the code, and I own the decisions and the evidence that the code works.
This page explains the workflow and, more usefully, what the verification caught.

## Workflow

1. **Understand before typing.** I read the brief, listed the implicit requirements
   (the open-ended retry design, clean history, the likely follow-up interview) and chose the
   architecture up front: PostgreSQL as queue, Redis for limits and events, stdlib HTTP.
   Decisions with real alternatives were written down as ADRs.
2. **Context over prompts.** [`CLAUDE.md`](../CLAUDE.md) gives the agent the map, the commands
   and the invariants that must never be broken (for example, "queue timestamps come from the
   database clock" and "status changes only under the lease guard"). The invariants were
   written into that file after the bugs below were found, so they cannot come back.
3. **Small, reviewable steps.** Domain → store → engine → API → binaries → infrastructure, each
   building on its own, each a commit whose message explains the why.
4. **Treat generated code as untrusted until something independent proves it.** The proof
   comes from real infrastructure and measurements, never from the agent saying it is done.

## Verification the agent cannot fake

| Check | How |
|---|---|
| Real dependencies | Integration tests run on Postgres 17 and Redis 7 containers (testcontainers), not mocks |
| Concurrency | `-race` on every run; ten goroutines competing for one queue; twenty simultaneous idempotent creates |
| The rate limit as the provider sees it | Timestamps are recorded at the provider (fake provider in tests, the mock provider's `/stats` in load runs), not in the limiter |
| End to end | CI boots the compose stack and waits for a notification to reach `sent`; a manual run against a real webhook.site URL checked the request body and headers |
| Lint | golangci-lint v2 with bodyclose, errorlint, gosec, rowserrcheck, sqlclosecheck, noctx |

## Defects caught before submission

Each was found by a check from the table above, not by reading generated code, and each has a
commit explaining the fix.

1. **Clock skew made new notifications invisible.** `next_attempt_at` was set from the API
   host's clock and compared with Postgres `now()`. A few ms of skew between the Docker VM and
   the host meant a just-created row was "not yet due". Found by the first store integration
   test. Fix: every queue timestamp is computed by the database. (`fix(queue): ...`)
2. **The token bucket allowed 110 per second.** The two-worker test measured 55 sends in a
   one-second window at a 50/s limit, which is correct token-bucket behaviour (rate + burst) but
   violates "maximum". Fix: strict sliding-window log. (`feat(ratelimit): ...`)
3. **Permit-to-send skew.** Even the strict limiter showed 60 at a limit of 50 when measured at
   the provider, because permits are granted a few ms before the request leaves. Fix: a 50 ms
   window margin; measured max is now exactly the limit. (ADR 0002)
4. **Rolling deploy collision.** Restarting the running stack on the new build failed every
   rate-limit call with `WRONGTYPE`: the old build's hash was still under the same Redis key.
   Fix: versioned key prefix. (`fix(ratelimit): ...`)
5. **Tracing silently exported nowhere.** With an explicit endpoint URL the OTLP exporter posts
   to `/` instead of `/v1/traces`; nothing errors, Jaeger just stays empty. Found by checking
   Jaeger for the trace ID that webhook.site received in `traceparent`. (`fix(tracing): ...`)

## Independent review

After the suite was green, a second agent reviewed the code cold: it got the files and the
requirements but not my reasoning or my conclusions, so it could not just agree with me. It
confirmed the core (dispatcher, leases, limiter, breaker, cursor) and reported seven edge
cases, all fixed with regression tests in one commit (`fix: edge cases found by an
independent review pass`): a re-sent batch returning a batch ID that 404'd, `\u0000` causing
a 500 instead of a 422, a shutdown window that could strand claimed rows, a possible deadlock
between batches with overlapping idempotency keys, replays with a different schedule being
accepted, unvalidated timing settings, and missing error codes in the OpenAPI spec.

None of the first five defects would have been caught by unit tests with mocks, and all five would have shipped
if "the code compiles and the agent says it works" were the bar.

## What stays human

Architecture choices and their trade-offs, the retry semantics, deciding what "maximum 100 per
second" means and how to measure it, reading every diff, and being able to explain any line in
this repository.
