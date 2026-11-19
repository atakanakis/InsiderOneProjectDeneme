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

## Review loop with independent agents

Passing my own checks is not the same as being right: an author, human or agent, tests the
cases they thought of. So after the suite was green, the finished repository went through a
review loop driven from the main Claude Code session:

```text
main session ──spawn──▶ reviewer agent (cold: requirements + repo only, read-only)
     ▲                         │ ranked findings, each with a reproduction
     │                         ▼
     └── reproduce each finding as a failing test or live run ─▶ fix ─▶ regression test ─▶ commit
                               │
                               ▼
            fresh reviewer agent: re-check every earlier finding, then review again from scratch
```

- **Cold context.** The reviewer gets the requirements and the repository, never my design
  notes or conclusions. A reviewer handed the author's reasoning tends to confirm it.
- **A different model.** The second round ran on a different model (Fable) from the one
  that wrote the code, so the two do not share the same blind spots.
- **Proof, not opinion.** The reviewer is read-only and must reproduce what it reports:
  commands, requests and failure injection against a running stack. Nothing it says is taken
  on trust either. Each finding was reproduced again on our side before it was fixed.
- **Fresh eyes each round.** A new reviewer instance re-checks every earlier finding and then
  reviews the whole repository again, instead of continuing the previous conversation.
- **Reusable.** The reviewer is a committed agent definition,
  [`.claude/agents/reviewer.md`](../.claude/agents/reviewer.md), so the same loop runs on any
  later change.

| Round | Reviewer | Result |
|---|---|---|
| 0 | Same model, cold context | Confirmed the core design. Found 7 edge cases (batch replay link, `\u0000` as a 500, a shutdown window stranding rows, a cross-batch deadlock, schedule mismatch on replay, unvalidated timings, missing error codes in the spec), all fixed in `fix: edge cases found by an independent review pass`. |
| 1 | Different model (Fable), cold context, failure injection | Found two major problems the earlier passes missed, plus ten smaller ones. Template rendering let one request allocate 2.9 GB. The rate limit was exceeded after a 0.6s database pause (150 at a limit of 100). Details, evidence and the fixing commit for each: [`docs/reviews/01-independent-review.md`](reviews/01-independent-review.md). |
| 2 | A new Fable instance, given only the written record of round 1 | Verified each round 1 fix with its own fault injection: the stall fix held under every pattern it tried. It then found that two round 1 fixes had problems of their own. The template fix closed the attack per item but not per batch, and the claim fix had given up atomicity. It also found a duplicate send after one failed result write. All were fixed, each with an injected-failure test: [`docs/reviews/02-independent-review.md`](reviews/02-independent-review.md). |

| 3 | Another new Fable instance, given both written records | Confirmed the stall, late-success and atomicity fixes under harder faults (a 36s database pause, `kill -9`). It then found the limit exceeded on cold connections with no fault at all, a third variant of the amplification problem (on the error path), and a claim query that sorted tied rows. It also caught our own review record overstating its test coverage. [`docs/reviews/03-independent-review.md`](reviews/03-independent-review.md). |
| 4 | A new instance on a third model (Opus), narrower scope | Verified every round 3 fix and found nothing major in a fresh pass. It found two narrow gaps in the newest rate-limit code and one untested branch, and disproved a claim in our own record about what the HTTP library does. [`docs/reviews/04-independent-review.md`](reviews/04-independent-review.md). |

Each round also improved the process itself. The template problem sat in a feature that round 0
had passed, because round 0 checked correctness and not abuse. The rate-limit problem broke a
claim the README made confidently, because our load test never paused the database. Both
lessons are now rules in the reviewer definition. Round 2 added a third: two of its three
major findings were inside round 1's fix commits, so the reviewer now treats fix commits as
its first suspects, and every fix is tested against an injected failure, not only against the
reproduction that prompted it. Round 3 added the most important one: the same problem class
came back three times because each fix addressed one reproduction. The rule now is to fix the
class, state the invariant and write it into `CLAUDE.md`, and the written review records are
themselves treated as claims that the next reviewer verifies.

None of the first five defects would have been caught by unit tests with mocks, and all five would have shipped
if "the code compiles and the agent says it works" were the bar. The two major findings of round 1 would
have shipped if "my own tests pass" were the bar.

## From reviews to self-verification

The reviews found real bugs, but each found one more, and every test they left behind
reproduced one known failure. A new kind of failure could only be found by the next reviewer.
Two additions changed that ([`docs/verification.md`](verification.md)):

- **A protection map.** Each of 15 guards was disabled in turn to see which test turned red.
  All 15 are caught. Two first mutations did not compile and were redone, so that the
  compiler would not be counted as a test.
- **A randomised fault test.** Faults are mixed at random, and three invariants are checked
  after every scenario. 203 of 203 scenarios held. It also caught a broken guard that the
  targeted stall test missed in one of seven runs.

## What stays human

Architecture choices and their trade-offs, the retry semantics, deciding what "maximum 100 per
second" means and how to measure it, reading every diff, and being able to explain any line in
this repository.
