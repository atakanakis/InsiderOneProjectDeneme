# Review 1: independent reviewer agent

- **Reviewer:** the [`reviewer`](../../.claude/agents/reviewer.md) agent, running on a different
  model (Fable) from the one that wrote the code, with no access to the authoring session.
- **Input:** the original requirements and the repository at `e0c674f`. No summary of the
  design and no list of known issues, so the review could not just agree with the author.
- **What it ran:** build, vet, lint, the full suite with `-race` (and three packages four more
  times), coverage, and live probing. For probing it built HEAD into a scratch directory and
  ran it against its own throwaway Postgres and Redis, with a provider that recorded arrival
  times. It used failure injection: pausing Postgres, killing a worker mid-burst, stopping the
  provider, and SIGTERM during a burst.
- **Confirmed as working:** the claim/lease/guard design, the breaker preserving retry budgets,
  crash recovery, graceful shutdown, and the limit at exactly 100/s in steady state on all
  three channels.

## Findings and what was done

Every verified finding was reproduced again on our side before fixing (a failing test or a
live run), and each fix has a regression test.

| # | Severity | Finding (reviewer's evidence) | Resolution | Commit |
|---|---|---|---|---|
| 1 | major | Templates used `text/template` on caller-supplied bodies. Ten `{{printf "%999999d" 1}}` in one template plus a 1000-item batch took the API from 82 MB to 2.9 GB. | Templates support `{{.name}}` placeholders and nothing else. Bodies are capped at the channel limit, and the output size is computed before allocating. | `2977d3b` |
| 2 | major | Rate limit exceeded after a stall: five 0.6s Postgres pauses during a burst gave 150 requests in one second at a limit of 100. | If Take plus the claim took longer than the margin, the permits are refunded and fresh ones taken before sending. The new stall test showed 75 at 50 before the fix and 50 after. A live rerun of the reviewer's scenario: 100 at 100. | `2b73c54` |
| 3 | minor | The "commit history is the engineering log" claim did not hold: early commits share timestamps. | The README now says how the early history was produced. Later commits are made as the work happens. | docs commit |
| 4 | minor | `Idempotency-Key` on the batch endpoint was silently ignored, so a retried batch was sent twice. | The header now derives `<key>:<index>` per item. | `88fad5b` |
| 5 | minor | The pgx pool and HTTP transport were left at their defaults: 4 connections, and 2 idle connections per host. | `DATABASE_MAX_CONNS` is added, and the transport now keeps `WORKER_CONCURRENCY` × channels idle connections. | `dc05f54` |
| 6 | minor | A late "sent" was discarded after the lease expired, so the message was sent twice. | A late success now wins while no other worker has claimed the row again. | `994cdeb` |
| 7 | minor | Strict priority has no aging; the README heading overstated it. | Kept on purpose and documented, together with the fix we would apply if needed. The heading was rewritten. | docs commit |
| 8 | minor | There was no authentication, and the WebSocket accepted any origin. | `API_KEYS` (Bearer, `X-API-Key`, or `?apiKey=` for the WebSocket) and `WS_ALLOWED_ORIGINS` were added. | `88fad5b` |
| 9 | nit | The load test measured calendar seconds, not sliding windows. | The mock provider now reports the maximum in any sliding second. | `21a4783` |
| 10 | nit | 404/405 responses were plain text, and decode errors leaked Go type names. | All errors now use the JSON shape, and messages describe the client's JSON. | `88fad5b` |
| 11 | nit | Compose forwarded only 4 variables. | Services load an optional `.env`. | `21a4783` |
| 12 | nit | `a@b` was accepted as an email, `from > to` was accepted, times came back in local time, there was no `pending` event, and migrate had no `down`. | All five fixed. | `88fad5b` |
| S1 | suspicion | The claim query walks future high-priority rows. | Not measured; fixed anyway because the fix is small and removes the question. The claim now runs one exact index range per priority, covered by `TestFutureHighPriorityBacklogDoesNotBlockDueRows`. | `994cdeb` |
| S2 | suspicion | Idempotency keys never expire. | Documented as a retention item in the README trade-offs. | docs commit |
| S3 | suspicion | A half-open breaker can be closed by a result from before the trip. | Not changed: it needs a provider timeout longer than the open duration (5s vs 15s by default). Noted for a follow-up. | n/a |
| S4 | suspicion | The SMS limit ignores the 70-character segments used for UCS-2 text. | Documented next to the content limits. | docs commit |

## What the review changed in the process

The worst finding (1) was in a bonus feature that the first review pass had looked at and
passed: that pass checked for correctness, while this one went looking for abuse. Finding 2
broke a claim the README made confidently, and it came from injecting a failure (pausing the
database) that our own load test never did. Both lessons now live in the reviewer's
instructions: look for abuse as well as bugs, and inject failures instead of only measuring
the happy path.
