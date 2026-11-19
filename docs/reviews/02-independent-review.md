# Review 2: a fresh reviewer re-checks review 1 and the fixes

- **Reviewer:** a new instance of the [`reviewer`](../../.claude/agents/reviewer.md) agent on the
  different model (Fable). It had no memory of review 1, only its written record
  ([`01-independent-review.md`](01-independent-review.md)). The "resolution" column of that
  record was treated as claims to verify.
- **Scope:** (A) is every review 1 finding really fixed, and did any fix break something?
  (B) A full fresh review, with the fix commits as the main suspects.
- **What it ran:** build, vet, the race suite (80% coverage, matching the README), and fault
  injection on HEAD binaries against its own Postgres and Redis. The faults were:
  - Postgres paused 5×0.6s, 8×0.3s, and once for 12s.
  - Redis paused 5×0.6s.
  - Redis flushed mid-burst.
  - SIGSTOP of one or both workers.
  - A trigger that fails the second claim statement.
  - A result write that fails once after a successful send.

## Part A: were the review 1 fixes real?

| Review 1 item | Verdict |
|---|---|
| Rate limit under a stall | **Fixed.** 100 in any sliding second under every pause pattern above and under SIGSTOP; the re-take path fired 18 times. |
| Template rendering cost | **Partly fixed.** `printf` was gone, but 16,000 `{{.a}}` placeholders times a 1000-item batch still cost 8-9 s and 665 MB, and the cap was 4× the limit in bytes. |
| Batch `Idempotency-Key`, pools, auth and origins, sliding-window stats, JSON errors, compose `.env`, small API items | Fixed, with nits: lowercase `bearer` was rejected, and a derived key could collide with a single request's key. |
| Late success | Fixed. One gap: a cancel between the reap and the late success keeps the row `cancelled`. |
| Claim scanning future rows | **Regressed.** The scan was fixed, but the claim became three statements without a transaction. |
| History statement | Partly: two post-review commits shared a timestamp, while the README said they were "made as the work happened". |

## Part B: new findings and what was done

| # | Severity | Finding (reviewer's evidence) | Resolution | Commit |
|---|---|---|---|---|
| 1 | major | Template amplification across a batch (above). | Templates are compiled once per request. Length is computed from per-variable counts, so the cost no longer grows with the number of placeholders. The cap is in characters at the channel limit, and a request may render at most 16 MiB in total. The reviewer's request now gets a 422 in 0.2 s, and `TestTemplateBatchCannotAmplifyIntoGigabytes` covers it. | `cce1f4b` |
| 2 | major | The claim was not atomic (a regression from the review 1 fix). With a trigger failing the normal-priority statement, high-priority rows spent 3 attempts in 14 s with no provider call. | The per-priority statements now share one transaction. `TestClaimIsAllOrNothing` uses the same trigger technique. | `6c1b93c` |
| 3 | major | One failed result write after a successful send gave a duplicate: two provider calls 3.5 s apart. | Result writes are retried with backoff while the lease lasts, and longer for a success. A unique `(notification_id, attempt_number)` index keeps a retried write from adding a second audit row. `TestFailedResultWriteIsRetriedNotResent` covers it. | `6c1b93c` |
| 4 | minor/major | Redis losing its state mid-burst (four flushes) allowed 200 at a limit of 100. | Documented in ADR 0002 with the trade-off. Guarding against it costs more than the event, which needs operator action anyway. The provider's 429 with `Retry-After` is the backstop. | `1a43f39` |
| 5 | minor | Rows abandoned while waiting for permits never released the breaker's half-open probe. | Rows are released with `ReleaseClaims`, which refunds the attempt, and their breaker reservations are released too. | `6c1b93c` |
| 6 | minor | Abandoned rows lost an attempt and came back through the reaper. | Same fix as 5. | `6c1b93c` |
| 7 | minor | The permit wait ignored shutdown and could take up to 15 s, longer than Docker's 10 s grace period. | The wait now stops on shutdown and releases the rows. Workers get a 30 s `stop_grace_period`. | `6c1b93c`, `1a43f39` |
| 8 | minor | A derived batch key `K:0` collided with a single request's key `K:0`. Adding an item to a re-sent batch created it. | Derived keys are now `batch:<key>:<index>`. Item-by-item semantics are documented. | `1a43f39` |
| 9 | nit | Output is bursty: 100 sends, then a pause of about 0.95 s. | Accepted. The requirement is a maximum per second, and smoothing would cost throughput. | n/a |
| S | suspicion | `?apiKey=` could end up in span attributes. | The key is moved into a header and stripped from the URL before tracing and logging run. | `1a43f39` |
| | nits | Lowercase `bearer` was rejected. `/v1/metrics?window=24h` ran an unbounded percentile query. | The Bearer scheme is now case-insensitive, and the window is capped at 1 h. | `1a43f39` |

Not changed, on purpose: a cancel that races with a late success keeps the row `cancelled`
(the user's last action wins in the API); the `GET /v1/notifications/batch` path answers 400
`invalid_id` (it is `GET /v1/notifications/{id}` with a non-UUID id).

## What this round taught the loop

Two of the three major findings were **inside the review 1 fix commits**. The claim regression
came from a performance fix that changed one statement into three. The template fix closed
the reported attack but not the dimension next to it: per item rather than per batch. A fix is
new code written to satisfy one reproduction, so it deserves the hardest look in the next
round. That is now a rule in the reviewer definition. Every fix in this round was also
checked against an injected failure (a trigger, a failing write, a stall) and not only against
the original reproduction.
