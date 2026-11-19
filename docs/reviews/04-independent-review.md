# Review 4: a shorter check of the review 3 fixes, on a third model

- **Reviewer:** a new reviewer instance on a different model again (Opus), with the
  requirements, the repository at `d51b1b7` and the three earlier records. The scope was
  deliberately narrower than rounds 1 to 3: verify the review 3 resolutions, which nobody had
  checked yet, then make one short fresh pass.
- **What it ran:** build, vet, the race suite (82.8% coverage), live requests against the
  running stack, and three throwaway tests of its own against the repo's `HTTPProvider`.

## Part A: were the review 3 fixes real?

| Review 3 item | Verdict |
|---|---|
| Claim in index order | Fixed. Unverified suspicion: once the statement is cached, Postgres could switch to a generic plan, which the plan test does not cover. |
| Savepoint per priority | Fixed in the store. The engine's handling of a partial or failed claim had no test. |
| Bounded validation output, paged template list, reserved `batch:` keys | Fixed, checked live. |
| Permit re-check at connection-ready | **Partly fixed.** The cold-connection case holds, including over HTTP/2. Two gaps remained (below). |
| Release and shutdown tests, second-signal exit | Fixed. |

The fresh pass found nothing major that the three earlier reviews had missed.

## Findings and what was done

| # | Severity | Finding (reviewer's evidence) | Resolution |
|---|---|---|---|
| 1 | minor to major | When no fresh permit could be had, the gate cancelled the request and the engine released the row as "not sent" with its attempt refunded. net/http does not check the context between connection-ready and the write, so the request can still go out: 3 of 2,000 concurrent cancelled sends reached the server. That would be an unrecorded send. Our commit message and review 3 record said "the request is not written", which was wrong. | The outcome is now treated as ambiguous, like a timeout: the attempt counts, the row is retried, and the `Idempotency-Key` covers the case where the provider did receive it. It is not counted against the breaker. |
| 2 | minor | The gate ran once per send (`sync.Once`). net/http replays a request that carries `Idempotency-Key` on a new connection when a reused one is dead, and that second write skipped the check. Shown by reading the code. | The gate runs on every connection-ready event and checks against the newest permit. |
| 3 | minor | The engine's partial-claim and failed-claim branches had 0% coverage. | `TestRowsFromAPartlyFailedClaimAreDelivered` injects both. |
| 4 | nit | A stale permit is not refunded when a fresh one is taken, so a cold burst costs about a second of throughput. | Left as is: it errs on the safe side of the limit. |

All four are in the commit `fix(delivery): treat a cancelled send as ambiguous, gate every connection`.

## What this round adds

The remaining defects were narrow, and the one that mattered was again in a written claim:
"the request is not written" was an assumption about a library, stated as a fact. The
reviewer disproved it by reading net/http and then measuring. A claim about what a dependency
does needs the same evidence as a claim about our own code.
