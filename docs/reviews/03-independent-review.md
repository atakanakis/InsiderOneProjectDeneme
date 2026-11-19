# Review 3: a third fresh reviewer checks both earlier records

- **Reviewer:** a new instance of the [`reviewer`](../../.claude/agents/reviewer.md) agent on the
  different model (Fable), given only the requirements, the repository at `9e83400` and the
  two written records. Every "resolution" in them was treated as a claim to verify.
- **What it ran:** build, vet, lint, the race suite and coverage (80.7%), then fault injection
  on HEAD binaries against its own Postgres and Redis:
  - Postgres paused 5×0.6s, and once for 36s (longer than the lease) mid-burst of 3000.
  - Redis paused 5×0.6s.
  - Workers SIGSTOPped, and one killed with `kill -9` mid-burst.
  - Triggers failing a claim statement and the attempt insert.
  - A TCP proxy delaying every new connection to the provider.
  - A local OTLP sink to inspect exported spans.

## Part A: were the earlier fixes real?

| Earlier item | Verdict |
|---|---|
| Rate limit under stalls (review 1) | **Fixed.** 100 in any sliding second under every pause and SIGSTOP pattern. |
| Late success (review 1) | **Fixed.** After the 36s pause: 3000 sent with one attempt each, 0 duplicates at the provider. |
| Claim atomicity (review 2) | **Fixed, with a side effect.** Nothing stranded, but one failing priority now blocked the others. |
| Duplicate after a failed result write (review 2) | **Fixed within the lease.** One provider call and six retries logged. A write outage longer than the lease still resends, which is the stated at-least-once limit. |
| Release of unsent rows, shutdown during the permit wait (review 2) | **Fixed, but untested in the repo.** The reviewer's own tests passed, yet `release()` had 0% coverage. Our record had claimed "each with an injected-failure test", which was not true for these items. |
| `?apiKey=` in traces (review 2) | **Fixed.** 0 occurrences in captured spans and logs. |
| Template amplification (reviews 1 and 2) | **Class still open.** Both reported requests are bounded, but the error path was not (finding 2 below). |
| Claim scanning (review 1) | **Partly fixed.** Future rows are skipped, but the rewritten query sorted tied rows (finding 3). |
| Derived batch keys (review 2) | **Partly fixed.** The `batch:` prefix was not reserved. |
| History statement | Still not accurate. |

## Part B: new findings and what was done

| # | Severity | Finding (reviewer's evidence) | Resolution | Commit |
|---|---|---|---|---|
| 1 | major | Limit exceeded 2× on cold connections, with no fault: a proxy delaying new connections by 120 ms gave 200 at a limit of 100 on a burst from idle. The permit check ended before connection setup. | The permit is checked again at connection-ready, right before the request is written, and re-taken if stale. `TestRateLimitHoldsOnColdConnections` showed 100 at 50 before and 50 after. | `c9e3a9f` |
| 2 | major | A template with 9,900 variables and a 1000-item batch without variables turned a 99 KB request into a 68 MB response (2.5 GB RSS with eight in parallel). | Validation output is bounded as a class: at most 100 field errors of 300 characters, five variable names per message, and early exit once full. `TestValidationResponseStaysSmall` covers it. | `d496f15` |
| 3 | major at scale | `ORDER BY next_attempt_at, id` did not match the index: 500k rows sharing a timestamp were sorted on every claim (220 ms, 19 MB on-disk sort). | The order now matches the index. `TestClaimDoesNotSortTiedBacklog` checks the plan on 50k tied rows and fails on the old query. | `ec87e95` |
| 4 | minor | `GET /v1/templates` was unpaginated with full bodies (99 MB for 1000 templates). | It returns summaries without bodies, paged by name. | `d496f15` |
| 5 | minor | The review 2 release and shutdown paths had no test. | Two engine tests added, modelled on the reviewer's. | `c9e3a9f` |
| 6 | minor | `kill -9` resent all 100 in-flight messages; only a provider honouring `Idempotency-Key` hides that. | Not fixable at this layer. The README now states the cost plainly instead of "rare duplicate". | docs commit |
| 7 | minor | The atomic claim blocked every priority when one failed. | A savepoint per priority: the failing one rolls back, the rest are claimed and delivered. | `ec87e95` |
| 8 | nit | The `batch:` key namespace was not reserved. | Client keys with that prefix are rejected. | `d496f15` |
| S | suspicions | Shutdown could outlast the 30s grace period; a second SIGTERM was ignored. | Grace period 75s; a second signal ends the process. | `c9e3a9f` |

Left as is, with the reason:

- **429 does not pause the channel.** A `Retry-After` delays only that message. A channel-wide
  pause is the right next step, but with a provider that throttles 2% of requests at random
  (the mock) it would halve throughput, so it needs a threshold design, not a quick patch.
- **A cancel that races a late success** keeps the row `cancelled`.
- **A row the reaper failed and a late success then flips to `sent`** counts in both
  `notifications_finished_total` series.
- **Bursty output** and the **half-open breaker edge** from earlier rounds.

*Correction after review 4:* the fix for finding 1 was described (in its commit message) as
"the request is not written" when no fresh permit can be had. That is not guaranteed; see
[review 4](04-independent-review.md), which changed that path.

## What this round taught the loop

The amplification problem was "fixed" twice and reproduced a third time in a minute, each time
one step from the previous reproduction: `printf`, then placeholders per batch, then the error
path. Fixing reproductions was the mistake. The rule is now **fix the class**: state the
invariant ("anything a request can make the server allocate or return is bounded"), apply it
to every path, and write it where the next author will read it (`CLAUDE.md`, invariant 10).
The rate limit taught the same lesson from the other side: the guarantee has to be checked
where the request leaves, not where the code that sends it starts (invariant 11).

The round also caught our own record overstating: "each fix has an injected-failure test" was
false for three items. The records are claims too, and the next reviewer checks them.
