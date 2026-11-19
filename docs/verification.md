# Verification: does every guard have a test, and do the invariants hold?

After four independent reviews, two questions remained:

- Do the tests actually protect each fix, or do they only pass?
- Can the system find the next bug itself, before a reviewer does?

This page answers both with measurements. Nothing on it changes how messages flow.

## 1. Protection map: break each guard, see which test turns red

Each guard below is a line of code that exists because a test, a review or a measurement
showed it was needed. To check that a test really protects it, each guard was disabled on
purpose, the tests of its package were run, and the file was restored. This is mutation
testing done by hand, one guard at a time.

| Guard | What breaks without it | Caught by |
|---|---|---|
| Re-take permits when a claim outlived them | Limit exceeded after a database stall | `TestInvariantsHoldUnderRandomFaults`, `TestRateLimitHoldsWhenClaimsStall`*, `TestRowsWithoutFreshPermitsAreReleasedNotSpent`, `TestShutdownWhileWaitingForPermitsIsPrompt` |
| Check the permit again at connection-ready | Limit exceeded on cold connections | `TestRateLimitHoldsOnColdConnections` |
| Window of 1s plus a margin | Limit exceeded at the provider | `TestRateLimitHoldsAcrossTwoWorkers`, `TestRateLimitHoldsWhenClaimsStall`, `TestRateLimitHoldsOnColdConnections`, `TestInvariantsHoldUnderRandomFaults` |
| Retry a failed result write | Duplicate send after one failed write | `TestFailedResultWriteIsRetriedNotResent`, `TestInvariantsHoldUnderRandomFaults` |
| Deliver rows returned with a claim error | Claimed rows stranded | `TestRowsFromAPartlyFailedClaimAreDelivered` |
| Do not retry a permanent 4xx | Pointless retries | `TestClassify`, `TestPermanentFailureIsNotRetried` |
| Savepoint per priority | One failing priority strands or blocks the others | `TestFailingPriorityNeitherStrandsNorBlocksOthers` |
| Claim `ORDER BY` matches the index | Full sort of tied rows on every claim | `TestClaimDoesNotSortTiedBacklog` |
| `FOR UPDATE SKIP LOCKED` | Two workers claim the same row | `TestConcurrentClaimsNeverOverlap` |
| Lease guard on results | A stale worker overwrites the new owner | `TestCompleteAttemptRejectsStaleLease`, `TestLateSuccessWinsUntilTheRowIsClaimedAgain`, `TestRequeueFailedGrantsFreshAttempts` |
| A late success wins until re-claimed | Duplicate send after a late result | `TestLateSuccessWinsUntilTheRowIsClaimedAgain` |
| Releasing an unsent claim refunds its attempt | Attempts spent without a send | `TestReleaseClaimsRefundsTheAttempt` |
| 16 MiB rendering budget per request | A small request allocates gigabytes | `TestTemplateBatchCannotAmplifyIntoGigabytes` |
| At most 100 field errors per response | A small request returns a 68 MB error | `TestValidationOutputIsBounded`, `TestValidationResponseStaysSmall` |
| Client keys may not start with `batch:` | A single request collides with a batch item | `TestBatchIdempotencyKeyHeader` |

**Result: 15 of 15 guards are caught by at least one test.** No guard is untested, and none
turned out to be dead code.

Two notes on how the map was made:

- **Mutations must compile.** On the first pass, two mutations did not compile (an unused
  variable), so the compiler failed, not a test. They were redone with mutations that compile,
  and both were then caught by tests. Counting a build failure as "caught" would have
  overstated the map.
- **(\*) A targeted test can be timing-dependent.** With the stall guard disabled,
  `TestRateLimitHoldsWhenClaimsStall` failed in 6 of 7 runs and passed once. The randomised
  fault test caught the same broken guard in every run. This is the reason the randomised test
  exists.

## 2. Randomised fault testing

`TestInvariantsHoldUnderRandomFaults`
([`chaos_integration_test.go`](../internal/delivery/chaos_integration_test.go)) draws a random
mix of faults per scenario:

- claim stalls
- failing result writes
- 503 and 429 answers from the provider
- provider latency
- slow new connections through the real HTTP client

After each scenario it checks three invariants:

1. The provider never sees more than the limit in any sliding second.
2. Every notification is delivered.
3. No notification is accepted twice.

CI runs three fixed scenarios on every push. The soak below ran 200 more.

| | Result |
|---|---|
| Scenarios | 203 (105 through the real HTTP client) |
| All three invariants held | **203/203 (100%)** |
| Rate limit held | 203/203 |
| Nothing lost (60,900 notifications) | 203/203 |
| Nothing accepted twice | 203/203 |
| Peak in any second | exactly the limit in 194 runs, 45-49 in 9 |
| Provider requests | 65,636 for 60,900 notifications (7.8% retries after injected failures) |

To reproduce:

```bash
CHAOS_RUNS=200 CHAOS_SEED=1000 go test -count=1 -run TestInvariantsHoldUnderRandomFaults \
    -v -timeout 60m ./internal/delivery/ | tee chaos.log
python3 scripts/chaos_summary.py chaos.log
```

## Limits

- **Faults not tried.** A clean soak means no failure was found among the faults tried, not
  that none exists.
- **Faults left out on purpose.** A worker killed outright (`kill -9`), and result writes
  failing for longer than a lease, cause a resend by design (at-least-once).
- **Redis losing its state.** Not covered; see
  [ADR 0002](adr/0002-sliding-window-rate-limit.md).
- **Mutation coverage.** The protection map covers the guards added by reviews and the core
  queue guarantees. It does not mutate every line.
