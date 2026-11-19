# CLAUDE.md

Context for AI coding agents (and humans) working in this repository.

## What this is

Event-driven notification service in Go: an API accepts notifications, PostgreSQL stores them
and acts as the work queue, worker replicas deliver them to an HTTP provider with a strict
per-channel rate limit (Redis), priorities, retries, a circuit breaker and a dead-letter state.
Design rationale: `README.md` and `docs/adr/`.

## Commands

```bash
docker compose up --build -d     # full stack; API docs at localhost:8080/docs
make test                        # unit + integration (needs Docker), -race
make test-unit                   # no Docker
make lint                        # golangci-lint v2 (Docker)
make load                        # burst test against the running stack
```

## Map

| Path | Role |
|---|---|
| `internal/domain` | Types, status state machine, validation, templates. No infrastructure imports. |
| `internal/store` | All SQL. `queue.go` = claim/complete/reap; `migrations/` = goose, versioned, append-only |
| `internal/service` | Use cases behind the API (validation, templates, idempotency, events) |
| `internal/api` | HTTP handlers, middleware, WebSocket hub. Spec: `api/openapi.yaml` |
| `internal/delivery` | Engine (dispatcher per channel), retry policy, breaker, provider client |
| `internal/ratelimit` | Redis sliding-window limiter (Lua) |
| `internal/testutil` | testcontainers setup; integration tests call `testutil.Setup(t)` |

## Invariants (do not break)

1. **One clock for the queue.** `next_attempt_at`, leases and backoff are computed by Postgres
   (`now()`); the rate limiter uses Redis `TIME`. Never compare queue times with `time.Now()`
   from Go.
2. **Status changes after a claim only under the lease guard** (`status='processing' AND
   attempts=$n`). Late results must lose (`ErrStaleLease`).
3. **Take rate-limit permits before claiming rows**, refund what was not used, and never send
   on a permit older than the window margin (`PermitMaxAge`): re-take it instead.
4. **Classification lives in `delivery.Classify`.** Only transient failures are retried, and
   only transient failures count against the breaker.
5. **Migrations are append-only.** Never edit an applied migration; add a new numbered file.
6. **API changes update `api/openapi.yaml`** and the integration tests in `internal/api`.
7. **Redis key layouts are versioned** (`ratelimit:v2:`); bump the version when changing them.

## Conventions

- Errors: wrap with `%w`; map to HTTP only in `internal/api/respond.go`.
- Logs: `log/slog` with context (`InfoContext` etc.) so correlation and trace IDs are attached.
- Tests: behaviour-named (`TestHighPriorityOvertakesBacklog`), real Postgres/Redis for anything
  touching SQL or Lua, `-short` skips them.
- Commits: conventional prefix, body explains why; one logical change per commit, and each
  commit builds.
