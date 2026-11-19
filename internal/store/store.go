// Package store is the PostgreSQL persistence layer. PostgreSQL is both the
// source of truth for notification state and the durable work queue (see
// docs/adr/0001-postgres-as-queue.md).
package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

var (
	ErrNotFound = errors.New("not found")
	// ErrInvalidState: the row exists but its status does not allow the operation.
	ErrInvalidState = errors.New("invalid state for this operation")
	// ErrIdempotencyMismatch: the key was already used for a different payload.
	ErrIdempotencyMismatch = errors.New("idempotency key already used with a different payload")
	// ErrStaleLease: the worker's lease expired and the row moved on without it.
	ErrStaleLease = errors.New("lease no longer held")
	ErrDuplicate  = errors.New("already exists")
)

//go:embed migrations/*.sql
var migrations embed.FS

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Connect opens a connection pool and retries for a while, because in
// docker-compose the database may still be starting when we boot.
//
// maxConns sizes the pool (0 keeps pgx's default of max(4, CPUs), which is
// far below what a worker with dozens of concurrent deliveries needs; every
// delivery records its result in its own short transaction).
func Connect(ctx context.Context, url string, maxConns int) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = int32(min(maxConns, 1000))
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err = pool.Ping(pingCtx)
		cancel()
		if err == nil {
			return pool, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			pool.Close()
			return nil, fmt.Errorf("database not reachable: %w", err)
		}
		time.Sleep(time.Second)
	}
}

// Migrate applies all pending versioned migrations embedded in the binary.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()
	provider, err := goose.NewProvider(goose.DialectPostgres, db, mustSub(migrations, "migrations"))
	if err != nil {
		return fmt.Errorf("goose provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// querier is satisfied by both the pool and a transaction, so the same query
// helpers work inside and outside transactions.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
