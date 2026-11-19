// Package testutil starts throwaway Postgres and Redis containers for
// integration tests (testcontainers-go). Tests that need them are skipped
// with `go test -short`.
package testutil

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/atakanakis/notification-engine/internal/store"
)

// Env holds connections shared by every test in a package.
type Env struct {
	Pool  *pgxpool.Pool
	Redis *redis.Client
}

var env *Env

// Main is called from a package's TestMain. It starts the containers once,
// applies migrations, runs the tests and tears everything down.
func Main(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	ctx := context.Background()
	pg, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("notifications"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		tcpostgres.BasicWaitStrategies(),
	)
	exitOn(err, "start postgres")
	rc, err := tcredis.Run(ctx, "redis:7-alpine")
	exitOn(err, "start redis")

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	exitOn(err, "postgres dsn")
	pool, err := store.Connect(ctx, dsn)
	exitOn(err, "connect postgres")
	exitOn(store.Migrate(ctx, pool), "migrate")

	redisURL, err := rc.ConnectionString(ctx)
	exitOn(err, "redis url")
	ropts, err := redis.ParseURL(redisURL)
	exitOn(err, "parse redis url")

	env = &Env{Pool: pool, Redis: redis.NewClient(ropts)}
	code := m.Run()

	env.Redis.Close()
	pool.Close()
	_ = testcontainers.TerminateContainer(pg)
	_ = testcontainers.TerminateContainer(rc)
	os.Exit(code)
}

// Setup returns a clean environment or skips the test in -short mode.
func Setup(t *testing.T) *Env {
	t.Helper()
	if env == nil {
		t.Skip("integration test: run without -short (needs Docker)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := env.Pool.Exec(ctx, `TRUNCATE notification_attempts, notifications, templates CASCADE`)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if err := env.Redis.FlushAll(ctx).Err(); err != nil {
		t.Fatalf("flush redis: %v", err)
	}
	return env
}

func exitOn(err error, what string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration setup failed (%s): %v\n", what, err)
		os.Exit(1)
	}
}
