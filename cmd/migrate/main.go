// Command migrate applies the versioned database migrations embedded in the
// binary. docker-compose runs it once before the API and workers start.
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/atakanakis/notification-engine/internal/config"
	"github.com/atakanakis/notification-engine/internal/observability"
	"github.com/atakanakis/notification-engine/internal/store"
)

func main() {
	cfg, err := config.Load("notification-migrate")
	if err != nil {
		slog.Error("config", "error", err)
		os.Exit(1)
	}
	log := observability.NewLogger(cfg.ServiceName, cfg.LogLevel)
	ctx := context.Background()
	pool, err := store.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("connect", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	if err := store.Migrate(ctx, pool); err != nil {
		log.Error("migrate", "error", err)
		os.Exit(1)
	}
	log.Info("migrations applied")
}
