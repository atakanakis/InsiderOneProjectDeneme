// Command worker runs the delivery engine. Run as many replicas as needed:
// the queue (SKIP LOCKED) and the rate limiter (Redis) are shared, so adding
// workers adds concurrency without breaking the 100 msg/s per channel limit.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/atakanakis/notification-engine/internal/config"
	"github.com/atakanakis/notification-engine/internal/delivery"
	"github.com/atakanakis/notification-engine/internal/events"
	"github.com/atakanakis/notification-engine/internal/observability"
	"github.com/atakanakis/notification-engine/internal/ratelimit"
	"github.com/atakanakis/notification-engine/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("worker exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("notification-worker")
	if err != nil {
		return err
	}
	log := observability.NewLogger(cfg.ServiceName, cfg.LogLevel)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.SetupTracing(ctx, cfg.ServiceName, cfg.OTLPEndpoint)
	if err != nil {
		return err
	}
	defer shutdownTracing(context.Background()) //nolint:errcheck

	pool, err := store.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	ropts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return err
	}
	rdb := redis.NewClient(ropts)
	defer rdb.Close()

	st := store.New(pool)
	engine := delivery.NewEngine(
		st,
		ratelimit.New(rdb, cfg.RateLimitPerSecond, cfg.RateLimitBurst),
		delivery.NewHTTPProvider(cfg.ProviderURL, cfg.ProviderTimeout),
		events.NewRedisPublisher(rdb, log),
		log,
		delivery.Options{
			Concurrency:      cfg.WorkerConcurrency,
			ClaimBatchSize:   cfg.ClaimBatchSize,
			PollInterval:     cfg.PollInterval,
			Lease:            cfg.LeaseDuration,
			ReapInterval:     cfg.ReapInterval,
			Retry:            delivery.RetryPolicy{BaseDelay: cfg.RetryBaseDelay, MaxDelay: cfg.RetryMaxDelay},
			BreakerThreshold: cfg.BreakerFailureThreshold,
			BreakerOpenFor:   cfg.BreakerOpenDuration,
		},
	)

	// Small side server for Prometheus scraping and container health checks.
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		c, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if st.Ping(c) != nil || rdb.Ping(c).Err() != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	side := &http.Server{Addr: cfg.WorkerMetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := side.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server failed", "error", err)
		}
	}()

	log.Info("worker started", "provider_url", cfg.ProviderURL, "rate_limit_per_second", cfg.RateLimitPerSecond,
		"concurrency_per_channel", cfg.WorkerConcurrency, "max_attempts", cfg.MaxAttempts)
	engine.Run(ctx) // returns after in-flight deliveries are recorded
	log.Info("worker stopped")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return side.Shutdown(shutdownCtx)
}
