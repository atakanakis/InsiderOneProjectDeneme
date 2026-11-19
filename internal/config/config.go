// Package config reads all settings from environment variables (12-factor),
// with defaults that work out of the box in docker-compose.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL string
	RedisURL    string
	LogLevel    string

	// API
	HTTPAddr string

	// Worker
	WorkerMetricsAddr  string
	ProviderURL        string
	ProviderTimeout    time.Duration
	RateLimitPerSecond float64
	RateLimitBurst     int
	WorkerConcurrency  int
	ClaimBatchSize     int
	PollInterval       time.Duration
	LeaseDuration      time.Duration
	ReapInterval       time.Duration

	// Retry policy
	MaxAttempts    int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration

	// Circuit breaker
	BreakerFailureThreshold int
	BreakerOpenDuration     time.Duration

	// Tracing: empty endpoint disables export.
	OTLPEndpoint string
	ServiceName  string
}

func Load(serviceName string) (Config, error) {
	var errs []error
	c := Config{
		DatabaseURL: str("DATABASE_URL", "postgres://notifications:notifications@localhost:5432/notifications?sslmode=disable"),
		RedisURL:    str("REDIS_URL", "redis://localhost:6379/0"),
		LogLevel:    str("LOG_LEVEL", "info"),

		HTTPAddr: str("HTTP_ADDR", ":8080"),

		WorkerMetricsAddr:  str("WORKER_METRICS_ADDR", ":9091"),
		ProviderURL:        str("PROVIDER_URL", "http://localhost:9090/send"),
		ProviderTimeout:    dur("PROVIDER_TIMEOUT", 5*time.Second, &errs),
		RateLimitPerSecond: float("RATE_LIMIT_PER_SECOND", 100, &errs),
		RateLimitBurst:     integer("RATE_LIMIT_BURST", 10, &errs),
		WorkerConcurrency:  integer("WORKER_CONCURRENCY", 50, &errs),
		ClaimBatchSize:     integer("CLAIM_BATCH_SIZE", 25, &errs),
		PollInterval:       dur("POLL_INTERVAL", 200*time.Millisecond, &errs),
		LeaseDuration:      dur("LEASE_DURATION", 30*time.Second, &errs),
		ReapInterval:       dur("REAP_INTERVAL", 10*time.Second, &errs),

		MaxAttempts:    integer("MAX_ATTEMPTS", 5, &errs),
		RetryBaseDelay: dur("RETRY_BASE_DELAY", 2*time.Second, &errs),
		RetryMaxDelay:  dur("RETRY_MAX_DELAY", 5*time.Minute, &errs),

		BreakerFailureThreshold: integer("BREAKER_FAILURE_THRESHOLD", 5, &errs),
		BreakerOpenDuration:     dur("BREAKER_OPEN_DURATION", 15*time.Second, &errs),

		OTLPEndpoint: str("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		ServiceName:  str("OTEL_SERVICE_NAME", serviceName),
	}
	if c.RateLimitPerSecond <= 0 || c.RateLimitBurst < 1 {
		errs = append(errs, fmt.Errorf("rate limit must be > 0 and burst >= 1"))
	}
	if c.MaxAttempts < 1 || c.WorkerConcurrency < 1 || c.ClaimBatchSize < 1 {
		errs = append(errs, fmt.Errorf("MAX_ATTEMPTS, WORKER_CONCURRENCY and CLAIM_BATCH_SIZE must be >= 1"))
	}
	if c.LeaseDuration <= c.ProviderTimeout {
		errs = append(errs, fmt.Errorf("LEASE_DURATION (%s) must exceed PROVIDER_TIMEOUT (%s)", c.LeaseDuration, c.ProviderTimeout))
	}
	if len(errs) > 0 {
		return c, fmt.Errorf("invalid configuration: %v", errs)
	}
	return c, nil
}

func str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func integer(key string, def int, errs *[]error) int {
	v := str(key, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return n
}

func float(key string, def float64, errs *[]error) float64 {
	v := str(key, "")
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return f
}

func dur(key string, def time.Duration, errs *[]error) time.Duration {
	v := str(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return d
}
