package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics. The API and the worker are separate processes and each
// exposes its own /metrics; Prometheus (or any scraper) aggregates them.
var (
	NotificationsCreated = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notifications_created_total",
		Help: "Notifications accepted by the API.",
	}, []string{"channel", "priority"})

	DeliveryAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notification_delivery_attempts_total",
		Help: "Calls to the provider by outcome (delivered, retryable, permanent).",
	}, []string{"channel", "outcome"})

	NotificationsFinished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notifications_finished_total",
		Help: "Notifications reaching a final state from the worker (sent, failed).",
	}, []string{"channel", "status"})

	ProviderLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "notification_provider_request_duration_seconds",
		Help:    "Latency of a single provider call.",
		Buckets: prometheus.DefBuckets,
	}, []string{"channel"})

	EndToEndLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "notification_end_to_end_latency_seconds",
		Help:    "Time from becoming due (created or scheduled) to sent.",
		Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300, 900},
	}, []string{"channel"})

	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "notification_queue_depth",
		Help: "Notifications ready for delivery and waiting for a worker.",
	}, []string{"channel", "priority"})

	ScheduledDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "notification_scheduled_depth",
		Help: "Notifications waiting for their scheduled time or retry backoff.",
	}, []string{"channel", "priority"})

	RateLimited = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notification_rate_limited_total",
		Help: "Dispatcher loops that found no rate-limit tokens available.",
	}, []string{"channel"})

	CircuitState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "notification_circuit_breaker_state",
		Help: "Per-channel circuit breaker state: 0 closed, 1 half-open, 2 open.",
	}, []string{"channel"})

	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "API requests by route and status code.",
	}, []string{"method", "route", "code"})

	HTTPLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "API request latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route"})
)
