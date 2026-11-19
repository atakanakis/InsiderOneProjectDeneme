package delivery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/atakanakis/notification-engine/internal/domain"
	"github.com/atakanakis/notification-engine/internal/events"
	"github.com/atakanakis/notification-engine/internal/observability"
	"github.com/atakanakis/notification-engine/internal/ratelimit"
	"github.com/atakanakis/notification-engine/internal/store"
)

// Queue is the part of the store the engine needs.
type Queue interface {
	ClaimDue(ctx context.Context, ch domain.Channel, limit int, lease time.Duration) ([]domain.Notification, error)
	CompleteAttempt(ctx context.Context, r store.AttemptResult) (domain.Notification, error)
	ReapExpiredLeases(ctx context.Context) ([]domain.Notification, error)
	QueueDepth(ctx context.Context) ([]store.QueueDepth, error)
}

// RateLimiter hands out send permits per channel.
type RateLimiter interface {
	Take(ctx context.Context, key string, n int) (ratelimit.Grant, error)
	Refund(ctx context.Context, key string, g ratelimit.Grant, unused int) error
}

type Options struct {
	Concurrency      int           // max in-flight provider calls per channel
	ClaimBatchSize   int           // max notifications claimed per loop
	PollInterval     time.Duration // sleep when the queue is empty
	Lease            time.Duration // how long a claim is valid
	ReapInterval     time.Duration
	GaugeInterval    time.Duration
	Retry            RetryPolicy
	BreakerThreshold int
	BreakerOpenFor   time.Duration
}

type Engine struct {
	queue     Queue
	limiter   RateLimiter
	provider  Provider
	publisher events.Publisher
	log       *slog.Logger
	opts      Options
	breakers  map[domain.Channel]*Breaker
	tracer    trace.Tracer
}

func NewEngine(q Queue, l RateLimiter, p Provider, pub events.Publisher, log *slog.Logger, opts Options) *Engine {
	if opts.GaugeInterval == 0 {
		opts.GaugeInterval = 5 * time.Second
	}
	e := &Engine{
		queue: q, limiter: l, provider: p, publisher: pub, log: log, opts: opts,
		breakers: map[domain.Channel]*Breaker{},
		tracer:   otel.Tracer("notification-engine/delivery"),
	}
	for _, ch := range domain.Channels {
		observability.CircuitState.WithLabelValues(string(ch)).Set(0)
		e.breakers[ch] = NewBreaker(opts.BreakerThreshold, opts.BreakerOpenFor, func(s BreakerState) {
			observability.CircuitState.WithLabelValues(string(ch)).Set(float64(s))
			log.Warn("circuit breaker state changed", "channel", ch, "state", s.String())
		})
	}
	return e
}

// Run blocks until ctx is cancelled. On shutdown it stops claiming new work
// and waits for in-flight deliveries to finish and record their result, so a
// deploy does not leave rows stuck in "processing" until their lease expires.
func (e *Engine) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, ch := range domain.Channels {
		wg.Go(func() { e.runChannel(ctx, ch) })
	}
	wg.Go(func() { e.every(ctx, e.opts.ReapInterval, e.reap) })
	wg.Go(func() { e.every(ctx, e.opts.GaugeInterval, e.updateGauges) })
	wg.Wait()
}

// runChannel is the dispatcher loop for one channel:
//
//  1. Is there a free concurrency slot?        (otherwise wait for one)
//  2. Does the circuit breaker allow sending?  (otherwise wait for cool-down)
//  3. Take rate-limit permits from Redis.      (otherwise wait for the window)
//  4. Claim that many due rows, highest priority first.
//  5. Deliver each row in its own goroutine.
//
// Permits are taken before rows are claimed. The other order (claim, then
// wait for permits) would hold leased low-priority rows while a
// high-priority message arriving in the meantime has to wait behind them.
func (e *Engine) runChannel(ctx context.Context, ch domain.Channel) {
	breaker := e.breakers[ch]
	slots := make(chan struct{}, e.opts.Concurrency)
	var inflight sync.WaitGroup
	defer inflight.Wait()

	// Deliveries outlive a shutdown signal so they can finish cleanly.
	deliverCtx := context.WithoutCancel(ctx)
	key := string(ch)

	for ctx.Err() == nil {
		free := cap(slots) - len(slots)
		if free == 0 {
			select { // wait until any delivery finishes
			case slots <- struct{}{}:
				<-slots
			case <-ctx.Done():
				return
			}
			continue
		}

		want := breaker.Allow(min(free, e.opts.ClaimBatchSize))
		if want == 0 {
			sleep(ctx, max(min(breaker.OpenRemaining(), time.Second), e.opts.PollInterval))
			continue
		}

		grant, err := e.limiter.Take(ctx, key, want)
		granted := grant.N
		if err != nil {
			breaker.Release(want)
			e.log.Error("rate limiter unavailable", "channel", ch, "error", err)
			sleep(ctx, time.Second)
			continue
		}
		if granted == 0 {
			breaker.Release(want)
			observability.RateLimited.WithLabelValues(key).Inc()
			sleep(ctx, grant.Wait)
			continue
		}
		breaker.Release(want - granted)

		// The claim must not be cancelled half way: if Postgres commits the
		// UPDATE but we drop the rows, they sit in "processing" until the
		// lease expires and may lose their last attempt. Check for shutdown
		// first, then claim on a context detached from the signal.
		if ctx.Err() != nil {
			breaker.Release(granted)
			e.refund(ctx, key, grant, granted)
			return
		}
		claimCtx, cancelClaim := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		items, err := e.queue.ClaimDue(claimCtx, ch, granted, e.opts.Lease)
		cancelClaim()
		if err != nil {
			breaker.Release(granted)
			e.refund(ctx, key, grant, granted)
			if ctx.Err() == nil {
				e.log.Error("claim failed", "channel", ch, "error", err)
			}
			sleep(ctx, time.Second)
			continue
		}
		if unused := granted - len(items); unused > 0 {
			breaker.Release(unused)
			e.refund(ctx, key, grant, unused)
		}

		for _, n := range items {
			slots <- struct{}{}
			inflight.Go(func() {
				defer func() { <-slots }()
				e.deliver(deliverCtx, breaker, n)
			})
		}
		if len(items) < granted {
			sleep(ctx, e.opts.PollInterval) // queue drained, poll again shortly
		}
	}
}

func (e *Engine) refund(ctx context.Context, key string, g ratelimit.Grant, unused int) {
	if err := e.limiter.Refund(context.WithoutCancel(ctx), key, g, unused); err != nil {
		e.log.Warn("rate limit refund failed", "channel", key, "error", err)
	}
}

// deliver performs one attempt and records what happens next.
func (e *Engine) deliver(ctx context.Context, breaker *Breaker, n domain.Notification) {
	ctx = observability.ExtractTraceParent(ctx, n.TraceParent)
	ctx = observability.WithCorrelationID(ctx, n.CorrelationID)
	ctx, span := e.tracer.Start(ctx, "notification.deliver", trace.WithAttributes(
		attribute.String("notification.id", n.ID.String()),
		attribute.String("notification.channel", string(n.Channel)),
		attribute.String("notification.priority", string(n.Priority)),
		attribute.Int("notification.attempt", n.Attempts),
	))
	defer span.End()

	res := e.provider.Send(ctx, Message{
		NotificationID: n.ID.String(),
		CorrelationID:  n.CorrelationID,
		To:             n.Recipient,
		Channel:        string(n.Channel),
		Content:        n.Content,
	})
	outcome := Classify(res)
	breaker.Record(outcome != domain.OutcomeRetryable)

	ch := string(n.Channel)
	observability.DeliveryAttempts.WithLabelValues(ch, string(outcome)).Inc()
	observability.ProviderLatency.WithLabelValues(ch).Observe(res.Duration.Seconds())

	result := store.AttemptResult{
		NotificationID: n.ID,
		AttemptNumber:  n.Attempts,
		Channel:        n.Channel,
		Outcome:        outcome,
		Duration:       res.Duration,
	}
	if res.StatusCode != 0 {
		code := res.StatusCode
		result.HTTPStatus = &code
	}

	switch outcome {
	case domain.OutcomeDelivered:
		result.NewStatus = domain.StatusSent
		if res.MessageID != "" {
			id := res.MessageID
			result.ProviderMessageID = &id
		}
	case domain.OutcomeRetryable:
		msg := describeFailure(res)
		result.Error = &msg
		if n.Attempts < n.MaxAttempts {
			result.NewStatus = domain.StatusRetrying
			result.RetryDelay = e.opts.Retry.NextDelay(n.Attempts, res.RetryAfter)
		} else {
			result.NewStatus = domain.StatusFailed
			msg = fmt.Sprintf("giving up after %d attempts: %s", n.Attempts, msg)
		}
	case domain.OutcomePermanent:
		msg := describeFailure(res)
		result.Error = &msg
		result.NewStatus = domain.StatusFailed
	}
	if result.Error != nil {
		span.SetStatus(codes.Error, *result.Error)
	}
	span.SetAttributes(attribute.String("notification.outcome", string(outcome)),
		attribute.String("notification.status", string(result.NewStatus)))

	dbCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	updated, err := e.queue.CompleteAttempt(dbCtx, result)
	if errors.Is(err, store.ErrStaleLease) {
		e.log.WarnContext(ctx, "lease expired before result was recorded; newer owner wins",
			"notification_id", n.ID, "attempt", n.Attempts, "outcome", outcome)
		return
	}
	if err != nil {
		// The lease will expire and the reaper will put the row back, so the
		// message is not lost; the provider idempotency key prevents a
		// visible duplicate if this attempt had in fact been delivered.
		e.log.ErrorContext(ctx, "recording attempt failed", "notification_id", n.ID, "error", err)
		return
	}

	logArgs := []any{"notification_id", n.ID, "channel", n.Channel, "attempt", n.Attempts,
		"outcome", outcome, "status", updated.Status, "http_status", res.StatusCode,
		"duration_ms", res.Duration.Milliseconds()}
	switch updated.Status {
	case domain.StatusSent:
		observability.NotificationsFinished.WithLabelValues(ch, string(updated.Status)).Inc()
		if updated.SentAt != nil {
			due := updated.CreatedAt
			if updated.ScheduledAt != nil && updated.ScheduledAt.After(due) {
				due = *updated.ScheduledAt
			}
			observability.EndToEndLatency.WithLabelValues(ch).Observe(updated.SentAt.Sub(due).Seconds())
		}
		e.log.InfoContext(ctx, "notification delivered", logArgs...)
	case domain.StatusFailed:
		observability.NotificationsFinished.WithLabelValues(ch, string(updated.Status)).Inc()
		e.log.WarnContext(ctx, "notification failed", append(logArgs, "error", deref(updated.LastError))...)
	default:
		e.log.InfoContext(ctx, "delivery attempt failed, will retry",
			append(logArgs, "next_attempt_at", updated.NextAttemptAt, "error", deref(updated.LastError))...)
	}
	e.publisher.Publish(ctx, domain.EventFor(updated))
}

func (e *Engine) reap(ctx context.Context) {
	items, err := e.queue.ReapExpiredLeases(ctx)
	if err != nil {
		if ctx.Err() == nil {
			e.log.Error("reaping expired leases failed", "error", err)
		}
		return
	}
	for _, n := range items {
		e.log.Warn("reclaimed notification with expired lease", "notification_id", n.ID,
			"correlation_id", n.CorrelationID, "status", n.Status, "attempts", n.Attempts)
		if n.Status == domain.StatusFailed {
			observability.NotificationsFinished.WithLabelValues(string(n.Channel), string(n.Status)).Inc()
		}
		e.publisher.Publish(ctx, domain.EventFor(n))
	}
}

func (e *Engine) updateGauges(ctx context.Context) {
	depth, err := e.queue.QueueDepth(ctx)
	if err != nil {
		return
	}
	observability.QueueDepth.Reset()
	observability.ScheduledDepth.Reset()
	for _, ch := range domain.Channels {
		for _, p := range domain.Priorities {
			observability.QueueDepth.WithLabelValues(string(ch), string(p)).Set(0)
			observability.ScheduledDepth.WithLabelValues(string(ch), string(p)).Set(0)
		}
	}
	for _, d := range depth {
		observability.QueueDepth.WithLabelValues(string(d.Channel), string(d.Priority)).Set(float64(d.Ready))
		observability.ScheduledDepth.WithLabelValues(string(d.Channel), string(d.Priority)).Set(float64(d.Scheduled))
	}
}

func (e *Engine) every(ctx context.Context, interval time.Duration, fn func(context.Context)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn(ctx)
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
