package delivery

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/atakanakis/notification-engine/internal/domain"
)

func TestClassify(t *testing.T) {
	cases := map[string]struct {
		res  Result
		want domain.Outcome
	}{
		"202 accepted":        {Result{StatusCode: 202}, domain.OutcomeDelivered},
		"200 ok":              {Result{StatusCode: 200}, domain.OutcomeDelivered},
		"timeout":             {Result{Err: errors.New("context deadline exceeded")}, domain.OutcomeRetryable},
		"429 throttled":       {Result{StatusCode: 429}, domain.OutcomeRetryable},
		"408 request timeout": {Result{StatusCode: 408}, domain.OutcomeRetryable},
		"500 server error":    {Result{StatusCode: 500}, domain.OutcomeRetryable},
		"503 unavailable":     {Result{StatusCode: 503}, domain.OutcomeRetryable},
		"400 bad request":     {Result{StatusCode: 400}, domain.OutcomePermanent},
		"401 unauthorized":    {Result{StatusCode: 401}, domain.OutcomePermanent},
		"422 unprocessable":   {Result{StatusCode: 422}, domain.OutcomePermanent},
		"301 unexpected":      {Result{StatusCode: 301}, domain.OutcomePermanent},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, Classify(tc.res))
		})
	}
}

func TestBackoffGrowsExponentiallyWithinJitterBounds(t *testing.T) {
	p := RetryPolicy{BaseDelay: 2 * time.Second, MaxDelay: 5 * time.Minute}
	for attempt, ceiling := range map[int]time.Duration{
		1: 2 * time.Second, 2: 4 * time.Second, 3: 8 * time.Second, 4: 16 * time.Second,
		10: 5 * time.Minute, // capped
		99: 5 * time.Minute, // no overflow
	} {
		p.Rand = func() float64 { return 0 }
		assert.Equal(t, ceiling/2, p.Backoff(attempt), "lower bound for attempt %d", attempt)
		p.Rand = func() float64 { return 0.999999 }
		got := p.Backoff(attempt)
		assert.LessOrEqual(t, got, ceiling, "upper bound for attempt %d", attempt)
		assert.Greater(t, got, ceiling*99/100)
	}
}

func TestBackoffIsJittered(t *testing.T) {
	p := RetryPolicy{BaseDelay: time.Second, MaxDelay: time.Minute}
	seen := map[time.Duration]bool{}
	for range 50 {
		seen[p.Backoff(3)] = true
	}
	assert.Greater(t, len(seen), 10, "retries of the same attempt must not line up")
}

func TestNextDelayHonoursRetryAfterUpToCap(t *testing.T) {
	p := RetryPolicy{BaseDelay: time.Second, MaxDelay: time.Minute, Rand: func() float64 { return 0 }}
	assert.Equal(t, 30*time.Second, p.NextDelay(1, 30*time.Second))
	assert.Equal(t, time.Minute, p.NextDelay(1, 24*time.Hour), "bogus Retry-After is capped")
	assert.Equal(t, 500*time.Millisecond, p.NextDelay(1, 0))
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	assert.Equal(t, 3*time.Second, parseRetryAfter("3", now))
	assert.Equal(t, 10*time.Second, parseRetryAfter(now.Add(10*time.Second).Format(http.TimeFormat), now))
	assert.Zero(t, parseRetryAfter("", now))
	assert.Zero(t, parseRetryAfter("soon", now))
	assert.Zero(t, parseRetryAfter("-5", now))
}
