// Package delivery is the processing engine: it pulls due notifications from
// the queue, respects the per-channel rate limit and circuit breaker, calls
// the provider and decides what happens after each attempt.
package delivery

import (
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/atakanakis/notification-engine/internal/domain"
)

// Classify turns a provider response into a retry decision.
//
//	2xx                      -> delivered
//	network error / timeout  -> retryable (the provider may be briefly down)
//	408, 425, 429            -> retryable (timeout, too early, rate limited)
//	5xx                      -> retryable (provider-side failure)
//	other 4xx                -> permanent (our request is wrong; resending
//	                            the same bytes will fail the same way)
func Classify(res Result) domain.Outcome {
	if res.Err != nil {
		return domain.OutcomeRetryable
	}
	switch c := res.StatusCode; {
	case c >= 200 && c < 300:
		return domain.OutcomeDelivered
	case c == http.StatusRequestTimeout, c == http.StatusTooEarly, c == http.StatusTooManyRequests:
		return domain.OutcomeRetryable
	case c >= 500:
		return domain.OutcomeRetryable
	default:
		return domain.OutcomePermanent
	}
}

// RetryPolicy computes when a failed notification should be tried again.
type RetryPolicy struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	// Rand returns a number in [0,1). Overridable for deterministic tests.
	Rand func() float64
}

// Backoff is exponential with "equal jitter":
//
//	ceiling = min(MaxDelay, BaseDelay * 2^(attempt-1))
//	delay   = ceiling/2 + random(0, ceiling/2)
//
// Doubling gives a struggling provider room to recover. The random half
// spreads retries out so thousands of messages that failed in the same
// second (e.g. during a flash sale) do not all come back in the same second
// and knock the provider over again. Keeping the first half fixed means a
// retry never happens almost immediately by bad luck.
//
// With the defaults (2s base, 5 attempts) the waits are roughly
// 1-2s, 2-4s, 4-8s, 8-16s.
func (p RetryPolicy) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	ceiling := p.MaxDelay
	if attempt <= 32 {
		if d := p.BaseDelay << (attempt - 1); d > 0 && d < p.MaxDelay {
			ceiling = d
		}
	}
	r := rand.Float64
	if p.Rand != nil {
		r = p.Rand
	}
	half := ceiling / 2
	return half + time.Duration(r()*float64(half))
}

// NextDelay honours a provider's Retry-After header when it asks for longer
// than our own backoff, capped at MaxDelay so a bogus header cannot park a
// message for days.
func (p RetryPolicy) NextDelay(attempt int, retryAfter time.Duration) time.Duration {
	d := p.Backoff(attempt)
	if retryAfter > d {
		d = min(retryAfter, p.MaxDelay)
	}
	return d
}
