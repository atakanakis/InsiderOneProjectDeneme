package delivery

import (
	"sync"
	"time"
)

type BreakerState int

const (
	BreakerClosed   BreakerState = iota // normal operation
	BreakerHalfOpen                     // cool-down over, one probe allowed
	BreakerOpen                         // provider considered down, nothing is sent
)

func (s BreakerState) String() string {
	switch s {
	case BreakerHalfOpen:
		return "half-open"
	case BreakerOpen:
		return "open"
	default:
		return "closed"
	}
}

// Breaker is a per-channel circuit breaker.
//
// Why: when the provider is down, every attempt fails and burns one of the
// message's retries. After `threshold` consecutive transient failures the
// breaker opens and the dispatcher stops claiming work for that channel, so
// messages wait in the queue with their attempts intact. After `openFor` a
// single probe message is let through; if it succeeds the breaker closes,
// otherwise it opens again.
//
// The breaker is local to a worker process. That is deliberate: each worker
// discovers an outage on its own within `threshold` calls, which keeps the
// design simple and avoids another shared dependency.
type Breaker struct {
	mu        sync.Mutex
	state     BreakerState
	failures  int
	threshold int
	openFor   time.Duration
	openedAt  time.Time
	probing   bool
	now       func() time.Time
	onChange  func(BreakerState)
}

func NewBreaker(threshold int, openFor time.Duration, onChange func(BreakerState)) *Breaker {
	if onChange == nil {
		onChange = func(BreakerState) {}
	}
	return &Breaker{threshold: threshold, openFor: openFor, now: time.Now, onChange: onChange}
}

// Allow returns how many of the wanted items may be dispatched right now:
// all of them when closed, none when open, one probe when half-open.
func (b *Breaker) Allow(want int) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == BreakerOpen && b.now().Sub(b.openedAt) >= b.openFor {
		b.setState(BreakerHalfOpen)
		b.probing = false
	}
	switch b.state {
	case BreakerClosed:
		return want
	case BreakerHalfOpen:
		if b.probing || want == 0 {
			return 0
		}
		b.probing = true
		return 1
	default:
		return 0
	}
}

// Release hands back permission that was granted by Allow but not used
// (e.g. the queue was empty), so a half-open breaker can probe again.
func (b *Breaker) Release(unused int) {
	if unused <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == BreakerHalfOpen {
		b.probing = false
	}
}

// Record reports whether the provider behaved healthily on one call.
// A permanent 4xx counts as healthy: the provider answered, our request was
// the problem.
func (b *Breaker) Record(healthy bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case BreakerClosed:
		if healthy {
			b.failures = 0
			return
		}
		b.failures++
		if b.failures >= b.threshold {
			b.trip()
		}
	case BreakerHalfOpen:
		b.probing = false
		if healthy {
			b.failures = 0
			b.setState(BreakerClosed)
		} else {
			b.trip()
		}
	case BreakerOpen:
		// Late results from calls that started before the breaker opened.
	}
}

// OpenRemaining is how long until an open breaker allows a probe.
func (b *Breaker) OpenRemaining() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != BreakerOpen {
		return 0
	}
	return max(0, b.openFor-b.now().Sub(b.openedAt))
}

func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *Breaker) trip() {
	b.openedAt = b.now()
	b.failures = 0
	b.setState(BreakerOpen)
}

func (b *Breaker) setState(s BreakerState) {
	if b.state != s {
		b.state = s
		b.onChange(s)
	}
}
