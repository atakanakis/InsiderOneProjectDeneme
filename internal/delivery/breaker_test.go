package delivery

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestBreaker(threshold int, openFor time.Duration) (*Breaker, *fakeClock, *[]BreakerState) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	var changes []BreakerState
	b := NewBreaker(threshold, openFor, func(s BreakerState) { changes = append(changes, s) })
	b.now = clock.now
	return b, clock, &changes
}

func TestBreakerOpensAfterConsecutiveFailures(t *testing.T) {
	b, _, changes := newTestBreaker(3, 10*time.Second)
	assert.Equal(t, 25, b.Allow(25))

	b.Record(false)
	b.Record(false)
	b.Record(true) // success resets the streak
	b.Record(false)
	b.Record(false)
	assert.Equal(t, BreakerClosed, b.State())

	b.Record(false)
	assert.Equal(t, BreakerOpen, b.State())
	assert.Equal(t, 0, b.Allow(25), "open breaker lets nothing through")
	assert.Equal(t, []BreakerState{BreakerOpen}, *changes)
}

func TestBreakerHalfOpenAllowsSingleProbe(t *testing.T) {
	b, clock, _ := newTestBreaker(1, 10*time.Second)
	b.Record(false)
	assert.Equal(t, 10*time.Second, b.OpenRemaining())

	clock.advance(10 * time.Second)
	assert.Equal(t, 1, b.Allow(25), "one probe after cool-down")
	assert.Equal(t, BreakerHalfOpen, b.State())
	assert.Equal(t, 0, b.Allow(25), "no second probe while the first is in flight")

	b.Record(true)
	assert.Equal(t, BreakerClosed, b.State())
	assert.Equal(t, 25, b.Allow(25))
}

func TestBreakerFailedProbeReopens(t *testing.T) {
	b, clock, _ := newTestBreaker(1, 5*time.Second)
	b.Record(false)
	clock.advance(5 * time.Second)
	assert.Equal(t, 1, b.Allow(10))

	b.Record(false)
	assert.Equal(t, BreakerOpen, b.State())
	assert.Equal(t, 5*time.Second, b.OpenRemaining(), "cool-down restarts")
}

func TestBreakerReleaseReturnsUnusedProbe(t *testing.T) {
	b, clock, _ := newTestBreaker(1, time.Second)
	b.Record(false)
	clock.advance(time.Second)
	assert.Equal(t, 1, b.Allow(10))
	b.Release(1) // queue was empty, probe not used
	assert.Equal(t, 1, b.Allow(10), "probe can be taken again")
}

func TestBreakerIgnoresLateResultsWhileOpen(t *testing.T) {
	b, _, _ := newTestBreaker(1, time.Minute)
	b.Record(false)
	b.Record(true) // a call that started before the breaker opened
	assert.Equal(t, BreakerOpen, b.State())
}
