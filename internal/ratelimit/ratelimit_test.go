package ratelimit_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/atakanakis/notification-engine/internal/ratelimit"
	"github.com/atakanakis/notification-engine/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

func TestTakeNeverExceedsLimitWithinOneSecond(t *testing.T) {
	env := testutil.Setup(t)
	l := ratelimit.New(env.Redis, 10, 0)
	ctx := context.Background()

	g, err := l.Take(ctx, "sms", 100)
	require.NoError(t, err)
	assert.Equal(t, 10, g.N)
	assert.Greater(t, g.Wait, 900*time.Millisecond, "the next permit frees up when the oldest expires")

	g, err = l.Take(ctx, "sms", 1)
	require.NoError(t, err)
	assert.Equal(t, 0, g.N)

	time.Sleep(g.Wait + 20*time.Millisecond)
	g, err = l.Take(ctx, "sms", 100)
	require.NoError(t, err)
	assert.Equal(t, 10, g.N, "a full window is available again")
}

func TestMarginWidensTheWindow(t *testing.T) {
	env := testutil.Setup(t)
	l := ratelimit.New(env.Redis, 5, 200*time.Millisecond)
	ctx := context.Background()

	_, err := l.Take(ctx, "sms", 5)
	require.NoError(t, err)
	time.Sleep(time.Second + 50*time.Millisecond)
	g, err := l.Take(ctx, "sms", 5)
	require.NoError(t, err)
	assert.Equal(t, 0, g.N, "permits still count during the margin")
}

func TestConcurrentTakersShareOneLimit(t *testing.T) {
	env := testutil.Setup(t)
	l := ratelimit.New(env.Redis, 100, 0)
	ctx := context.Background()

	var mu sync.Mutex
	total := 0
	var wg sync.WaitGroup
	for range 20 { // twenty workers asking at the same moment
		wg.Go(func() {
			g, err := l.Take(ctx, "email", 25)
			if assert.NoError(t, err) {
				mu.Lock()
				total += g.N
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	assert.Equal(t, 100, total)
}

func TestBucketsAreIndependentPerKey(t *testing.T) {
	env := testutil.Setup(t)
	l := ratelimit.New(env.Redis, 5, 0)
	ctx := context.Background()

	_, err := l.Take(ctx, "sms", 5)
	require.NoError(t, err)
	g, err := l.Take(ctx, "email", 5)
	require.NoError(t, err)
	assert.Equal(t, 5, g.N, "draining sms does not affect email")
}

func TestRefundReturnsUnusedPermits(t *testing.T) {
	env := testutil.Setup(t)
	l := ratelimit.New(env.Redis, 5, 0)
	ctx := context.Background()

	g, err := l.Take(ctx, "push", 5)
	require.NoError(t, err)
	require.Equal(t, 5, g.N)
	require.NoError(t, l.Refund(ctx, "push", g, 3))

	g2, err := l.Take(ctx, "push", 5)
	require.NoError(t, err)
	assert.Equal(t, 3, g2.N)

	require.NoError(t, l.Refund(ctx, "push", g2, 100), "refunding more than granted is clamped")
	g3, err := l.Take(ctx, "push", 5)
	require.NoError(t, err)
	assert.Equal(t, 3, g3.N)
}
