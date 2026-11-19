package delivery_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/atakanakis/notification-engine/internal/delivery"
	"github.com/atakanakis/notification-engine/internal/domain"
	"github.com/atakanakis/notification-engine/internal/events"
	"github.com/atakanakis/notification-engine/internal/ratelimit"
	"github.com/atakanakis/notification-engine/internal/store"
	"github.com/atakanakis/notification-engine/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

// fakeProvider records every call and answers with a scripted response.
type fakeProvider struct {
	mu      sync.Mutex
	calls   []fakeCall
	perID   map[string]int
	respond func(m delivery.Message, attempt int) delivery.Result
}

type fakeCall struct {
	msg delivery.Message
	at  time.Time
}

func newFakeProvider(respond func(m delivery.Message, attempt int) delivery.Result) *fakeProvider {
	return &fakeProvider{perID: map[string]int{}, respond: respond}
}

func (f *fakeProvider) Send(_ context.Context, m delivery.Message) delivery.Result {
	f.mu.Lock()
	f.perID[m.NotificationID]++
	attempt := f.perID[m.NotificationID]
	f.calls = append(f.calls, fakeCall{msg: m, at: time.Now()})
	f.mu.Unlock()
	res := f.respond(m, attempt)
	res.Duration = time.Millisecond
	return res
}

func (f *fakeProvider) snapshot() []fakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeCall(nil), f.calls...)
}

func accepted(delivery.Message, int) delivery.Result {
	return delivery.Result{StatusCode: http.StatusAccepted, MessageID: uuid.NewString()}
}

func testOptions() delivery.Options {
	return delivery.Options{
		Concurrency:      10,
		ClaimBatchSize:   10,
		PollInterval:     10 * time.Millisecond,
		Lease:            5 * time.Second,
		ReapInterval:     time.Second,
		GaugeInterval:    time.Second,
		Retry:            delivery.RetryPolicy{BaseDelay: 10 * time.Millisecond, MaxDelay: 40 * time.Millisecond},
		BreakerThreshold: 1000,
		BreakerOpenFor:   time.Minute,
	}
}

func startEngine(t *testing.T, env *testutil.Env, p delivery.Provider, opts delivery.Options, rate int) func() {
	t.Helper()
	return startEngineOn(t, env, store.New(env.Pool), p, opts, rate)
}

func startEngineOn(t *testing.T, env *testutil.Env, q delivery.Queue, p delivery.Provider, opts delivery.Options, rate int) func() {
	t.Helper()
	engine := delivery.NewEngine(q, ratelimit.New(env.Redis, rate, 50*time.Millisecond), p,
		events.Nop{}, slog.New(slog.NewTextHandler(io.Discard, nil)), opts)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		engine.Run(ctx)
		close(done)
	}()
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			cancel()
			<-done
		}
	}
	t.Cleanup(stop)
	return stop
}

func enqueue(t *testing.T, s *store.Store, ch domain.Channel, p domain.Priority, count int) []domain.Notification {
	t.Helper()
	items := make([]domain.Notification, count)
	for i := range items {
		items[i] = domain.Notification{
			ID: uuid.New(), Recipient: "+905551234567", Channel: ch, Content: "hi",
			Priority: p, Status: domain.StatusPending, MaxAttempts: 5,
		}
	}
	_, err := s.CreateBatch(context.Background(), items)
	require.NoError(t, err)
	return items
}

func waitForStatus(t *testing.T, s *store.Store, id uuid.UUID, want domain.Status) domain.Notification {
	t.Helper()
	var n domain.Notification
	require.Eventually(t, func() bool {
		var err error
		n, err = s.GetNotification(context.Background(), id)
		return err == nil && n.Status == want
	}, 10*time.Second, 20*time.Millisecond, "notification never reached %s (last: %s)", want, n.Status)
	return n
}

func TestTransientFailuresAreRetriedUntilDelivered(t *testing.T) {
	env := testutil.Setup(t)
	s := store.New(env.Pool)
	p := newFakeProvider(func(m delivery.Message, attempt int) delivery.Result {
		if attempt <= 2 {
			return delivery.Result{StatusCode: http.StatusServiceUnavailable}
		}
		return accepted(m, attempt)
	})
	n := enqueue(t, s, domain.ChannelSMS, domain.PriorityNormal, 1)[0]
	startEngine(t, env, p, testOptions(), 1000)

	got := waitForStatus(t, s, n.ID, domain.StatusSent)
	assert.Equal(t, 3, got.Attempts)
	assert.NotNil(t, got.ProviderMessageID)
	assert.NotNil(t, got.SentAt)

	attempts, err := s.ListAttempts(context.Background(), n.ID)
	require.NoError(t, err)
	require.Len(t, attempts, 3)
	assert.Equal(t, domain.OutcomeRetryable, attempts[0].Outcome)
	assert.Equal(t, domain.OutcomeRetryable, attempts[1].Outcome)
	assert.Equal(t, domain.OutcomeDelivered, attempts[2].Outcome)
}

func TestPermanentFailureIsNotRetried(t *testing.T) {
	env := testutil.Setup(t)
	s := store.New(env.Pool)
	p := newFakeProvider(func(delivery.Message, int) delivery.Result {
		return delivery.Result{StatusCode: http.StatusBadRequest, Body: "invalid recipient"}
	})
	n := enqueue(t, s, domain.ChannelEmail, domain.PriorityNormal, 1)[0]
	startEngine(t, env, p, testOptions(), 1000)

	got := waitForStatus(t, s, n.ID, domain.StatusFailed)
	assert.Equal(t, 1, got.Attempts)
	require.NotNil(t, got.LastError)
	assert.Contains(t, *got.LastError, "HTTP 400")
	time.Sleep(100 * time.Millisecond)
	assert.Len(t, p.snapshot(), 1, "a 4xx is never resent")
}

func TestGivesUpAfterMaxAttempts(t *testing.T) {
	env := testutil.Setup(t)
	s := store.New(env.Pool)
	p := newFakeProvider(func(delivery.Message, int) delivery.Result {
		return delivery.Result{StatusCode: http.StatusInternalServerError}
	})
	n := enqueue(t, s, domain.ChannelPush, domain.PriorityNormal, 1)[0]
	startEngine(t, env, p, testOptions(), 1000)

	got := waitForStatus(t, s, n.ID, domain.StatusFailed)
	assert.Equal(t, 5, got.Attempts)
	require.NotNil(t, got.LastError)
	assert.Contains(t, *got.LastError, "giving up after 5 attempts")
}

func TestHighPriorityOvertakesBacklog(t *testing.T) {
	env := testutil.Setup(t)
	s := store.New(env.Pool)
	enqueue(t, s, domain.ChannelSMS, domain.PriorityLow, 20)
	high := enqueue(t, s, domain.ChannelSMS, domain.PriorityHigh, 1)[0]

	p := newFakeProvider(accepted)
	opts := testOptions()
	opts.Concurrency, opts.ClaimBatchSize = 1, 1 // strictly one at a time
	startEngine(t, env, p, opts, 1000)

	waitForStatus(t, s, high.ID, domain.StatusSent)
	calls := p.snapshot()
	require.NotEmpty(t, calls)
	assert.Equal(t, high.ID.String(), calls[0].msg.NotificationID, "queued last, delivered first")
}

func TestCircuitBreakerPreservesAttemptsDuringOutage(t *testing.T) {
	env := testutil.Setup(t)
	s := store.New(env.Pool)
	p := newFakeProvider(func(delivery.Message, int) delivery.Result {
		return delivery.Result{StatusCode: http.StatusServiceUnavailable}
	})
	items := enqueue(t, s, domain.ChannelSMS, domain.PriorityNormal, 10)

	opts := testOptions()
	opts.Concurrency, opts.ClaimBatchSize = 1, 1
	opts.BreakerThreshold = 3
	opts.BreakerOpenFor = time.Minute
	stop := startEngine(t, env, p, opts, 1000)

	require.Eventually(t, func() bool { return len(p.snapshot()) >= 3 }, 5*time.Second, 10*time.Millisecond)
	time.Sleep(300 * time.Millisecond) // give a broken breaker time to misbehave
	stop()

	assert.Len(t, p.snapshot(), 3, "breaker opened after 3 consecutive failures")
	untouched := 0
	for _, n := range items {
		got, err := s.GetNotification(context.Background(), n.ID)
		require.NoError(t, err)
		if got.Attempts == 0 {
			untouched++
		}
	}
	assert.GreaterOrEqual(t, untouched, 7, "messages wait out the outage without spending retries")
}

func TestRateLimitHoldsAcrossTwoWorkers(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	env := testutil.Setup(t)
	s := store.New(env.Pool)
	const rate, total = 50, 150
	enqueue(t, s, domain.ChannelSMS, domain.PriorityNormal, total)

	p := newFakeProvider(accepted) // shared, so we see the combined traffic
	start := time.Now()
	startEngine(t, env, p, testOptions(), rate)
	startEngine(t, env, p, testOptions(), rate)

	require.Eventually(t, func() bool { return len(p.snapshot()) >= total }, 15*time.Second, 20*time.Millisecond)
	elapsed := time.Since(start)
	calls := p.snapshot()
	assert.Len(t, calls, total, "no notification delivered twice")

	maxInWindow := maxInAnySecond(calls)
	t.Logf("delivered %d in %s, max %d in any 1s window", total, elapsed, maxInWindow)
	assert.LessOrEqual(t, maxInWindow, rate, "hard limit in every sliding one-second window")
	assert.Greater(t, elapsed, 2*time.Second, "150 messages at 50/s need at least two full windows")
}

// maxInAnySecond is the largest number of provider calls inside any sliding
// one-second window, measured where the provider receives them.
func maxInAnySecond(calls []fakeCall) int {
	times := make([]time.Time, len(calls))
	for i, c := range calls {
		times[i] = c.at
	}
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	maxInWindow := 0
	for i := range times {
		j := i
		for j < len(times) && times[j].Sub(times[i]) < time.Second {
			j++
		}
		maxInWindow = max(maxInWindow, j-i)
	}
	return maxInWindow
}

// stallingQueue makes every few claims return late, like a Postgres pause,
// failover or long GC: the rows are claimed but reach the dispatcher long
// after its rate-limit permits were stamped.
type stallingQueue struct {
	*store.Store
	mu    sync.Mutex
	calls int
	every int
	stall time.Duration
}

func (q *stallingQueue) ClaimDue(ctx context.Context, ch domain.Channel, limit int, lease time.Duration) ([]domain.Notification, error) {
	items, err := q.Store.ClaimDue(ctx, ch, limit, lease)
	q.mu.Lock()
	q.calls++
	stall := len(items) > 0 && q.calls%q.every == 0
	q.mu.Unlock()
	if stall {
		time.Sleep(q.stall)
	}
	return items, err
}

// Regression: permits used to be trusted however long the claim took, so a
// 0.6s database stall let the provider see 150 requests in one second at a
// limit of 100. The dispatcher now re-takes permits that are older than the
// margin before sending.
func TestRateLimitHoldsWhenClaimsStall(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	env := testutil.Setup(t)
	s := store.New(env.Pool)
	const rate, total = 50, 300
	enqueue(t, s, domain.ChannelSMS, domain.PriorityNormal, total)

	p := newFakeProvider(accepted)
	opts := testOptions()
	opts.Concurrency, opts.ClaimBatchSize = 50, 25
	startEngineOn(t, env, &stallingQueue{Store: s, every: 3, stall: 400 * time.Millisecond}, p, opts, rate)
	startEngineOn(t, env, &stallingQueue{Store: s, every: 4, stall: 600 * time.Millisecond}, p, opts, rate)

	require.Eventually(t, func() bool { return len(p.snapshot()) >= total }, 30*time.Second, 20*time.Millisecond)
	calls := p.snapshot()
	assert.Len(t, calls, total, "no notification delivered twice")
	maxInWindow := maxInAnySecond(calls)
	t.Logf("max %d in any 1s window with stalled claims (limit %d)", maxInWindow, rate)
	assert.LessOrEqual(t, maxInWindow, rate, "hard limit holds even when claims stall")
}
