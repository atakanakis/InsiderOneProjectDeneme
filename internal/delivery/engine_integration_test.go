package delivery_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
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

// flakyCompletion fails the first few result writes, like a dropped
// connection or a failover right after the provider accepted the message.
type flakyCompletion struct {
	*store.Store
	mu       sync.Mutex
	failures int
}

func (q *flakyCompletion) CompleteAttempt(ctx context.Context, r store.AttemptResult) (domain.Notification, error) {
	q.mu.Lock()
	fail := q.failures > 0
	if fail {
		q.failures--
	}
	q.mu.Unlock()
	if fail {
		return domain.Notification{}, errors.New("injected: connection reset")
	}
	return q.Store.CompleteAttempt(ctx, r)
}

// Regression (review 2): one failed result write made the worker give up;
// the reaper then resent a message the provider had already accepted.
func TestFailedResultWriteIsRetriedNotResent(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	env := testutil.Setup(t)
	s := store.New(env.Pool)
	items := enqueue(t, s, domain.ChannelSMS, domain.PriorityNormal, 1)

	p := newFakeProvider(accepted)
	opts := testOptions()
	opts.Lease = 3 * time.Second
	startEngineOn(t, env, &flakyCompletion{Store: s, failures: 2}, p, opts, 100)

	waitForStatus(t, s, items[0].ID, domain.StatusSent)
	time.Sleep(opts.Lease + 2*opts.ReapInterval) // give a wrong resend the time to happen
	assert.Len(t, p.snapshot(), 1, "delivered exactly once")
}

// Regression (review 3): the permit-age check ended when the delivery
// goroutine started, so connection setup was not covered. On the first
// burst after idle every request waits for a new connection while the next
// second's requests reuse warm ones, and the provider saw 200 at a limit of
// 100. The permit is now checked again when the connection is ready.
func TestRateLimitHoldsOnColdConnections(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	env := testutil.Setup(t)
	s := store.New(env.Pool)
	const rate, total = 50, 200
	enqueue(t, s, domain.ChannelSMS, domain.PriorityNormal, total)

	var mu sync.Mutex
	var arrivals []fakeCall
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		arrivals = append(arrivals, fakeCall{at: time.Now()})
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"m","status":"accepted"}`))
	}))
	defer srv.Close()
	snapshot := func() []fakeCall {
		mu.Lock()
		defer mu.Unlock()
		return append([]fakeCall(nil), arrivals...)
	}

	slowDial := func(tr *http.Transport) { // every new connection takes 150 ms, like a TLS handshake
		d := &net.Dialer{}
		tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			time.Sleep(150 * time.Millisecond)
			return d.DialContext(ctx, network, addr)
		}
	}
	opts := testOptions()
	opts.Concurrency, opts.ClaimBatchSize = 50, 25
	for range 2 {
		startEngine(t, env, delivery.NewHTTPProvider(srv.URL, 5*time.Second, 50, slowDial), opts, rate)
	}

	require.Eventually(t, func() bool { return len(snapshot()) >= total }, 30*time.Second, 20*time.Millisecond)
	calls := snapshot()
	assert.Len(t, calls, total)
	maxInWindow := maxInAnySecond(calls)
	t.Logf("max %d in any 1s window with 150 ms connection setup (limit %d)", maxInWindow, rate)
	assert.LessOrEqual(t, maxInWindow, rate, "hard limit holds when connections are cold")
}

// dyingLimiter grants the first Take per channel and then fails, like Redis
// going away right after the dispatcher took its permits.
type dyingLimiter struct {
	mu    sync.Mutex
	takes map[string]int
}

func (l *dyingLimiter) Take(_ context.Context, key string, n int) (ratelimit.Grant, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.takes == nil {
		l.takes = map[string]int{}
	}
	l.takes[key]++
	if l.takes[key] > 1 {
		return ratelimit.Grant{}, errors.New("injected: redis down")
	}
	return ratelimit.Grant{N: n}, nil
}

func (l *dyingLimiter) Refund(context.Context, string, ratelimit.Grant, int) error { return nil }

func runEngineWith(q delivery.Queue, l delivery.RateLimiter, p delivery.Provider, opts delivery.Options) (stop func() time.Duration) {
	e := delivery.NewEngine(q, l, p, events.Nop{}, slog.New(slog.NewTextHandler(io.Discard, nil)), opts)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		e.Run(ctx)
		close(done)
	}()
	return func() time.Duration {
		start := time.Now()
		cancel()
		<-done
		return time.Since(start)
	}
}

// Rows claimed with permits that went stale, when no fresh permits can be
// had, go back to the queue with their attempt refunded and are not sent.
func TestRowsWithoutFreshPermitsAreReleasedNotSpent(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	env := testutil.Setup(t)
	s := store.New(env.Pool)
	items := enqueue(t, s, domain.ChannelSMS, domain.PriorityNormal, 3)
	p := newFakeProvider(accepted)
	opts := testOptions()
	opts.Lease = 2 * time.Second // rows are released after lease/2
	stop := runEngineWith(&stallingQueue{Store: s, every: 1, stall: 200 * time.Millisecond}, &dyingLimiter{}, p, opts)
	defer stop()

	require.Eventually(t, func() bool {
		n, err := s.GetNotification(context.Background(), items[0].ID)
		return err == nil && n.Status == domain.StatusPending && n.Attempts == 0 && n.UpdatedAt.After(n.CreatedAt)
	}, 5*time.Second, 50*time.Millisecond, "the claim is handed back with its attempt refunded")
	assert.Empty(t, p.snapshot(), "nothing was sent without a permit")
}

// A shutdown while the dispatcher waits for fresh permits must not hang
// until the permits arrive; the claimed rows are released.
func TestShutdownWhileWaitingForPermitsIsPrompt(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	env := testutil.Setup(t)
	s := store.New(env.Pool)
	items := enqueue(t, s, domain.ChannelSMS, domain.PriorityNormal, 3)
	p := newFakeProvider(accepted)
	opts := testOptions()
	opts.Lease = 30 * time.Second // lease/2 is far away: only shutdown can end the wait
	stop := runEngineWith(&stallingQueue{Store: s, every: 1, stall: 200 * time.Millisecond}, &dyingLimiter{}, p, opts)
	waitForStatus(t, s, items[0].ID, domain.StatusProcessing) // claimed, now waiting for permits
	time.Sleep(300 * time.Millisecond)

	assert.Less(t, stop(), 2*time.Second)
	n, err := s.GetNotification(context.Background(), items[0].ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusPending, n.Status)
	assert.Equal(t, 0, n.Attempts)
	assert.Empty(t, p.snapshot())
}
