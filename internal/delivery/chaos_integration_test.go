package delivery_test

// Randomised fault scenarios. The other engine tests each reproduce one
// known failure. These combine many at random (claim stalls, failing result
// writes, slow or failing provider calls, slow new connections) and check
// the invariants that must hold whatever happens:
//
//   - the provider never sees more than the limit in any sliding second;
//   - every notification is delivered;
//   - no notification is accepted by the provider twice.
//
// Timing makes each run different even with the same seed, so a failure
// logs its seed and settings. CHAOS_RUNS=n adds n more scenarios with seeds
// CHAOS_SEED, CHAOS_SEED+1, ... (CHAOS_SEED defaults to a random value) for
// a long soak. Every scenario ends with one "CHAOS_RESULT" log line that
// scripts/chaos_summary.py turns into pass rates.

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/atakanakis/notification-engine/internal/delivery"
	"github.com/atakanakis/notification-engine/internal/domain"
	"github.com/atakanakis/notification-engine/internal/store"
	"github.com/atakanakis/notification-engine/internal/testutil"
)

type chaosScenario struct {
	seed       uint64
	http       bool          // real HTTPProvider with slow new connections, else a direct fake
	stallP     float64       // chance a non-empty claim returns late
	stallMax   time.Duration // longest claim stall
	writeFailP float64       // chance a result write fails (it is retried)
	failP      float64       // chance the provider answers 503
	throttleP  float64       // chance the provider answers 429
	latencyMax time.Duration // provider latency
	dialMax    time.Duration // new-connection setup time (http only)
}

func (s chaosScenario) String() string {
	return fmt.Sprintf("seed=%d http=%v stall=%.2f/%s writeFail=%.2f fail=%.2f throttle=%.2f latency<%s dial<%s",
		s.seed, s.http, s.stallP, s.stallMax, s.writeFailP, s.failP, s.throttleP, s.latencyMax, s.dialMax)
}

func randomScenario(seed uint64) chaosScenario {
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	return chaosScenario{
		seed:       seed,
		http:       r.IntN(2) == 0,
		stallP:     0.1 + 0.3*r.Float64(),
		stallMax:   time.Duration(200+r.IntN(500)) * time.Millisecond,
		writeFailP: 0.1 * r.Float64(),
		failP:      0.1 * r.Float64(),
		throttleP:  0.05 * r.Float64(),
		latencyMax: time.Duration(5+r.IntN(60)) * time.Millisecond,
		dialMax:    time.Duration(r.IntN(250)) * time.Millisecond,
	}
}

// dice is a goroutine-safe random source for one scenario.
type dice struct {
	mu sync.Mutex
	r  *rand.Rand
}

func newDice(seed uint64) *dice { return &dice{r: rand.New(rand.NewPCG(seed, seed+1))} }

func (d *dice) chance(p float64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.r.Float64() < p
}

func (d *dice) upTo(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return time.Duration(d.r.Int64N(int64(max)))
}

// chaosQueue stalls claims and fails result writes at random.
type chaosQueue struct {
	*store.Store
	d *dice
	s chaosScenario
}

func (q *chaosQueue) ClaimDue(ctx context.Context, ch domain.Channel, limit int, lease time.Duration) ([]domain.Notification, error) {
	items, err := q.Store.ClaimDue(ctx, ch, limit, lease)
	if len(items) > 0 && q.d.chance(q.s.stallP) {
		time.Sleep(q.d.upTo(q.s.stallMax))
	}
	return items, err
}

func (q *chaosQueue) CompleteAttempt(ctx context.Context, r store.AttemptResult) (domain.Notification, error) {
	if q.d.chance(q.s.writeFailP) {
		return domain.Notification{}, errors.New("chaos: result write failed")
	}
	return q.Store.CompleteAttempt(ctx, r)
}

// chaosProvider records what the provider side sees: every arrival, and
// which notifications it accepted.
type chaosProvider struct {
	d        *dice
	s        chaosScenario
	mu       sync.Mutex
	arrivals []fakeCall
	accepted map[string]int
}

// respond decides the outcome of one arrival and records it.
func (p *chaosProvider) respond(id string) int {
	time.Sleep(p.d.upTo(p.s.latencyMax))
	status := http.StatusAccepted
	switch {
	case p.d.chance(p.s.failP):
		status = http.StatusServiceUnavailable
	case p.d.chance(p.s.throttleP):
		status = http.StatusTooManyRequests
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.arrivals = append(p.arrivals, fakeCall{at: time.Now()})
	if status == http.StatusAccepted {
		p.accepted[id]++
	}
	return status
}

func (p *chaosProvider) snapshot() ([]fakeCall, map[string]int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	acc := make(map[string]int, len(p.accepted))
	for k, v := range p.accepted {
		acc[k] = v
	}
	return append([]fakeCall(nil), p.arrivals...), acc
}

// Send makes chaosProvider a delivery.Provider for the direct variant.
func (p *chaosProvider) Send(_ context.Context, m delivery.Message) delivery.Result {
	start := time.Now()
	status := p.respond(m.NotificationID)
	return delivery.Result{StatusCode: status, MessageID: uuid.NewString(), Duration: time.Since(start)}
}

func TestInvariantsHoldUnderRandomFaults(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	scenarios := []chaosScenario{randomScenario(1), randomScenario(2), randomScenario(3)}
	// Make sure both provider paths are exercised whatever the fixed seeds pick.
	scenarios[0].http, scenarios[1].http = false, true
	if n, _ := strconv.Atoi(os.Getenv("CHAOS_RUNS")); n > 0 {
		base, err := strconv.ParseUint(os.Getenv("CHAOS_SEED"), 10, 64)
		if err != nil {
			base = rand.Uint64() >> 1
		}
		t.Logf("soak: %d extra scenarios from seed %d", n, base)
		for i := range uint64(n) {
			scenarios = append(scenarios, randomScenario(base+i))
		}
	}
	for _, sc := range scenarios {
		t.Run(fmt.Sprintf("seed-%d", sc.seed), func(t *testing.T) {
			runChaos(t, sc)
		})
	}
}

func runChaos(t *testing.T, sc chaosScenario) {
	t.Logf("scenario: %s", sc)
	// One machine-readable line per scenario, written even when an assertion
	// fails, so a soak can report pass rates per invariant.
	var arrivalsN, acceptedN, maxWindow, dupes int
	delivered := false
	defer func() {
		t.Logf("CHAOS_RESULT seed=%d http=%v arrivals=%d accepted=%d max_window=%d limit=%d duplicates=%d all_delivered=%v passed=%v",
			sc.seed, sc.http, arrivalsN, acceptedN, maxWindow, 50, dupes, delivered, !t.Failed())
	}()
	env := testutil.Setup(t)
	s := store.New(env.Pool)
	const rate, total = 50, 300

	items := make([]domain.Notification, total)
	for i := range items {
		items[i] = domain.Notification{
			ID: uuid.New(), Recipient: "+905551234567", Channel: domain.ChannelSMS, Content: "hi",
			Priority: domain.Priorities[i%len(domain.Priorities)], Status: domain.StatusPending,
			MaxAttempts: 25, // random 503s must not dead-letter anything
		}
	}
	_, err := s.CreateBatch(context.Background(), items)
	require.NoError(t, err)

	d := newDice(sc.seed)
	p := &chaosProvider{d: d, s: sc, accepted: map[string]int{}}
	var provider delivery.Provider = p
	if sc.http {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(p.respond(r.Header.Get("Idempotency-Key")))
			_, _ = w.Write([]byte(`{"messageId":"m","status":"accepted"}`))
		}))
		t.Cleanup(srv.Close)
		slowAndFewConns := func(tr *http.Transport) {
			tr.MaxIdleConnsPerHost = 2 // most sends need a new connection
			dialer := &net.Dialer{}
			tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				time.Sleep(d.upTo(sc.dialMax))
				return dialer.DialContext(ctx, network, addr)
			}
		}
		provider = delivery.NewHTTPProvider(srv.URL, 5*time.Second, 50, slowAndFewConns)
	}

	opts := testOptions()
	opts.Concurrency, opts.ClaimBatchSize = 50, 25
	for range 2 {
		startEngineOn(t, env, &chaosQueue{Store: s, d: d, s: sc}, provider, opts, rate)
	}

	deliveredAll := assert.Eventually(t, func() bool {
		_, acc := p.snapshot()
		return len(acc) >= total
	}, 60*time.Second, 50*time.Millisecond, "not every notification was delivered (%s)", sc)

	arrivals, accepted := p.snapshot()
	maxInWindow := maxInAnySecond(arrivals)
	arrivalsN, acceptedN, maxWindow = len(arrivals), len(accepted), maxInWindow
	for _, n := range accepted {
		if n > 1 {
			dupes++
		}
	}
	if !deliveredAll {
		return
	}
	t.Logf("arrivals=%d accepted=%d max in any 1s window=%d (limit %d)", len(arrivals), len(accepted), maxInWindow, rate)
	assert.LessOrEqual(t, maxInWindow, rate, "rate limit exceeded at the provider (%s)", sc)
	for id, n := range accepted {
		assert.Equal(t, 1, n, "notification %s accepted %d times (%s)", id, n, sc)
	}
	for _, it := range items {
		waitForStatus(t, s, it.ID, domain.StatusSent)
	}
	delivered = true
}
