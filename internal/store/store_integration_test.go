package store_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/atakanakis/notification-engine/internal/domain"
	"github.com/atakanakis/notification-engine/internal/store"
	"github.com/atakanakis/notification-engine/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

func newNotification(ch domain.Channel, p domain.Priority) domain.Notification {
	return domain.Notification{
		ID: uuid.New(), Recipient: "+905551234567", Channel: ch, Content: "hello",
		Priority: p, Status: domain.StatusPending, MaxAttempts: 3,
	}
}

func ptr[T any](v T) *T { return &v }

func TestIdempotentCreate(t *testing.T) {
	s := store.New(testutil.Setup(t).Pool)
	ctx := context.Background()

	n := newNotification(domain.ChannelSMS, domain.PriorityNormal)
	n.IdempotencyKey = ptr("order-42")
	first, err := s.CreateNotification(ctx, n)
	require.NoError(t, err)
	assert.True(t, first.Created)

	again := n
	again.ID = uuid.New()
	second, err := s.CreateNotification(ctx, again)
	require.NoError(t, err)
	assert.False(t, second.Created)
	assert.Equal(t, first.Notification.ID, second.Notification.ID, "same key returns the original")

	different := again
	different.Content = "something else"
	_, err = s.CreateNotification(ctx, different)
	assert.ErrorIs(t, err, store.ErrIdempotencyMismatch)
}

func TestConcurrentIdempotentCreatesYieldOneRow(t *testing.T) {
	s := store.New(testutil.Setup(t).Pool)
	ctx := context.Background()

	var wg sync.WaitGroup
	ids := make(chan uuid.UUID, 20)
	for range 20 {
		wg.Go(func() {
			n := newNotification(domain.ChannelSMS, domain.PriorityNormal)
			n.IdempotencyKey = ptr("double-click")
			res, err := s.CreateNotification(ctx, n)
			if assert.NoError(t, err) {
				ids <- res.Notification.ID
			}
		})
	}
	wg.Wait()
	close(ids)
	unique := map[uuid.UUID]bool{}
	for id := range ids {
		unique[id] = true
	}
	assert.Len(t, unique, 1, "20 simultaneous requests with one key create one notification")
}

func TestBatchIsAtomic(t *testing.T) {
	s := store.New(testutil.Setup(t).Pool)
	ctx := context.Background()

	existing := newNotification(domain.ChannelSMS, domain.PriorityNormal)
	existing.IdempotencyKey = ptr("k1")
	_, err := s.CreateNotification(ctx, existing)
	require.NoError(t, err)

	ok := newNotification(domain.ChannelEmail, domain.PriorityLow)
	clash := newNotification(domain.ChannelSMS, domain.PriorityNormal)
	clash.IdempotencyKey = ptr("k1")
	clash.Content = "different payload"
	_, err = s.CreateBatch(ctx, []domain.Notification{ok, clash})
	require.ErrorIs(t, err, store.ErrIdempotencyMismatch)

	_, err = s.GetNotification(ctx, ok.ID)
	assert.ErrorIs(t, err, store.ErrNotFound, "the valid item was rolled back with the batch")
}

func TestClaimOrdersByPriorityAndRespectsSchedule(t *testing.T) {
	s := store.New(testutil.Setup(t).Pool)
	ctx := context.Background()

	low := newNotification(domain.ChannelSMS, domain.PriorityLow)
	normal := newNotification(domain.ChannelSMS, domain.PriorityNormal)
	high := newNotification(domain.ChannelSMS, domain.PriorityHigh)
	future := newNotification(domain.ChannelSMS, domain.PriorityHigh)
	future.ScheduledAt = ptr(time.Now().Add(time.Hour))
	otherChannel := newNotification(domain.ChannelEmail, domain.PriorityHigh)
	_, err := s.CreateBatch(ctx, []domain.Notification{low, normal, high, future, otherChannel})
	require.NoError(t, err)

	got, err := s.ClaimDue(ctx, domain.ChannelSMS, 2, time.Minute)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, high.ID, got[0].ID)
	assert.Equal(t, normal.ID, got[1].ID)
	assert.Equal(t, domain.StatusProcessing, got[0].Status)
	assert.Equal(t, 1, got[0].Attempts)

	got, err = s.ClaimDue(ctx, domain.ChannelSMS, 10, time.Minute)
	require.NoError(t, err)
	require.Len(t, got, 1, "scheduled and other-channel rows are not claimed")
	assert.Equal(t, low.ID, got[0].ID)
}

func TestConcurrentClaimsNeverOverlap(t *testing.T) {
	s := store.New(testutil.Setup(t).Pool)
	ctx := context.Background()

	items := make([]domain.Notification, 500)
	for i := range items {
		items[i] = newNotification(domain.ChannelPush, domain.PriorityNormal)
	}
	_, err := s.CreateBatch(ctx, items)
	require.NoError(t, err)

	var mu sync.Mutex
	seen := map[uuid.UUID]int{}
	var wg sync.WaitGroup
	for range 10 { // ten "workers" racing for the same queue
		wg.Go(func() {
			for {
				got, err := s.ClaimDue(ctx, domain.ChannelPush, 7, time.Minute)
				if !assert.NoError(t, err) || len(got) == 0 {
					return
				}
				mu.Lock()
				for _, n := range got {
					seen[n.ID]++
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	assert.Len(t, seen, 500)
	for id, count := range seen {
		assert.Equal(t, 1, count, "notification %s claimed twice", id)
	}
}

func TestCompleteAttemptRejectsStaleLease(t *testing.T) {
	s := store.New(testutil.Setup(t).Pool)
	ctx := context.Background()

	n := newNotification(domain.ChannelSMS, domain.PriorityNormal)
	_, err := s.CreateNotification(ctx, n)
	require.NoError(t, err)

	// Worker A claims with a tiny lease and stalls.
	claimed, err := s.ClaimDue(ctx, domain.ChannelSMS, 1, 10*time.Millisecond)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	time.Sleep(50 * time.Millisecond)

	reaped, err := s.ReapExpiredLeases(ctx)
	require.NoError(t, err)
	require.Len(t, reaped, 1)
	assert.Equal(t, domain.StatusRetrying, reaped[0].Status)

	// Worker B picks it up again (attempt 2).
	again, err := s.ClaimDue(ctx, domain.ChannelSMS, 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, again, 1)
	assert.Equal(t, 2, again[0].Attempts)

	// Worker A wakes up and reports attempt 1: must not overwrite B's state.
	_, err = s.CompleteAttempt(ctx, store.AttemptResult{
		NotificationID: n.ID, AttemptNumber: 1, Channel: domain.ChannelSMS,
		Outcome: domain.OutcomeDelivered, NewStatus: domain.StatusSent,
	})
	assert.ErrorIs(t, err, store.ErrStaleLease)

	current, err := s.GetNotification(ctx, n.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusProcessing, current.Status)

	attempts, err := s.ListAttempts(ctx, n.ID)
	require.NoError(t, err)
	assert.Len(t, attempts, 1, "the stale attempt is still recorded for audit")
}

// A worker whose lease expired still delivered the message. If the row has
// not been claimed again, its success must win, or the customer gets the
// message twice.
func TestLateSuccessWinsUntilTheRowIsClaimedAgain(t *testing.T) {
	s := store.New(testutil.Setup(t).Pool)
	ctx := context.Background()
	n := newNotification(domain.ChannelSMS, domain.PriorityNormal)
	_, err := s.CreateNotification(ctx, n)
	require.NoError(t, err)

	_, err = s.ClaimDue(ctx, domain.ChannelSMS, 1, 10*time.Millisecond)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	reaped, err := s.ReapExpiredLeases(ctx)
	require.NoError(t, err)
	require.Len(t, reaped, 1)

	late := store.AttemptResult{
		NotificationID: n.ID, AttemptNumber: 1, Channel: domain.ChannelSMS,
		Outcome: domain.OutcomeRetryable, NewStatus: domain.StatusRetrying,
	}
	_, err = s.CompleteAttempt(ctx, late)
	assert.ErrorIs(t, err, store.ErrStaleLease, "a late failure never overrides the reaper")

	late.Outcome, late.NewStatus = domain.OutcomeDelivered, domain.StatusSent
	got, err := s.CompleteAttempt(ctx, late)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusSent, got.Status)

	again, err := s.ClaimDue(ctx, domain.ChannelSMS, 1, time.Minute)
	require.NoError(t, err)
	assert.Empty(t, again, "a sent notification is not delivered again")
}

// Claims run per priority so that a deep backlog of future high-priority
// rows does not hide due rows of lower priority.
func TestFutureHighPriorityBacklogDoesNotBlockDueRows(t *testing.T) {
	s := store.New(testutil.Setup(t).Pool)
	ctx := context.Background()
	tomorrow := time.Now().Add(24 * time.Hour)
	var rows []domain.Notification
	for range 200 {
		f := newNotification(domain.ChannelSMS, domain.PriorityHigh)
		f.ScheduledAt = &tomorrow
		rows = append(rows, f)
	}
	low := newNotification(domain.ChannelSMS, domain.PriorityLow)
	high := newNotification(domain.ChannelSMS, domain.PriorityHigh)
	rows = append(rows, low, high)
	_, err := s.CreateBatch(ctx, rows)
	require.NoError(t, err)

	got, err := s.ClaimDue(ctx, domain.ChannelSMS, 10, time.Minute)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, high.ID, got[0].ID, "due high priority first")
	assert.Equal(t, low.ID, got[1].ID)
}

// Regression (reviews 2 and 3): claims run one statement per priority. A
// failure in one priority must neither strand rows from another (claimed
// but not returned, an attempt spent) nor block the other priorities.
func TestFailingPriorityNeitherStrandsNorBlocksOthers(t *testing.T) {
	env := testutil.Setup(t)
	s := store.New(env.Pool)
	ctx := context.Background()
	high := newNotification(domain.ChannelSMS, domain.PriorityHigh)
	normal := newNotification(domain.ChannelSMS, domain.PriorityNormal)
	_, err := s.CreateBatch(ctx, []domain.Notification{high, normal})
	require.NoError(t, err)

	_, err = env.Pool.Exec(ctx, `
		CREATE FUNCTION fail_normal_claims() RETURNS trigger AS $$
		BEGIN
			IF NEW.priority = 1 AND NEW.status = 'processing' THEN RAISE EXCEPTION 'injected'; END IF;
			RETURN NEW;
		END $$ LANGUAGE plpgsql;
		CREATE TRIGGER fail_normal_claims BEFORE UPDATE ON notifications
			FOR EACH ROW EXECUTE FUNCTION fail_normal_claims();`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = env.Pool.Exec(context.Background(),
			`DROP TRIGGER fail_normal_claims ON notifications; DROP FUNCTION fail_normal_claims();`)
	})

	got, err := s.ClaimDue(ctx, domain.ChannelSMS, 10, time.Minute)
	require.Error(t, err, "the failure is reported")
	require.Len(t, got, 1, "high priority is still claimed and returned")
	assert.Equal(t, high.ID, got[0].ID)

	n, err := s.GetNotification(ctx, normal.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusPending, n.Status, "the failed priority is rolled back")
	assert.Equal(t, 0, n.Attempts)
}

// Regression (review 3): with "id" as a tie-break the claim sorted every
// due row sharing a timestamp. The plan must read the index in order.
func TestClaimDoesNotSortTiedBacklog(t *testing.T) {
	env := testutil.Setup(t)
	ctx := context.Background()
	_, err := env.Pool.Exec(ctx, `
		INSERT INTO notifications (id, recipient, channel, content, priority, status, max_attempts,
		                           next_attempt_at, correlation_id)
		SELECT gen_random_uuid(), '+905551234567', 'sms', 'hi', 2, 'pending', 5,
		       '2026-01-01T09:00:00Z', 'c'
		FROM generate_series(1, 50000)`)
	require.NoError(t, err)
	_, err = env.Pool.Exec(ctx, `ANALYZE notifications`)
	require.NoError(t, err)

	rows, err := env.Pool.Query(ctx, `EXPLAIN `+store.ClaimSQL+`n.id`, "sms", 25, 30.0, 2)
	require.NoError(t, err)
	var lines []string
	for rows.Next() {
		var l string
		require.NoError(t, rows.Scan(&l))
		lines = append(lines, l)
	}
	require.NoError(t, rows.Err())
	full := strings.Join(lines, "\n")
	assert.NotContains(t, full, "Sort", full)
	assert.Contains(t, full, "idx_notifications_dispatch", full)

	start := time.Now()
	got, err := store.New(env.Pool).ClaimDue(ctx, domain.ChannelSMS, 25, time.Minute)
	require.NoError(t, err)
	assert.Len(t, got, 25)
	t.Logf("claim of 25 from 50k tied rows took %s", time.Since(start))
}

func TestReleaseClaimsRefundsTheAttempt(t *testing.T) {
	s := store.New(testutil.Setup(t).Pool)
	ctx := context.Background()
	n := newNotification(domain.ChannelSMS, domain.PriorityNormal)
	_, err := s.CreateNotification(ctx, n)
	require.NoError(t, err)

	claimed, err := s.ClaimDue(ctx, domain.ChannelSMS, 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.NoError(t, s.ReleaseClaims(ctx, claimed))

	got, err := s.GetNotification(ctx, n.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusPending, got.Status)
	assert.Equal(t, 0, got.Attempts, "releasing an unsent claim does not spend an attempt")

	again, err := s.ClaimDue(ctx, domain.ChannelSMS, 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, again, 1)
	assert.Equal(t, 1, again[0].Attempts)
}

func TestReapFailsRowsWithoutAttemptsLeft(t *testing.T) {
	s := store.New(testutil.Setup(t).Pool)
	ctx := context.Background()
	n := newNotification(domain.ChannelSMS, domain.PriorityNormal)
	n.MaxAttempts = 1
	_, err := s.CreateNotification(ctx, n)
	require.NoError(t, err)
	_, err = s.ClaimDue(ctx, domain.ChannelSMS, 1, time.Millisecond)
	require.NoError(t, err)
	time.Sleep(20 * time.Millisecond)

	reaped, err := s.ReapExpiredLeases(ctx)
	require.NoError(t, err)
	require.Len(t, reaped, 1)
	assert.Equal(t, domain.StatusFailed, reaped[0].Status)
}

func TestCancelOnlyBeforeDelivery(t *testing.T) {
	s := store.New(testutil.Setup(t).Pool)
	ctx := context.Background()

	pending := newNotification(domain.ChannelSMS, domain.PriorityNormal)
	processing := newNotification(domain.ChannelEmail, domain.PriorityNormal)
	_, err := s.CreateBatch(ctx, []domain.Notification{pending, processing})
	require.NoError(t, err)
	_, err = s.ClaimDue(ctx, domain.ChannelEmail, 1, time.Minute)
	require.NoError(t, err)

	got, err := s.CancelNotification(ctx, pending.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusCancelled, got.Status)

	got, err = s.CancelNotification(ctx, processing.ID)
	assert.ErrorIs(t, err, store.ErrInvalidState)
	assert.Equal(t, domain.StatusProcessing, got.Status)

	_, err = s.CancelNotification(ctx, uuid.New())
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestRequeueFailedGrantsFreshAttempts(t *testing.T) {
	s := store.New(testutil.Setup(t).Pool)
	ctx := context.Background()
	n := newNotification(domain.ChannelSMS, domain.PriorityNormal)
	_, err := s.CreateNotification(ctx, n)
	require.NoError(t, err)
	_, err = s.ClaimDue(ctx, domain.ChannelSMS, 1, time.Minute)
	require.NoError(t, err)
	_, err = s.CompleteAttempt(ctx, store.AttemptResult{
		NotificationID: n.ID, AttemptNumber: 1, Channel: domain.ChannelSMS,
		Outcome: domain.OutcomePermanent, NewStatus: domain.StatusFailed, Error: ptr("HTTP 400"),
	})
	require.NoError(t, err)

	got, err := s.RequeueFailed(ctx, n.ID, 3)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusPending, got.Status)
	assert.Equal(t, 4, got.MaxAttempts, "1 used + 3 fresh")
	assert.Nil(t, got.LastError)

	_, err = s.RequeueFailed(ctx, n.ID, 3)
	assert.ErrorIs(t, err, store.ErrInvalidState, "only failed notifications can be re-queued")
}

func TestListPaginationAndFilters(t *testing.T) {
	s := store.New(testutil.Setup(t).Pool)
	ctx := context.Background()

	for i := range 25 {
		ch := domain.ChannelSMS
		if i%5 == 0 {
			ch = domain.ChannelEmail
		}
		_, err := s.CreateNotification(ctx, newNotification(ch, domain.PriorityNormal))
		require.NoError(t, err)
	}

	var all []domain.Notification
	f := store.ListFilter{Limit: 7}
	for page := 0; ; page++ {
		items, next, err := s.ListNotifications(ctx, f)
		require.NoError(t, err)
		all = append(all, items...)
		if next == nil {
			break
		}
		f.After = next
		require.Less(t, page, 10)
	}
	assert.Len(t, all, 25)
	unique := map[uuid.UUID]bool{}
	for i, n := range all {
		unique[n.ID] = true
		if i > 0 {
			assert.False(t, n.CreatedAt.After(all[i-1].CreatedAt), "newest first")
		}
	}
	assert.Len(t, unique, 25, "no row repeated across pages")

	email := domain.ChannelEmail
	items, _, err := s.ListNotifications(ctx, store.ListFilter{Channel: &email, Limit: 50})
	require.NoError(t, err)
	assert.Len(t, items, 5)

	future := time.Now().Add(time.Hour)
	items, _, err = s.ListNotifications(ctx, store.ListFilter{From: &future, Limit: 50})
	require.NoError(t, err)
	assert.Empty(t, items)
}

func TestBatchSummary(t *testing.T) {
	s := store.New(testutil.Setup(t).Pool)
	ctx := context.Background()
	batchID := uuid.New()
	items := make([]domain.Notification, 3)
	for i := range items {
		items[i] = newNotification(domain.ChannelSMS, domain.PriorityNormal)
		items[i].BatchID = &batchID
	}
	_, err := s.CreateBatch(ctx, items)
	require.NoError(t, err)
	_, err = s.CancelNotification(ctx, items[0].ID)
	require.NoError(t, err)

	counts, total, err := s.BatchSummary(ctx, batchID)
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Equal(t, 2, counts[domain.StatusPending])
	assert.Equal(t, 1, counts[domain.StatusCancelled])

	_, _, err = s.BatchSummary(ctx, uuid.New())
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestIdempotencyMismatchIncludesScheduleAndTemplate(t *testing.T) {
	s := store.New(testutil.Setup(t).Pool)
	ctx := context.Background()
	at := time.Now().Add(time.Hour)

	n := newNotification(domain.ChannelSMS, domain.PriorityNormal)
	n.IdempotencyKey = ptr("reminder-1")
	n.ScheduledAt = &at
	_, err := s.CreateNotification(ctx, n)
	require.NoError(t, err)

	same := n
	same.ID = uuid.New()
	res, err := s.CreateNotification(ctx, same)
	require.NoError(t, err, "nanosecond vs microsecond precision must still match")
	assert.False(t, res.Created)

	now := n
	now.ID = uuid.New()
	now.ScheduledAt = nil // "send now" instead of "in an hour"
	_, err = s.CreateNotification(ctx, now)
	assert.ErrorIs(t, err, store.ErrIdempotencyMismatch)
}

func TestConcurrentBatchesWithOverlappingKeysDoNotDeadlock(t *testing.T) {
	s := store.New(testutil.Setup(t).Pool)
	ctx := context.Background()

	batch := func(keys ...string) []domain.Notification {
		out := make([]domain.Notification, len(keys))
		for i, k := range keys {
			out[i] = newNotification(domain.ChannelSMS, domain.PriorityNormal)
			out[i].IdempotencyKey = ptr(k)
		}
		return out
	}
	var wg sync.WaitGroup
	for i := range 10 {
		keys := []string{"k1", "k2", "k3", "k4"}
		if i%2 == 1 {
			keys = []string{"k4", "k3", "k2", "k1"} // opposite order
		}
		wg.Go(func() {
			_, err := s.CreateBatch(ctx, batch(keys...))
			assert.NoError(t, err)
		})
	}
	wg.Wait()
	items, _, err := s.ListNotifications(ctx, store.ListFilter{Limit: 50})
	require.NoError(t, err)
	assert.Len(t, items, 4)
}
