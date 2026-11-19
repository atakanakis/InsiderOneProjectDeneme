package store_test

import (
	"context"
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
