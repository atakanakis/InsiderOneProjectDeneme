package store

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/atakanakis/notification-engine/internal/domain"
)

// ClaimDue atomically takes up to limit ready notifications of one channel,
// most urgent first, and leases them to the caller.
//
// FOR UPDATE SKIP LOCKED is what makes this safe with many workers: rows
// another worker is claiming at the same moment are skipped instead of
// waited on, so two workers never receive the same notification.
func (s *Store) ClaimDue(ctx context.Context, ch domain.Channel, limit int, lease time.Duration) ([]domain.Notification, error) {
	rows, err := s.pool.Query(ctx, `
		WITH due AS (
			SELECT id FROM notifications
			WHERE channel = $1
			  AND status IN ('pending', 'retrying')
			  AND next_attempt_at <= now()
			ORDER BY priority, next_attempt_at, id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE notifications n
		SET status = 'processing',
		    attempts = n.attempts + 1,
		    locked_until = now() + make_interval(secs => $3),
		    updated_at = now()
		FROM due
		WHERE n.id = due.id
		RETURNING `+prefixed("n", notificationColumns),
		string(ch), limit, lease.Seconds())
	if err != nil {
		return nil, fmt.Errorf("claim due: %w", err)
	}
	items, err := collectNotifications(rows)
	if err != nil {
		return nil, err
	}
	// UPDATE ... RETURNING does not preserve the CTE order.
	slices.SortFunc(items, func(a, b domain.Notification) int {
		if d := int(a.Priority.Rank()) - int(b.Priority.Rank()); d != 0 {
			return d
		}
		return a.NextAttemptAt.Compare(b.NextAttemptAt)
	})
	return items, nil
}

// AttemptResult is what a worker reports after calling the provider.
type AttemptResult struct {
	NotificationID    uuid.UUID
	AttemptNumber     int // must equal notifications.attempts as returned by ClaimDue
	Channel           domain.Channel
	Outcome           domain.Outcome
	HTTPStatus        *int
	Error             *string
	Duration          time.Duration
	NewStatus         domain.Status // sent, retrying or failed
	RetryDelay        time.Duration // only for retrying; added to the database clock
	ProviderMessageID *string
}

// CompleteAttempt records the attempt and moves the notification to its next
// status. The status update only applies while the caller still holds the
// lease for exactly this attempt; if the lease expired and the row was
// handed to someone else, ErrStaleLease is returned and the newer owner's
// state wins. The attempt row is written either way: it really happened.
func (s *Store) CompleteAttempt(ctx context.Context, r AttemptResult) (domain.Notification, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Notification{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	if _, err := tx.Exec(ctx, `
		INSERT INTO notification_attempts
			(notification_id, attempt_number, channel, outcome, http_status, error, duration_ms)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		r.NotificationID, r.AttemptNumber, string(r.Channel), string(r.Outcome), r.HTTPStatus,
		r.Error, r.Duration.Milliseconds()); err != nil {
		return domain.Notification{}, fmt.Errorf("insert attempt: %w", err)
	}

	n, err := scanNotification(tx.QueryRow(ctx, `
		UPDATE notifications
		SET status = $3,
		    next_attempt_at = CASE WHEN $3 = 'retrying' THEN now() + make_interval(secs => $4) ELSE next_attempt_at END,
		    provider_message_id = COALESCE($5, provider_message_id),
		    last_error = $6,
		    sent_at = CASE WHEN $3 = 'sent' THEN now() ELSE sent_at END,
		    locked_until = NULL,
		    updated_at = now()
		WHERE id = $1 AND status = 'processing' AND attempts = $2
		RETURNING `+notificationColumns,
		r.NotificationID, r.AttemptNumber, string(r.NewStatus), r.RetryDelay.Seconds(),
		r.ProviderMessageID, r.Error))
	stale := err != nil
	if stale && !isNoRows(err) {
		return domain.Notification{}, fmt.Errorf("update notification: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Notification{}, err
	}
	if stale {
		return domain.Notification{}, ErrStaleLease
	}
	return n, nil
}

// ReapExpiredLeases returns notifications whose worker vanished (crash,
// deploy, network partition) back to the queue, or to failed if they have
// no attempts left.
func (s *Store) ReapExpiredLeases(ctx context.Context) ([]domain.Notification, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE notifications
		SET status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'retrying' END,
		    next_attempt_at = now(),
		    locked_until = NULL,
		    last_error = 'lease expired before the delivery result was recorded',
		    updated_at = now()
		WHERE id IN (
			SELECT id FROM notifications
			WHERE status = 'processing' AND locked_until < now()
			LIMIT 500
			FOR UPDATE SKIP LOCKED
		)
		RETURNING `+notificationColumns)
	if err != nil {
		return nil, err
	}
	return collectNotifications(rows)
}

// QueueDepth is the number of notifications waiting per channel and priority.
type QueueDepth struct {
	Channel   domain.Channel  `json:"channel"`
	Priority  domain.Priority `json:"priority"`
	Ready     int             `json:"ready"`     // due now, waiting for a worker
	Scheduled int             `json:"scheduled"` // waiting for scheduledAt or a retry backoff
}

func (s *Store) QueueDepth(ctx context.Context) ([]QueueDepth, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT channel, priority,
		       count(*) FILTER (WHERE next_attempt_at <= now()),
		       count(*) FILTER (WHERE next_attempt_at > now())
		FROM notifications
		WHERE status IN ('pending', 'retrying')
		GROUP BY channel, priority`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QueueDepth
	for rows.Next() {
		var d QueueDepth
		var ch string
		var prio int16
		if err := rows.Scan(&ch, &prio, &d.Ready, &d.Scheduled); err != nil {
			return nil, err
		}
		d.Channel = domain.Channel(ch)
		d.Priority = domain.PriorityFromRank(prio)
		out = append(out, d)
	}
	return out, rows.Err()
}

// CountByStatus is used for the in-flight number on /v1/metrics.
func (s *Store) CountByStatus(ctx context.Context, st domain.Status) (map[domain.Channel]int, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT channel, count(*) FROM notifications WHERE status = $1 GROUP BY channel`, string(st))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[domain.Channel]int{}
	for rows.Next() {
		var ch string
		var c int
		if err := rows.Scan(&ch, &c); err != nil {
			return nil, err
		}
		out[domain.Channel(ch)] = c
	}
	return out, rows.Err()
}

// ChannelStats summarises the last window of activity for one channel.
type ChannelStats struct {
	Channel            domain.Channel `json:"channel"`
	Delivered          int            `json:"delivered"`
	RetryableFailures  int            `json:"retryableFailures"`
	PermanentFailures  int            `json:"permanentFailures"`
	SuccessRate        float64        `json:"successRate"` // delivered / attempts
	ProviderLatencyP50 float64        `json:"providerLatencyP50Ms"`
	ProviderLatencyP95 float64        `json:"providerLatencyP95Ms"`
	EndToEndP50        float64        `json:"endToEndLatencyP50Ms"` // ready -> sent
	EndToEndP95        float64        `json:"endToEndLatencyP95Ms"`
}

func (s *Store) Stats(ctx context.Context, window time.Duration) ([]ChannelStats, error) {
	byChannel := map[domain.Channel]*ChannelStats{}
	get := func(ch domain.Channel) *ChannelStats {
		if st, ok := byChannel[ch]; ok {
			return st
		}
		st := &ChannelStats{Channel: ch}
		byChannel[ch] = st
		return st
	}
	secs := window.Seconds()

	rows, err := s.pool.Query(ctx, `
		SELECT channel,
		       count(*) FILTER (WHERE outcome = 'delivered'),
		       count(*) FILTER (WHERE outcome = 'retryable'),
		       count(*) FILTER (WHERE outcome = 'permanent'),
		       COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms), 0),
		       COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms), 0)
		FROM notification_attempts
		WHERE created_at > now() - make_interval(secs => $1)
		GROUP BY channel`, secs)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var ch string
		var d, rf, pf int
		var p50, p95 float64
		if err := rows.Scan(&ch, &d, &rf, &pf, &p50, &p95); err != nil {
			rows.Close()
			return nil, err
		}
		st := get(domain.Channel(ch))
		st.Delivered, st.RetryableFailures, st.PermanentFailures = d, rf, pf
		st.ProviderLatencyP50, st.ProviderLatencyP95 = p50, p95
		if total := d + rf + pf; total > 0 {
			st.SuccessRate = float64(d) / float64(total)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = s.pool.Query(ctx, `
		SELECT channel,
		       COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY ms), 0),
		       COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY ms), 0)
		FROM (
			SELECT channel,
			       extract(epoch FROM sent_at - GREATEST(created_at, COALESCE(scheduled_at, created_at))) * 1000 AS ms
			FROM notifications
			WHERE status = 'sent' AND sent_at > now() - make_interval(secs => $1)
		) t
		GROUP BY channel`, secs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ch string
		var p50, p95 float64
		if err := rows.Scan(&ch, &p50, &p95); err != nil {
			return nil, err
		}
		st := get(domain.Channel(ch))
		st.EndToEndP50, st.EndToEndP95 = p50, p95
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]ChannelStats, 0, len(domain.Channels))
	for _, ch := range domain.Channels {
		out = append(out, *get(ch))
	}
	return out, nil
}
