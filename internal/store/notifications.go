package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/atakanakis/notification-engine/internal/domain"
)

const notificationColumns = `id, batch_id, idempotency_key, recipient, channel, content, priority, status,
	template_id, attempts, max_attempts, scheduled_at, next_attempt_at, provider_message_id,
	last_error, correlation_id, trace_parent, created_at, updated_at, sent_at`

func scanNotification(row pgx.Row) (domain.Notification, error) {
	var (
		n        domain.Notification
		channel  string
		status   string
		priority int16
	)
	err := row.Scan(&n.ID, &n.BatchID, &n.IdempotencyKey, &n.Recipient, &channel, &n.Content, &priority,
		&status, &n.TemplateID, &n.Attempts, &n.MaxAttempts, &n.ScheduledAt, &n.NextAttemptAt,
		&n.ProviderMessageID, &n.LastError, &n.CorrelationID, &n.TraceParent, &n.CreatedAt,
		&n.UpdatedAt, &n.SentAt)
	if err != nil {
		return n, err
	}
	n.Channel = domain.Channel(channel)
	n.Status = domain.Status(status)
	n.Priority = domain.PriorityFromRank(priority)
	return n, nil
}

func collectNotifications(rows pgx.Rows) ([]domain.Notification, error) {
	defer rows.Close()
	var out []domain.Notification
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// next_attempt_at comes from the database clock (later of now() and the
// schedule). Every queue time is compared against Postgres' now(), so using
// one clock avoids skew between API hosts and the database.
const insertSQL = `
INSERT INTO notifications (id, batch_id, idempotency_key, recipient, channel, content, priority, status,
	template_id, max_attempts, scheduled_at, next_attempt_at, correlation_id, trace_parent)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8, $9, $10, GREATEST(now(), COALESCE($10, now())), $11, $12)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING ` + notificationColumns

func insertArgs(n domain.Notification) []any {
	return []any{n.ID, n.BatchID, n.IdempotencyKey, n.Recipient, string(n.Channel), n.Content,
		n.Priority.Rank(), n.TemplateID, n.MaxAttempts, n.ScheduledAt, n.CorrelationID, n.TraceParent}
}

// CreateResult tells the caller whether a row was newly created or an earlier
// request with the same idempotency key was returned instead.
type CreateResult struct {
	Notification domain.Notification
	Created      bool
}

// CreateNotification inserts one notification. If its idempotency key was
// used before, the existing notification is returned (Created=false) as long
// as the payload matches; otherwise ErrIdempotencyMismatch.
func (s *Store) CreateNotification(ctx context.Context, n domain.Notification) (CreateResult, error) {
	created, err := scanNotification(s.pool.QueryRow(ctx, insertSQL, insertArgs(n)...))
	if err == nil {
		return CreateResult{Notification: created, Created: true}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return CreateResult{}, fmt.Errorf("insert notification: %w", err)
	}
	existing, err := s.resolveConflict(ctx, s.pool, n)
	if err != nil {
		return CreateResult{}, err
	}
	return CreateResult{Notification: existing, Created: false}, nil
}

// CreateBatch inserts all notifications in one transaction: either the whole
// batch is accepted or none of it is. Inserts are pipelined in a single
// round trip using pgx.Batch.
func (s *Store) CreateBatch(ctx context.Context, items []domain.Notification) ([]CreateResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	batch := &pgx.Batch{}
	for _, n := range items {
		batch.Queue(insertSQL, insertArgs(n)...)
	}
	results := make([]CreateResult, len(items))
	conflicts := make([]int, 0)
	br := tx.SendBatch(ctx, batch)
	for i := range items {
		created, err := scanNotification(br.QueryRow())
		switch {
		case err == nil:
			results[i] = CreateResult{Notification: created, Created: true}
		case errors.Is(err, pgx.ErrNoRows):
			conflicts = append(conflicts, i)
		default:
			_ = br.Close()
			return nil, fmt.Errorf("insert batch item %d: %w", i, err)
		}
	}
	if err := br.Close(); err != nil {
		return nil, fmt.Errorf("insert batch: %w", err)
	}
	for _, i := range conflicts {
		existing, err := s.resolveConflict(ctx, tx, items[i])
		if err != nil {
			return nil, fmt.Errorf("item %d: %w", i, err)
		}
		results[i] = CreateResult{Notification: existing, Created: false}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return results, nil
}

func (s *Store) resolveConflict(ctx context.Context, q querier, n domain.Notification) (domain.Notification, error) {
	if n.IdempotencyKey == nil {
		return domain.Notification{}, errors.New("insert returned no row without an idempotency key")
	}
	existing, err := scanNotification(q.QueryRow(ctx,
		`SELECT `+notificationColumns+` FROM notifications WHERE idempotency_key = $1`, *n.IdempotencyKey))
	if err != nil {
		return domain.Notification{}, fmt.Errorf("load idempotent notification: %w", err)
	}
	if existing.Recipient != n.Recipient || existing.Channel != n.Channel ||
		existing.Content != n.Content || existing.Priority != n.Priority {
		return domain.Notification{}, ErrIdempotencyMismatch
	}
	return existing, nil
}

func (s *Store) GetNotification(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	n, err := scanNotification(s.pool.QueryRow(ctx,
		`SELECT `+notificationColumns+` FROM notifications WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return n, ErrNotFound
	}
	return n, err
}

func (s *Store) ListAttempts(ctx context.Context, id uuid.UUID) ([]domain.Attempt, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, notification_id, attempt_number, outcome, http_status, error, duration_ms, created_at
		FROM notification_attempts WHERE notification_id = $1 ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.Attempt{}
	for rows.Next() {
		var a domain.Attempt
		var outcome string
		if err := rows.Scan(&a.ID, &a.NotificationID, &a.AttemptNumber, &outcome, &a.HTTPStatus,
			&a.Error, &a.DurationMS, &a.CreatedAt); err != nil {
			return nil, err
		}
		a.Outcome = domain.Outcome(outcome)
		out = append(out, a)
	}
	return out, rows.Err()
}

// Cursor marks a position in the newest-first listing. Keyset pagination
// ("give me rows older than this one") stays fast on deep pages, unlike
// OFFSET which has to skip every previous row.
type Cursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

type ListFilter struct {
	Status  *domain.Status
	Channel *domain.Channel
	BatchID *uuid.UUID
	From    *time.Time // inclusive, on created_at
	To      *time.Time // exclusive, on created_at
	After   *Cursor
	Limit   int
}

// ListNotifications returns up to Limit rows and, if there are more, the
// cursor for the next page.
func (s *Store) ListNotifications(ctx context.Context, f ListFilter) ([]domain.Notification, *Cursor, error) {
	var (
		where []string
		args  []any
	)
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if f.Status != nil {
		add("status = $%d", string(*f.Status))
	}
	if f.Channel != nil {
		add("channel = $%d", string(*f.Channel))
	}
	if f.BatchID != nil {
		add("batch_id = $%d", *f.BatchID)
	}
	if f.From != nil {
		add("created_at >= $%d", *f.From)
	}
	if f.To != nil {
		add("created_at < $%d", *f.To)
	}
	if f.After != nil {
		args = append(args, f.After.CreatedAt, f.After.ID)
		where = append(where, fmt.Sprintf("(created_at, id) < ($%d, $%d)", len(args)-1, len(args)))
	}
	query := `SELECT ` + notificationColumns + ` FROM notifications`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	args = append(args, f.Limit+1) // fetch one extra row to know if another page exists
	query += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}
	items, err := collectNotifications(rows)
	if err != nil {
		return nil, nil, err
	}
	if len(items) <= f.Limit {
		return items, nil, nil
	}
	items = items[:f.Limit]
	last := items[len(items)-1]
	return items, &Cursor{CreatedAt: last.CreatedAt, ID: last.ID}, nil
}

// CancelNotification moves a pending/retrying notification to cancelled.
// ErrInvalidState is returned (with the current row) when it is too late.
func (s *Store) CancelNotification(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	n, err := scanNotification(s.pool.QueryRow(ctx, `
		UPDATE notifications SET status = 'cancelled', updated_at = now()
		WHERE id = $1 AND status IN ('pending', 'retrying')
		RETURNING `+notificationColumns, id))
	if err == nil {
		return n, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return n, err
	}
	current, err := s.GetNotification(ctx, id)
	if err != nil {
		return current, err
	}
	return current, ErrInvalidState
}

// CancelBatch cancels every notification of a batch that has not started.
func (s *Store) CancelBatch(ctx context.Context, batchID uuid.UUID) ([]domain.Notification, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE notifications SET status = 'cancelled', updated_at = now()
		WHERE batch_id = $1 AND status IN ('pending', 'retrying')
		RETURNING `+notificationColumns, batchID)
	if err != nil {
		return nil, err
	}
	return collectNotifications(rows)
}

// RequeueFailed takes a notification out of the dead-letter state and grants
// it a fresh set of attempts. History in notification_attempts is kept.
func (s *Store) RequeueFailed(ctx context.Context, id uuid.UUID, extraAttempts int) (domain.Notification, error) {
	n, err := scanNotification(s.pool.QueryRow(ctx, `
		UPDATE notifications
		SET status = 'pending', max_attempts = attempts + $2, next_attempt_at = now(),
		    last_error = NULL, updated_at = now()
		WHERE id = $1 AND status = 'failed'
		RETURNING `+notificationColumns, id, extraAttempts))
	if err == nil {
		return n, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return n, err
	}
	current, err := s.GetNotification(ctx, id)
	if err != nil {
		return current, err
	}
	return current, ErrInvalidState
}

// BatchSummary counts the notifications of a batch per status.
func (s *Store) BatchSummary(ctx context.Context, batchID uuid.UUID) (map[domain.Status]int, int, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT status, count(*) FROM notifications WHERE batch_id = $1 GROUP BY status`, batchID)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	counts := map[domain.Status]int{}
	total := 0
	for rows.Next() {
		var st string
		var c int
		if err := rows.Scan(&st, &c); err != nil {
			return nil, 0, err
		}
		counts[domain.Status(st)] = c
		total += c
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	if total == 0 {
		return nil, 0, ErrNotFound
	}
	return counts, total, nil
}
