// Package service holds the application use cases behind the HTTP API:
// validating requests, rendering templates, enforcing idempotency and
// emitting events. Handlers only translate HTTP to and from these calls.
package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/atakanakis/notification-engine/internal/domain"
	"github.com/atakanakis/notification-engine/internal/events"
	"github.com/atakanakis/notification-engine/internal/observability"
	"github.com/atakanakis/notification-engine/internal/store"
)

type Service struct {
	store       *store.Store
	publisher   events.Publisher
	maxAttempts int
	now         func() time.Time
}

func New(s *store.Store, pub events.Publisher, maxAttempts int) *Service {
	return &Service{store: s, publisher: pub, maxAttempts: maxAttempts, now: time.Now}
}

// CreateInput is one notification as sent by an API client.
type CreateInput struct {
	Recipient      string            `json:"recipient"`
	Channel        domain.Channel    `json:"channel"`
	Content        string            `json:"content,omitempty"`
	Priority       domain.Priority   `json:"priority,omitempty"`
	IdempotencyKey *string           `json:"idempotencyKey,omitempty"`
	ScheduledAt    *time.Time        `json:"scheduledAt,omitempty"`
	TemplateID     *uuid.UUID        `json:"templateId,omitempty"`
	Variables      map[string]string `json:"variables,omitempty"`
}

// RequestMeta carries per-request context that is stored with notifications.
type RequestMeta struct {
	CorrelationID string
	TraceParent   string
}

// Create validates and enqueues one notification. created=false means an
// earlier request with the same idempotency key was returned instead.
func (s *Service) Create(ctx context.Context, in CreateInput, meta RequestMeta) (domain.Notification, bool, error) {
	var verr domain.ValidationError
	n := s.prepare(ctx, in, "", nil, meta, &verr, newRenderState())
	if err := verr.Err(); err != nil {
		return domain.Notification{}, false, err
	}
	res, err := s.store.CreateNotification(ctx, n)
	if err != nil {
		return domain.Notification{}, false, err
	}
	if res.Created {
		observability.NotificationsCreated.WithLabelValues(string(n.Channel), string(n.Priority)).Inc()
		s.publisher.Publish(ctx, domain.EventFor(res.Notification))
	}
	return res.Notification, res.Created, nil
}

type BatchResult struct {
	// BatchID groups the notifications this request created. Replayed items
	// (idempotency key seen before) keep the batch they were created in. If
	// nothing new was created, BatchID is the original batch when every item
	// came from the same one (a client re-sending a whole batch), else nil.
	BatchID *uuid.UUID
	Items   []store.CreateResult
}

// CreateBatch validates every item first and only then inserts all of them in
// one transaction. A single invalid item rejects the whole batch, with the
// error pointing at its index, so a client never has to work out which half
// of a batch went through.
func (s *Service) CreateBatch(ctx context.Context, inputs []CreateInput, meta RequestMeta) (BatchResult, error) {
	var verr domain.ValidationError
	if len(inputs) == 0 {
		verr.Add("notifications", "must contain at least one notification")
		return BatchResult{}, &verr
	}
	if len(inputs) > domain.MaxBatchSize {
		verr.Add("notifications", "contains %d notifications, the maximum is %d", len(inputs), domain.MaxBatchSize)
		return BatchResult{}, &verr
	}
	batchID := uuid.New()
	templates := newRenderState()
	seenKeys := map[string]int{}
	items := make([]domain.Notification, len(inputs))
	for i, in := range inputs {
		if verr.Full() {
			// Enough to act on; validating the rest would only cost CPU.
			verr.Omitted += len(inputs) - i
			break
		}
		prefix := fmt.Sprintf("notifications[%d].", i)
		if in.IdempotencyKey != nil {
			if j, dup := seenKeys[*in.IdempotencyKey]; dup {
				verr.Add(prefix+"idempotencyKey", "duplicates notifications[%d].idempotencyKey", j)
			}
			seenKeys[*in.IdempotencyKey] = i
		}
		items[i] = s.prepare(ctx, in, prefix, &batchID, meta, &verr, templates)
	}
	if err := verr.Err(); err != nil {
		return BatchResult{}, err
	}
	results, err := s.store.CreateBatch(ctx, items)
	if err != nil {
		return BatchResult{}, err
	}
	created := 0
	var events []domain.StatusEvent
	for _, r := range results {
		if r.Created {
			created++
			observability.NotificationsCreated.WithLabelValues(string(r.Notification.Channel), string(r.Notification.Priority)).Inc()
			events = append(events, domain.EventFor(r.Notification))
		}
	}
	// Up to 1000 "pending" events: publish them without holding the response.
	go func() {
		bg := context.WithoutCancel(ctx)
		for _, ev := range events {
			s.publisher.Publish(bg, ev)
		}
	}()
	if created > 0 {
		return BatchResult{BatchID: &batchID, Items: results}, nil
	}
	return BatchResult{BatchID: commonBatch(results), Items: results}, nil
}

func commonBatch(results []store.CreateResult) *uuid.UUID {
	first := results[0].Notification.BatchID
	if first == nil {
		return nil
	}
	for _, r := range results[1:] {
		if b := r.Notification.BatchID; b == nil || *b != *first {
			return nil
		}
	}
	return first
}

// prepare turns client input into a notification ready to insert, recording
// every validation problem under the given field prefix.
func (s *Service) prepare(ctx context.Context, in CreateInput, prefix string, batchID *uuid.UUID,
	meta RequestMeta, verr *domain.ValidationError, templates *renderState) domain.Notification {
	field := func(name string) string { return prefix + name }
	local := domain.ValidationError{}
	now := s.now()

	if in.Priority == "" {
		in.Priority = domain.PriorityNormal
	}
	if !in.Priority.Valid() {
		local.Add("priority", "must be one of high, normal, low")
	}
	channelOK := in.Channel.Valid()
	if !channelOK {
		local.Add("channel", "must be one of sms, email, push")
	}

	content := in.Content
	contentKnown := true // false when a template could not be loaded or rendered
	switch {
	case in.TemplateID != nil && in.Content != "":
		local.Add("content", "must be empty when templateId is set")
	case in.TemplateID != nil:
		tpl, err := s.template(ctx, *in.TemplateID, templates)
		switch {
		case errors.Is(err, store.ErrNotFound):
			contentKnown = false
			local.Add("templateId", "template %s does not exist", in.TemplateID)
		case err != nil:
			contentKnown = false
			local.Add("templateId", "could not load template: %v", err)
		default:
			if channelOK && tpl.Channel != in.Channel {
				local.Add("templateId", "template is for channel %s, not %s", tpl.Channel, in.Channel)
			}
			rendered, err := tpl.compiled.Render(in.Variables, domain.MaxContentLength[tpl.Channel])
			switch {
			case err != nil:
				contentKnown = false
				local.Add("variables", "%v", err)
			case templates.renderedBytes+len(rendered) > domain.MaxBatchRenderedBytes:
				// Stop keeping rendered content once over budget, so a
				// rejected request cannot hold more than the budget in memory.
				contentKnown = false
				local.Add("variables", "the request's rendered content exceeds %d MiB; send fewer or shorter notifications per request",
					domain.MaxBatchRenderedBytes>>20)
			default:
				templates.renderedBytes += len(rendered)
				content = rendered
			}
		}
	}

	if channelOK {
		domain.ValidateRecipient(in.Channel, in.Recipient, &local)
		if contentKnown {
			domain.ValidateContent(in.Channel, content, &local)
		}
	}
	domain.ValidateSchedule(in.ScheduledAt, now, &local)
	domain.ValidateIdempotencyKey(in.IdempotencyKey, &local)

	for _, f := range local.Fields {
		verr.Add(field(f.Field), "%s", f.Message)
	}

	var scheduled *time.Time
	if in.ScheduledAt != nil {
		at := in.ScheduledAt.UTC()
		scheduled = &at
	}
	return domain.Notification{
		ID:             uuid.New(),
		BatchID:        batchID,
		IdempotencyKey: in.IdempotencyKey,
		Recipient:      strings.TrimSpace(in.Recipient),
		Channel:        in.Channel,
		Content:        content,
		Priority:       in.Priority,
		Status:         domain.StatusPending,
		TemplateID:     in.TemplateID,
		MaxAttempts:    s.maxAttempts,
		ScheduledAt:    scheduled, // next_attempt_at is derived from it by the database
		CorrelationID:  meta.CorrelationID,
		TraceParent:    meta.TraceParent,
	}
}

// renderState is per request: each template is loaded and compiled once,
// and the total rendered content is counted against a budget.
type renderState struct {
	templates     map[uuid.UUID]*loadedTemplate
	renderedBytes int
}

type loadedTemplate struct {
	domain.Template
	compiled *domain.CompiledTemplate
}

func newRenderState() *renderState {
	return &renderState{templates: map[uuid.UUID]*loadedTemplate{}}
}

func (s *Service) template(ctx context.Context, id uuid.UUID, rs *renderState) (*loadedTemplate, error) {
	if t, ok := rs.templates[id]; ok {
		return t, nil
	}
	t, err := s.store.GetTemplate(ctx, id)
	if err != nil {
		return nil, err
	}
	c, err := domain.CompileTemplate(t.Body)
	if err != nil {
		return nil, fmt.Errorf("stored template is invalid: %w", err)
	}
	lt := &loadedTemplate{Template: t, compiled: c}
	rs.templates[id] = lt
	return lt, nil
}

type NotificationDetail struct {
	domain.Notification
	AttemptHistory []domain.Attempt `json:"attemptHistory"`
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (NotificationDetail, error) {
	n, err := s.store.GetNotification(ctx, id)
	if err != nil {
		return NotificationDetail{}, err
	}
	attempts, err := s.store.ListAttempts(ctx, id)
	if err != nil {
		return NotificationDetail{}, err
	}
	return NotificationDetail{Notification: n, AttemptHistory: attempts}, nil
}

func (s *Service) List(ctx context.Context, f store.ListFilter) ([]domain.Notification, *store.Cursor, error) {
	return s.store.ListNotifications(ctx, f)
}

func (s *Service) Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	n, err := s.store.CancelNotification(ctx, id)
	if err == nil {
		s.publisher.Publish(ctx, domain.EventFor(n))
	}
	return n, err
}

// Retry moves a failed (dead-lettered) notification back into the queue with
// a fresh set of attempts.
func (s *Service) Retry(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	n, err := s.store.RequeueFailed(ctx, id, s.maxAttempts)
	if err == nil {
		s.publisher.Publish(ctx, domain.EventFor(n))
	}
	return n, err
}

type BatchSummary struct {
	BatchID  uuid.UUID             `json:"batchId"`
	Total    int                   `json:"total"`
	Counts   map[domain.Status]int `json:"counts"`
	Finished bool                  `json:"finished"` // every notification is in a final state
}

func (s *Service) Batch(ctx context.Context, id uuid.UUID) (BatchSummary, error) {
	counts, total, err := s.store.BatchSummary(ctx, id)
	if err != nil {
		return BatchSummary{}, err
	}
	full := map[domain.Status]int{}
	finished := 0
	for _, st := range domain.Statuses {
		full[st] = counts[st]
		if st.Terminal() {
			finished += counts[st]
		}
	}
	return BatchSummary{BatchID: id, Total: total, Counts: full, Finished: finished == total}, nil
}

func (s *Service) CancelBatch(ctx context.Context, id uuid.UUID) (int, error) {
	if _, _, err := s.store.BatchSummary(ctx, id); err != nil {
		return 0, err
	}
	cancelled, err := s.store.CancelBatch(ctx, id)
	if err != nil {
		return 0, err
	}
	for _, n := range cancelled {
		s.publisher.Publish(ctx, domain.EventFor(n))
	}
	return len(cancelled), nil
}

type TemplateInput struct {
	Name    string         `json:"name"`
	Channel domain.Channel `json:"channel"`
	Body    string         `json:"body"`
}

func (s *Service) CreateTemplate(ctx context.Context, in TemplateInput) (domain.Template, error) {
	var verr domain.ValidationError
	if n := len(strings.TrimSpace(in.Name)); n == 0 || n > 100 || domain.HasNUL(in.Name) {
		verr.Add("name", "must be 1-100 characters without NUL")
	}
	if !in.Channel.Valid() {
		verr.Add("channel", "must be one of sms, email, push")
	}
	switch {
	case strings.TrimSpace(in.Body) == "" || domain.HasNUL(in.Body):
		verr.Add("body", "is required and must not contain NUL characters")
	case in.Channel.Valid() && utf8.RuneCountInString(in.Body) > domain.MaxContentLength[in.Channel]:
		verr.Add("body", "is longer than the %s limit of %d characters", in.Channel, domain.MaxContentLength[in.Channel])
	default:
		if _, err := domain.CompileTemplate(in.Body); err != nil {
			verr.Add("body", "is not a valid template: %v", err)
		}
	}
	if err := verr.Err(); err != nil {
		return domain.Template{}, err
	}
	return s.store.CreateTemplate(ctx, domain.Template{
		ID: uuid.New(), Name: strings.TrimSpace(in.Name), Channel: in.Channel, Body: in.Body,
	})
}

func (s *Service) GetTemplate(ctx context.Context, id uuid.UUID) (domain.Template, error) {
	return s.store.GetTemplate(ctx, id)
}

func (s *Service) ListTemplates(ctx context.Context, after string, limit int) ([]store.TemplateSummary, error) {
	return s.store.ListTemplates(ctx, after, limit)
}

// MetricsSnapshot is the human-friendly real-time view served on /v1/metrics.
type MetricsSnapshot struct {
	GeneratedAt time.Time              `json:"generatedAt"`
	Window      string                 `json:"window"`
	Queue       []store.QueueDepth     `json:"queue"`
	InFlight    map[domain.Channel]int `json:"inFlight"`
	Channels    []store.ChannelStats   `json:"channels"`
}

func (s *Service) Metrics(ctx context.Context, window time.Duration) (MetricsSnapshot, error) {
	depth, err := s.store.QueueDepth(ctx)
	if err != nil {
		return MetricsSnapshot{}, err
	}
	inflight, err := s.store.CountByStatus(ctx, domain.StatusProcessing)
	if err != nil {
		return MetricsSnapshot{}, err
	}
	stats, err := s.store.Stats(ctx, window)
	if err != nil {
		return MetricsSnapshot{}, err
	}
	if depth == nil {
		depth = []store.QueueDepth{}
	}
	return MetricsSnapshot{
		GeneratedAt: s.now().UTC(), Window: window.String(),
		Queue: depth, InFlight: inflight, Channels: stats,
	}, nil
}

func (s *Service) Ready(ctx context.Context) error { return s.store.Ping(ctx) }
