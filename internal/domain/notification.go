// Package domain holds the core types of the notification system and the
// rules that do not depend on any infrastructure (validation, status
// transitions, template rendering).
package domain

import (
	"time"

	"github.com/google/uuid"
)

// Channel is the delivery medium of a notification.
type Channel string

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// Channels lists every supported channel. Workers run one dispatcher per entry.
var Channels = []Channel{ChannelSMS, ChannelEmail, ChannelPush}

func (c Channel) Valid() bool {
	switch c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return true
	}
	return false
}

// Priority decides the order in which ready notifications are picked up.
type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

var Priorities = []Priority{PriorityHigh, PriorityNormal, PriorityLow}

func (p Priority) Valid() bool {
	switch p {
	case PriorityHigh, PriorityNormal, PriorityLow:
		return true
	}
	return false
}

// Rank is the value stored in the database. Lower ranks are dispatched first,
// so "ORDER BY priority" yields high -> normal -> low.
func (p Priority) Rank() int16 {
	switch p {
	case PriorityHigh:
		return 0
	case PriorityLow:
		return 2
	default:
		return 1
	}
}

func PriorityFromRank(r int16) Priority {
	switch r {
	case 0:
		return PriorityHigh
	case 2:
		return PriorityLow
	default:
		return PriorityNormal
	}
}

// Status is the lifecycle state of a notification.
//
//	pending ──claim──▶ processing ──2xx──────────────▶ sent
//	   ▲                   │
//	   │                   ├─retryable, attempts left─▶ retrying ──due──▶ processing
//	   │                   └─permanent / exhausted────▶ failed (dead letter)
//	   │                                                  │
//	   └────────────── manual retry (POST /retry) ────────┘
//
//	pending | retrying ──cancel──▶ cancelled
type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusRetrying   Status = "retrying"
	StatusSent       Status = "sent"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

var Statuses = []Status{StatusPending, StatusProcessing, StatusRetrying, StatusSent, StatusFailed, StatusCancelled}

func (s Status) Valid() bool {
	for _, v := range Statuses {
		if s == v {
			return true
		}
	}
	return false
}

// Cancellable reports whether a notification in this status can still be
// stopped. Once a worker has claimed it (processing) the request may already
// be on the wire, so cancelling would be a lie.
func (s Status) Cancellable() bool {
	return s == StatusPending || s == StatusRetrying
}

// Terminal reports whether no further automatic transition will happen.
func (s Status) Terminal() bool {
	return s == StatusSent || s == StatusFailed || s == StatusCancelled
}

// Notification is a single message to a single recipient on a single channel.
type Notification struct {
	ID                uuid.UUID  `json:"id"`
	BatchID           *uuid.UUID `json:"batchId,omitempty"`
	IdempotencyKey    *string    `json:"idempotencyKey,omitempty"`
	Recipient         string     `json:"recipient"`
	Channel           Channel    `json:"channel"`
	Content           string     `json:"content"`
	Priority          Priority   `json:"priority"`
	Status            Status     `json:"status"`
	TemplateID        *uuid.UUID `json:"templateId,omitempty"`
	Attempts          int        `json:"attempts"`
	MaxAttempts       int        `json:"maxAttempts"`
	ScheduledAt       *time.Time `json:"scheduledAt,omitempty"`
	NextAttemptAt     time.Time  `json:"nextAttemptAt"`
	ProviderMessageID *string    `json:"providerMessageId,omitempty"`
	LastError         *string    `json:"lastError,omitempty"`
	CorrelationID     string     `json:"correlationId,omitempty"`
	CreatedAt         time.Time  `json:"createdAt"`
	UpdatedAt         time.Time  `json:"updatedAt"`
	SentAt            *time.Time `json:"sentAt,omitempty"`

	// TraceParent is the W3C trace context captured when the notification was
	// accepted, so the asynchronous delivery shows up in the same trace.
	TraceParent string `json:"-"`
}

// Attempt is one call to the external provider.
type Attempt struct {
	ID             int64     `json:"id"`
	NotificationID uuid.UUID `json:"-"`
	AttemptNumber  int       `json:"attemptNumber"`
	Outcome        Outcome   `json:"outcome"`
	HTTPStatus     *int      `json:"httpStatus,omitempty"`
	Error          *string   `json:"error,omitempty"`
	DurationMS     int64     `json:"durationMs"`
	CreatedAt      time.Time `json:"createdAt"`
}

// Outcome is the classification of a single delivery attempt.
type Outcome string

const (
	// OutcomeDelivered: the provider accepted the message.
	OutcomeDelivered Outcome = "delivered"
	// OutcomeRetryable: a transient failure (5xx, 429, timeout, network).
	OutcomeRetryable Outcome = "retryable"
	// OutcomePermanent: retrying cannot help (most 4xx responses).
	OutcomePermanent Outcome = "permanent"
)

// Template is a reusable message body with {{.variable}} placeholders.
type Template struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Channel   Channel   `json:"channel"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"createdAt"`
}

// StatusEvent is published whenever a notification changes status. The API
// relays these to WebSocket subscribers.
type StatusEvent struct {
	NotificationID uuid.UUID  `json:"notificationId"`
	BatchID        *uuid.UUID `json:"batchId,omitempty"`
	Channel        Channel    `json:"channel"`
	Status         Status     `json:"status"`
	Attempts       int        `json:"attempts"`
	LastError      *string    `json:"lastError,omitempty"`
	At             time.Time  `json:"at"`
}

func EventFor(n Notification) StatusEvent {
	return StatusEvent{
		NotificationID: n.ID,
		BatchID:        n.BatchID,
		Channel:        n.Channel,
		Status:         n.Status,
		Attempts:       n.Attempts,
		LastError:      n.LastError,
		At:             n.UpdatedAt,
	}
}
