// Package events broadcasts notification status changes over Redis Pub/Sub.
//
// Workers and the API publish; every API instance subscribes and forwards
// matching events to its WebSocket clients. Pub/Sub is fire-and-forget: an
// event missed while a client is disconnected is not replayed, which is fine
// because the database is the source of truth and clients can always GET the
// current status.
package events

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/redis/go-redis/v9"

	"github.com/atakanakis/notification-engine/internal/domain"
)

const Channel = "notification-events"

type Publisher interface {
	Publish(ctx context.Context, ev domain.StatusEvent)
}

type RedisPublisher struct {
	rdb *redis.Client
	log *slog.Logger
}

func NewRedisPublisher(rdb *redis.Client, log *slog.Logger) *RedisPublisher {
	return &RedisPublisher{rdb: rdb, log: log}
}

// Publish never fails the caller: a lost real-time update must not turn a
// successful delivery into an error.
func (p *RedisPublisher) Publish(ctx context.Context, ev domain.StatusEvent) {
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	if err := p.rdb.Publish(ctx, Channel, b).Err(); err != nil {
		p.log.WarnContext(ctx, "publish status event failed", "error", err, "notification_id", ev.NotificationID)
	}
}

// Subscribe delivers every event to fn until ctx is cancelled.
func Subscribe(ctx context.Context, rdb *redis.Client, log *slog.Logger, fn func(domain.StatusEvent)) {
	sub := rdb.Subscribe(ctx, Channel)
	defer sub.Close()
	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var ev domain.StatusEvent
			if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
				log.Warn("bad status event payload", "error", err)
				continue
			}
			fn(ev)
		}
	}
}

// Nop is used in tests and when Redis is not wanted.
type Nop struct{}

func (Nop) Publish(context.Context, domain.StatusEvent) {}
