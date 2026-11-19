package api

import (
	"context"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/google/uuid"

	"github.com/atakanakis/notification-engine/internal/domain"
)

// Hub fans status events out to connected WebSocket clients. Each API
// instance has one Hub fed by a single Redis subscription.
type Hub struct {
	mu      sync.RWMutex
	clients map[*wsClient]struct{}
}

type wsClient struct {
	notificationID *uuid.UUID
	batchID        *uuid.UUID
	send           chan domain.StatusEvent
}

func NewHub() *Hub { return &Hub{clients: map[*wsClient]struct{}{}} }

func (c *wsClient) wants(ev domain.StatusEvent) bool {
	if c.notificationID != nil && *c.notificationID != ev.NotificationID {
		return false
	}
	if c.batchID != nil && (ev.BatchID == nil || *c.batchID != *ev.BatchID) {
		return false
	}
	return true
}

// Broadcast never blocks: a client that cannot keep up loses events rather
// than slowing everyone else down. It can always GET the current status.
func (h *Hub) Broadcast(ev domain.StatusEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		if !c.wants(ev) {
			continue
		}
		select {
		case c.send <- ev:
		default:
		}
	}
}

func (h *Hub) add(c *wsClient) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *Hub) remove(c *wsClient) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
}

// GET /v1/ws?notificationId=...&batchId=...
// Streams StatusEvent JSON messages. Without filters every event is sent,
// which is handy for an internal live dashboard.
func (s *Server) websocket(w http.ResponseWriter, r *http.Request) {
	c := &wsClient{send: make(chan domain.StatusEvent, 256)}
	for param, dst := range map[string]**uuid.UUID{"notificationId": &c.notificationID, "batchId": &c.batchID} {
		if v := r.URL.Query().Get(param); v != "" {
			id, err := uuid.Parse(v)
			if err != nil {
				writeError(w, http.StatusBadRequest, "invalid_"+param, param+" must be a UUID")
				return
			}
			*dst = &id
		}
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:     s.opts.AllowedOrigins,
		InsecureSkipVerify: slices.Contains(s.opts.AllowedOrigins, "*"),
	})
	if err != nil {
		return
	}
	defer func() { _ = conn.CloseNow() }()

	s.hub.add(c)
	defer s.hub.remove(c)

	// CloseRead handles pings/close frames and cancels ctx when the client leaves.
	ctx := conn.CloseRead(r.Context())
	keepAlive := time.NewTicker(30 * time.Second)
	defer keepAlive.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-c.send:
			wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := wsjson.Write(wctx, conn, ev)
			cancel()
			if err != nil {
				return
			}
		case <-keepAlive.C:
			pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := conn.Ping(pctx)
			cancel()
			if err != nil {
				return
			}
		}
	}
}
