package api

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/atakanakis/notification-engine/internal/domain"
	"github.com/atakanakis/notification-engine/internal/observability"
	"github.com/atakanakis/notification-engine/internal/service"
	"github.com/atakanakis/notification-engine/internal/store"
)

func meta(r *http.Request) service.RequestMeta {
	return service.RequestMeta{
		CorrelationID: observability.CorrelationID(r.Context()),
		TraceParent:   observability.InjectTraceParent(r.Context()),
	}
}

// POST /v1/notifications
//
// Returns 202 Accepted: the notification is queued, not yet delivered.
// Repeating the request with the same idempotency key returns the original
// notification with 200 and "Idempotent-Replayed: true".
func (s *Server) createNotification(w http.ResponseWriter, r *http.Request) {
	var in service.CreateInput
	if !decodeJSON(w, r, maxSingleBody, &in) {
		return
	}
	if key := r.Header.Get("Idempotency-Key"); key != "" {
		if in.IdempotencyKey != nil && *in.IdempotencyKey != key {
			writeError(w, http.StatusBadRequest, "idempotency_key_mismatch",
				"Idempotency-Key header and idempotencyKey field differ")
			return
		}
		in.IdempotencyKey = &key
	}
	n, created, err := s.svc.Create(r.Context(), in, meta(r))
	if err != nil {
		writeServiceError(w, r, s.log, err)
		return
	}
	w.Header().Set("Location", "/v1/notifications/"+n.ID.String())
	if !created {
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusOK, n)
		return
	}
	writeJSON(w, http.StatusAccepted, n)
}

// Room for ":<index>" within domain.MaxIdempotencyKeyLength.
const maxBatchKeyLength = domain.MaxIdempotencyKeyLength - 5

type batchRequest struct {
	Notifications []service.CreateInput `json:"notifications"`
}

type batchItem struct {
	ID             uuid.UUID     `json:"id"`
	IdempotencyKey *string       `json:"idempotencyKey,omitempty"`
	Status         domain.Status `json:"status"`
	// Replayed is true when this item matched an earlier request's
	// idempotency key; ID then points at that earlier notification.
	Replayed bool `json:"replayed"`
}

type batchResponse struct {
	BatchID  *uuid.UUID  `json:"batchId"`
	Accepted int         `json:"accepted"`
	Replayed int         `json:"replayed"`
	Items    []batchItem `json:"items"`
}

// POST /v1/notifications/batch (up to 1000 items, all-or-nothing).
//
// An Idempotency-Key header covers the whole batch: items without their own
// key get "<header>:<index>", so re-sending the same batch after a timeout
// replays it instead of sending every message twice.
func (s *Server) createBatch(w http.ResponseWriter, r *http.Request) {
	var req batchRequest
	if !decodeJSON(w, r, maxBatchBody, &req) {
		return
	}
	if key := r.Header.Get("Idempotency-Key"); key != "" {
		if len(key) > maxBatchKeyLength {
			writeError(w, http.StatusBadRequest, "invalid_idempotency_key",
				fmt.Sprintf("a batch Idempotency-Key may be at most %d characters", maxBatchKeyLength))
			return
		}
		for i := range req.Notifications {
			if req.Notifications[i].IdempotencyKey == nil {
				k := fmt.Sprintf("%s:%d", key, i)
				req.Notifications[i].IdempotencyKey = &k
			}
		}
	}
	res, err := s.svc.CreateBatch(r.Context(), req.Notifications, meta(r))
	if err != nil {
		writeServiceError(w, r, s.log, err)
		return
	}
	out := batchResponse{BatchID: res.BatchID, Items: make([]batchItem, len(res.Items))}
	for i, it := range res.Items {
		out.Items[i] = batchItem{
			ID: it.Notification.ID, IdempotencyKey: it.Notification.IdempotencyKey,
			Status: it.Notification.Status, Replayed: !it.Created,
		}
		if it.Created {
			out.Accepted++
		} else {
			out.Replayed++
		}
	}
	if res.BatchID != nil {
		w.Header().Set("Location", "/v1/batches/"+res.BatchID.String())
	}
	writeJSON(w, http.StatusAccepted, out)
}

func (s *Server) getNotification(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}
	n, err := s.svc.Get(r.Context(), id)
	if err != nil {
		writeServiceError(w, r, s.log, err)
		return
	}
	writeJSON(w, http.StatusOK, n)
}

type listResponse struct {
	Items      []domain.Notification `json:"items"`
	NextCursor *string               `json:"nextCursor"`
}

// GET /v1/notifications?status=&channel=&batchId=&from=&to=&limit=&cursor=
func (s *Server) listNotifications(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var verr domain.ValidationError
	f := store.ListFilter{Limit: 50}

	if v := q.Get("status"); v != "" {
		st := domain.Status(v)
		if !st.Valid() {
			verr.Add("status", "unknown status %q", v)
		}
		f.Status = &st
	}
	if v := q.Get("channel"); v != "" {
		ch := domain.Channel(v)
		if !ch.Valid() {
			verr.Add("channel", "unknown channel %q", v)
		}
		f.Channel = &ch
	}
	if v := q.Get("batchId"); v != "" {
		id, err := uuid.Parse(v)
		if err != nil {
			verr.Add("batchId", "must be a UUID")
		}
		f.BatchID = &id
	}
	f.From = parseTime(q.Get("from"), "from", &verr)
	f.To = parseTime(q.Get("to"), "to", &verr)
	if f.From != nil && f.To != nil && f.From.After(*f.To) {
		verr.Add("from", "must not be after to")
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 200 {
			verr.Add("limit", "must be between 1 and 200")
		}
		f.Limit = n
	}
	if v := q.Get("cursor"); v != "" {
		c, err := decodeCursor(v)
		if err != nil {
			verr.Add("cursor", "is invalid")
		}
		f.After = c
	}
	if err := verr.Err(); err != nil {
		writeServiceError(w, r, s.log, err)
		return
	}

	items, next, err := s.svc.List(r.Context(), f)
	if err != nil {
		writeServiceError(w, r, s.log, err)
		return
	}
	resp := listResponse{Items: items}
	if resp.Items == nil {
		resp.Items = []domain.Notification{}
	}
	if next != nil {
		c := encodeCursor(*next)
		resp.NextCursor = &c
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) cancelNotification(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}
	n, err := s.svc.Cancel(r.Context(), id)
	if errors.Is(err, store.ErrInvalidState) {
		writeError(w, http.StatusConflict, "not_cancellable",
			fmt.Sprintf("notification is %s; only pending or retrying notifications can be cancelled", n.Status))
		return
	}
	if err != nil {
		writeServiceError(w, r, s.log, err)
		return
	}
	writeJSON(w, http.StatusOK, n)
}

func (s *Server) retryNotification(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}
	n, err := s.svc.Retry(r.Context(), id)
	if errors.Is(err, store.ErrInvalidState) {
		writeError(w, http.StatusConflict, "not_retryable",
			fmt.Sprintf("notification is %s; only failed notifications can be retried", n.Status))
		return
	}
	if err != nil {
		writeServiceError(w, r, s.log, err)
		return
	}
	writeJSON(w, http.StatusAccepted, n)
}

func (s *Server) getBatch(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}
	b, err := s.svc.Batch(r.Context(), id)
	if err != nil {
		writeServiceError(w, r, s.log, err)
		return
	}
	writeJSON(w, http.StatusOK, b)
}

func (s *Server) cancelBatch(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}
	n, err := s.svc.CancelBatch(r.Context(), id)
	if err != nil {
		writeServiceError(w, r, s.log, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"batchId": id, "cancelled": n})
}

func (s *Server) createTemplate(w http.ResponseWriter, r *http.Request) {
	var in service.TemplateInput
	if !decodeJSON(w, r, maxSingleBody, &in) {
		return
	}
	t, err := s.svc.CreateTemplate(r.Context(), in)
	if err != nil {
		writeServiceError(w, r, s.log, err)
		return
	}
	w.Header().Set("Location", "/v1/templates/"+t.ID.String())
	writeJSON(w, http.StatusCreated, t)
}

func (s *Server) listTemplates(w http.ResponseWriter, r *http.Request) {
	ts, err := s.svc.ListTemplates(r.Context())
	if err != nil {
		writeServiceError(w, r, s.log, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": ts})
}

func (s *Server) getTemplate(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r)
	if !ok {
		return
	}
	t, err := s.svc.GetTemplate(r.Context(), id)
	if err != nil {
		writeServiceError(w, r, s.log, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

// GET /v1/metrics?window=5m: queue depth, success/failure rates and latency.
func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	window := 5 * time.Minute
	if v := r.URL.Query().Get("window"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < time.Second || d > 24*time.Hour {
			writeError(w, http.StatusBadRequest, "invalid_window", "window must be a duration between 1s and 24h, e.g. 5m")
			return
		}
		window = d
	}
	m, err := s.svc.Metrics(r.Context(), window)
	if err != nil {
		writeServiceError(w, r, s.log, err)
		return
	}
	writeJSON(w, http.StatusOK, m)
}

func pathUUID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_id", "id must be a UUID")
		return uuid.Nil, false
	}
	return id, true
}

func parseTime(v, field string, verr *domain.ValidationError) *time.Time {
	if v == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		verr.Add(field, "must be an RFC 3339 timestamp such as 2026-01-02T15:04:05Z")
		return nil
	}
	return &t
}

// Cursors are opaque to clients: base64("<unix nanos>|<uuid>").
func encodeCursor(c store.Cursor) string {
	raw := strconv.FormatInt(c.CreatedAt.UnixNano(), 10) + "|" + c.ID.String()
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(s string) (*store.Cursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	ts, id, ok := strings.Cut(string(raw), "|")
	if !ok {
		return nil, errors.New("malformed cursor")
	}
	nanos, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return nil, err
	}
	uid, err := uuid.Parse(id)
	if err != nil {
		return nil, err
	}
	return &store.Cursor{CreatedAt: time.Unix(0, nanos), ID: uid}, nil
}
