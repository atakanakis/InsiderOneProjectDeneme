package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/atakanakis/notification-engine/internal/api"
	"github.com/atakanakis/notification-engine/internal/domain"
	"github.com/atakanakis/notification-engine/internal/events"
	"github.com/atakanakis/notification-engine/internal/service"
	"github.com/atakanakis/notification-engine/internal/store"
	"github.com/atakanakis/notification-engine/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

type client struct {
	t    *testing.T
	base string
}

func newServer(t *testing.T) *client {
	t.Helper()
	env := testutil.Setup(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub := api.NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go events.Subscribe(ctx, env.Redis, log, hub.Broadcast)

	svc := service.New(store.New(env.Pool), events.NewRedisPublisher(env.Redis, log), 5)
	srv := httptest.NewServer(api.NewServer(svc, env.Redis, hub, log).Handler())
	t.Cleanup(srv.Close)
	return &client{t: t, base: srv.URL}
}

func (c *client) do(method, path string, body any, headers ...string) (*http.Response, map[string]any) {
	c.t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(c.t, err)
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.base+path, r)
	require.NoError(c.t, err)
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(c.t, err)
	defer resp.Body.Close()
	var out map[string]any
	raw, _ := io.ReadAll(resp.Body)
	if len(raw) > 0 && strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		require.NoError(c.t, json.Unmarshal(raw, &out), string(raw))
	}
	return resp, out
}

func sms(content string) map[string]any {
	return map[string]any{"recipient": "+905551234567", "channel": "sms", "content": content}
}

func errorFields(body map[string]any) []string {
	var fields []string
	details, _ := body["error"].(map[string]any)["details"].([]any)
	for _, d := range details {
		fields = append(fields, d.(map[string]any)["field"].(string))
	}
	return fields
}

func TestCreateAndGetNotification(t *testing.T) {
	c := newServer(t)
	resp, body := c.do("POST", "/v1/notifications", sms("hello"), "X-Correlation-ID", "req-123")
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	assert.Equal(t, "req-123", resp.Header.Get("X-Correlation-ID"))
	assert.Equal(t, "pending", body["status"])
	assert.Equal(t, "normal", body["priority"], "default priority")
	assert.Equal(t, "req-123", body["correlationId"])
	id := body["id"].(string)
	assert.Equal(t, "/v1/notifications/"+id, resp.Header.Get("Location"))

	resp, body = c.do("GET", "/v1/notifications/"+id, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, id, body["id"])
	assert.Equal(t, []any{}, body["attemptHistory"])
}

func TestCorrelationIDIsGeneratedWhenMissingOrUnsafe(t *testing.T) {
	c := newServer(t)
	resp, _ := c.do("GET", "/healthz", nil, "X-Correlation-ID", "<script>alert(1)</script>")
	id := resp.Header.Get("X-Correlation-ID")
	assert.Len(t, id, 36, "replaced by a generated UUID")
}

func TestIdempotencyKeyHeader(t *testing.T) {
	c := newServer(t)
	resp, first := c.do("POST", "/v1/notifications", sms("hi"), "Idempotency-Key", "abc")
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	resp, second := c.do("POST", "/v1/notifications", sms("hi"), "Idempotency-Key", "abc")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "true", resp.Header.Get("Idempotent-Replayed"))
	assert.Equal(t, first["id"], second["id"])

	resp, body := c.do("POST", "/v1/notifications", sms("changed"), "Idempotency-Key", "abc")
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	assert.Equal(t, "idempotency_key_reused", body["error"].(map[string]any)["code"])
}

func TestValidationReportsEveryField(t *testing.T) {
	c := newServer(t)
	resp, body := c.do("POST", "/v1/notifications", map[string]any{
		"recipient": "12345", "channel": "sms", "content": strings.Repeat("x", 1601), "priority": "urgent",
	})
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
	assert.ElementsMatch(t, []string{"priority", "recipient", "content"}, errorFields(body))

	resp, body = c.do("POST", "/v1/notifications", map[string]any{"recipient": "x", "channel": "fax", "content": "hi"})
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
	assert.Contains(t, errorFields(body), "channel")
}

func TestMalformedRequests(t *testing.T) {
	c := newServer(t)
	resp, body := c.do("POST", "/v1/notifications", map[string]any{"recipient": "+905551234567", "channel": "sms", "content": "hi", "priorty": "high"})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "typos in field names are not silently ignored")
	assert.Equal(t, "invalid_json", body["error"].(map[string]any)["code"])

	resp, _ = c.do("GET", "/v1/notifications/not-a-uuid", nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	resp, _ = c.do("GET", "/v1/notifications/7b1c6c80-9a8e-4a5b-9d32-6f2b3a4c5d6e", nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestBatchCreateIsAllOrNothing(t *testing.T) {
	c := newServer(t)
	items := []any{sms("one"), sms("two"), map[string]any{"recipient": "bad", "channel": "sms", "content": "three"}}
	resp, body := c.do("POST", "/v1/notifications/batch", map[string]any{"notifications": items})
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
	assert.Equal(t, []string{"notifications[2].recipient"}, errorFields(body))

	_, list := c.do("GET", "/v1/notifications", nil)
	assert.Empty(t, list["items"], "nothing from the rejected batch was stored")

	resp, body = c.do("POST", "/v1/notifications/batch", map[string]any{"notifications": items[:2]})
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	assert.EqualValues(t, 2, body["accepted"])
	batchID := body["batchId"].(string)

	resp, summary := c.do("GET", "/v1/batches/"+batchID, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.EqualValues(t, 2, summary["total"])
	assert.EqualValues(t, 2, summary["counts"].(map[string]any)["pending"])
	assert.Equal(t, false, summary["finished"])

	resp, out := c.do("POST", "/v1/batches/"+batchID+"/cancel", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.EqualValues(t, 2, out["cancelled"])
	_, summary = c.do("GET", "/v1/batches/"+batchID, nil)
	assert.Equal(t, true, summary["finished"])
}

func TestBatchLimits(t *testing.T) {
	c := newServer(t)
	items := make([]any, domain.MaxBatchSize+1)
	for i := range items {
		items[i] = sms("x")
	}
	resp, body := c.do("POST", "/v1/notifications/batch", map[string]any{"notifications": items})
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
	assert.Equal(t, []string{"notifications"}, errorFields(body))

	resp, _ = c.do("POST", "/v1/notifications/batch", map[string]any{"notifications": items[:1000]})
	assert.Equal(t, http.StatusAccepted, resp.StatusCode, "exactly 1000 is allowed")

	dup := func() map[string]any { m := sms("x"); m["idempotencyKey"] = "same"; return m }
	resp, body = c.do("POST", "/v1/notifications/batch", map[string]any{"notifications": []any{dup(), dup()}})
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
	assert.Equal(t, []string{"notifications[1].idempotencyKey"}, errorFields(body))
}

func TestCancel(t *testing.T) {
	c := newServer(t)
	_, n := c.do("POST", "/v1/notifications", sms("hi"))
	id := n["id"].(string)

	resp, body := c.do("POST", "/v1/notifications/"+id+"/cancel", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "cancelled", body["status"])

	resp, body = c.do("POST", "/v1/notifications/"+id+"/cancel", nil)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	assert.Equal(t, "not_cancellable", body["error"].(map[string]any)["code"])

	resp, _ = c.do("POST", "/v1/notifications/"+id+"/retry", nil)
	assert.Equal(t, http.StatusConflict, resp.StatusCode, "only failed notifications can be retried")
}

func TestListFiltersAndCursor(t *testing.T) {
	c := newServer(t)
	for i := range 5 {
		c.do("POST", "/v1/notifications", sms(fmt.Sprint(i)))
	}
	c.do("POST", "/v1/notifications", map[string]any{"recipient": "a@b.co", "channel": "email", "content": "e"})

	_, page := c.do("GET", "/v1/notifications?channel=sms&limit=3", nil)
	assert.Len(t, page["items"], 3)
	cursor, ok := page["nextCursor"].(string)
	require.True(t, ok)

	_, page = c.do("GET", "/v1/notifications?channel=sms&limit=3&cursor="+cursor, nil)
	assert.Len(t, page["items"], 2)
	assert.Nil(t, page["nextCursor"])

	_, page = c.do("GET", "/v1/notifications?status=pending&channel=email", nil)
	assert.Len(t, page["items"], 1)

	future := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	_, page = c.do("GET", "/v1/notifications?from="+future, nil)
	assert.Empty(t, page["items"])

	resp, body := c.do("GET", "/v1/notifications?status=lost&limit=500&from=yesterday", nil)
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
	assert.ElementsMatch(t, []string{"status", "limit", "from"}, errorFields(body))
}

func TestTemplatesAndScheduling(t *testing.T) {
	c := newServer(t)
	resp, tpl := c.do("POST", "/v1/templates", map[string]any{
		"name": "flash-sale", "channel": "sms", "body": "Hi {{.name}}, {{.discount}}% off today!",
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	tplID := tpl["id"].(string)

	resp, _ = c.do("POST", "/v1/templates", map[string]any{"name": "flash-sale", "channel": "sms", "body": "x"})
	assert.Equal(t, http.StatusConflict, resp.StatusCode, "names are unique")

	at := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	resp, n := c.do("POST", "/v1/notifications", map[string]any{
		"recipient": "+905551234567", "channel": "sms", "templateId": tplID,
		"variables": map[string]string{"name": "Ada", "discount": "20"}, "scheduledAt": at.Format(time.RFC3339),
	})
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	assert.Equal(t, "Hi Ada, 20% off today!", n["content"])
	assert.Equal(t, tplID, n["templateId"])
	next, err := time.Parse(time.RFC3339Nano, n["nextAttemptAt"].(string))
	require.NoError(t, err)
	assert.True(t, next.Equal(at), "a scheduled notification is not due before scheduledAt")

	resp, body := c.do("POST", "/v1/notifications", map[string]any{
		"recipient": "+905551234567", "channel": "sms", "templateId": tplID, "variables": map[string]string{"name": "Ada"},
	})
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
	assert.Equal(t, []string{"variables"}, errorFields(body))

	resp, body = c.do("POST", "/v1/notifications", map[string]any{
		"recipient": "a@b.co", "channel": "email", "templateId": tplID,
		"variables": map[string]string{"name": "Ada", "discount": "1"},
	})
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
	assert.Equal(t, []string{"templateId"}, errorFields(body), "template channel must match")

	resp, body = c.do("POST", "/v1/templates", map[string]any{"name": "broken", "channel": "sms", "body": "Hi {{.name"})
	require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
	assert.Equal(t, []string{"body"}, errorFields(body))
}

func TestWebSocketStreamsStatusChanges(t *testing.T) {
	c := newServer(t)
	_, n := c.do("POST", "/v1/notifications", sms("hi"))
	_, other := c.do("POST", "/v1/notifications", sms("other"))
	id := n["id"].(string)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(c.base, "http") + "/v1/ws?notificationId=" + id
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer conn.CloseNow()
	time.Sleep(100 * time.Millisecond) // let the hub register the client

	c.do("POST", "/v1/notifications/"+other["id"].(string)+"/cancel", nil) // filtered out
	c.do("POST", "/v1/notifications/"+id+"/cancel", nil)

	var ev domain.StatusEvent
	require.NoError(t, wsjson.Read(ctx, conn, &ev))
	assert.Equal(t, id, ev.NotificationID.String())
	assert.Equal(t, domain.StatusCancelled, ev.Status)
}

func TestObservabilityEndpoints(t *testing.T) {
	c := newServer(t)
	c.do("POST", "/v1/notifications", map[string]any{"recipient": "+905551234567", "channel": "sms", "content": "x", "priority": "high"})

	resp, body := c.do("GET", "/readyz", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "ready", body["status"])

	resp, m := c.do("GET", "/v1/metrics?window=1m", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "1m0s", m["window"])
	queue := m["queue"].([]any)
	require.Len(t, queue, 1)
	assert.Equal(t, map[string]any{"channel": "sms", "priority": "high", "ready": 1.0, "scheduled": 0.0}, queue[0])
	assert.Len(t, m["channels"], 3)

	resp, _ = c.do("GET", "/v1/metrics?window=forever", nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	resp, _ = c.do("GET", "/metrics", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp, _ = c.do("GET", "/openapi.yaml", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
