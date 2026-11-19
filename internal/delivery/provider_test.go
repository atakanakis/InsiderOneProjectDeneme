package delivery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPProviderSendsContractAndParsesResponse(t *testing.T) {
	var got providerRequest
	var headers http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers = r.Header.Clone()
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"m-123","status":"accepted","timestamp":"2026-01-01T00:00:00Z"}`))
	}))
	defer srv.Close()

	res := NewHTTPProvider(srv.URL, time.Second, 4).Send(context.Background(), Message{
		NotificationID: "n-1", CorrelationID: "c-1", To: "+905551234567", Channel: "sms", Content: "hi",
	})

	require.NoError(t, res.Err)
	assert.Equal(t, http.StatusAccepted, res.StatusCode)
	assert.Equal(t, "m-123", res.MessageID)
	assert.Equal(t, providerRequest{To: "+905551234567", Channel: "sms", Content: "hi"}, got)
	assert.Equal(t, "n-1", headers.Get("Idempotency-Key"), "stable across retries")
	assert.Equal(t, "c-1", headers.Get("X-Correlation-ID"))
	assert.Equal(t, "application/json", headers.Get("Content-Type"))
}

func TestHTTPProviderReportsRetryAfterAndBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "7")
		http.Error(w, "slow down", http.StatusTooManyRequests)
	}))
	defer srv.Close()

	res := NewHTTPProvider(srv.URL, time.Second, 4).Send(context.Background(), Message{NotificationID: "n"})
	assert.Equal(t, http.StatusTooManyRequests, res.StatusCode)
	assert.Equal(t, 7*time.Second, res.RetryAfter)
	assert.Contains(t, describeFailure(res), "HTTP 429: slow down")
}

func TestHTTPProviderTimeoutIsATransportError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	}))
	defer srv.Close()

	res := NewHTTPProvider(srv.URL, 50*time.Millisecond, 4).Send(context.Background(), Message{NotificationID: "n"})
	require.Error(t, res.Err)
	assert.Equal(t, "retryable", string(Classify(res)))
}

func TestHTTPProviderToleratesUnparseableSuccessBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	res := NewHTTPProvider(srv.URL, time.Second, 4).Send(context.Background(), Message{NotificationID: "n"})
	assert.Equal(t, "delivered", string(Classify(res)), "the provider accepted it; the ID is optional")
	assert.Empty(t, res.MessageID)
}
