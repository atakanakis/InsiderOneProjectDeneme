package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// Message is what we send to the external provider.
type Message struct {
	NotificationID string
	CorrelationID  string
	To             string
	Channel        string
	Content        string
}

// Result is the raw outcome of one provider call; Classify interprets it.
type Result struct {
	StatusCode int
	MessageID  string
	RetryAfter time.Duration
	Body       string // truncated, for error messages
	Err        error  // transport error: timeout, connection refused, ...
	Duration   time.Duration
}

type Provider interface {
	Send(ctx context.Context, m Message) Result
}

// HTTPProvider talks to the webhook.site style provider:
//
//	POST {url}  {"to": "...", "channel": "sms", "content": "..."}
//	202         {"messageId": "...", "status": "accepted", "timestamp": "..."}
type HTTPProvider struct {
	url    string
	client *http.Client
}

// NewHTTPProvider builds a client that keeps up to maxConns idle connections
// to the provider. http.DefaultTransport keeps only 2 per host, so with
// dozens of concurrent sends most requests would open a new TCP and TLS
// connection, adding latency between taking a rate-limit permit and the
// request arriving.
//
// tune can adjust the transport (tests use it to slow down dialing).
func NewHTTPProvider(url string, timeout time.Duration, maxConns int, tune ...func(*http.Transport)) *HTTPProvider {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = max(maxConns, 2)
	tr.MaxIdleConnsPerHost = max(maxConns, 2)
	for _, fn := range tune {
		fn(tr)
	}
	return &HTTPProvider{
		url: url,
		client: &http.Client{
			Timeout:   timeout,
			Transport: otelhttp.NewTransport(tr),
		},
	}
}

type sendGateKey struct{}

// WithSendGate registers a function that an HTTP provider calls once a
// connection is ready, immediately before it writes the request. Dialing
// and the TLS handshake happen before the gate, so whatever the gate checks
// (the age of a rate-limit permit) is checked at the moment the request
// really leaves, not when the send was started.
func WithSendGate(ctx context.Context, gate func()) context.Context {
	return context.WithValue(ctx, sendGateKey{}, gate)
}

type providerRequest struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type providerResponse struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

func (p *HTTPProvider) Send(ctx context.Context, m Message) Result {
	start := time.Now()
	res := p.send(ctx, m)
	res.Duration = time.Since(start)
	return res
}

func (p *HTTPProvider) send(ctx context.Context, m Message) Result {
	body, err := json.Marshal(providerRequest{To: m.To, Channel: m.Channel, Content: m.Content})
	if err != nil {
		return Result{Err: err}
	}
	if gate, ok := ctx.Value(sendGateKey{}).(func()); ok {
		var once sync.Once // GotConn can fire again on a transport-level retry
		ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
			GotConn: func(httptrace.GotConnInfo) { once.Do(gate) },
		})
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(body))
	if err != nil {
		return Result{Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	// The notification ID is stable across retries, so a provider that
	// supports idempotency keys can drop a duplicate if our previous attempt
	// actually got through but we never saw the response.
	req.Header.Set("Idempotency-Key", m.NotificationID)
	if m.CorrelationID != "" {
		req.Header.Set("X-Correlation-ID", m.CorrelationID)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return Result{Err: err}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	res := Result{StatusCode: resp.StatusCode, Body: truncate(string(raw), 300)}
	res.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var pr providerResponse
		if json.Unmarshal(raw, &pr) == nil {
			res.MessageID = pr.MessageID
		}
	}
	return res
}

// parseRetryAfter supports both forms allowed by RFC 9110: delta-seconds and
// an HTTP date.
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}

func describeFailure(res Result) string {
	if res.Err != nil {
		return res.Err.Error()
	}
	msg := fmt.Sprintf("provider returned HTTP %d", res.StatusCode)
	if res.Body != "" {
		msg += ": " + res.Body
	}
	return msg
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
