package api

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/atakanakis/notification-engine/internal/observability"
)

const CorrelationHeader = "X-Correlation-ID"

var safeCorrelationID = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// correlationID takes the caller's X-Correlation-ID (or generates one), puts
// it in the request context for logging and echoes it in the response. It is
// stored with each notification so worker logs for an asynchronous delivery
// can be matched to the API request that created it.
func correlationID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(CorrelationHeader)
		if !safeCorrelationID.MatchString(id) {
			id = uuid.NewString()
		}
		w.Header().Set(CorrelationHeader, id)
		next.ServeHTTP(w, r.WithContext(observability.WithCorrelationID(r.Context(), id)))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController and the WebSocket upgrade reach the
// underlying writer (needed for Hijack).
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// accessLog writes one structured log line per request and records the
// request metrics. It must sit directly in front of the ServeMux so that
// r.Pattern (the matched route, e.g. "GET /v1/notifications/{id}") is set on
// the same request value; using the pattern keeps metric cardinality low.
func accessLog(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		elapsed := time.Since(start)
		observability.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(rec.status)).Inc()
		observability.HTTPLatency.WithLabelValues(r.Method, route).Observe(elapsed.Seconds())
		if span := trace.SpanFromContext(r.Context()); r.Pattern != "" {
			span.SetName(r.Pattern)
		}
		level := slog.LevelInfo
		if rec.status >= 500 {
			level = slog.LevelError
		}
		log.Log(r.Context(), level, "http request",
			"method", r.Method, "path", r.URL.Path, "route", route,
			"status", rec.status, "duration_ms", elapsed.Milliseconds())
	})
}

// requireAPIKey rejects /v1 requests without one of the configured keys.
// Health, readiness, Prometheus metrics and the API docs stay open so load
// balancers, scrapers and humans can reach them.
func requireAPIKey(keys []string, next http.Handler) http.Handler {
	if len(keys) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/v1/") {
			next.ServeHTTP(w, r)
			return
		}
		got := r.Header.Get("X-API-Key")
		if scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " "); ok && strings.EqualFold(scheme, "Bearer") {
			got = token
		}
		for _, k := range keys {
			if subtle.ConstantTimeCompare([]byte(got), []byte(k)) == 1 {
				next.ServeHTTP(w, r)
				return
			}
		}
		w.Header().Set("WWW-Authenticate", `Bearer realm="notification-api"`)
		writeError(w, http.StatusUnauthorized, "unauthorized", "a valid API key is required")
	})
}

// apiKeyFromQuery moves a WebSocket client's ?apiKey= into the X-API-Key
// header (browsers cannot set headers on a WebSocket) and removes it from
// the URL. It runs before tracing and logging so the key never ends up in a
// span attribute or a log line.
func apiKeyFromQuery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/ws" && r.URL.Query().Has("apiKey") {
			q := r.URL.Query()
			key := q.Get("apiKey")
			q.Del("apiKey")
			r = r.Clone(r.Context())
			r.URL.RawQuery = q.Encode()
			r.RequestURI = r.URL.RequestURI()
			if r.Header.Get("X-API-Key") == "" {
				r.Header.Set("X-API-Key", key)
			}
		}
		next.ServeHTTP(w, r)
	})
}

// jsonFallback answers unmatched routes with the same JSON error shape as
// every other endpoint, instead of ServeMux's plain-text 404 and 405.
func jsonFallback(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h, pattern := mux.Handler(r)
		if pattern != "" {
			mux.ServeHTTP(w, r)
			return
		}
		probe := &discardWriter{header: http.Header{}, status: http.StatusOK}
		h.ServeHTTP(probe, r)
		if probe.status == http.StatusMethodNotAllowed {
			allow := probe.header.Get("Allow")
			w.Header().Set("Allow", allow)
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed",
				fmt.Sprintf("%s is not allowed on %s; allowed: %s", r.Method, r.URL.Path, allow))
			return
		}
		writeError(w, http.StatusNotFound, "not_found", "no route for "+r.URL.Path)
	})
}

type discardWriter struct {
	header http.Header
	status int
}

func (d *discardWriter) Header() http.Header         { return d.header }
func (d *discardWriter) Write(b []byte) (int, error) { return len(b), nil }
func (d *discardWriter) WriteHeader(code int)        { d.status = code }

func recoverer(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if err, ok := v.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					panic(v)
				}
				log.ErrorContext(r.Context(), "panic", "value", v, "stack", string(debug.Stack()))
				writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
