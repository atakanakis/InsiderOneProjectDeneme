package api

import (
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"strconv"
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
