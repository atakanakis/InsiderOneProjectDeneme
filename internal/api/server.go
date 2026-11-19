// Package api is the HTTP layer: routing, request parsing, response shaping.
package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	apispec "github.com/atakanakis/notification-engine/api"
	"github.com/atakanakis/notification-engine/internal/service"
)

type Server struct {
	svc   *service.Service
	redis *redis.Client
	hub   *Hub
	log   *slog.Logger
}

func NewServer(svc *service.Service, rdb *redis.Client, hub *Hub, log *slog.Logger) *Server {
	return &Server{svc: svc, redis: rdb, hub: hub, log: log}
}

const (
	maxSingleBody = 1 << 20  // 1 MiB
	maxBatchBody  = 16 << 20 // 16 MiB: 1000 items with generous content
)

// Handler wires routes and middleware. Go 1.22+ ServeMux supports method and
// path-parameter patterns, so no router dependency is needed.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/notifications", s.createNotification)
	mux.HandleFunc("POST /v1/notifications/batch", s.createBatch)
	mux.HandleFunc("GET /v1/notifications", s.listNotifications)
	mux.HandleFunc("GET /v1/notifications/{id}", s.getNotification)
	mux.HandleFunc("POST /v1/notifications/{id}/cancel", s.cancelNotification)
	mux.HandleFunc("POST /v1/notifications/{id}/retry", s.retryNotification)

	mux.HandleFunc("GET /v1/batches/{id}", s.getBatch)
	mux.HandleFunc("POST /v1/batches/{id}/cancel", s.cancelBatch)

	mux.HandleFunc("POST /v1/templates", s.createTemplate)
	mux.HandleFunc("GET /v1/templates", s.listTemplates)
	mux.HandleFunc("GET /v1/templates/{id}", s.getTemplate)

	mux.HandleFunc("GET /v1/metrics", s.metrics)
	mux.HandleFunc("GET /v1/ws", s.websocket)

	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.Handle("GET /metrics", promhttp.Handler())

	mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(apispec.Spec)
	})
	mux.HandleFunc("GET /docs", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(swaggerUI))
	})
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/docs", http.StatusFound)
	})

	// Outermost first: correlation ID -> panic recovery -> tracing -> access log -> routes.
	h := accessLog(s.log, mux)
	h = otelhttp.NewHandler(h, "notification-api",
		otelhttp.WithFilter(func(r *http.Request) bool {
			return r.URL.Path != "/metrics" && r.URL.Path != "/healthz" && r.URL.Path != "/readyz"
		}))
	h = recoverer(s.log, h)
	return correlationID(h)
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz checks the dependencies the API needs to serve traffic. A load
// balancer should only route to instances that answer 200 here.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	checks := map[string]string{"postgres": "ok", "redis": "ok"}
	status := http.StatusOK
	if err := s.svc.Ready(ctx); err != nil {
		checks["postgres"] = err.Error()
		status = http.StatusServiceUnavailable
	}
	if err := s.redis.Ping(ctx).Err(); err != nil {
		checks["redis"] = err.Error()
		status = http.StatusServiceUnavailable
	}
	state := "ready"
	if status != http.StatusOK {
		state = "not_ready"
	}
	writeJSON(w, status, map[string]any{"status": state, "checks": checks})
}

const swaggerUI = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Notification API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js"></script>
  <script>
    window.ui = SwaggerUIBundle({ url: "/openapi.yaml", dom_id: "#swagger-ui" });
  </script>
</body>
</html>`
