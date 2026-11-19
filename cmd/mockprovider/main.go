// Command mockprovider is a local stand-in for the external SMS/email/push
// provider (webhook.site in the assessment). It speaks the same contract and
// adds what webhook.site cannot: configurable failures and latency for
// exercising retries and the circuit breaker, idempotency-key de-duplication,
// and per-second throughput counters for verifying the rate limit.
//
//	FAILURE_RATE    fraction of requests answered with 503   (default 0)
//	THROTTLE_RATE   fraction answered with 429 + Retry-After  (default 0)
//	REJECT_RATE     fraction answered with 400                (default 0)
//	LATENCY_MS      base latency per request                  (default 30)
//
// Rates can also be changed at runtime: POST /config {"failureRate":0.5}.
package main

import (
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

type settings struct {
	FailureRate  float64 `json:"failureRate"`
	ThrottleRate float64 `json:"throttleRate"`
	RejectRate   float64 `json:"rejectRate"`
	LatencyMS    int     `json:"latencyMs"`
}

// channelStats counts every request that reaches the provider, successful or
// not, because the rate limit applies to all calls we make.
type channelStats struct {
	Received     int           `json:"received"`
	Accepted     int           `json:"accepted"`
	MaxPerSecond int           `json:"maxRequestsPerSecond"`
	perSecond    map[int64]int // unix second -> requests received
}

type provider struct {
	mu         sync.Mutex
	cfg        settings
	seen       map[string]string // Idempotency-Key -> messageId
	duplicates int
	failed     int
	channels   map[string]*channelStats
}

type sendRequest struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

func main() {
	p := &provider{
		cfg: settings{
			FailureRate:  envFloat("FAILURE_RATE", 0),
			ThrottleRate: envFloat("THROTTLE_RATE", 0),
			RejectRate:   envFloat("REJECT_RATE", 0),
			LatencyMS:    int(envFloat("LATENCY_MS", 30)),
		},
		seen:     map[string]string{},
		channels: map[string]*channelStats{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /config", p.updateConfig)
	mux.HandleFunc("GET /stats", p.stats)
	mux.HandleFunc("POST /reset", p.reset)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("POST /", p.send) // any other POST path, like webhook.site/{uuid}

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":9090"
	}
	slog.Info("mock provider listening", "addr", addr, "settings", p.cfg)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("mock provider stopped", "error", err)
		os.Exit(1)
	}
}

func (p *provider) send(w http.ResponseWriter, r *http.Request) {
	var req sendRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil ||
		req.To == "" || req.Channel == "" || req.Content == "" {
		http.Error(w, `{"error":"to, channel and content are required"}`, http.StatusBadRequest)
		return
	}

	p.mu.Lock()
	cfg := p.cfg
	cs := p.channels[req.Channel]
	if cs == nil {
		cs = &channelStats{perSecond: map[int64]int{}}
		p.channels[req.Channel] = cs
	}
	cs.Received++
	sec := time.Now().Unix()
	cs.perSecond[sec]++
	cs.MaxPerSecond = max(cs.MaxPerSecond, cs.perSecond[sec])
	delete(cs.perSecond, sec-120)
	p.mu.Unlock()
	time.Sleep(time.Duration(cfg.LatencyMS+rand.IntN(cfg.LatencyMS/2+1)) * time.Millisecond)

	roll := rand.Float64()
	switch {
	case roll < cfg.FailureRate:
		p.countFailure()
		http.Error(w, `{"error":"provider temporarily unavailable"}`, http.StatusServiceUnavailable)
		return
	case roll < cfg.FailureRate+cfg.ThrottleRate:
		p.countFailure()
		w.Header().Set("Retry-After", "1")
		http.Error(w, `{"error":"too many requests"}`, http.StatusTooManyRequests)
		return
	case roll < cfg.FailureRate+cfg.ThrottleRate+cfg.RejectRate:
		p.countFailure()
		http.Error(w, `{"error":"recipient rejected"}`, http.StatusBadRequest)
		return
	}

	key := r.Header.Get("Idempotency-Key")
	p.mu.Lock()
	msgID, dup := p.seen[key]
	if key != "" && dup {
		p.duplicates++
	} else {
		msgID = uuid.NewString()
		if key != "" {
			p.seen[key] = msgID
		}
		cs.Accepted++
	}
	p.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"messageId": msgID,
		"status":    "accepted",
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func (p *provider) countFailure() {
	p.mu.Lock()
	p.failed++
	p.mu.Unlock()
}

func (p *provider) stats(w http.ResponseWriter, _ *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()
	type out struct {
		*channelStats
		LastSeconds map[string]int `json:"lastSeconds"`
	}
	chans := map[string]out{}
	now := time.Now().Unix()
	for name, cs := range p.channels {
		last := map[string]int{}
		for s := now - 10; s < now; s++ {
			last[strconv.FormatInt(s-now, 10)+"s"] = cs.perSecond[s]
		}
		chans[name] = out{channelStats: cs, LastSeconds: last}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"settings":             p.cfg,
		"channels":             chans,
		"failedResponses":      p.failed,
		"duplicatesSuppressed": p.duplicates,
	})
}

func (p *provider) updateConfig(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()
	next := p.cfg
	if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	p.cfg = next
	slog.Info("mock provider settings changed", "settings", p.cfg)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(p.cfg)
}

func (p *provider) reset(w http.ResponseWriter, _ *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen = map[string]string{}
	p.channels = map[string]*channelStats{}
	p.duplicates, p.failed = 0, 0
	w.WriteHeader(http.StatusNoContent)
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}
