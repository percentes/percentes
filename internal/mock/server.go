// Package mock implements the Phase 0 mock inference server (SPEC.md §2
// "Local-first"): an OpenAI-compatible SSE server with configurable TTFT
// and per-token latency and scriptable fault modes — stall, error,
// stream-abort, silent-hang (no RST), and slow-reload-on-reschedule.
//
// The data plane is /v1/chat/completions and /health. /admin and /metrics
// are out-of-band harness instrumentation and stay reachable during
// every fault mode.
package mock

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/itsveems/chaosserve/internal/config"
)

// Server is one mock replica.
type Server struct {
	cfg    config.Mock
	engine *engine

	start   time.Time // monotonic anchor
	readyAt time.Time // start + slow-reload duration

	httpSrv  *http.Server
	listener net.Listener
	done     chan struct{}

	reqCounter atomic.Int64

	registry      *prometheus.Registry
	requestsTotal *prometheus.CounterVec
	tokensTotal   prometheus.Counter
	activeStreams prometheus.Gauge
	faultsFired   *prometheus.CounterVec
}

// New builds a server from the mock section of the run config. The config
// must already have passed config validation.
func New(cfg config.Mock) *Server {
	s := &Server{cfg: cfg, done: make(chan struct{})}

	s.registry = prometheus.NewRegistry()
	s.requestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "chaosserve_mock_requests_total",
		Help: "Inference requests by terminal outcome (200, 400, 500, 503, rst, hung, abandoned). Per-replica counter for the §1 share gate.",
	}, []string{"outcome"})
	s.tokensTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "chaosserve_mock_tokens_emitted_total",
		Help: "SSE content tokens emitted.",
	})
	s.activeStreams = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "chaosserve_mock_active_streams",
		Help: "Streams currently being served.",
	})
	s.faultsFired = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "chaosserve_mock_faults_fired_total",
		Help: "Fault windows fired, by mode.",
	}, []string{"mode"})
	s.registry.MustRegister(s.requestsTotal, s.tokensTotal, s.activeStreams, s.faultsFired)
	return s
}

// Start listens on cfg.ListenAddr, anchors the monotonic clock, arms the
// config-scripted fault schedule, and serves in the background.
func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("mock: listen %s: %w", s.cfg.ListenAddr, err)
	}
	s.listener = l
	s.start = time.Now()
	if s.cfg.SlowReload.Enabled {
		s.readyAt = s.start.Add(time.Duration(s.cfg.SlowReload.DurationS * float64(time.Second)))
	} else {
		s.readyAt = s.start
	}

	s.engine = newEngine(s.start, s.done)
	s.engine.onFire = func(mode string) { s.faultsFired.WithLabelValues(mode).Inc() }
	for _, f := range s.cfg.FaultSchedule {
		if _, err := s.engine.arm(f.Mode, "schedule", f.StartOffsetS, f.DurationS, f.AbortAfterTokens); err != nil {
			l.Close()
			return fmt.Errorf("mock: arming schedule: %w", err)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", s.handleChatCompletions)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/admin/faults", s.handleAdminFaults)
	mux.Handle("/metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))

	// No server-side timeouts: streams may legitimately outlive any fixed
	// budget, and silent-hang must be able to hold connections open
	// indefinitely without the server FIN/RST-ing them.
	s.httpSrv = &http.Server{Handler: mux}
	go s.httpSrv.Serve(l) //nolint:errcheck // ErrServerClosed on shutdown
	return nil
}

// Addr returns the bound address (useful with ":0" in tests).
func (s *Server) Addr() string { return s.listener.Addr().String() }

// Close force-closes the server, releasing hung handlers.
func (s *Server) Close() error {
	close(s.done)
	return s.httpSrv.Close()
}

func (s *Server) ready() bool { return time.Now().After(s.readyAt) }

// blackhole detaches the connection from net/http and never writes to it:
// no bytes, no FIN, no RST — not even the implicit 200 that net/http
// emits when a handler returns, and immune to Server.Close teardown
// (hijacked connections are not tracked by the http.Server). It parks
// until the CLIENT abandons the connection (its close makes the read
// fail) and only then releases the fd. Silent-hang window expiry
// deliberately does not release captured connections: a healed partition
// does not resurrect dead in-flight work.
func (s *Server) blackhole(w http.ResponseWriter) {
	conn, _, err := http.NewResponseController(w).Hijack()
	if err != nil {
		// Cannot detach: park, then abort without the implicit response.
		s.engine.hangForever()
		panic(http.ErrAbortHandler)
	}
	conn.SetDeadline(time.Time{}) //nolint:errcheck
	buf := make([]byte, 256)
	for {
		if _, err := conn.Read(buf); err != nil {
			conn.Close() //nolint:errcheck // client is already gone
			return
		}
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.ServeHealthDuringSilentHang && s.engine.silentHangActive() {
		// Faithful to a full-node partition: health does not answer.
		s.blackhole(w)
		return
	}
	if !s.ready() {
		http.Error(w, "reloading", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "ok")
}

// adminArmRequest is the orchestrator-facing arming API.
type adminArmRequest struct {
	Mode             string  `json:"mode"`
	DelayS           float64 `json:"delay_s"`
	DurationS        float64 `json:"duration_s"`
	AbortAfterTokens int     `json:"abort_after_tokens"`
}

func (s *Server) handleAdminFaults(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{
			"server_start": s.start,
			"uptime_s":     s.engine.offsetS(),
			"faults":       s.engine.snapshot(),
		})
	case http.MethodPost:
		var req adminArmRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		switch req.Mode {
		case config.MockFaultStall, config.MockFaultError, config.MockFaultStreamAbort, config.MockFaultSilentHang:
		default:
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("unknown mode %q", req.Mode)})
			return
		}
		if req.DelayS < 0 || req.DurationS <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "delay_s must be >= 0 and duration_s > 0"})
			return
		}
		if req.AbortAfterTokens != 0 && req.Mode != config.MockFaultStreamAbort {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "abort_after_tokens only valid for stream_abort"})
			return
		}
		rec, err := s.engine.arm(req.Mode, "admin", s.engine.offsetS()+req.DelayS, req.DurationS, req.AbortAfterTokens)
		if err != nil {
			writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusCreated, rec)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}
