package mock

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/itsveems/chaosserve/internal/config"
)

var hostname = sync.OnceValue(func() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
})

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model     string        `json:"model"`
	Messages  []chatMessage `json:"messages"`
	Stream    bool          `json:"stream"`
	MaxTokens int           `json:"max_tokens"`
	IgnoreEOS bool          `json:"ignore_eos"`
}

type chunkChoice struct {
	Index        int            `json:"index"`
	Delta        map[string]any `json:"delta"`
	FinishReason *string        `json:"finish_reason"`
}

type chatChunk struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []chunkChoice `json:"choices"`
}

// handleChatCompletions is the OpenAI-compatible streaming endpoint. The
// mock is streaming-only (the harness client is an SSE client, §2) and
// always emits exactly max_tokens content tokens: ignore_eos semantics,
// there is no early stop.
func (s *Server) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	// Silent-hang outranks everything, including the slow-reload 503 and
	// request validation: during a full-node partition nothing on the
	// node answers anything, correct or malformed.
	if s.engine.silentHangActive() {
		s.requestsTotal.WithLabelValues("hung").Inc()
		s.blackhole(w)
		return
	}

	if !s.ready() {
		s.requestsTotal.WithLabelValues("503").Inc()
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "model is reloading"})
		return
	}

	var req chatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.requestsTotal.WithLabelValues("400").Inc()
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "malformed request body"})
		return
	}
	if !req.Stream || req.MaxTokens < 1 || len(req.Messages) == 0 {
		s.requestsTotal.WithLabelValues("400").Inc()
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "stream must be true, max_tokens >= 1, messages required"})
		return
	}

	verdict := s.engine.admit()
	switch {
	case verdict.httpError != 0:
		s.requestsTotal.WithLabelValues(fmt.Sprint(verdict.httpError)).Inc()
		writeJSON(w, verdict.httpError, map[string]any{"error": "injected fault: error mode"})
		return
	case verdict.act == actHang:
		// The window fired between the check above and admit: same fate.
		s.requestsTotal.WithLabelValues("hung").Inc()
		s.blackhole(w)
		return
	case verdict.abortAfterTokens == 0:
		// stream_abort window with abort-at-admit: RST before any bytes.
		s.requestsTotal.WithLabelValues("rst").Inc()
		rst(w)
		return
	}

	s.activeStreams.Inc()
	defer s.activeStreams.Dec()

	reqID := s.reqCounter.Add(1)
	rng := rand.New(rand.NewSource(s.cfg.Seed + reqID*0x9E3779B9))
	ctx := r.Context()
	rc := http.NewResponseController(w)

	// A stream admitted during a stream_abort window aborts via its own
	// token counter, not the fire-time gate (which is what RSTs streams
	// that were already in flight when the window opened). It is served
	// with normal TTFT/ITL pacing until that counter is reached.
	exempt := verdict.abortAfterTokens > 0

	// Emission targets are an absolute schedule from request admit time
	// (TTFT, then cumulative ITLs), so per-sleep timer overshoot does not
	// accumulate across 256 tokens: the injected distribution stays a
	// *known* distribution at AC1 precision. After a stall, overdue
	// targets flush immediately.
	admitAt := time.Now()
	targets := make([]time.Duration, req.MaxTokens)
	acc := sample(s.cfg.TTFT, rng)
	for i := range targets {
		if i > 0 {
			acc += sample(s.cfg.ITL, rng)
		}
		targets[i] = acc
	}

	// TTFT wait, interruptible by abort/hang faults firing mid-sleep.
	switch s.engine.sleep(ctx, targets[0], exempt) {
	case actAbort:
		s.requestsTotal.WithLabelValues("rst").Inc()
		rst(w)
		return
	case actHang:
		s.requestsTotal.WithLabelValues("hung").Inc()
		s.blackhole(w)
		return
	case actAbandon:
		s.requestsTotal.WithLabelValues("abandoned").Inc()
		return
	}

	id := fmt.Sprintf("chatcmpl-mock-%d", reqID)
	model := req.Model
	if model == "" {
		model = "chaosserve-mock"
	}

	for token := 1; token <= req.MaxTokens; token++ {
		if token > 1 {
			// Overdue targets (post-stall flush) skip the timer entirely —
			// the gateEmit below still checks fault state — so a released
			// backlog does not thrash the runtime timer subsystem.
			if remaining := targets[token-1] - time.Since(admitAt); remaining > 0 {
				switch s.engine.sleep(ctx, remaining, exempt) {
				case actAbort:
					s.requestsTotal.WithLabelValues("rst").Inc()
					rst(w)
					return
				case actHang:
					s.requestsTotal.WithLabelValues("hung").Inc()
					s.blackhole(w)
					return
				case actAbandon:
					s.requestsTotal.WithLabelValues("abandoned").Inc()
					return
				}
			}
		}
		// Gate at the write: a stall holds the stream here (in-flight and
		// new streams both), then emission resumes — staggered by a
		// deterministic per-stream jitter (0-100 ms) so a released
		// backlog drains as a spread, not an instantaneous storm.
		act, stalled := s.engine.gateEmit(ctx, exempt)
		if act == actProceed && stalled {
			jitter := time.Duration(rng.Float64() * float64(100*time.Millisecond))
			act = s.engine.sleep(ctx, jitter, exempt)
		}
		switch act {
		case actAbort:
			s.requestsTotal.WithLabelValues("rst").Inc()
			rst(w)
			return
		case actHang:
			s.requestsTotal.WithLabelValues("hung").Inc()
			s.blackhole(w)
			return
		case actAbandon:
			s.requestsTotal.WithLabelValues("abandoned").Inc()
			return
		}

		if token == 1 {
			h := w.Header()
			h.Set("Content-Type", "text/event-stream")
			h.Set("Cache-Control", "no-cache")
			h.Set("X-Chaosserve-Request-Id", id)
			// Replica identity for in-flight loss attribution (§3): in
			// Kubernetes the hostname is the pod name.
			h.Set("X-Chaosserve-Replica", hostname())
			w.WriteHeader(http.StatusOK)
		}
		if err := writeChunk(w, rc, chatChunk{
			ID: id, Object: "chat.completion.chunk", Created: time.Now().Unix(), Model: model,
			Choices: []chunkChoice{{Index: 0, Delta: map[string]any{"content": fmt.Sprintf("t%d ", token)}, FinishReason: nil}},
		}); err != nil {
			s.requestsTotal.WithLabelValues("abandoned").Inc()
			return
		}
		s.tokensTotal.Inc()

		// stream_abort window: new streams are RST after the configured
		// number of tokens.
		if verdict.abortAfterTokens > 0 && token >= verdict.abortAfterTokens {
			s.requestsTotal.WithLabelValues("rst").Inc()
			rst(w)
			return
		}
	}

	finish := "length" // full budget always emitted (ignore_eos semantics)
	if err := writeChunk(w, rc, chatChunk{
		ID: id, Object: "chat.completion.chunk", Created: time.Now().Unix(), Model: model,
		Choices: []chunkChoice{{Index: 0, Delta: map[string]any{}, FinishReason: &finish}},
	}); err != nil {
		s.requestsTotal.WithLabelValues("abandoned").Inc()
		return
	}
	if _, err := fmt.Fprint(w, "data: [DONE]\n\n"); err != nil {
		s.requestsTotal.WithLabelValues("abandoned").Inc()
		return
	}
	rc.Flush() //nolint:errcheck
	s.requestsTotal.WithLabelValues("200").Inc()
}

func writeChunk(w http.ResponseWriter, rc *http.ResponseController, c chatChunk) error {
	raw, err := json.Marshal(c)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", raw); err != nil {
		return err
	}
	return rc.Flush()
}

// sample draws one latency from a config-pinned analytic distribution.
func sample(d config.LatencyDist, rng *rand.Rand) time.Duration {
	var ms float64
	switch d.Distribution {
	case "uniform":
		ms = d.MinMs + rng.Float64()*(d.MaxMs-d.MinMs)
	default: // "fixed" (config validation guarantees the set)
		ms = d.FixedMs
	}
	return time.Duration(ms * float64(time.Millisecond))
}

// rst force-closes the client connection with SO_LINGER=0 so the kernel
// sends a TCP RST — the stream_abort signature (abrupt replica deletion
// RSTs in-flight connections, §1). Contrast silent_hang, which never
// closes at all.
func rst(w http.ResponseWriter) {
	conn, _, err := http.NewResponseController(w).Hijack()
	if err != nil {
		// No hijack support: abort the stream the strongest way available.
		panic(http.ErrAbortHandler)
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		tcp.SetLinger(0) //nolint:errcheck
	}
	conn.Close() //nolint:errcheck
}
