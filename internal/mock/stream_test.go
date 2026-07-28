package mock

import (
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
)

// TestStreamTimingFixed: configurable TTFT and per-token latency are the
// mock's core contract (§2). Fixed distributions make nominal timings
// exact lower bounds (sleeps never return early); upper bounds are
// generous for CI scheduling noise.
func TestStreamTimingFixed(t *testing.T) {
	cfg := baseMockCfg()
	cfg.TTFT = fixed(300)
	cfg.ITL = fixed(20)
	s := startServer(t, cfg)
	base := "http://" + s.Addr()

	res := doStream(t, base, 10)
	if res.err != nil || res.status != 200 {
		t.Fatalf("stream failed: status=%d err=%v", res.status, res.err)
	}
	if len(res.contentTimes) != 10 {
		t.Fatalf("want exactly max_tokens=10 content chunks (ignore_eos semantics), got %d", len(res.contentTimes))
	}
	if res.finishChunks != 1 || !res.done {
		t.Fatalf("want one finish_reason chunk and [DONE], got finish=%d done=%v", res.finishChunks, res.done)
	}
	if ttft := res.ttft(); ttft < 295*time.Millisecond || ttft > 900*time.Millisecond {
		t.Errorf("TTFT: want ~300ms, got %v", ttft)
	}
	nominal := 300*time.Millisecond + 9*20*time.Millisecond
	if total := res.total(); total < nominal-10*time.Millisecond || total > nominal+600*time.Millisecond {
		t.Errorf("total: want ~%v, got %v", nominal, total)
	}
	// Mean inter-token latency over the 9 gaps.
	meanITL := res.contentTimes[9].Sub(res.contentTimes[0]) / 9
	if meanITL < 19*time.Millisecond || meanITL > 80*time.Millisecond {
		t.Errorf("mean ITL: want ~20ms, got %v", meanITL)
	}
}

// TestUniformDistribution: seeded uniform TTFT stays within its analytic
// bounds and actually varies (AC1 needs a known injected distribution).
func TestUniformDistribution(t *testing.T) {
	cfg := baseMockCfg()
	cfg.TTFT = config.LatencyDist{Distribution: "uniform", MinMs: 100, MaxMs: 220}
	cfg.ITL = fixed(2)
	s := startServer(t, cfg)
	base := "http://" + s.Addr()

	var ttfts []time.Duration
	for i := 0; i < 8; i++ {
		res := doStream(t, base, 2)
		if res.err != nil || res.status != 200 {
			t.Fatalf("request %d failed: status=%d err=%v", i, res.status, res.err)
		}
		ttfts = append(ttfts, res.ttft())
	}
	lo, hi := ttfts[0], ttfts[0]
	for _, v := range ttfts {
		lo = min(lo, v)
		hi = max(hi, v)
	}
	if lo < 95*time.Millisecond {
		t.Errorf("uniform lower bound violated: min TTFT %v < 100ms", lo)
	}
	if hi > 220*time.Millisecond+500*time.Millisecond {
		t.Errorf("uniform upper bound (plus CI slack) violated: max TTFT %v", hi)
	}
	if spread := hi - lo; spread < 10*time.Millisecond {
		t.Errorf("uniform TTFT shows no variation: spread %v over 8 requests", spread)
	}
}

func TestRequestValidation(t *testing.T) {
	s := startServer(t, baseMockCfg())
	base := "http://" + s.Addr()

	res := doStream(t, base, 0) // max_tokens < 1
	if res.status != 400 {
		t.Errorf("max_tokens=0: want 400, got %d (err=%v)", res.status, res.err)
	}
	// Non-streaming requests are rejected: the mock is streaming-only (§2).
	status, err := postJSON(t, base+"/v1/chat/completions",
		`{"model":"m","messages":[{"role":"user","content":"x"}],"stream":false,"max_tokens":4}`)
	if err != nil || status != 400 {
		t.Errorf("stream=false: want 400, got %d err=%v", status, err)
	}
	if !strings.Contains(metricsText(t, base), `percentes_mock_requests_total{outcome="400"}`) {
		t.Error("400 outcomes must be counted in percentes_mock_requests_total")
	}
}

// TestMetricsCounters: per-replica request counters are what the §1 share
// gate reads; they must count terminal outcomes and emitted tokens.
func TestMetricsCounters(t *testing.T) {
	cfg := baseMockCfg()
	cfg.TTFT = fixed(5)
	cfg.ITL = fixed(1)
	s := startServer(t, cfg)
	base := "http://" + s.Addr()

	for i := 0; i < 3; i++ {
		if res := doStream(t, base, 4); res.err != nil || res.status != 200 {
			t.Fatalf("request %d: status=%d err=%v", i, res.status, res.err)
		}
	}
	text := metricsText(t, base)
	if !strings.Contains(text, `percentes_mock_requests_total{outcome="200"} 3`) {
		t.Errorf("want 3 completed requests counted, metrics:\n%s", grepMetrics(text))
	}
	if !strings.Contains(text, "percentes_mock_tokens_emitted_total 12") {
		t.Errorf("want 12 tokens counted (3 requests x 4 tokens), metrics:\n%s", grepMetrics(text))
	}
}

func grepMetrics(text string) string {
	var out []string
	for _, l := range strings.Split(text, "\n") {
		if strings.HasPrefix(l, "percentes_") {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}
