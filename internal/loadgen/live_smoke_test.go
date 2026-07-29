package loadgen

import (
	"context"
	"os"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

// TestLiveHostedSmoke is the hosted-endpoint smoke test: a handful of real,
// authenticated streaming completions against a hosted OpenAI-compatible
// endpoint, through the unmodified open-loop pipeline.
//
// Opt-in only — `make test` and CI never touch the network:
//
//	PERCENTES_LIVE_SMOKE=1 GROQ_API_KEY=... go test ./internal/loadgen -run TestLiveHostedSmoke -v
//
// The offered rate is deliberately tiny (0.25 rps, 3 requests, 32 output
// tokens each) so the run sits far under any provider free-tier limit —
// rate-limit discipline proper is a separate build (the 429 controller).
func TestLiveHostedSmoke(t *testing.T) {
	if os.Getenv("PERCENTES_LIVE_SMOKE") != "1" {
		t.Skip("live smoke is opt-in: set PERCENTES_LIVE_SMOKE=1")
	}
	const keyEnv = "GROQ_API_KEY"
	if os.Getenv(keyEnv) == "" {
		t.Skipf("%s not set", keyEnv)
	}
	model := os.Getenv("PERCENTES_SMOKE_MODEL")
	if model == "" {
		model = "llama-3.1-8b-instant"
	}

	cfg, err := config.LoadFile("../../configs/ac.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	// Overrides for a minimal live probe of the auth/body path; not a
	// measurement run.
	cfg.Target = config.Target{
		BaseURL:   "https://api.groq.com/openai",
		Replicas:  1,
		Hosted:    true,
		ModelName: model,
		APIKeyEnv: keyEnv,
	}
	cfg.Load.RateRPS = 0.25
	cfg.Load.ArrivalProcess = "deterministic"
	cfg.Load.Connections = 4
	cfg.Load.MaxTokens = 32
	cfg.Load.InputLengthTokens = 16
	cfg.Run.Phases = config.Phases{WarmupS: 0, BaselineS: 8, FaultWindowTimeoutS: 4, CooldownS: 0}
	cfg.Fault.TInjectOffsetS = 8

	res, err := Run(context.Background(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	completed := 0
	for i := range res.Requests {
		r := &res.Requests[i]
		t.Logf("req %d: outcome=%s errclass=%q tokens=%d ttft=%.1fms e2e=%.1fms",
			r.Index, r.Outcome, r.ErrClass, r.Tokens,
			float64(r.TTFTNs())/1e6, float64(r.E2ENs())/1e6)
		if r.Outcome == OutcomeCompleted {
			completed++
			if r.Tokens == 0 || r.FirstTokNs == 0 {
				t.Errorf("req %d completed with no content tokens — the SSE parse did not see deltas", r.Index)
			}
		}
	}
	if completed < 1 {
		t.Fatalf("no request completed against %s — auth or body path broken", model)
	}
	t.Logf("send-skew gate: p99=%dus max=%dus pass=%v (client-validity on this host is informational for a smoke test)",
		res.Gates.SendSkewP99Us, res.Gates.SendSkewMaxUs, res.Gates.SendSkewPass)
}
