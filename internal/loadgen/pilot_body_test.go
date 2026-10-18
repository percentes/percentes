package loadgen

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

// pilotBodyCfg is the pilot reference with its first target as the run's
// target at endpoint, scaled to n requests at 10 rps.
func pilotBodyCfg(t *testing.T, endpoint string, n int) *config.Config {
	t.Helper()
	cfg, err := config.LoadFile("../../configs/pilot.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Target = cfg.Pilot.Targets[0]
	cfg.Target.EndpointURL = endpoint
	cfg.Load.RateRPS = 10
	cfg.Run.Phases = config.Phases{BaselineS: float64(n) / 10}
	t.Setenv(cfg.Target.APIKeyEnv, "pilot-body-test-key")
	return cfg
}

// A config with a pilot body posts that body, with each request's case
// text, to the target's complete endpoint URL.
func TestPilotBodyAndEndpoint(t *testing.T) {
	var mu sync.Mutex
	got := map[string]string{}
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		got[string(b)] = r.URL.Path
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, completeStream) //nolint:errcheck
	}))
	defer srv.Close()

	cfg := pilotBodyCfg(t, srv.URL+"/openai/v1/chat/completions", 3)
	cases := []string{"first ticket", "second \"ticket\"", "third <ticket>"}
	res, err := Run(context.Background(), cfg, &Hooks{Cases: cases})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Requests) != 3 || len(paths) != 3 {
		t.Fatalf("%d requests, %d received", len(res.Requests), len(paths))
	}
	for _, c := range cases {
		path, ok := got[string(cfg.Pilot.RequestBody(&cfg.Target, c))]
		if !ok {
			t.Fatalf("no request carried the body for %q; received %v", c, got)
		}
		if path != "/openai/v1/chat/completions" {
			t.Errorf("case %q posted to %s", c, path)
		}
	}
}

// A pilot-body run without one case text per scheduled request sends
// nothing.
func TestPilotBodyCaseCount(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits++ }))
	defer srv.Close()
	cfg := pilotBodyCfg(t, srv.URL+"/v1/chat/completions", 3)
	for _, hooks := range []*Hooks{nil, {Cases: []string{"a", "b"}}} {
		res, err := Run(context.Background(), cfg, hooks)
		if err == nil || res != nil || !strings.Contains(err.Error(), "case texts") {
			t.Errorf("hooks %+v: res %v, err %v", hooks, res, err)
		}
	}
	if hits != 0 {
		t.Errorf("%d requests sent", hits)
	}
}
