package run_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/run"
)

// Execute refuses, before any request, a configuration it cannot run.
func TestCheckConfigRefusesWhatExecuteCannotRun(t *testing.T) {
	for _, ref := range []string{"../../configs/ac.reference.yaml", "../../configs/experiment.reference.yaml"} {
		cfg, err := config.LoadFile(ref)
		if err != nil {
			t.Fatal(err)
		}
		if err := run.CheckConfig(cfg); err != nil {
			t.Errorf("%s: %v", ref, err)
		}
	}

	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	defer srv.Close()
	pilot, err := config.LoadFile("../../configs/pilot.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	pilot.Target = pilot.Pilot.Targets[0]
	pilot.Target.EndpointURL = srv.URL + "/v1/chat/completions"
	literal := func() *config.Config {
		cfg, err := config.LoadFile("../../configs/ac.reference.yaml")
		if err != nil {
			t.Fatal(err)
		}
		cfg.Target.BaseURL = srv.URL
		cfg.Target.Body = `{"stream":true,"x":"{{case}}"}`
		return cfg
	}()
	for _, tc := range []struct {
		name string
		cfg  *config.Config
		want string
	}{
		{"pilot", pilot, "run: a pilot configuration runs under percentes-pilot only"},
		{"literal body", literal, "run: target.body needs one case text per request"},
	} {
		if err := run.CheckConfig(tc.cfg); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: CheckConfig %v", tc.name, err)
		}
		art, err := run.Execute(context.Background(), tc.cfg, run.Options{})
		if err == nil || art != nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: Execute %v, %v", tc.name, art, err)
		}
	}
	if n := hits.Load(); n != 0 {
		t.Errorf("%d requests sent", n)
	}
}
