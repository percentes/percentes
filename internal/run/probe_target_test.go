package run

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/detect"
	"github.com/percentes/percentes/internal/loadgen"
)

// The probes request the configured model with the token named by
// api_key_env, and know a hosted target.
func TestProbeTargetFromConfig(t *testing.T) {
	t.Setenv("PERCENTES_TEST_KEY", "synthetic-key-29sep")
	cfg := &config.Config{}
	cfg.Target.ModelName, cfg.Target.Hosted, cfg.Target.APIKeyEnv = "served-model", true, "PERCENTES_TEST_KEY"
	got := probeTargetFor(cfg)
	if got.Model != "served-model" || !got.Hosted || got.APIKey != "synthetic-key-29sep" {
		t.Fatalf("got %+v", got)
	}
	if got := probeTargetFor(&config.Config{}); got.APIKey != "" || got.Model != "" || got.Hosted {
		t.Fatalf("an unconfigured target carried %+v", got)
	}
}

// A failed probe leaves its segment unmeasured with the reason noted; a
// later success on another segment does not touch it.
func TestRecordProbeNotesTheFailure(t *testing.T) {
	d := detect.NewPhase0Decomposition()
	fire := time.Now()
	recordProbe(d, "traffic_restored", fire, time.Time{}, errors.New("probe: no recovery before deadline"))
	recordProbe(d, "replica_ready", fire, fire.Add(time.Second), nil)
	for _, s := range d.Segments {
		switch s.Name {
		case "traffic_restored":
			if s.Measured || !strings.HasPrefix(s.Note, "unmeasured: probe: no recovery") {
				t.Fatalf("failed probe recorded as %+v", s)
			}
		case "replica_ready":
			if !s.Measured || s.DurationS() == nil {
				t.Fatalf("successful probe recorded as %+v", s)
			}
		}
	}
}

// The survivor is the one baseline replica that is not the victim; no
// attribution, a second candidate or an unknown victim names none.
func TestSurvivorOf(t *testing.T) {
	reqs := []loadgen.Request{
		{IntendedNs: 61e9, Replica: "victim"}, {IntendedNs: 62e9, Replica: "other"},
		{IntendedNs: 400e9, Replica: "late"}, {IntendedNs: 63e9},
	}
	if got := survivorOf(reqs, 60e9, 330e9, "victim"); got != "other" {
		t.Fatalf("got %q, want other", got)
	}
	if got := survivorOf(reqs, 60e9, 330e9, ""); got != "" {
		t.Fatalf("an unknown victim named %q", got)
	}
	reqs = append(reqs, loadgen.Request{IntendedNs: 64e9, Replica: "third"})
	if got := survivorOf(reqs, 60e9, 330e9, "victim"); got != "" {
		t.Fatalf("two candidates named %q", got)
	}
}

// A replica-filtered window gets no receive-path or server-side entry.
func TestAttachObservationsSkipsReplicaWindows(t *testing.T) {
	art := &Artifacts{Loadgen: &loadgen.Result{}, Windows: map[string]*collect.Stats{
		"fault":          {Window: collect.Window{Name: "fault", StartNs: 0, EndNs: 10e9}},
		"fault_survivor": {Window: collect.Window{Name: "fault_survivor", StartNs: 0, EndNs: 10e9, Replica: "b"}},
	}}
	art.AttachObservations(Observed{})
	if _, ok := art.ReceivePath["fault"]; !ok {
		t.Fatal("the pooled window lost its receive-path report")
	}
	if _, ok := art.ReceivePath["fault_survivor"]; ok {
		t.Fatal("a replica-filtered window carried a receive-path report")
	}
}
