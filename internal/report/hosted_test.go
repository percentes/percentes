package report

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/detect"
	"github.com/percentes/percentes/internal/run"
)

// sloArtifacts is minimalArtifacts with configuration file bytes, an SLO
// block, a pin, a window carrying goodput and a sweep point, a computed
// threshold analysis, and the mock variant's decomposition, which has a
// goodput_restored segment.
func sloArtifacts() *run.Artifacts {
	art := minimalArtifacts()
	art.Config.Raw = []byte("config file\n")
	art.Config.SLO = config.SLO{TTFTMs: 1000, E2EMs: 14000, Sweep: config.SLOSweep{TTFTMs: []int{800}, E2EMs: []int{12000}}}
	art.Config.Pins.VLLM.Version = "v0.0.0"
	art.Windows["baseline"] = &collect.Stats{
		Scheduled: 10, Completed: 10, ThroughputRPS: 1, GoodputRPS: 1, GoodputFrac: 1,
		GoodputSweep: []collect.SweepPoint{{TTFTMs: 800, E2EMs: 12000, GoodputFrac: 1}},
	}
	art.ThresholdAnalysis = collect.ThresholdAnalysis{Valid: true}
	art.Decomposition = detect.NewDecomposition(config.VariantMock)
	return art
}

func decodeReport(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// keysOf is every object key at any depth of v.
func keysOf(v any, into map[string]bool) {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			into[k] = true
			keysOf(e, into)
		}
	case []any:
		for _, e := range x {
			keysOf(e, into)
		}
	}
}

// A hosted report pair carries no conditional headline, goodput, SLO
// sweep, threshold analysis, goodput recovery detector, recovery
// decomposition or SPEC.md §6 pin, and the hosted caveat replaces the
// mock caveat (SPEC-HOSTED §9). The JSON is otherwise the self-hosted
// report's.
func TestHostedReportCarriesNoSLOFields(t *testing.T) {
	selfRaw, _, err := Generate(sloArtifacts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name      string
		setHosted func(*config.Config)
		wantCfg   func(map[string]any)
	}{
		{"hosted target", func(c *config.Config) { c.Target.Hosted = true },
			func(m map[string]any) { m["target"].(map[string]any)["hosted"] = true }},
		{"pilot profile", func(c *config.Config) { c.Profile = config.ProfilePilot },
			func(m map[string]any) { m["profile"] = string(config.ProfilePilot) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			art := sloArtifacts()
			c.setHosted(art.Config)
			raw, text, err := Generate(art, nil)
			if err != nil {
				t.Fatal(err)
			}
			got := decodeReport(t, raw)
			keys := map[string]bool{}
			keysOf(got, keys)
			for _, k := range []string{"conditional_headline", "detector", "decomposition", "threshold_analysis", "slo", "pins"} {
				if keys[k] {
					t.Errorf("report.json carries %q", k)
				}
			}
			for k := range keys {
				if strings.Contains(k, "sweep") {
					t.Errorf("report.json carries %q", k)
				}
			}
			if strings.Contains(string(raw), "goodput") {
				t.Error("report.json carries a goodput field")
			}

			want := decodeReport(t, selfRaw)
			delete(want, "conditional_headline")
			delete(want, "detector")
			delete(want, "decomposition")
			delete(want, "threshold_analysis")
			cfg := want["config"].(map[string]any)
			delete(cfg, "slo")
			delete(cfg, "pins")
			c.wantCfg(cfg)
			for _, w := range want["windows"].(map[string]any) {
				for _, k := range []string{"goodput_rps", "goodput_frac", "goodput_sweep"} {
					delete(w.(map[string]any), k)
				}
			}
			want["caveat"] = CaveatHosted
			pins := make([]any, len(HostedPinsNotVerifiable))
			for i, p := range HostedPinsNotVerifiable {
				pins[i] = p
			}
			want["pins_not_verifiable"] = pins
			if !reflect.DeepEqual(got, want) {
				g, _ := json.Marshal(got)
				w, _ := json.Marshal(want)
				t.Errorf("hosted report.json\n%s\nwant\n%s", g, w)
			}

			if n := strings.Count(text, CaveatHosted); n != 2 {
				t.Errorf("report.txt carries the hosted caveat %d times, want 2", n)
			}
			for _, s := range []string{Caveat, "goodput", "sweep", "threshold", "Conditional headline", "Recovery (two baselines", "Sensitivity table", "Recovery decomposition"} {
				if strings.Contains(text, s) {
					t.Errorf("report.txt carries %q:\n%s", s, text)
				}
			}
			for _, s := range []string{"throughput=1.00 rps\n", "not applicable or not verifiable against a hosted endpoint: ignore_eos and any output-length forcing; "} {
				if !strings.Contains(text, s) {
					t.Errorf("report.txt lacks %q:\n%s", s, text)
				}
			}
		})
	}
}

// A self-hosted report keeps every SLO field and the mock caveat.
func TestSelfHostedReportKeepsSLOFields(t *testing.T) {
	raw, text, err := Generate(sloArtifacts(), nil)
	if err != nil {
		t.Fatal(err)
	}
	got := decodeReport(t, raw)
	keys := map[string]bool{}
	keysOf(got, keys)
	for _, k := range []string{"conditional_headline", "detector", "decomposition", "threshold_analysis", "slo", "pins", "goodput_rps", "goodput_frac", "goodput_sweep"} {
		if !keys[k] {
			t.Errorf("report.json lacks %q", k)
		}
	}
	if keys["pins_not_verifiable"] || got["caveat"] != Caveat {
		t.Errorf("caveat %q, pins_not_verifiable present %v", got["caveat"], keys["pins_not_verifiable"])
	}
	for _, s := range []string{"== Conditional headline", "goodput-versus-threshold sweep (§4):", "Modal during-fault latency vs thresholds", "Recovery (two baselines", "Recovery decomposition", "goodput_restored", "== Environment pins (§6) =="} {
		if !strings.Contains(text, s) {
			t.Errorf("report.txt lacks %q", s)
		}
	}
	if strings.Contains(text, CaveatHosted) {
		t.Error("report.txt carries the hosted caveat")
	}
}
