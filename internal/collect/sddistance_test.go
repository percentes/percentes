package collect

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

// A zero baseline SD mandates signed infinity (§4); the artifact must
// still marshal, as the strings "+inf" / "-inf".
func TestThresholdAnalysisMarshalsInfinity(t *testing.T) {
	cfg := &config.Config{}
	cfg.SLO.Sweep.TTFTMs = []int{1000}
	cfg.SLO.Sweep.E2EMs = []int{14000}
	baseline := &Stats{RawTTFTUs: []int64{1_000_000, 1_000_000}, RawE2EUs: []int64{2_000_000, 2_000_000}}
	fault := &Stats{RawTTFTUs: []int64{5_000_000}, RawE2EUs: []int64{6_000_000}}

	ta := AnalyzeThresholds(cfg, baseline, fault)
	raw, err := json.Marshal(ta)
	if err != nil {
		t.Fatalf("threshold analysis must marshal with infinite distances: %v", err)
	}
	if !strings.Contains(string(raw), `"+inf"`) && !strings.Contains(string(raw), `"-inf"`) {
		t.Fatalf("expected a signed-infinity string in %s", raw)
	}
	var back ThresholdAnalysis
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("round-trip: %v", err)
	}
}
