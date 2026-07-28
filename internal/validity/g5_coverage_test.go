package validity

import (
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

// G5 requires equality ACROSS replicas (§10): a fingerprint set covering
// only one of two replicas is an incomplete observation and must fail —
// never pass vacuously because the present values happen to match.
func TestG5RequiresReplicaCoverage(t *testing.T) {
	obs := Observations{GPUFingerprints: []GPUFingerprint{
		{Replica: "a", Run: 1, Fingerprint: "1410MHz/300W"},
		{Replica: "a", Run: 2, Fingerprint: "1410MHz/300W"},
	}}
	g5 := gateByID(Evaluate(cleanArt(config.VariantCleanDelete), obs), "G5")
	if g5.Pass || g5.Observed {
		t.Fatalf("G5 must fail on incomplete replica coverage: %+v", g5)
	}
	if !strings.Contains(g5.Detail, "1 of 2") {
		t.Errorf("G5 detail must name the coverage gap: %q", g5.Detail)
	}
}
