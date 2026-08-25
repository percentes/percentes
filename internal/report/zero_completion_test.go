package report

import (
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/collect"
)

// A fault window with no completed samples must not headline a measured
// p50 of 0 ms.
func TestHeadlineRefusesZeroCompletionP50(t *testing.T) {
	art := minimalArtifacts()
	base := &collect.Stats{}
	base.TTFTConditional.Count = 20
	base.TTFTConditional.P50Us = 20_000
	fault := &collect.Stats{} // zero completed samples
	art.Windows["baseline"] = base
	art.Windows["fault"] = fault

	_, humanText, err := Generate(art, nil)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(humanText, "to p50 0 ms") {
		t.Fatal("headline fabricated a measured 0 ms from an empty window")
	}
	if !strings.Contains(humanText, "to no completed samples (fault window)") {
		t.Fatalf("headline must name the empty window, got: %s", humanText[:300])
	}
}
