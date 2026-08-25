package campaign

import (
	"testing"
)

// A gate-invalid run keeps its §5 table row and stays out of every §7
// endpoint summary.
func TestSummariesExcludeGateInvalidRuns(t *testing.T) {
	eq := func(v float64) *float64 { return &v }
	arts := []Scalars{
		extractScalars(1, artWith(eq(10), nil, true, 0.1, 100, 5, true)),
		extractScalars(2, artWith(eq(9000), nil, true, 0.9, 9000, 500, false)),
	}
	sums, excluded := summarize(arts, "clean_delete")
	if excluded != 1 {
		t.Fatalf("want 1 excluded gate-invalid run, got %d", excluded)
	}
	for _, sc := range sums {
		if sc.ContributingN > 1 {
			t.Fatalf("%s summary must hold only the valid run, n=%d", sc.Name, sc.ContributingN)
		}
		if sc.Name == "ttr_equilibrium_s" && sc.Summary.Median != 10 {
			t.Fatalf("invalid run's 9000 leaked into the median: %v", sc.Summary.Median)
		}
	}
}
