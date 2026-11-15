package report

import (
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

// A refusal line prints a window's bound at generation with its unit.
func TestRefusalLineAtGenerationBoundUnit(t *testing.T) {
	bound, spend := 87312.0, 0.25
	r := &Refusal{Label: LabelUnresolved, Status: 429, Windows: []WindowCount{
		{Dimension: config.LimitTPM, WindowS: 60, Requests: 41, Count: 18832, Earlier: 40, Bound: &bound, Counting: "as counted", Share: 80000, Value: 80000},
		{Dimension: config.LimitSpendWindow, WindowS: 60, Requests: 41, Count: 0.1, Earlier: 40, Bound: &spend, Share: 1, Value: 1},
	}}
	line := refusalLine(r)
	for _, want := range []string{"87312 tokens with the 40 sends written before it", "US$0.250000 with the 40 sends written before it"} {
		if !strings.Contains(line, want) {
			t.Errorf("missing %q in %s", want, line)
		}
	}
}
