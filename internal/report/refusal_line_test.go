package report

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
)

// refusalLineFor is the refusal line of a refusal written at at, attributed
// from sends against limits.
func refusalLineFor(t *testing.T, limits []config.Limit, at time.Time, sends ...Send) (*Refusal, string) {
	t.Helper()
	ix, err := newHistoryIndex(&History{Sends: sends})
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{}
	cfg.Target.Limits = limits
	r := &Refusal{Index: 9, WroteUTC: &at, Status: 429}
	if err := ix.attribute(r, cfg, make([]bool, len(limits)), 0); err != nil {
		t.Fatal(err)
	}
	return r, refusalLine(r)
}

// A saturated prompt count makes a window count a lower bound, printed
// exactly as at least the sum; a send with no usable prompt count is
// named as such; the token amounts of an unresolved comparison carry
// their unit.
func TestRefusalLineWindowCountText(t *testing.T) {
	at := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	send := func(wrote, end time.Time, prompt *int) Send {
		return Send{Route: "azure", WroteUTC: &wrote, DispatchUTC: wrote, LatestUTC: end, EndUTC: end, PromptTokens: prompt, InputBound: 1200, Ceiling: 16}
	}
	most, small := math.MaxInt, 1000
	tpm := []config.Limit{{Dimension: config.LimitTPM, Value: 80000, Marking: config.MarkingPublic}}
	r, line := refusalLineFor(t, tpm, at, send(at, at, &most), send(at, at, &small), send(at, at, nil))
	if r.Label != LabelAbove || r.Windows[0].Saturated != 1 {
		t.Errorf("saturated: label %s, window %+v", r.Label, r.Windows[0])
	}
	want := "tokens_per_minute over 60 s: 3 requests, at least 9223372036854778055 tokens as counted in reported prompt tokens plus the ceiling " +
		"(1 at the input bound, no usable prompt count reported) (1 at a prompt count of at least 9223372036854775807) against a share of 80000 of a limit of 80000, as recorded"
	if !strings.Contains(line, want) {
		t.Errorf("line %q lacks %q", line, want)
	}

	old := at.Add(-61 * time.Second)
	out := []config.Limit{{Dimension: config.LimitOutputTPM, Value: 20, Marking: config.MarkingPublic, OutputAtGeneration: true}}
	r, line = refusalLineFor(t, out, at, send(old, at, nil), send(old, at, nil), send(at, at, nil))
	want = "output_tokens_per_minute over 60 s counts output at generation: with the 2 sends written before the window that may have ended in it, the window may hold 48 tokens against a share of 20 tokens"
	if r.Label != LabelUnresolved || !strings.Contains(line, want) || !strings.Contains(line, ", 48 tokens with the 2 sends written before it") {
		t.Errorf("label %s, line %q lacks %q", r.Label, line, want)
	}
}

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
