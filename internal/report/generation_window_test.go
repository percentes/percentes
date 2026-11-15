package report

import (
	"math"
	"slices"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
)

// In a window of a limit that counts output at generation, a send written
// before the window that may have ended in it counts as an earlier send:
// one whose prompt count is saturated counts as saturated, and one with
// no recorded price on a spend window leaves the window unresolved,
// naming it.
func TestGenerationWindowEarlierSends(t *testing.T) {
	at := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	old := at.Add(-61 * time.Second)
	most := math.MaxInt
	send := func(wrote time.Time, prompt *int) Send {
		return Send{Route: "azure", WroteUTC: &wrote, DispatchUTC: wrote, LatestUTC: at, EndUTC: at, Ceiling: 16, InputBound: 1200, PromptTokens: prompt}
	}
	for _, c := range []struct {
		name      string
		limit     config.Limit
		saturated int
		want      string
	}{
		{"saturated prompt", config.Limit{Dimension: config.LimitTPM, Value: 1e9, Marking: config.MarkingPublic, OutputAtGeneration: true}, 1, ""},
		{"no recorded price", config.Limit{Dimension: config.LimitSpendWindow, Value: 1, WindowS: 60, Marking: config.MarkingPublic, OutputAtGeneration: true}, 0,
			"1 sends written before the window that may have ended in it have no recorded price"},
	} {
		ix, err := newHistoryIndex(&History{Sends: []Send{send(old, &most), send(at, nil)}})
		if err != nil {
			t.Fatal(err)
		}
		cfg := &config.Config{}
		cfg.Target.Limits = []config.Limit{c.limit}
		r := &Refusal{WroteUTC: &at}
		if err := ix.attribute(r, cfg, make([]bool, 1), 0); err != nil {
			t.Fatal(err)
		}
		wc := r.Windows[0]
		if wc.Earlier != 1 || wc.EarlierSaturated != c.saturated || c.want != "" && !slices.Contains(wc.Unresolved, c.want) {
			t.Errorf("%s: window %+v", c.name, wc)
		}
	}
}

// The attribution names each public limit that counts output at
// generation and the hard timeout a send's end takes.
func TestAttributionNamesGenerationLimits(t *testing.T) {
	cfg, _ := stepsConfig(t)
	cfg.Target.Limits[1].OutputAtGeneration = true
	cfg, v := ruled(t, cfg, nil)
	epoch := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	cell := timedCell("weekday-12", epoch, sent(1, 1, 100), refused(1, 1.5, true))
	st, err := NewStepTables(cfg, v, []CellRecords{cell})
	if err != nil {
		t.Fatal(err)
	}
	h := &History{Group: "groq", Routes: []string{"groq"}, Sends: RecordSends("groq", "weekday-12", epoch, cell.Records, 1200, 90)}
	if err := st.Attribute(cfg, v, h); err != nil {
		t.Fatal(err)
	}
	if a := st.Attribution; !slices.Equal(a.AtGeneration, []string{cfg.Target.Limits[1].Dimension}) || a.HardTimeoutS != cfg.Client.HTTPTimeoutS || a.HardTimeoutS == 0 {
		t.Fatalf("attribution: at generation %q, hard timeout %d s", a.AtGeneration, a.HardTimeoutS)
	}
}

// A completed record's nonnegative reasoning count enters the step's
// reasoning figures.
func TestStepFiguresKeepReasoningTokens(t *testing.T) {
	reasoning, tokens := 40, 300
	tl := newStepTally()
	r := evidence.Record{IntendedNs: 1, FirstTokNs: 2, DoneNs: 3, InTime: true, CompletionTokens: &tokens, ReasoningTokens: &reasoning}
	if err := tl.timing.add(LineInTime, &r, 0); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(tl.reasoning, []int{40}) || !slices.Equal(tl.tokens, []int{300}) {
		t.Fatalf("tokens %v, reasoning %v", tl.tokens, tl.reasoning)
	}
}
