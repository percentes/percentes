package report

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/loadgen"
)

// withTokens sets the provider-reported completion tokens of the first n
// completions at step to tokens each.
func withTokens(c CellRecords, step, n, tokens int) CellRecords {
	for i := range c.Records {
		r := &c.Records[i]
		if n > 0 && !r.Warmup && r.Step == step && r.Outcome == loadgen.OutcomeCompleted {
			v := tokens
			r.CompletionTokens = &v
			n--
		}
	}
	return c
}

// SPEC-HOSTED §8 throughput per step: a cell step's completions, in time
// or late, over the configured step window, and the provider-reported
// completion tokens over it where completions carried them; none on a
// step not attempted.
func TestStepTablesThroughputCell(t *testing.T) {
	cfg, v := stepsConfig(t)
	cell := cellOf("weekday-00",
		outcome{1, LineInTime, 0, 100}, outcome{1, LineLate, 0, 20}, outcome{1, loadgen.ErrStatus429, 0, 4}, outcome{1, LineCensored, 0, 1},
		outcome{2, LineInTime, 0, 240}, outcome{3, LineUnsent, 0, 480}, outcome{4, LineUnsent, 0, 960}, outcome{5, LineUnsent, 0, 1920})
	cell = withTokens(cell, 1, 96, 250)
	st, err := NewStepTables(cfg, v, []CellRecords{cell})
	if err != nil {
		t.Fatal(err)
	}
	if st.StepS != 480 {
		t.Fatalf("step window %g s", st.StepS)
	}
	s := st.Cells[0].Steps
	tp := s[0].Throughput
	if tp == nil || tp.WindowS != 480 || tp.Completions != 120 || !near(tp.PerS, 0.25) || tp.TokenCompletions != 96 ||
		tp.Tokens != 24000 || tp.TokensPerS == nil || !near(*tp.TokensPerS, 50) {
		t.Fatalf("step 1 throughput %+v", tp)
	}
	if tp := s[1].Throughput; tp == nil || tp.Completions != 240 || !near(tp.PerS, 0.5) || tp.TokenCompletions != 0 || tp.TokensPerS != nil {
		t.Errorf("step 2 throughput %+v", tp)
	}
	if s[2].Throughput != nil {
		t.Errorf("step 3, not attempted: throughput %+v", s[2].Throughput)
	}
	text := st.Text()
	for _, frag := range []string{
		"  throughput over the step's configured 480 s window: 120 completions, 0.250 per second; 24000 provider-reported completion tokens from 96 of 120 completions, 50.00 per second\n",
		"  throughput over the step's configured 480 s window: 240 completions, 0.500 per second; no completion carried provider-reported completion tokens\n",
	} {
		if !strings.Contains(text, frag) {
			t.Errorf("text lacks %q:\n%s", frag, text)
		}
	}
	b, _ := json.Marshal(st.Cells[0].Steps[1])
	if !strings.Contains(string(b), `"throughput":{"window_s":480,"completions":240,"completions_per_s":0.5,"completions_with_tokens":0,"completion_tokens":0}`) {
		t.Errorf("step 2 JSON: %s", b)
	}

	cfg.Release.Ladder.StepS = 0
	if _, err := NewStepTables(cfg, v, []CellRecords{cell}); err == nil || !strings.Contains(err.Error(), "a step window of 0 s") {
		t.Errorf("no step window: %v", err)
	}
}

// Pooled throughput: the cells' completions over their summed windows,
// the range of the cells' completions per second, and the tokens of the
// completions that carried them.
func TestStepTablesThroughputPooled(t *testing.T) {
	cfg, v := stepsConfig(t)
	var cells []CellRecords
	for i := 0; i < 12; i++ {
		outs := []outcome{{1, LineInTime, 0, 120 - i}, {1, loadgen.ErrReset, 0, i}, {2, LineInTime, 0, 240}}
		if i < 10 {
			outs = append(outs, outcome{3, LineInTime, 0, 480})
		} else {
			outs = append(outs, outcome{3, LineUnsent, 0, 480})
		}
		outs = append(outs, outcome{4, LineUnsent, 0, 960}, outcome{5, LineUnsent, 0, 1920})
		c := cellOf(fmt.Sprintf("cell-%02d", i), outs...)
		if i < 3 {
			c = withTokens(c, 1, 10, 100)
		}
		cells = append(cells, c)
	}
	st, err := NewStepTables(cfg, v, cells)
	if err != nil {
		t.Fatal(err)
	}
	p := st.Pooled[0].Throughput
	if p == nil || p.WindowS != 12*480 || p.Completions != 1374 || !near(p.PerS, 1374.0/5760) || !near(p.CellMin, 109.0/480) ||
		!near(p.CellMax, 0.25) || p.TokenCompletions != 30 || p.Tokens != 3000 || p.TokensPerS == nil || !near(*p.TokensPerS, 3000.0/5760) {
		t.Fatalf("step 1 pooled throughput %+v", p)
	}
	if p := st.Pooled[2].Throughput; p == nil || p.WindowS != 10*480 || p.Completions != 4800 || !near(p.PerS, 1) || p.TokensPerS != nil {
		t.Errorf("step 3 pooled throughput %+v", p)
	}
	if p := st.Pooled[3].Throughput; p != nil {
		t.Errorf("step 4, excluded: pooled throughput %+v", p)
	}
	text := st.Text()
	for _, frag := range []string{
		"  throughput over 12 configured step windows of 480 s: 1374 completions, 0.239 per second; cell values 0.227 to 0.250 per second; 3000 provider-reported completion tokens from 30 of 1374 completions, 0.52 per second\n",
		"  throughput over 10 configured step windows of 480 s: 4800 completions, 1.000 per second; cell values 1.000 to 1.000 per second; no completion carried provider-reported completion tokens\n",
	} {
		if !strings.Contains(text, frag) {
			t.Errorf("text lacks %q:\n%s", frag, text)
		}
	}
}
