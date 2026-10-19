package report

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
)

func near(a, b float64) bool { return math.Abs(a-b) <= 1e-9*math.Max(1, math.Abs(b)) }

func repeat(v, n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = v
	}
	return out
}

// The 95 percent Wilson interval: 0 of n has upper bound z²/(n+z²), and
// 12 of 1,440 is 0.48 to 1.45 percent.
func TestWilson(t *testing.T) {
	for _, n := range []float64{120, 1440, 23040} {
		lo, hi := wilson(0, n)
		if !near(lo, 0) || !near(hi, z95*z95/(n+z95*z95)) {
			t.Errorf("0 of %g: [%g, %g]", n, lo, hi)
		}
	}
	if lo, hi := wilson(12.0/1440, 1440); !near(lo, 0.004773400685629972) || !near(hi, 0.014509505141764583) {
		t.Errorf("12 of 1440: [%g, %g]", lo, hi)
	}
	if lo, hi := wilson(1, 50); !near(hi, 1) || hi > 1 || lo < 0.9 {
		t.Errorf("50 of 50: [%g, %g]", lo, hi)
	}
}

// Twelve cells of 120: one event in each cell
// gives DEFF 1; all twelve in one cell give DEFF 1440/119 and an
// effective size of 119, with the interval 0.15 to 4.60 percent. A
// spread that cannot be estimated gives DEFF 1, marked assumed.
func TestDesignEffect(t *testing.T) {
	n := repeat(120, 12)
	if d, assumed := designEffect(repeat(1, 12), n); d != 1 || assumed {
		t.Errorf("one event per cell: DEFF %g, assumed %v", d, assumed)
	}
	p := newPooled(append([]int{12}, make([]int, 11)...), n)
	switch {
	case !near(p.DEFF, 1440.0/119), !near(p.EffectiveN, 119), p.AssumesIndependence:
		t.Errorf("all in one cell: %+v", p)
	case !near(p.Lo, 0.0014639412078975636), !near(p.Hi, 0.04595321149995117):
		t.Errorf("all in one cell: interval [%g, %g]", p.Lo, p.Hi)
	case p.Count != 12 || p.Scheduled != 1440 || p.Cells != 12 || p.CellsWithEvents != 1 || p.CellMin != 0 || p.CellMax != 0.1:
		t.Errorf("all in one cell: %+v", p)
	}
	for name, c := range map[string]struct{ k, n []int }{
		"no event":        {make([]int, 12), n},
		"every request":   {n, n},
		"one cell":        {[]int{3}, []int{120}},
		"no cells at all": {nil, nil},
	} {
		if d, assumed := designEffect(c.k, c.n); d != 1 || !assumed {
			t.Errorf("%s: DEFF %g, assumed %v", name, d, assumed)
		}
	}
	// Unequal cells: the spread below independence clips to 1.
	if d, assumed := designEffect([]int{1, 2}, []int{100, 200}); d != 1 || assumed {
		t.Errorf("proportional cells: DEFF %g, assumed %v", d, assumed)
	}
}

// stepsConfig is the release reference with documented 429 and 529
// statuses. At 1,712 tokens a request against 250,000 tokens per minute,
// steps 1 to 3 are admitted and steps 4 and 5 are not.
func stepsConfig(t *testing.T) (*config.Config, *admission.Verdict) {
	t.Helper()
	cfg, err := config.LoadFile("../../configs/release.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Target.StatusDocs = []config.StatusDoc{
		{Status: 429, Meaning: "rate limit reached", Source: "https://example.test/errors", Retrieved: "2026-10-07"},
		{Status: 529, Meaning: "overloaded", Source: "errors.png", Retrieved: "2026-10-07"},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	v, err := admission.CeilingRule(cfg, admission.Price{InputUSDPerMTok: "0.15", OutputUSDPerMTok: "0.6", Source: "https://example.test/pricing", Retrieved: "2026-10-07"}, admission.ExpectedPlan(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if got := v.Admitted(); !slices.Equal(got, []float64{0.25, 0.5, 1}) {
		t.Fatalf("admitted steps %v", got)
	}
	return cfg, v
}

// outcome is n records of one line at one step; status is a
// status_other record's HTTP status.
type outcome struct {
	step   int
	line   string
	status int
	n      int
}

// cellOf builds a cell's records: a warm-up record that fits no line,
// the outcomes in order, and every step listed in unsent as unsent
// requests (count n). A completion or error ends 1 ns after intended
// dispatch.
func cellOf(label string, outs ...outcome) CellRecords {
	c := CellRecords{Cell: label, Outcome: evidence.CellCounted}
	add := func(r evidence.Record) {
		r.Index, r.Shape, r.CeilingTokens = int64(len(c.Records)), "S", 512
		c.Records = append(c.Records, r)
	}
	add(evidence.Record{Warmup: true, Step: 1, DispatchNs: 1, Outcome: loadgen.OutcomeErrored, ErrClass: "bogus"})
	for _, o := range outs {
		for i := 0; i < o.n; i++ {
			r := evidence.Record{Step: o.step, DispatchNs: 1}
			switch o.line {
			case LineInTime:
				r.Outcome, r.InTime, r.FirstTokNs, r.DoneNs = loadgen.OutcomeCompleted, true, 1, 1
			case LineLate:
				r.Outcome, r.Late, r.FirstTokNs, r.DoneNs = loadgen.OutcomeCompleted, true, 1, 1
			case LineCensored:
				r.Outcome = loadgen.OutcomeCensored
			case LineUnsent:
				r.DispatchNs = 0
			default:
				r.Outcome, r.ErrClass, r.Status, r.DoneNs = loadgen.OutcomeErrored, o.line, o.status, 1
				if o.line == loadgen.ErrStatus429 {
					r.Status = 429
				}
			}
			add(r)
		}
	}
	return c
}

func lineAt(t *testing.T, lines []Line, name string, status int) Line {
	t.Helper()
	for _, l := range lines {
		if l.Name == name && (l.Status == status || name == loadgen.ErrStatus429) {
			return l
		}
	}
	t.Fatalf("no line %s %d", name, status)
	return Line{}
}

// One cell: every line counted over the step's scheduled requests with
// its Wilson interval, status lines per code with the documented
// meaning, hosted-G6 with and without connect and reset, the step states,
// and no pooled figures for an incomplete frame.
func TestStepTablesCell(t *testing.T) {
	cfg, v := stepsConfig(t)
	cell := cellOf("weekday-00",
		outcome{1, LineInTime, 0, 100}, outcome{1, LineLate, 0, 3}, outcome{1, loadgen.ErrStatus429, 0, 2},
		outcome{1, loadgen.ErrStatusOther, 529, 2}, outcome{1, loadgen.ErrStatusOther, 503, 1},
		outcome{1, loadgen.ErrConnect, 0, 1}, outcome{1, loadgen.ErrReset, 0, 1}, outcome{1, loadgen.ErrEmptyStream, 0, 1},
		outcome{1, loadgen.ErrMalformedStream, 0, 1}, outcome{1, LineCensored, 0, 2},
		outcome{2, LineInTime, 0, 238}, outcome{2, LineUnsent, 0, 2},
		outcome{3, LineUnsent, 0, 470}, outcome{4, LineUnsent, 0, 950}, outcome{5, LineUnsent, 0, 1900},
	)
	st, err := NewStepTables(cfg, v, []CellRecords{cell})
	if err != nil {
		t.Fatal(err)
	}
	if st.Pooled != nil || st.FrameCells != 12 || len(st.Cells) != 1 || !reflect.DeepEqual(st.Limits, v.Steps) {
		t.Fatalf("tables: pooled %v, frame %d, cells %d", st.Pooled, st.FrameCells, len(st.Cells))
	}
	steps := st.Cells[0].Steps
	var states []string
	for _, s := range steps {
		states = append(states, s.State)
	}
	if want := []string{StepAttempted, StepAttempted, StepNotAttempted, StepExcluded, StepExcluded}; !slices.Equal(states, want) {
		t.Errorf("states %q, want %q", states, want)
	}
	s := steps[0]
	if s.Scheduled != 114 || s.RPS != 0.25 {
		t.Fatalf("step 1: %d scheduled at %g rps", s.Scheduled, s.RPS)
	}
	var names []string
	for _, l := range s.Lines {
		names = append(names, fmt.Sprint(l.Name, l.Status, l.InBand, l.Finish))
	}
	want := []string{"completed_in_time0", "completed_late0", "completed0length", "completed0(none)", "status_429429", "status_other0",
		"status_other503", "status_other529", "connect0", "reset0", "empty_stream0", "empty_stream0(none)", "malformed_stream0",
		"malformed_stream0(none)", "censored0", "unsent0"}
	if !slices.Equal(names, want) {
		t.Errorf("lines %q, want %q", names, want)
	}
	sum := 0
	for _, l := range s.Lines {
		if l.Status == 0 && l.InBand == "" && l.Finish == "" || l.Name == loadgen.ErrStatus429 {
			sum += l.Count
		}
		lo, hi := wilson(float64(l.Count)/114, 114)
		if l.Scheduled != 114 || !near(l.Value, float64(l.Count)/114) || l.Lo != lo || l.Hi != hi {
			t.Errorf("line %s %d: %+v", l.Name, l.Status, l.Rate)
		}
	}
	if sum != 114 {
		t.Errorf("lines sum to %d of 114", sum)
	}
	for _, c := range []struct {
		name   string
		status int
		count  int
		doc    string
	}{
		{LineInTime, 0, 100, ""}, {LineLate, 0, 3, ""}, {loadgen.ErrStatus429, 429, 2, "rate limit reached"},
		{loadgen.ErrStatusOther, 0, 3, ""}, {loadgen.ErrStatusOther, 529, 2, "overloaded"}, {loadgen.ErrStatusOther, 503, 1, ""},
		{loadgen.ErrConnect, 0, 1, ""}, {LineCensored, 0, 2, ""}, {LineUnsent, 0, 0, ""},
	} {
		l := lineAt(t, s.Lines, c.name, c.status)
		doc := ""
		if l.Doc != nil {
			doc = l.Doc.Meaning
		}
		if l.Count != c.count || doc != c.doc {
			t.Errorf("line %s %d: count %d documented %q, want %d %q", c.name, c.status, l.Count, doc, c.count, c.doc)
		}
	}
	g := s.G6
	if g == nil || g.InTime != newRate(100, 114) || g.Pass || g.ExcludingConnectReset == nil || *g.ExcludingConnectReset != newRate(100, 112) {
		t.Errorf("hosted-G6 %+v", g)
	}
	if s2 := steps[1]; s2.Scheduled != 240 || !s2.G6.Pass || lineAt(t, s2.Lines, LineUnsent, 0).Count != 2 {
		t.Errorf("step 2: %+v", s2)
	}
	if s3 := steps[2]; s3.Scheduled != 470 || s3.Lines != nil || s3.G6 != nil {
		t.Errorf("step 3: %+v", s3)
	}

	text := st.Text()
	for _, frag := range []string{
		"== Per-step tables: route groq, ceiling 512 tokens, deadline 30 s ==",
		"step 1, 0.25 rps: admitted; requests_per_minute 1.5% of 1000 (half test: pass); tokens_per_minute 10.3% of 250000 (half test: pass); route_cap_usd ",
		"step 4, 2 rps: excluded, binding limit tokens_per_minute; requests_per_minute 12.0% of 1000 (half test: pass); tokens_per_minute 82.2% of 250000 (half test: FAIL)",
		"-- Cell weekday-00: 95 percent Wilson intervals, treating the step's requests as independent --",
		"step 1, 0.25 rps, 114 scheduled:",
		fmt.Sprintf("  completed_in_time      100/114 87.72%% [%.2f%%, %.2f%%]\n", 100*newRate(100, 114).Lo, 100*newRate(100, 114).Hi),
		"  status_429             2/114 1.75% [",
		"; 429 documented as \"rate limit reached\" (https://example.test/errors, retrieved 2026-10-07)\n",
		"  status_other 503       1/114 0.88% [",
		"; 503: no documented meaning recorded\n",
		"; 529 documented as \"overloaded\" (errors.png, retrieved 2026-10-07)\n",
		"  hosted-G6 (completion within 30 s over scheduled): 87.72% [",
		"against 0.99: FAIL (reported, not run-invalidating)",
		"  hosted-G6 excluding connect and reset: 100/112 89.29% [",
		"step 3, 1 rps: not attempted (the cell's ladder ended before this step)",
		"step 5, 4 rps: excluded by the ceiling rule",
		"-- Incomplete frame: 1 of 12 cells; no pooled figures --",
	} {
		if !strings.Contains(text, frag) {
			t.Errorf("text lacks %q:\n%s", frag, text)
		}
	}
	if strings.Contains(text, "Pooled") || strings.Contains(text, "DEFF") {
		t.Errorf("an incomplete frame prints pooled figures:\n%s", text)
	}
}

// A complete frame pools each step over the cells that attempted it:
// the count over the pooled denominator, the cell-rate range, the cells
// with events and the DEFF-adjusted interval, per status as well; a line
// no cell saw assumes independent requests; hosted-G6 is judged on the
// pooled estimate. Cell order does not change the tables.
func TestStepTablesPooled(t *testing.T) {
	cfg, v := stepsConfig(t)
	var cells []CellRecords
	for i := 0; i < 12; i++ {
		var outs []outcome
		if i == 0 {
			outs = append(outs, outcome{1, loadgen.ErrStatus429, 0, 12}, outcome{1, LineInTime, 0, 108})
		} else {
			outs = append(outs, outcome{1, LineInTime, 0, 120})
		}
		outs = append(outs, outcome{2, loadgen.ErrStatusOther, 529, 1}, outcome{2, LineInTime, 0, 239})
		if i < 10 {
			outs = append(outs, outcome{3, LineInTime, 0, 480})
		} else {
			outs = append(outs, outcome{3, LineUnsent, 0, 480})
		}
		outs = append(outs, outcome{4, LineUnsent, 0, 960}, outcome{5, LineUnsent, 0, 1920})
		cells = append(cells, cellOf(fmt.Sprintf("cell-%02d", i), outs...))
	}
	st, err := NewStepTables(cfg, v, cells)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Pooled) != 5 {
		t.Fatalf("%d pooled steps", len(st.Pooled))
	}
	pooledAt := func(k int, name string, status int) PooledLine {
		for _, l := range st.Pooled[k].Lines {
			if l.Name == name && (l.Status == status || name == loadgen.ErrStatus429) {
				return l
			}
		}
		t.Fatalf("step %d: no pooled line %s %d", k+1, name, status)
		return PooledLine{}
	}

	p1 := st.Pooled[0]
	if p1.Cells != 12 || p1.Scheduled != 1440 || !p1.Admitted {
		t.Errorf("step 1: %+v", p1)
	}
	l := pooledAt(0, loadgen.ErrStatus429, 429)
	if l.Count != 12 || !near(l.DEFF, 1440.0/119) || !near(l.Lo, 0.0014639412078975636) || !near(l.Hi, 0.04595321149995117) ||
		l.CellsWithEvents != 1 || l.CellMax != 0.1 || l.CellMin != 0 || l.AssumesIndependence || l.Doc == nil {
		t.Errorf("step 1 status_429: %+v", l)
	}
	if c := pooledAt(0, LineCensored, 0); c.Count != 0 || !c.AssumesIndependence || c.DEFF != 1 || !near(c.Lo, 0) || !near(c.Hi, z95*z95/(1440+z95*z95)) {
		t.Errorf("step 1 censored: %+v", c)
	}
	if g := p1.G6; g == nil || g.InTime.Count != 1428 || !g.Pass || g.ExcludingConnectReset == nil || g.ExcludingConnectReset.Scheduled != 1440 {
		t.Errorf("step 1 hosted-G6: %+v", g)
	}

	l = pooledAt(1, loadgen.ErrStatusOther, 529)
	lo, hi := wilson(12.0/2880, 2880)
	if l.Count != 12 || l.Scheduled != 2880 || l.DEFF != 1 || l.AssumesIndependence || l.Lo != lo || l.Hi != hi || l.CellsWithEvents != 12 || l.Doc.Meaning != "overloaded" {
		t.Errorf("step 2 status_other 529: %+v", l)
	}
	if p3 := st.Pooled[2]; p3.Cells != 10 || p3.Scheduled != 4800 || p3.G6.InTime.Cells != 10 || !p3.G6.Pass || !p3.G6.InTime.AssumesIndependence {
		t.Errorf("step 3: %+v", p3)
	}
	for k := 3; k < 5; k++ {
		if p := st.Pooled[k]; p.Admitted || p.Cells != 0 || p.Lines != nil || p.G6 != nil {
			t.Errorf("step %d: %+v", k+1, p)
		}
	}

	text := st.Text()
	for _, frag := range []string{
		"-- Pooled per step over the frame's 12 cells: each step pools the cells that attempted it --",
		"step 1, 0.25 rps, 12 of 12 cells, 1440 scheduled:",
		"  status_429             12/1440 0.83%; cell rates 0.00% to 10.00%; 1 of 12 cells with events; DEFF 12.10; effective n 119.0; 95% Wilson [0.15%, 4.60%]; 429 documented as",
		"  censored               0/1440 0.00%; cell rates 0.00% to 0.00%; 0 of 12 cells with events; DEFF 1.00, assumes independent requests; effective n 1440.0; 95% Wilson [0.00%, 0.27%]",
		"step 3, 1 rps, 10 of 12 cells, 4800 scheduled:",
		"step 4, 2 rps: excluded by the ceiling rule",
	} {
		if !strings.Contains(text, frag) {
			t.Errorf("text lacks %q:\n%s", frag, text)
		}
	}

	reversed := slices.Clone(cells)
	slices.Reverse(reversed)
	again, err := NewStepTables(cfg, v, reversed)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(st)
	b, _ := json.Marshal(again)
	if string(a) != string(b) || st.Cells[0].Cell != "cell-00" {
		t.Error("cell order changed the tables")
	}
}

// finishCell is a cell whose step 1 holds, per finish reason, n[reason]
// completions in time, late[reason] late completions and empty[reason]
// empty streams, and whose step 2 is 240 completions in time with no
// finish reason; its later steps are unsent.
func finishCell(label string, n, late, empty map[string]int) CellRecords {
	var outs []outcome
	var reasons []string
	for _, m := range []map[string]int{n, late, empty} {
		for reason := range m {
			reasons = append(reasons, reason)
		}
	}
	slices.Sort(reasons)
	reasons = slices.Compact(reasons)
	for _, reason := range reasons {
		outs = append(outs, outcome{1, LineInTime, 0, n[reason]}, outcome{1, LineLate, 0, late[reason]},
			outcome{1, loadgen.ErrEmptyStream, 0, empty[reason]})
	}
	outs = append(outs, outcome{2, LineInTime, 0, 240}, outcome{3, LineUnsent, 0, 480}, outcome{4, LineUnsent, 0, 960},
		outcome{5, LineUnsent, 0, 1920})
	c := cellOf(label, outs...)
	i := 1
	for _, reason := range reasons {
		for _, k := range []int{n[reason], late[reason], empty[reason]} {
			for j := 0; j < k; j++ {
				c.Records[i].FinishReason = reason
				i++
			}
		}
	}
	return c
}

// HA3 point 8: the empty_stream line splits by finish reason, the
// completions at the adapter's length value publish on their own line,
// and a completion with no finish reason is noted as unknown
// termination; per cell and pooled, and without changing the index's
// lines.
func TestStepTablesFinishReasons(t *testing.T) {
	finishLines := func(lines []Line) map[string]int {
		out := map[string]int{}
		for _, l := range lines {
			if l.Finish != "" {
				out[l.Name+" "+l.Finish] = l.Count
			}
		}
		return out
	}
	cfg, v := stepsConfig(t)
	cell := finishCell("weekday-00",
		map[string]int{"stop": 5, "length": 2, "": 1, "max_tokens": 1},
		map[string]int{"length": 1, "": 1},
		map[string]int{"length": 3, "stop": 1, "": 2})
	st, err := NewStepTables(cfg, v, []CellRecords{cell})
	if err != nil {
		t.Fatal(err)
	}
	s := st.Cells[0].Steps[0]
	if st.LengthValue != "length" || s.Scheduled != 17 {
		t.Fatalf("length value %q, %d scheduled", st.LengthValue, s.Scheduled)
	}
	var order []string
	for _, l := range s.Lines {
		if l.Finish != "" {
			order = append(order, l.Name+" "+l.Finish)
			if l.Rate != newRate(l.Count, 17) {
				t.Errorf("%s %s: %+v", l.Name, l.Finish, l.Rate)
			}
		}
	}
	if want := []string{"completed length", "completed (none)", "empty_stream (none)", "empty_stream length", "empty_stream stop"}; !slices.Equal(order, want) {
		t.Errorf("finish lines %q, want %q", order, want)
	}
	if got, want := finishLines(s.Lines), map[string]int{"completed length": 3, "completed (none)": 2,
		"empty_stream (none)": 2, "empty_stream length": 3, "empty_stream stop": 1}; !reflect.DeepEqual(got, want) {
		t.Errorf("finish counts %v, want %v", got, want)
	}
	if l := lineAt(t, s.Lines, loadgen.ErrEmptyStream, 0); l.Count != 6 || l.Finish != "" {
		t.Errorf("empty_stream %+v", l)
	}
	if got := finishLines(st.Cells[0].Steps[1].Lines); !reflect.DeepEqual(got, map[string]int{"completed length": 0, "completed (none)": 240}) {
		t.Errorf("step 2 finish counts %v", got)
	}
	text := st.Text()
	for _, frag := range []string{
		"  completed, finish reason length (stopped at the ceiling) 3/17 17.65% [",
		"  completed, no finish reason (unknown termination) 2/17 11.76% [",
		"  empty_stream           6/17 35.29% [",
		"  empty_stream, no finish reason 2/17 11.76% [",
		"  empty_stream, finish reason length (stopped at the ceiling) 3/17 17.65% [",
		"  empty_stream, finish reason stop 1/17 5.88% [",
	} {
		if !strings.Contains(text, frag) {
			t.Errorf("text lacks %q:\n%s", frag, text)
		}
	}
	if b, _ := json.Marshal(st); !strings.Contains(string(b), `"length_value":"length"`) ||
		!strings.Contains(string(b), `{"name":"empty_stream","finish_reason":"stop","count":1,`) {
		t.Errorf("JSON lacks the length value or a finish line: %s", b)
	}

	// A Messages route's length value is max_tokens.
	cfg.Target.Adapter = config.AdapterMessages
	st, err = NewStepTables(cfg, v, []CellRecords{cell})
	if err != nil {
		t.Fatal(err)
	}
	if got := finishLines(st.Cells[0].Steps[0].Lines); st.LengthValue != "max_tokens" || got["completed max_tokens"] != 1 || got["completed length"] != 0 {
		t.Errorf("messages: length value %q, finish counts %v", st.LengthValue, got)
	}
	if text := st.Text(); !strings.Contains(text, "  completed, finish reason max_tokens (stopped at the ceiling) 1/17 5.88% [") ||
		!strings.Contains(text, "  empty_stream, finish reason length 3/17 17.65% [") {
		t.Errorf("messages text:\n%s", text)
	}

	// Pooled: the union of the cells' finish reasons, each pooled over
	// every cell that attempted the step.
	cfg.Target.Adapter = config.AdapterChatCompletions
	cells := []CellRecords{cell}
	for i := 1; i < 12; i++ {
		empty := map[string]int{"stop": 1}
		if i == 1 {
			empty = map[string]int{"content_filter": 2}
		}
		cells = append(cells, finishCell(fmt.Sprintf("weekday-%02d", i), map[string]int{"stop": 15}, nil, empty))
	}
	st, err = NewStepTables(cfg, v, cells)
	if err != nil {
		t.Fatal(err)
	}
	p := st.Pooled[0]
	var pooled []string
	for _, l := range p.Lines {
		if l.Finish != "" {
			pooled = append(pooled, fmt.Sprintf("%s %s %d/%d %d", l.Name, l.Finish, l.Count, l.Scheduled, l.CellsWithEvents))
		}
	}
	if want := []string{"completed length 3/194 1", "completed (none) 2/194 1", "empty_stream (none) 2/194 1",
		"empty_stream content_filter 2/194 1", "empty_stream length 3/194 1", "empty_stream stop 11/194 11"}; !slices.Equal(pooled, want) {
		t.Errorf("pooled finish lines %q, want %q", pooled, want)
	}
	if text := st.Text(); !strings.Contains(text, "  empty_stream, finish reason content_filter 2/194 1.03%; cell rates 0.00% to 11.76%; 1 of 12 cells with events;") {
		t.Errorf("pooled text:\n%s", text)
	}
	ix, err := NewIndex([]IndexRoute{{Config: cfg, Verdict: v, Cells: cells, Page: "routes/groq.html"}})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, c := range ix.Rows[0].Counts {
		names = append(names, c.Line)
		if c.Line == loadgen.ErrEmptyStream && c.Count != 18 {
			t.Errorf("index empty_stream %d, want 18", c.Count)
		}
	}
	if !slices.Equal(names, stepLines) {
		t.Errorf("index lines %q, want %q", names, stepLines)
	}
}

// Inputs the tables cannot count are refused with the reason named.
func TestStepTablesRefusals(t *testing.T) {
	good := func() []CellRecords {
		return []CellRecords{cellOf("a", outcome{1, LineInTime, 0, 3}, outcome{4, LineUnsent, 0, 2})}
	}
	for _, c := range []struct {
		name   string
		mutate func(*config.Config, *admission.Verdict, *[]CellRecords) *admission.Verdict
		want   string
	}{
		{"not release", func(cfg *config.Config, v *admission.Verdict, _ *[]CellRecords) *admission.Verdict {
			cfg.Profile = config.ProfilePilot
			return v
		}, "not a release configuration"},
		{"no verdict", func(_ *config.Config, _ *admission.Verdict, _ *[]CellRecords) *admission.Verdict { return nil }, "no ceiling-rule verdict"},
		{"verdict route", func(_ *config.Config, v *admission.Verdict, _ *[]CellRecords) *admission.Verdict {
			v.Route = "other"
			return v
		}, `verdict for route "other" at 512, the configuration's "groq" at 512`},
		{"verdict ceiling", func(_ *config.Config, v *admission.Verdict, _ *[]CellRecords) *admission.Verdict {
			v.CeilingTokens = 1024
			return v
		}, "at 1024, the configuration's"},
		{"verdict steps", func(_ *config.Config, v *admission.Verdict, _ *[]CellRecords) *admission.Verdict {
			v.Steps = v.Steps[:4]
			return v
		}, "verdict has 4 steps, the ladder 5"},
		{"verdict rate", func(_ *config.Config, v *admission.Verdict, _ *[]CellRecords) *admission.Verdict {
			v.Steps[2].RPS = 1.5
			return v
		}, "verdict step 3 is 1.5 rps, the ladder's 1"},
		{"no binding", func(_ *config.Config, v *admission.Verdict, _ *[]CellRecords) *admission.Verdict {
			v.Steps[4].Binding = nil
			return v
		}, "verdict step 5 is excluded with no binding limit"},
		{"thirteen cells", func(_ *config.Config, v *admission.Verdict, cs *[]CellRecords) *admission.Verdict {
			for i := 0; i < 12; i++ {
				*cs = append(*cs, cellOf(fmt.Sprint(i)))
			}
			return v
		}, "13 cells, a frame holds 12"},
		{"labelled cell", func(_ *config.Config, v *admission.Verdict, cs *[]CellRecords) *admission.Verdict {
			(*cs)[0].Outcome = evidence.CellStopped
			return v
		}, `cell "a" is "stopped": a labelled cell never enters a route's frame`},
		{"no outcome", func(_ *config.Config, v *admission.Verdict, cs *[]CellRecords) *admission.Verdict {
			(*cs)[0].Outcome = ""
			return v
		}, `cell "a" is "": a labelled cell never enters a route's frame`},
		{"no label", func(_ *config.Config, v *admission.Verdict, cs *[]CellRecords) *admission.Verdict {
			(*cs)[0].Cell = ""
			return v
		}, "a cell has no label"},
		{"twice", func(_ *config.Config, v *admission.Verdict, cs *[]CellRecords) *admission.Verdict {
			*cs = append(*cs, cellOf("a"))
			return v
		}, `cell "a" given twice`},
		{"shape", func(_ *config.Config, v *admission.Verdict, cs *[]CellRecords) *admission.Verdict {
			(*cs)[0].Records[1].Shape = "L"
			return v
		}, `cell "a": record 1 is shape "L" at 512, the configuration's "S" at 512`},
		{"ceiling", func(_ *config.Config, v *admission.Verdict, cs *[]CellRecords) *admission.Verdict {
			(*cs)[0].Records[0].CeilingTokens = 1024
			return v
		}, `record 0 is shape "S" at 1024`},
		{"step 0", func(_ *config.Config, v *admission.Verdict, cs *[]CellRecords) *admission.Verdict {
			(*cs)[0].Records[1].Step = 0
			return v
		}, "record 1 is at step 0 of a 5-step ladder"},
		{"step 6", func(_ *config.Config, v *admission.Verdict, cs *[]CellRecords) *admission.Verdict {
			(*cs)[0].Records[2].Step = 6
			return v
		}, "record 2 is at step 6 of a 5-step ladder"},
		{"completed neither", func(_ *config.Config, v *admission.Verdict, cs *[]CellRecords) *admission.Verdict {
			(*cs)[0].Records[1].InTime = false
			return v
		}, "record 1: completed with in_time false and late false"},
		{"completed both", func(_ *config.Config, v *admission.Verdict, cs *[]CellRecords) *admission.Verdict {
			(*cs)[0].Records[1].Late = true
			return v
		}, "record 1: completed with in_time true and late true"},
		{"error class", func(_ *config.Config, v *admission.Verdict, cs *[]CellRecords) *admission.Verdict {
			r := &(*cs)[0].Records[1]
			r.Outcome, r.ErrClass = loadgen.OutcomeErrored, "timeout"
			return v
		}, `record 1: error class "timeout"`},
		{"outcome", func(_ *config.Config, v *admission.Verdict, cs *[]CellRecords) *admission.Verdict {
			(*cs)[0].Records[1].Outcome = "lost"
			return v
		}, `record 1: outcome "lost"`},
		{"dispatched without outcome", func(_ *config.Config, v *admission.Verdict, cs *[]CellRecords) *admission.Verdict {
			(*cs)[0].Records[1].Outcome = ""
			return v
		}, "record 1: dispatched with no outcome"},
		{"excluded step dispatched", func(_ *config.Config, v *admission.Verdict, cs *[]CellRecords) *admission.Verdict {
			(*cs)[0].Records[4].DispatchNs = 1
			(*cs)[0].Records[4].Outcome = loadgen.OutcomeCensored
			return v
		}, "step 4 (2 rps) is excluded by the ceiling rule and dispatched requests"},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg, v := stepsConfig(t)
			cells := good()
			v = c.mutate(cfg, v, &cells)
			if _, err := NewStepTables(cfg, v, cells); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("got %v, want %q", err, c.want)
			}
		})
	}
	cfg, v := stepsConfig(t)
	if _, err := NewStepTables(cfg, v, good()); err != nil {
		t.Errorf("unmutated: %v", err)
	}
}

// A confidential limit prints with its source and hash and whether each
// step's nominal demand was within the test's bound, never with its value
// or the fraction; the binding limit and the status_429 context say the
// same, and a verdict that disagrees with the recorded value is refused.
func TestStepTablesConfidential(t *testing.T) {
	cfg, _ := stepsConfig(t)
	tpm := &cfg.Target.Limits[1]
	tpm.Value, tpm.Marking, tpm.Source, tpm.SHA256 = 300001, config.MarkingConfidential, "console.png", strings.Repeat("ab", 32)
	cfg.Target.RetainedHeaders = []string{"x-ratelimit-*"}
	v, err := admission.CeilingRule(cfg, admission.Price{InputUSDPerMTok: "0.15", OutputUSDPerMTok: "0.6", Source: "https://example.test/pricing", Retrieved: "2026-10-07"}, admission.ExpectedPlan(cfg))
	if err != nil {
		t.Fatal(err)
	}
	cell := cellOf("weekday-00", outcome{1, LineInTime, 0, 100}, outcome{1, loadgen.ErrStatus429, 0, 2})
	st, err := NewStepTables(cfg, v, []CellRecords{cell})
	if err != nil {
		t.Fatal(err)
	}
	hash := strings.Repeat("ab", 32)
	text := st.Text()
	for _, frag := range []string{
		"step 1, 0.25 rps: admitted; requests_per_minute 1.5% of 1000 (half test: pass); tokens_per_minute confidential (console.png, sha256 " + hash +
			"): nominal demand at most half of it (half test: pass); route_cap_usd ",
		"step 4, 2 rps: excluded, binding limit tokens_per_minute, confidential (console.png, sha256 " + hash + "); requests_per_minute 12.0% of 1000 (half test: pass); " +
			"tokens_per_minute confidential (console.png, sha256 " + hash + "): nominal demand above half of it (half test: FAIL)",
		"    nominal demand at most 1.5% of each public half-tested recorded limit and at most half of each confidential one; no cause inferred",
	} {
		if !strings.Contains(text, frag) {
			t.Errorf("text lacks %q:\n%s", frag, text)
		}
	}
	raw, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(text, "300001") || strings.Contains(string(raw), "300001") || strings.Contains(string(raw), "3.00001e+05") {
		t.Errorf("the tables carry the confidential value")
	}
	v.Steps[0].Demands[1].Value = 300001
	if _, err := NewStepTables(cfg, v, []CellRecords{cell}); err == nil || !strings.Contains(err.Error(), "report: step tables: admission: published verdict: step 1, tokens_per_minute") {
		t.Errorf("verdict with the value: %v", err)
	}
}

// A route's page prints its naming lines verbatim under the heading; a
// cell's report carries none.
func TestStepTablesNaming(t *testing.T) {
	cfg, v := stepsConfig(t)
	cell := cellOf("weekday-00", outcome{1, LineInTime, 0, 10})
	st, err := NewStepTables(cfg, v, []CellRecords{cell})
	if err != nil {
		t.Fatal(err)
	}
	want := "no figure pools across steps or routes.\nPercentes is not affiliated with or endorsed by Groq LLC.\nGroq is a trademark of Groq LLC and/or its affiliates.\n\n"
	if text := st.Text(); !slices.Equal(st.Naming, cfg.Target.Naming.Lines) || !strings.Contains(text, want) {
		t.Errorf("naming %q; page lacks %q:\n%s", st.Naming, want, text)
	}
	inCell, err := stepTables(cfg, v, []CellRecords{cell})
	if err != nil {
		t.Fatal(err)
	}
	if inCell.Naming != nil || strings.Contains(inCell.text(false), "Groq LLC") {
		t.Errorf("a cell's tables carry the naming lines %q", inCell.Naming)
	}
}
