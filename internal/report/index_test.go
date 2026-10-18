package report

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
)

// indexRoute is the release reference renamed to route name of provider,
// with cells from cell(i) for i in 0..n-1.
func indexRoute(t *testing.T, name, provider string, n int, cell func(i int) []outcome) IndexRoute {
	t.Helper()
	cfg, v := stepsConfig(t)
	cfg.Target.Name, cfg.Target.Naming.Provider = name, provider
	v.Route = name
	r := IndexRoute{Config: cfg, Verdict: v, Page: "routes/" + name + ".html"}
	for i := 0; i < n; i++ {
		r.Cells = append(r.Cells, cellOf(fmt.Sprintf("cell-%02d", i), cell(i)...))
	}
	return r
}

// indexRoutes are three routes given out of order: groq (provider Groq)
// with a complete frame whose ladder ended after step 2 in two cells;
// fast (provider baseten, lower case so a case-sensitive order would put
// it after Groq) with a complete frame and every request in time; and
// solo (provider Anthropic) with one cell.
func indexRoutes(t *testing.T) []IndexRoute {
	t.Helper()
	groq := indexRoute(t, "groq", "Groq", 12, func(i int) []outcome {
		outs := []outcome{{1, LineInTime, 0, 110}, {1, LineLate, 0, 4}, {1, loadgen.ErrStatus429, 0, 2},
			{1, loadgen.ErrStatusOther, 529, 1}, {1, loadgen.ErrMalformedStream, 0, 1}, {1, LineCensored, 0, 2},
			{2, LineInTime, 0, 239}, {2, loadgen.ErrConnect, 0, 1}}
		if i < 10 {
			outs = append(outs, outcome{3, LineInTime, 0, 479}, outcome{3, loadgen.ErrReset, 0, 1})
		} else {
			outs = append(outs, outcome{3, LineUnsent, 0, 480})
		}
		return append(outs, outcome{4, LineUnsent, 0, 960}, outcome{5, LineUnsent, 0, 1920})
	})
	fast := indexRoute(t, "fast", "baseten", 12, func(int) []outcome {
		return []outcome{{1, LineInTime, 0, 120}, {2, LineInTime, 0, 240}, {3, LineInTime, 0, 480},
			{4, LineUnsent, 0, 960}, {5, LineUnsent, 0, 1920}}
	})
	solo := indexRoute(t, "solo", "Anthropic", 1, func(int) []outcome {
		return []outcome{{1, LineInTime, 0, 120}, {2, LineUnsent, 0, 240}, {3, LineUnsent, 0, 480},
			{4, LineUnsent, 0, 960}, {5, LineUnsent, 0, 1920}}
	})
	return []IndexRoute{groq, solo, fast}
}

// Rows run alphabetically by provider without regard to case, then by
// step; each row's counts are the route page's pooled counts and sum to
// the step's scheduled requests; an excluded step names its binding
// limit; an incomplete frame carries no counts.
func TestIndexRows(t *testing.T) {
	routes := indexRoutes(t)
	ix, err := NewIndex(routes)
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	for _, r := range ix.Rows {
		order = append(order, fmt.Sprintf("%s/%d", r.Route, r.Step))
	}
	want := []string{"solo/1", "solo/2", "solo/3", "solo/4", "solo/5", "fast/1", "fast/2", "fast/3", "fast/4", "fast/5",
		"groq/1", "groq/2", "groq/3", "groq/4", "groq/5"}
	if !slices.Equal(order, want) {
		t.Fatalf("row order %q, want %q", order, want)
	}

	st, err := NewStepTables(routes[0].Config, routes[0].Verdict, routes[0].Cells)
	if err != nil {
		t.Fatal(err)
	}
	for k, r := range ix.Rows[10:] {
		ps := st.Pooled[k]
		if r.Provider != "Groq" || r.Model != "openai/gpt-oss-120b" || r.RPS != ps.RPS || r.Admitted != ps.Admitted ||
			r.HighestAttempted != 3 || r.CeilingTokens != 512 || r.FrameCells != 12 || r.RecordedCells != 12 ||
			r.Cells != ps.Cells || r.Scheduled != ps.Scheduled || r.Page != "routes/groq.html" {
			t.Errorf("groq step %d: %+v", k+1, r)
		}
		if !r.Admitted {
			if r.Binding != "tokens_per_minute" || r.Counts != nil {
				t.Errorf("groq step %d excluded: %+v", k+1, r)
			}
			continue
		}
		var names []string
		sum := 0
		for _, c := range r.Counts {
			names = append(names, c.Line)
			sum += c.Count
			for _, l := range ps.Lines {
				if l.Name == c.Line && l.InBand == "" && (l.Status == 0 || l.Name == loadgen.ErrStatus429) && l.Count != c.Count {
					t.Errorf("groq step %d %s: %d, the route page %d", k+1, c.Line, c.Count, l.Count)
				}
			}
		}
		if !slices.Equal(names, stepLines) || sum != r.Scheduled || r.Binding != "" {
			t.Errorf("groq step %d: lines %q sum %d of %d", k+1, names, sum, r.Scheduled)
		}
	}
	g1, g3 := ix.Rows[10], ix.Rows[12]
	if g1.Scheduled != 1440 || g1.Counts[0] != (IndexCount{LineInTime, 1320}) || g1.Counts[3] != (IndexCount{loadgen.ErrStatusOther, 12}) ||
		g1.Counts[7] != (IndexCount{loadgen.ErrMalformedStream, 12}) {
		t.Errorf("groq step 1: %+v", g1)
	}
	if g3.Cells != 10 || g3.Scheduled != 4800 || g3.Counts[5] != (IndexCount{loadgen.ErrReset, 10}) {
		t.Errorf("groq step 3: %+v", g3)
	}
	for _, r := range ix.Rows[:5] {
		if r.RecordedCells != 1 || r.Cells != 0 || r.Scheduled != 0 || r.Counts != nil || r.HighestAttempted != 1 {
			t.Errorf("solo step %d: %+v", r.Step, r)
		}
	}

	md := ix.Markdown()
	for _, frag := range []string{
		"| Provider | Route | Model | Step | Offered load (rps) | Admission | Highest attempted step | Ceiling (tokens) | Cells attempted | Scheduled | completed_in_time | completed_late | status_429 | status_other | connect | reset | empty_stream | malformed_stream | censored | unsent | Route page |\n",
		"| Groq | groq | openai/gpt-oss-120b | 1 | 0.25 | admitted | 3 | 512 | 12 of 12 | 1440 | 1320/1440 | 48/1440 | 24/1440 | 12/1440 | 0/1440 | 0/1440 | 0/1440 | 12/1440 | 24/1440 | 0/1440 | [groq](routes/groq.html) |\n",
		"| Groq | groq | openai/gpt-oss-120b | 3 | 1 | admitted | 3 | 512 | 10 of 12 | 4800 | 4790/4800 | 0/4800 | 0/4800 | 0/4800 | 0/4800 | 10/4800 | 0/4800 | 0/4800 | 0/4800 | 0/4800 | [groq](routes/groq.html) |\n",
		"| Groq | groq | openai/gpt-oss-120b | 4 | 2 | excluded, binding limit tokens_per_minute | 3 | 512 | 0 of 12 | 0 |  |  |  |  |  |  |  |  |  |  | [groq](routes/groq.html) |\n",
		"| Anthropic | solo | openai/gpt-oss-120b | 1 | 0.25 | admitted | 1 | 512 | incomplete frame, 1 of 12 cells recorded |  |  |  |  |  |  |  |  |  |  |  | [solo](routes/solo.html) |\n",
	} {
		if !strings.Contains(md, frag) {
			t.Errorf("page lacks %q:\n%s", frag, md)
		}
	}

	slices.Reverse(routes)
	again, err := NewIndex(routes)
	if err != nil {
		t.Fatal(err)
	}
	if again.Markdown() != md {
		t.Error("route order changed the index")
	}
}

// The index regenerates from the attempts files: each cell's records
// written and read back give the same page and the same JSON.
func TestIndexReplay(t *testing.T) {
	routes := indexRoutes(t)
	ix, err := NewIndex(routes)
	if err != nil {
		t.Fatal(err)
	}
	for i := range routes {
		for j := range routes[i].Cells {
			var buf bytes.Buffer
			if err := evidence.Write(&buf, routes[i].Cells[j].Records); err != nil {
				t.Fatal(err)
			}
			recs, err := evidence.ReadRecords(&buf)
			if err != nil {
				t.Fatal(err)
			}
			routes[i].Cells[j].Records = recs
		}
	}
	again, err := NewIndex(routes)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(ix)
	b, _ := json.Marshal(again)
	if again.Markdown() != ix.Markdown() || !bytes.Equal(a, b) {
		t.Errorf("replay changed the index:\n%s\n%s", ix.Markdown(), again.Markdown())
	}
}

// The index page and its JSON carry no rank, winner, best, worst,
// interval, sort key or p99 string and no rate; a route whose printed
// fields would put one there is refused.
func TestIndexStrings(t *testing.T) {
	ix, err := NewIndex(indexRoutes(t))
	if err != nil {
		t.Fatal(err)
	}
	js, err := json.Marshal(ix)
	if err != nil {
		t.Fatal(err)
	}
	for _, page := range []string{ix.Markdown(), string(js)} {
		low := strings.ToLower(page)
		for _, f := range []string{"rank", "winner", "best", "worst", "interval", "sort key", "p99", "wilson", "deff", "%"} {
			if strings.Contains(low, f) {
				t.Errorf("index carries %q:\n%s", f, page)
			}
		}
	}

	for _, c := range []struct {
		name   string
		mutate func(*[]IndexRoute)
		want   string
	}{
		{"no routes", func(rs *[]IndexRoute) { *rs = nil }, "no routes"},
		{"provider word", func(rs *[]IndexRoute) { (*rs)[0].Config.Target.Naming.Provider = "Best Inference" }, `provider: carries "best"`},
		{"route word", func(rs *[]IndexRoute) { (*rs)[0].Config.Target.Name = "groq-p99"; (*rs)[0].Verdict.Route = "groq-p99" }, `route: carries "p99"`},
		{"model word", func(rs *[]IndexRoute) { (*rs)[0].Config.Target.ModelName = "Winner-1" }, `model: carries "winner"`},
		{"page word", func(rs *[]IndexRoute) { (*rs)[0].Page = "routes/ranked.html" }, `page: carries "rank"`},
		{"delimiter", func(rs *[]IndexRoute) { (*rs)[0].Config.Target.ModelName = "a|b" }, "model: carries a table delimiter"},
		{"line break", func(rs *[]IndexRoute) { (*rs)[0].Page = "a\nb" }, "page: carries a table delimiter or a line break"},
		{"empty page", func(rs *[]IndexRoute) { (*rs)[0].Page = " " }, "page: empty"},
		{"duplicate route", func(rs *[]IndexRoute) { (*rs)[1] = (*rs)[0] }, `route "groq" given twice`},
		{"duplicate route, two providers", func(rs *[]IndexRoute) {
			(*rs)[1].Config.Target.Name, (*rs)[1].Verdict.Route = "groq", "groq"
		}, `route "groq" given twice`},
		{"no naming", func(rs *[]IndexRoute) { (*rs)[0].Config.Target.Naming = nil }, "route 0 has no release configuration"},
		{"step tables", func(rs *[]IndexRoute) { (*rs)[0].Verdict = nil }, `route "groq": report: step tables: no ceiling-rule verdict`},
	} {
		t.Run(c.name, func(t *testing.T) {
			rs := indexRoutes(t)
			c.mutate(&rs)
			if _, err := NewIndex(rs); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %v, want %q", err, c.want)
			}
		})
	}
}
