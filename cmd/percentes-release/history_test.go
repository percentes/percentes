package main

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/report"
	"github.com/percentes/percentes/internal/schedule"
)

// The routes of an account group are those the configurations and the
// schedule's frozen configurations record against it.
func TestGroupMembers(t *testing.T) {
	cfg := func(name, group string) *config.Config {
		return &config.Config{Target: config.Target{Name: name}, Release: &config.Release{AccountGroup: group}}
	}
	sched := &schedule.Schedule{Input: schedule.Input{Configs: []schedule.RouteConfig{
		{Route: "azure-R", AccountGroup: "azure"}, {Route: "gemini-S", AccountGroup: "gemini"}, {Route: "azure-S", AccountGroup: "azure"}}}}
	got := groupMembers([]*config.Config{cfg("azure-S", "azure"), cfg("baseten-S", "baseten")}, sched)
	want := map[string][]string{"azure": {"azure-R", "azure-S"}, "baseten": {"baseten-S"}, "gemini": {"gemini-S"}}
	if len(got) != len(want) {
		t.Fatalf("groups %v, want %v", got, want)
	}
	for g, routes := range want {
		if !slices.Equal(got[g], routes) {
			t.Errorf("group %s: %v, want %v", g, got[g], routes)
		}
	}
}

// Each admission of a group route is checked against the sends it
// brackets: a settled count the records match leaves no gap; one they
// do not match is a gap over the admission; an unsettled cell admission
// is a gap to the latest write its attempt allows, any other unsettled
// admission a gap to its route's next admission, and one with no later
// admission an open gap. A send no admission brackets is unadmitted;
// another group's lines are not read.
func TestReconcile(t *testing.T) {
	t0 := time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)
	at := func(m float64) time.Time { return t0.Add(time.Duration(m * float64(time.Minute))) }
	send := func(route, kind string, m float64) report.Send {
		return report.Send{Route: route, Kind: kind, DispatchUTC: at(m)}
	}
	h := &report.History{Sends: []report.Send{
		send("azure-S", report.SendProbe, 1), send("azure-S", report.SendWarmup, 2), send("azure-S", report.SendLadder, 3),
		send("azure-R", report.SendLadder, -60),
		send("azure-R", report.SendProbe, 81), send("azure-R", report.SendLadder, 82),
		send("azure-S", report.SendCheck, 300), send("azure-S", report.SendCheck, 301),
		send("gemini-S", report.SendLadder, 3),
	}}
	entries := []admission.Entry{
		{Seq: 2, Kind: admission.KindAdmit, Time: at(0.5), Route: "azure-S", Purpose: admission.PurposeCell, Reserve: 4},
		{Seq: 3, Kind: admission.KindSettle, Time: at(30), Route: "azure-S", Admit: 2, Sent: &admission.Counts{Probe: 1, Warmup: 1, Measured: 1}},
		{Seq: 4, Kind: admission.KindAdmit, Time: at(80.5), Route: "azure-R", Purpose: admission.PurposeCell, Reserve: 3020},
		{Seq: 5, Kind: admission.KindAdmit, Time: at(299), Route: "azure-S", Purpose: admission.PurposeProbe, Reserve: 10},
		{Seq: 6, Kind: admission.KindSettle, Time: at(302), Route: "azure-S", Admit: 5, Sent: &admission.Counts{Probe: 10}},
		{Seq: 7, Kind: admission.KindAdmit, Time: at(400), Route: "azure-S", Purpose: admission.PurposeProbe, Reserve: 10},
		{Seq: 8, Kind: admission.KindAdmit, Time: at(1), Route: "gemini-S", Purpose: admission.PurposeCell, Reserve: 9},
		{Seq: 9, Kind: admission.KindAdmit, Time: at(420), Route: "azure-S", Purpose: admission.PurposeProbe, Reserve: 10},
		{Seq: 10, Kind: admission.KindSettle, Time: at(421), Route: "azure-S", Admit: 9, Sent: &admission.Counts{}},
		{Seq: 11, Kind: admission.KindAdmit, Time: at(500), Route: "azure-R", Purpose: admission.PurposeProbe, Reserve: 10},
	}
	spans := []attemptSpan{
		{route: "azure-R", began: at(-61), until: at(-10)},
		{route: "azure-R", began: at(80), until: at(80 + 15 + 28.5 + 1.5)},
		{route: "azure-S", began: at(0), until: at(45)},
	}
	reconcile(h, entries, []string{"azure-R", "azure-S"}, spans)
	until, settled, next := at(125), at(302), at(420)
	want := []report.HistoryGap{
		{Route: "azure-R", FromUTC: at(80.5), ToUTC: &until, Reason: "ledger line 4 admits 3020 cell requests with no settlement"},
		{Route: "azure-S", FromUTC: at(299), ToUTC: &settled, Reason: "ledger line 6 settles 10 probe requests, the records hold 2"},
		{Route: "azure-S", FromUTC: at(400), ToUTC: &next, Reason: "ledger line 7 admits 10 probe requests with no settlement"},
		{Route: "azure-R", FromUTC: at(500), Reason: "ledger line 11 admits 10 probe requests with no settlement"},
	}
	if len(h.Gaps) != len(want) {
		t.Fatalf("gaps %+v", h.Gaps)
	}
	for i, g := range h.Gaps {
		w := want[i]
		if g.Route != w.Route || !g.FromUTC.Equal(w.FromUTC) || g.Reason != w.Reason || (g.ToUTC == nil) != (w.ToUTC == nil) || g.ToUTC != nil && !g.ToUTC.Equal(*w.ToUTC) {
			t.Errorf("gap %d: %+v, want %+v", i, g, w)
		}
	}
	if h.Unadmitted != 2 {
		t.Errorf("%d unadmitted sends, want the azure-R send before any admission and the gemini-S send", h.Unadmitted)
	}
}

// An unsettled admission's gap ends at its route's next admission of any
// purpose, passing over a later line timed before it; a cell admission
// whose time no attempt span holds, an earlier attempt's included, takes
// the same bound.
func TestReconcileNextAdmission(t *testing.T) {
	t0 := time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)
	at := func(m float64) time.Time { return t0.Add(time.Duration(m * float64(time.Minute))) }
	h := &report.History{}
	reconcile(h, []admission.Entry{
		{Seq: 1, Kind: admission.KindAdmit, Time: at(0), Route: "azure-S", Purpose: admission.PurposeProbe, Reserve: 10},
		{Seq: 2, Kind: admission.KindAdmit, Time: at(30), Route: "azure-S", Purpose: admission.PurposeCell, Reserve: 100},
		{Seq: 3, Kind: admission.KindAdmit, Time: at(20), Route: "azure-S", Purpose: admission.PurposeProbe, Reserve: 10},
		{Seq: 4, Kind: admission.KindAdmit, Time: at(50), Route: "azure-S", Purpose: admission.PurposeProbe, Reserve: 10},
		{Seq: 5, Kind: admission.KindSettle, Time: at(51), Route: "azure-S", Admit: 4, Sent: &admission.Counts{}},
	}, []string{"azure-S"}, []attemptSpan{{route: "azure-S", began: at(-60), until: at(-10)}})
	to30, to50 := at(30), at(50)
	want := []report.HistoryGap{
		{Route: "azure-S", FromUTC: at(0), ToUTC: &to30, Reason: "ledger line 1 admits 10 probe requests with no settlement"},
		{Route: "azure-S", FromUTC: at(30), ToUTC: &to50, Reason: "ledger line 2 admits 100 cell requests with no settlement"},
		{Route: "azure-S", FromUTC: at(20), ToUTC: &to50, Reason: "ledger line 3 admits 10 probe requests with no settlement"},
	}
	if len(h.Gaps) != len(want) {
		t.Fatalf("gaps %+v", h.Gaps)
	}
	for i, g := range h.Gaps {
		w := want[i]
		if g.Route != w.Route || !g.FromUTC.Equal(w.FromUTC) || g.Reason != w.Reason || g.ToUTC == nil || !g.ToUTC.Equal(*w.ToUTC) {
			t.Errorf("gap %d: %+v, want %+v", i, g, w)
		}
	}
}

// An unsettled cell admission takes the until of the latest-begun span
// of its own route that holds its time, though an earlier-begun one ends
// later, an admission at a span's until included; a span that ended
// before it, one begun after it and another route's span are passed over.
func TestReconcileSpanChoice(t *testing.T) {
	t0 := time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)
	at := func(m float64) time.Time { return t0.Add(time.Duration(m * float64(time.Minute))) }
	h := &report.History{}
	reconcile(h, []admission.Entry{
		{Seq: 1, Kind: admission.KindAdmit, Time: at(10), Route: "azure-S", Purpose: admission.PurposeCell, Reserve: 100},
		{Seq: 2, Kind: admission.KindAdmit, Time: at(30), Route: "gemini-S", Purpose: admission.PurposeCell, Reserve: 100},
	}, []string{"azure-S", "gemini-S"}, []attemptSpan{
		{route: "azure-S", began: at(0), until: at(60)},
		{route: "azure-S", began: at(5), until: at(40)},
		{route: "azure-S", began: at(7), until: at(9)},
		{route: "azure-S", began: at(20), until: at(80)},
		{route: "azure-R", began: at(8), until: at(100)},
		{route: "gemini-S", began: at(0), until: at(30)},
	})
	want := []time.Time{at(40), at(30)}
	if len(h.Gaps) != len(want) {
		t.Fatalf("gaps %+v", h.Gaps)
	}
	for i, g := range h.Gaps {
		if g.ToUTC == nil || !g.ToUTC.Equal(want[i]) {
			t.Errorf("gap %d: %+v, want to %s", i, g, want[i])
		}
	}
}

// A route's price revisions at each pair of token bounds, in ledger
// order: each line retrieved no earlier than an earlier line's.
func TestGroupPrices(t *testing.T) {
	price := func(route string, in, out int, retrieved, usd string) admission.Entry {
		return admission.Entry{Kind: admission.KindPrice, Route: route, InputTokens: in, OutputTokens: out, Retrieved: retrieved, PerRequestUSD: usd}
	}
	got := groupPrices([]admission.Entry{
		price("azure-S", 1200, 512, "2026-10-24", "0.007520"), price("azure-S", 1200, 1024, "2026-10-24", "0.012640"),
		price("azure-S", 1200, 512, "2026-10-30", "0.008"), price("azure-S", 1200, 512, "2026-10-30", "0.009"),
		price("azure-S", 1200, 512, "2026-10-01", "0.001"), price("gemini-S", 1200, 512, "2026-10-30", "0.002820"),
	}, []string{"azure-S"})
	want := []report.RoutePrice{{Route: "azure-S", InputTokens: 1200, OutputTokens: 512, PerRequestUSD: "0.007520"},
		{Route: "azure-S", InputTokens: 1200, OutputTokens: 1024, PerRequestUSD: "0.012640"},
		{Route: "azure-S", InputTokens: 1200, OutputTokens: 512, PerRequestUSD: "0.008"},
		{Route: "azure-S", InputTokens: 1200, OutputTokens: 512, PerRequestUSD: "0.009"}}
	if !slices.Equal(got, want) {
		t.Errorf("prices %+v, want %+v", got, want)
	}
}

// A spend window prices each send at the price the ledger recorded by
// its write time: a price recorded after the refusal reprices nothing,
// and a send written before any price is unpriced.
func TestSpendWindowPriceAtWrite(t *testing.T) {
	at := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	cfg := &config.Config{Target: config.Target{Name: "gemini", Limits: []config.Limit{
		{Dimension: config.LimitSpendWindow, Value: 10, WindowS: 600, Marking: config.MarkingPublic},
	}}, Release: &config.Release{AccountGroup: "gemini"}}
	price := func(when time.Time, usd string) admission.Entry {
		return admission.Entry{Kind: admission.KindPrice, Route: "gemini", Time: when, Retrieved: when.Format(time.DateOnly),
			InputTokens: 1200, OutputTokens: 512, PerRequestUSD: usd}
	}
	var sends []report.Send
	for i := 0; i < 2000; i++ {
		wrote := at.Add(-time.Duration(i) * 250 * time.Millisecond)
		sends = append(sends, report.Send{Route: "gemini", DispatchUTC: wrote, WroteUTC: &wrote, InputBound: 1200, Ceiling: 512})
	}
	label := func(prices ...admission.Entry) report.Refusal {
		h := &report.History{Group: "gemini", Routes: []string{"gemini"}, Sends: sends, Prices: groupPrices(prices, []string{"gemini"})}
		seg := &report.SegmentRefusals{Refusals: []report.Refusal{{WroteUTC: &at}}}
		st := report.StepTables{Route: "gemini", Cells: []report.CellTable{{Warmup: seg}}}
		if err := st.Attribute(cfg, &admission.Verdict{}, h); err != nil {
			t.Fatal(err)
		}
		return seg.Refusals[0]
	}
	if got := label(price(at.Add(-time.Hour), "0.006"), price(at.Add(time.Hour), "0.004")); got.Label != report.LabelAbove || got.Windows[0].Count != 12 {
		t.Errorf("later price: label %s, spend %g, share %g", got.Label, got.Windows[0].Count, got.Windows[0].Share)
	}
	if got := label(price(at.Add(-time.Minute), "0.006")); got.Label != report.LabelUnresolved || got.Windows[0].Requests != 2000 {
		t.Errorf("sends before any price: label %s, %+v", got.Label, got.Windows[0])
	}
}

// A cell without complete per-request records is a gap over its
// dispatch series, from its first counted second to the hard timeout
// after its last; with no series, from its epoch to the hard timeout
// after its end, open with no end; with neither, nothing.
func TestDispatchGap(t *testing.T) {
	start := time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	b, err := json.Marshal(dispatchSeries{StartUTC: start, Counts: []int{0, 3, 0, 2, 0}})
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, dir, dispatchFile, b)
	h := &report.History{}
	if err := dispatchGap(h, dir, "azure-S", "weekday-08-a1", &cellRecord{}, 90); err != nil {
		t.Fatal(err)
	}
	if len(h.Gaps) != 1 || !h.Gaps[0].FromUTC.Equal(start.Add(time.Second)) || !h.Gaps[0].ToUTC.Equal(start.Add(94*time.Second)) ||
		h.Gaps[0].Reason != "cell weekday-08-a1 has no per-request records for its ladder; its dispatch series counts 5 dispatches" {
		t.Errorf("gaps %+v", h.Gaps)
	}

	empty := t.TempDir()
	epoch, end := start.Add(time.Minute), start.Add(20*time.Minute)
	for _, c := range []struct {
		rec  cellRecord
		want []report.HistoryGap
	}{
		{cellRecord{}, nil},
		{cellRecord{EpochUTC: &epoch}, []report.HistoryGap{{Route: "azure-S", FromUTC: epoch}}},
		{cellRecord{EpochUTC: &epoch, EndUTC: &end}, []report.HistoryGap{{Route: "azure-S", FromUTC: epoch, ToUTC: ptr(end.Add(90 * time.Second))}}},
	} {
		h := &report.History{}
		if err := dispatchGap(h, empty, "azure-S", "weekday-08-a1", &c.rec, 90); err != nil {
			t.Fatal(err)
		}
		if len(h.Gaps) != len(c.want) {
			t.Fatalf("epoch %v end %v: gaps %+v", c.rec.EpochUTC, c.rec.EndUTC, h.Gaps)
		}
		for i, g := range h.Gaps {
			w := c.want[i]
			if !g.FromUTC.Equal(w.FromUTC) || (g.ToUTC == nil) != (w.ToUTC == nil) || g.ToUTC != nil && !g.ToUTC.Equal(*w.ToUTC) ||
				g.Reason != "cell weekday-08-a1 has no per-request records for its ladder and no dispatch series" {
				t.Errorf("gap %+v, want %+v", g, w)
			}
		}
	}
}

func ptr[T any](v T) *T { return &v }

// cellSent counts the requests the cell in dir dispatched: its attempts
// file's dispatched records and its probe's requests.
func cellSent(t *testing.T, dir string) (sent, refused int) {
	t.Helper()
	if b, err := os.ReadFile(filepath.Join(dir, evidence.AttemptsFile)); err == nil {
		recs, err := evidence.ReadRecords(bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			if r.DispatchNs != 0 {
				sent++
			}
			if r.ErrClass == loadgen.ErrStatus429 {
				refused++
			}
		}
	}
	if b, err := os.ReadFile(filepath.Join(dir, probe.File)); err == nil {
		var pr probe.Record
		if err := json.Unmarshal(b, &pr); err != nil {
			t.Fatal(err)
		}
		sent += admission.ProbeSent(&pr)
	}
	return sent, refused
}

// A route page labels every status_429 from its account group's history:
// the counted cell's requests and probe, a labelled cell's probe and the
// ceiling-check records, each reconciled with the ledger the schedule
// froze, and each check record given that no cell carries, read once.
// Labelled cells feed the history and stay out of the step tables. A
// later interrupted cell with no records leaves its windows unresolved.
func TestPagesAttribution(t *testing.T) {
	var block atomic.Bool
	e := newEndpoint(t, func(n int64) (int, string) {
		switch {
		case block.Load():
			return http.StatusUnauthorized, `{"error":{"message":"invalid key"}}`
		case n > 2 && n%10 == 0:
			return http.StatusTooManyRequests, `{"error":{"type":"rate_limit_error","message":"slow down"}}`
		}
		return http.StatusOK, ""
	})
	release, sched := oneRelease(t, e.url(), "groq", "baseten")
	groq, groqCfg := pagesRoute(t, "groq", e.url(), "", "", release)
	shortLadder(groq)
	counted, blocked, interrupted := groq.runs[0], groq.runs[1], groq.runs[2]
	rec := runCellOK(t, groq, counted)
	if rec.Outcome != outcomeCounted {
		if !clientGateOnly(rec) {
			t.Fatalf("outcome %s %q", rec.Outcome, rec.Reasons)
		}
		t.Skipf("outcome %s (%q); no counted cell to page", rec.Outcome, rec.Reasons)
	}
	block.Store(true)
	if rec := runCellOK(t, groq, blocked); rec.Outcome != outcomeBlocked {
		t.Fatalf("outcome %s %q", rec.Outcome, rec.Reasons)
	}
	sent, refused := cellSent(t, groq.cellDir(counted))
	blockedSent, _ := cellSent(t, groq.cellDir(blocked))
	if refused == 0 || blockedSent == 0 {
		t.Fatalf("%d status_429 in the counted cell, %d requests in the blocked one", refused, blockedSent)
	}

	pages := t.TempDir()
	args := []string{"--out", groq.out, "--ledger", groq.ledgerPath, "--frames", groq.framesRoot, "--config", groqCfg, "--schedule", sched, "--pages", pages}
	tables := func() (*report.StepTables, string) {
		t.Helper()
		var code int
		var err error
		stdoutOf(t, func() { code, err = pagesCmd(args) })
		if code != exitOK || err != nil {
			t.Fatalf("exit %d: %v", code, err)
		}
		files := readPages(t, pages)
		var st report.StepTables
		if err := json.Unmarshal(files["routes/groq.json"], &st); err != nil {
			t.Fatal(err)
		}
		return &st, string(files["routes/groq.txt"])
	}
	labels := func(st *report.StepTables) map[string]int {
		n := map[string]int{}
		for _, c := range st.Cells {
			segs := []*report.SegmentRefusals{c.Warmup}
			for _, s := range c.Steps {
				segs = append(segs, s.Refusals)
			}
			for _, seg := range segs {
				if seg != nil {
					for _, r := range seg.Refusals {
						n[r.Label]++
					}
				}
			}
		}
		return n
	}

	st, page := tables()
	a := st.Attribution
	if a == nil || a.Group != "groq" || !slices.Equal(a.Routes, []string{"groq"}) || a.Sends != sent+blockedSent+probe.CeilingRequests ||
		a.Unadmitted != probe.CeilingRequests || a.Gaps != nil {
		t.Fatalf("attribution %+v; want %d sends of the counted cell, %d of the blocked one and the check's %d, unadmitted",
			a, sent, blockedSent, probe.CeilingRequests)
	}
	if n := labels(st); len(st.Cells) != 1 || n[report.LabelAtOrBelow] != refused || len(n) != 1 {
		t.Errorf("%d cells; labels %v, want %d at or below", len(st.Cells), n, refused)
	}
	for _, frag := range []string{
		"-- Account group groq: ",
		" recorded sends of routes groq, by write time, attribute each status_429 --\n",
		"  10 recorded sends fall in no ledger admission of their route\n",
		": refused with recorded sends at or below the limit's share in every window; requests_per_minute over 60 s: ",
	} {
		if !strings.Contains(page, frag) {
			t.Errorf("page lacks %q:\n%s", frag, page)
		}
	}

	m, err := evidence.ReadManifest(filepath.Join(groq.cellDir(counted), evidence.ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	var carried string
	var check probe.ReleaseRecord
	for _, ref := range m.Release.Probes {
		p := filepath.Join(groq.cellDir(counted), ref.Name)
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, &check); err != nil {
			t.Fatal(err)
		}
		if check.Route == "groq" {
			carried = p
			break
		}
	}
	for i := range check.Requests {
		check.Requests[i].Exchange.DispatchUTC = check.Requests[i].Exchange.DispatchUTC.Add(time.Hour)
	}
	repeated := writeCeiling(t, check)
	base := args
	args = append(slices.Clone(base), "--check-record", carried, "--check-record", repeated)
	if st, _ = tables(); st.Attribution.Sends != sent+blockedSent+2*probe.CeilingRequests || st.Attribution.Unadmitted != 2*probe.CeilingRequests {
		t.Errorf("with a carried and a repeated check record: %d sends, %d unadmitted", st.Attribution.Sends, st.Attribution.Unadmitted)
	}
	check.LedgerSHA256 = strings.Repeat("0", 64)
	args = append(slices.Clone(base), "--check-record", writeCeiling(t, check))
	if code, err := pagesCmd(args); code != exitError || err == nil || !strings.Contains(err.Error(), "--check-record") ||
		!strings.Contains(err.Error(), `admitted in the ledger of open-line sha256 "`+strings.Repeat("0", 64)+`"; the schedule froze `) {
		t.Errorf("a check record of another ledger: exit %d: %v", code, err)
	}
	args = base

	ir := groq.newRecord(interrupted)
	ir.Outcome, ir.EpochUTC = outcomeInterrupted, rec.EpochUTC
	if err := groq.writeNew(interrupted, ir); err != nil {
		t.Fatal(err)
	}
	st, page = tables()
	reason := "cell " + cellLabel(interrupted) + " has no per-request records for its ladder and no dispatch series"
	if a := st.Attribution; len(a.Gaps) != 1 || a.Gaps[0].Reason != reason || a.Gaps[0].ToUTC != nil {
		t.Errorf("gaps %+v", a.Gaps)
	}
	if n := labels(st); n[report.LabelUnresolved] != refused || len(n) != 1 {
		t.Errorf("labels %v, want %d unresolved", n, refused)
	}
	if !strings.Contains(page, ": comparison unresolved; the history may lack sends of route groq from ") {
		t.Errorf("page:\n%s", page)
	}
}

// A ceiling check with three reached requests and one that reached no
// connection, settled at Sent(), reconciles with no gap and no unwritten send.
func TestHistoryCheckUnreachedRequest(t *testing.T) {
	t0 := time.Date(2026, 11, 13, 20, 0, 0, 0, time.UTC)
	rec := probe.ReleaseRecord{Route: "gemini-S", Shape: config.ShapeS}
	for i := 0; i < 3; i++ {
		d := t0.Add(time.Duration(i) * 6 * time.Second)
		rec.Requests = append(rec.Requests, probe.CheckRequest{Index: i, Exchange: probe.Exchange{DispatchUTC: d, ConnAcquiredNs: 1e6, WroteRequestNs: 2e6, Status: 200, DoneNs: 1e9}})
	}
	rec.Requests = append(rec.Requests, probe.CheckRequest{Index: 3, Exchange: probe.Exchange{DispatchUTC: t0.Add(18 * time.Second), Error: "dial tcp: connection refused", DoneNs: 2e6}})
	if rec.Sent() != 3 {
		t.Fatalf("Sent() %d, want 3", rec.Sent())
	}
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "check.json")
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	h := &report.History{}
	if err := checkHistory(h, p, "check.json", []string{"gemini-S"}, 90); err != nil {
		t.Fatal(err)
	}
	if len(h.Sends) != 3 {
		t.Errorf("%d sends, want 3", len(h.Sends))
	}
	for _, s := range h.Sends {
		if s.WroteUTC == nil {
			t.Errorf("unwritten send dispatched %s", s.DispatchUTC)
		}
	}
	entries := []admission.Entry{
		{Seq: 1, Kind: admission.KindAdmit, Time: t0.Add(-time.Second), Route: "gemini-S", Purpose: admission.PurposeProbe, Reserve: 10},
		{Seq: 2, Kind: admission.KindSettle, Time: t0.Add(time.Minute), Route: "gemini-S", Admit: 1, Sent: &admission.Counts{Probe: rec.Sent()}},
	}
	reconcile(h, entries, []string{"gemini-S"}, nil)
	if len(h.Gaps) != 0 {
		t.Errorf("gaps %+v", h.Gaps)
	}
}

const probeUsageStream = "data: {\"model\":\"openai/gpt-oss-120b\",\"choices\":[{\"delta\":{\"content\":\"Two \"}}]}\n\n" +
	"data: {\"model\":\"openai/gpt-oss-120b\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
	"data: {\"model\":\"openai/gpt-oss-120b\",\"choices\":[],\"usage\":{\"prompt_tokens\":40,\"completion_tokens\":2,\"total_tokens\":42}}\n\n" +
	"data: [DONE]\n\n"

// historyProbeSend is the cell probe's send in the account-group history
// of r's out directory.
func historyProbeSend(t *testing.T, r *route) report.Send {
	t.Helper()
	h, err := readHistory(r.out, r.cfg.Target.Name, []string{r.cfg.Target.Name}, []*config.Config{r.cfg}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range h.Sends {
		if s.Kind == report.SendProbe && s.Index == 0 {
			return s
		}
	}
	t.Fatal("no probe send in the history")
	return report.Send{}
}

// A cell probe's reported counts reach probe.json and the account-group
// history, and the first refusal line counts only the refused request at
// the input bound. A probe.json carrying a saturated count gives
// math.MaxInt, and one without the counts leaves the probe at the bound.
func TestHistoryProbeReportedPrompt(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body) //nolint:errcheck
		if n := hits.Add(1); n > 2 && n%10 == 0 {
			w.WriteHeader(http.StatusTooManyRequests)
			io.WriteString(w, `{"error":{"type":"rate_limit_error","message":"slow down"}}`) //nolint:errcheck
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, probeUsageStream) //nolint:errcheck
	}))
	defer srv.Close()
	url := srv.URL + "/openai/v1/chat/completions"
	release, sched := oneRelease(t, url, "groq", "baseten")
	groq, groqCfg := pagesRoute(t, "groq", url, "", "", release)
	shortLadder(groq)
	groq.cfg.Release.Ladder = config.Ladder{StepFractions: []float64{2, 4, 8, 16, 32}, WarmupS: 0.5, StepS: 1, DrainS: 0.25}
	rec := runCellOK(t, groq, groq.runs[0])
	counted := rec.Outcome == outcomeCounted
	if !counted && !clientGateOnly(rec) {
		t.Fatalf("outcome %s %q", rec.Outcome, rec.Reasons)
	}
	probePath := filepath.Join(groq.cellDir(groq.runs[0]), probe.File)
	b, err := os.ReadFile(probePath)
	if err != nil {
		t.Fatal(err)
	}
	var pr probe.Record
	if err := json.Unmarshal(b, &pr); err != nil {
		t.Fatal(err)
	}
	if pr.PromptTokens == nil || *pr.PromptTokens != 40 || pr.OutputTokens == nil || *pr.OutputTokens != 2 {
		t.Fatalf("probe.json counts %v %v, want 40 and 2", pr.PromptTokens, pr.OutputTokens)
	}
	if s := historyProbeSend(t, groq); s.PromptTokens == nil || *s.PromptTokens != 40 {
		t.Errorf("history probe prompt %v, want 40", s.PromptTokens)
	}

	if counted {
		pages := t.TempDir()
		args := []string{"--out", groq.out, "--ledger", groq.ledgerPath, "--frames", groq.framesRoot, "--config", groqCfg, "--schedule", sched, "--pages", pages}
		var code int
		stdoutOf(t, func() { code, err = pagesCmd(args) })
		if code != exitOK || err != nil {
			t.Fatalf("pages: exit %d: %v", code, err)
		}
		page := string(readPages(t, pages)["routes/groq.txt"])
		first := ""
		for _, line := range strings.Split(page, "\n") {
			if strings.Contains(line, "tokens_per_minute over 60 s") && strings.Contains(line, "at the input bound") {
				first = line
				break
			}
		}
		if !strings.Contains(first, "(1 at the input bound, no usable prompt count reported)") {
			t.Errorf("first refusal's token window counts the probe at the bound:\n%s\n%s", first, page)
		}
	} else {
		t.Logf("outcome %s (%q); refusal lines not checked", rec.Outcome, rec.Reasons)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	raw["prompt_tokens"] = json.RawMessage("9223372036854775807")
	write := func() {
		t.Helper()
		nb, err := json.Marshal(raw)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(probePath, nb, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write()
	if s := historyProbeSend(t, groq); s.PromptTokens == nil || *s.PromptTokens != math.MaxInt {
		t.Errorf("saturated probe prompt %v, want math.MaxInt", s.PromptTokens)
	}
	delete(raw, "prompt_tokens")
	delete(raw, "output_tokens")
	write()
	if s := historyProbeSend(t, groq); s.PromptTokens != nil {
		t.Errorf("probe.json without counts: prompt %d, want none", *s.PromptTokens)
	}
}
