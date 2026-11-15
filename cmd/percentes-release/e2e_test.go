package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/percentes/percentes/configs"
	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/mock"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/report"
	"github.com/percentes/percentes/internal/schedule"
)

// e2eMinute is one minute of a route's pinned offset in the scaled run.
// The scaled cell (shortLadder) puts its 2 s top step 13 s after the
// epoch, so routes 10 offset minutes apart keep their top steps 2 s
// apart when both start on time.
const e2eMinute = 400 * time.Millisecond

const e2eAnswer = "Two sentences about the ticket."

// e2eReasoning is the reasoning tokens of a form that reasons.
const e2eReasoning = 3

func e2eKey(f *mockForm) string { return testKey + "-" + f.name }

// e2eForms are the rehearsal's request forms and a fifth, chat
// completions with max_completion_tokens, the key as a bearer token and
// reasoning under "reasoning".
var e2eForms = append(slices.Clone(rehearsalForms),
	mockForm{"groq", config.AdapterChatCompletions, config.AuthBearer, "/v1/chat/completions", "max_completion_tokens", "reasoning"})

// e2eRoute is form f's Shape S route in an account group of its own,
// with a route cap of US$40, a requests_per_minute limit of 1,000 and a
// tokens_per_minute limit of 1,000,000.
func e2eRoute(f *mockForm) *mockRoute {
	return &mockRoute{name: f.name, form: f.name, shape: config.ShapeS, group: f.name, routeUSD: "40", limits: func() []config.Limit {
		return []config.Limit{
			{Dimension: config.LimitRPM, Value: 1000},
			{Dimension: config.LimitTPM, Value: 1_000_000, Counting: config.CountingInputOutput},
		}
	}}
}

// reasons reports whether form f's mock streams reasoning.
func (f *mockForm) reasons() bool { return f.reasoningKey != "" || f.adapter == config.AdapterMessages }

// startE2E serves form f's mock, which refuses the key in any header but
// the form's.
func startE2E(t *testing.T, f *mockForm) *mock.Server {
	t.Helper()
	s, err := f.startMock(1, config.LatencyDist{Distribution: "fixed", FixedMs: 20}, config.LatencyDist{Distribution: "fixed", FixedMs: 1},
		mockStream{e2eReasoning, e2eAnswer}, e2eKey(f), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// e2eConfig is form f's route at endpoint with its key set in its own
// variable.
func e2eConfig(t *testing.T, f *mockForm, endpoint string) *config.Config {
	t.Helper()
	env := "PERCENTES_E2E_KEY_" + strings.ToUpper(f.name)
	cfg, err := routeConfig(configs.ReleaseReference, e2eRoute(f), f, 512, endpoint, env, "2026-10-07")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(env, e2eKey(f))
	return cfg
}

// clientValidityEnd is the ladder end the client-validity gate gives.
var clientValidityEnd = regexp.MustCompile(`^ladder ended at step [1-5]: ` + regexp.QuoteMeta(loadgen.LadderEndClientValidity) + `$`)

// gateReasons reports whether reasons hold the client-validity gate's
// failure and nothing else but the ladder end it caused.
func gateReasons(reasons []string) bool {
	failed := false
	for _, r := range reasons {
		switch r {
		case "client-validity gate failed (§2)", "client-validity gate failed (§2): host CPU unmeasured on this platform build":
			failed = true
		default:
			if !clientValidityEnd.MatchString(r) {
				return false
			}
		}
	}
	return failed
}

// clientGateOnly reports whether rec is client-invalid by the
// client-validity gate alone.
func clientGateOnly(rec *cellRecord) bool {
	return rec.Outcome == outcomeInvalid && gateReasons(rec.Reasons)
}

// hostGateFailed reports whether rec is client-invalid by the
// client-validity gate alone, with a failed step gate on record that
// ended its ladder and no refused write (gateSkipParts).
func hostGateFailed(rec *cellRecord) bool {
	if !clientGateOnly(rec) || rec.Ladder == nil || rec.Ladder.End != loadgen.LadderEndClientValidity {
		return false
	}
	failed := false
	for i := range rec.Ladder.Steps {
		if g := rec.Ladder.Steps[i].Gates; g != nil && !g.Pass {
			parts, defect := gateSkipParts(g)
			if defect {
				return false
			}
			failed = failed || len(parts) > 0
		}
	}
	return failed
}

// skipOnHostGate skips the rest of t for a cell hostGateFailed takes,
// which has no counted label to check. The failed-gate subtests check its
// client_invalid label on every run.
func skipOnHostGate(t *testing.T, rec *cellRecord) {
	t.Helper()
	if hostGateFailed(rec) {
		t.Skipf("client-validity gate failed (%q); no counted cell", rec.Reasons)
	}
}

// freshSends is what a fresh cell of scheduled ladder requests sends with
// its probe, less the requests rec's ladder left undispatched.
func freshSends(scheduled int, rec *cellRecord) int64 {
	n := int64(1 + scheduled)
	if rec.Ladder == nil {
		return n
	}
	for i := range rec.Ladder.Steps {
		if g := rec.Ladder.Steps[i].Gates; g != nil {
			n -= int64(g.Undispatched)
		}
	}
	return n
}

// gateSkipParts names the failed parts of g, a failed step gate under the
// widened bounds (hostGatesWide), and reports a defect where the release
// write limit refused a write.
func gateSkipParts(g *loadgen.GateReport) (parts []string, defect bool) {
	for _, p := range []struct {
		name string
		fail bool
	}{
		{"gc pause", !g.GCPass},
		{"cpu", !g.CPUPass},
		{"send skew", !g.SendSkewPass && g.WritesDenied == 0},
		{"writes denied", g.WritesDenied > 0},
		{"undispatched", !g.UndispatchedPass},
	} {
		if p.fail {
			parts = append(parts, p.name)
		}
	}
	return parts, g.WritesDenied > 0
}

// A refused write is a defect of the five-route test; any other failed
// part (gc pause, cpu, send skew or undispatched) skips it.
func TestGateSkipPartsFailsOnRefusedWrite(t *testing.T) {
	pass := loadgen.GateReport{SendSkewPass: true, UndispatchedPass: true, CPUMeasured: true, CPUPass: true, GCPass: true}
	for _, tc := range []struct {
		name   string
		set    func(g *loadgen.GateReport)
		parts  []string
		defect bool
	}{
		{"write denied", func(g *loadgen.GateReport) { g.WritesDenied, g.SendSkewPass = 1, false }, []string{"writes denied"}, true},
		{"write denied beside undispatched", func(g *loadgen.GateReport) {
			g.WritesDenied, g.SendSkewPass, g.Undispatched, g.UndispatchedPass = 2, false, 1, false
		}, []string{"writes denied", "undispatched"}, true},
		{"undispatched", func(g *loadgen.GateReport) { g.Undispatched, g.UndispatchedPass = 1, false }, []string{"undispatched"}, false},
		{"cpu unmeasured", func(g *loadgen.GateReport) { g.CPUMeasured, g.CPUPass = false, false }, []string{"cpu"}, false},
		{"gc pause", func(g *loadgen.GateReport) { g.GCPauseOpen, g.GCPass = true, false }, []string{"gc pause"}, false},
		{"send skew", func(g *loadgen.GateReport) { g.SendSkewMaxUs, g.SendSkewPass = 1_500_000, false }, []string{"send skew"}, false},
	} {
		g := pass
		tc.set(&g)
		if parts, defect := gateSkipParts(&g); !slices.Equal(parts, tc.parts) || defect != tc.defect {
			t.Errorf("%s: parts %q, defect %t; want %q, %t", tc.name, parts, defect, tc.parts, tc.defect)
		}
	}
}

// gateReasons excuses the client-validity gate's own reasons and no
// other: a reason that merely starts like them, a ladder end with no
// recorded failure, or a failure beside another reason.
func TestGateReasons(t *testing.T) {
	for _, reasons := range [][]string{
		{"client-validity gate failed; pricing-bound breach"},
		{"ladder ended at step billing: client_validity"},
		{"ladder ended at step 1: client_validity"},
		{"client-validity gate failed (§2)", "probe request returned 401, a setup defect"},
		nil,
	} {
		if gateReasons(reasons) {
			t.Errorf("excused %q", reasons)
		}
	}
	for _, reasons := range [][]string{
		{"client-validity gate failed (§2)"},
		{"client-validity gate failed (§2): host CPU unmeasured on this platform build", "ladder ended at step 3: client_validity"},
	} {
		if !gateReasons(reasons) {
			t.Errorf("not excused %q", reasons)
		}
	}
}

// mockCounts is the mock's count of finished requests by outcome label.
func mockCounts(t *testing.T, s *mock.Server) map[string]int {
	t.Helper()
	c, err := mockFinished(http.DefaultClient, "http://"+s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// Six mock routes in the release's mixed request forms run one cell each,
// concurrently from one process, up the whole ladder (scaled down by
// shortLadder) at their pinned offsets (scaled by e2eMinute), sharing one
// schedule, one ledger and one output directory. Each mock answers only
// the key in its route's header, so a completion is also the check that
// the key arrived there alone. Every cell counts with the client-validity
// gate passing at each of the five steps; the evidence of every route
// passes the completeness check, with its per-step table in its report
// pair. Under the race detector, with six routes sharing one process's
// garbage collector, the gate can fail; when that is the only reason a
// cell does not count, the rest is checked over the attempted steps and
// the test is skipped.
func TestReleaseLadderSixMockRoutes(t *testing.T) {
	enoughOpenFiles(t)
	fastDispatch(t)
	dir := t.TempDir()
	out, frames := t.TempDir(), t.TempDir()
	clockSync := fakeSNTP(t, "+0.005032")
	writeFile(t, dir, "r.txt", []byte("record"))
	opPath := writeFile(t, dir, "operator.json", []byte(operatorJSON))

	n := len(e2eForms)
	mocks := make([]*mock.Server, n)
	cfgs := make([]*config.Config, n)
	names := make([]string, n)
	for i := range e2eForms {
		f := &e2eForms[i]
		mocks[i] = startE2E(t, f)
		cfgs[i] = e2eConfig(t, f, "http://"+mocks[i].Addr()+f.path)
		names[i] = f.name
	}
	ledgerPath := filepath.Join(dir, "ledger.jsonl")
	caps, err := admission.ReleaseCaps(cfgs...)
	if err != nil {
		t.Fatal(err)
	}
	l, err := admission.Create(ledgerPath, caps)
	if err != nil {
		t.Fatal(err)
	}
	for _, cfg := range cfgs {
		if _, err := recordPrice(l, cfg, "0.15", "0.60", "https://example.test/pricing", "2026-10-06"); err != nil {
			t.Fatal(err)
		}
	}
	schedulePath := writeFile(t, dir, "schedule.json", ledgerSchedule(t, ledgerPath, cfgs, names...))

	routes := make([]*route, n)
	var offsets []int
	for i, cfg := range cfgs {
		name := cfg.Target.Name
		cp, err := probe.Encode(passedCeiling(cfg, 2000, ledgerOpenSHA256(t, ledgerPath)))
		if err != nil {
			t.Fatal(err)
		}
		refresh := filepath.Join(dir, "refresh-"+name)
		if err := writeMockRefresh(refresh, cfg); err != nil {
			t.Fatal(err)
		}
		r, err := newRoute(routeInput{
			cfg: cfg, configName: name + ".yaml", schedulePath: schedulePath, out: out, ledgerPath: ledgerPath, key: testReleaseKey,
			framesRoot: frames, clockSync: clockSync, statusCmd: "true", balanceCmd: mockBalanceCmd(cfg), refreshCmd: refresh + " " + filepath.Join(dir, "r.txt"), opPath: opPath,
			probeFiles: []string{writeFile(t, dir, "ceiling-"+name+".json", cp)}, byteCap: 2000, accountCreated: "2026-09-15", build: testBuild,
		})
		if err != nil {
			t.Fatal(err)
		}
		if got := r.verdict.Admitted(); !slices.Equal(got, config.PinnedReleaseStepsRPS) {
			t.Fatalf("%s: admitted %v, want the whole ladder", name, got)
		}
		shortLadder(r)
		routes[i] = r
		offsets = append(offsets, r.offsetMinutes)
	}
	slices.Sort(offsets)
	if !slices.Equal(offsets, []int{0, 10, 20, 30, 40, 50}) {
		t.Fatalf("offsets %v, want a permutation of 0 to 50 in tens", offsets)
	}

	label := routes[0].runs[0].Label
	runs := make([]schedule.Run, n)
	for i, r := range routes {
		k := slices.IndexFunc(r.runs, func(run schedule.Run) bool { return run.Label == label })
		if k < 0 {
			t.Fatalf("%s: no run of cell %s", names[i], label)
		}
		runs[i] = r.runs[k]
		if runs[i].Start.Minute() != r.offsetMinutes || runs[i].Start.Second() != 0 {
			t.Fatalf("%s: run starts at %s, not its slot plus %d min", names[i], runs[i].Start, r.offsetMinutes)
		}
	}
	recs := make([]*cellRecord, n)
	errs := make([]error, n)
	base := time.Now().Add(time.Second)
	var wg sync.WaitGroup
	for i, r := range routes {
		wg.Add(1)
		go func(i int, r *route) {
			defer wg.Done()
			if errs[i] = waitUntil(context.Background(), base.Add(time.Duration(r.offsetMinutes)*e2eMinute)); errs[i] == nil {
				recs[i], errs[i] = r.runCell(context.Background(), runs[i])
			}
		}(i, r)
	}
	wg.Wait()
	for i := range routes {
		if errs[i] != nil {
			t.Fatalf("%s: %v", names[i], errs[i])
		}
	}

	gateOnly := false
	var gateParts []string
	sent := 0
	var sends []time.Time
	st := ledgerState(t, ledgerPath)
	for i, r := range routes {
		name, rec := names[i], recs[i]
		t.Logf("%s at %d min: %s %q", name, r.offsetMinutes, rec.Outcome, rec.Reasons)
		top := len(config.PinnedReleaseStepsRPS)
		passed := top
		switch {
		case rec.Outcome == outcomeCounted:
			if l := rec.Ladder; l == nil || l.End != "" || len(l.Steps) != top {
				t.Errorf("%s: ladder %+v", name, l)
			}
		case rec.Outcome == outcomeInvalid && rec.Ladder != nil && gateReasons(rec.Reasons):
			gateOnly = true
			switch rec.Ladder.End {
			case loadgen.LadderEndClientValidity:
				top, passed = rec.Ladder.EndStep, rec.Ladder.EndStep-1
			case "":
			default:
				t.Errorf("%s: ladder ended: %s", name, rec.Ladder.End)
			}
		default:
			t.Errorf("%s: outcome %s %q", name, rec.Outcome, rec.Reasons)
			continue
		}
		for k, s := range rec.Ladder.Steps {
			if attempted := k < top; s.Attempted != attempted || attempted && (s.Gates == nil || k < passed && !s.Gates.Pass) {
				t.Errorf("%s: step %d attempted %t, gates %+v", name, k+1, s.Attempted, s.Gates)
			}
			if s.Gates != nil && !s.Gates.Pass {
				t.Logf("%s: step %d gate %+v", name, k+1, *s.Gates)
			}
		}
		if rec.Tripwires == nil || rec.Tripwires.Fired || len(rec.Canary) != top {
			t.Errorf("%s: tripwires %+v, canary on %d steps", name, rec.Tripwires, len(rec.Canary))
		}

		cellDir := r.cellDir(runs[i])
		b, err := os.ReadFile(filepath.Join(cellDir, evidence.AttemptsFile))
		if err != nil {
			t.Fatal(err)
		}
		records, err := evidence.ReadRecords(bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		reasoning, finish := 0, "stop"
		if e2eForms[i].reasons() {
			reasoning = e2eReasoning
		}
		if e2eForms[i].adapter == config.AdapterMessages {
			finish = "end_turn"
		}
		dispatched := 0
		for _, a := range records {
			if a.DispatchNs == 0 {
				continue
			}
			dispatched++
			sends = append(sends, rec.EpochUTC.Add(time.Duration(a.DispatchNs)))
			if a.Status != http.StatusOK || a.Outcome != loadgen.OutcomeCompleted || !a.InTime || a.Answer != nil || a.Step < 1 || a.Step > top ||
				a.ReasoningChunks != reasoning || a.FinishReason != finish || a.CompletionTokens == nil || a.ContentEvents == 0 {
				t.Fatalf("%s: record %+v", name, a)
			}
		}
		if rec.Sent.Probe != 1 || rec.Sent.Warmup+rec.Sent.Measured != dispatched {
			t.Errorf("%s: sent %+v, %d dispatched records", name, rec.Sent, dispatched)
		}
		sent += dispatched
		if c := mockCounts(t, mocks[i]); len(c) != 1 || c["200"] != dispatched+1 {
			t.Errorf("%s: the mock finished %v, want %d requests answered 200", name, c, dispatched+1)
		}
		if u := st.Routes[name]; u == nil || u.Reserved != 0 || u.Probe+u.Warmup+u.Measured != dispatched+1 {
			t.Errorf("%s: ledger usage %+v", name, u)
		}

		m, err := evidence.ReadManifest(filepath.Join(cellDir, evidence.ManifestFile))
		if err != nil {
			t.Fatal(err)
		}
		if c := m.Release; c == nil || c.Run != runs[i] || c.OffsetMinutes != r.offsetMinutes || m.RunValid != (rec.Outcome == outcomeCounted) {
			t.Errorf("%s: manifest cell %+v, valid %t", name, m.Release, m.RunValid)
		}
		if _, err := evidence.CheckRelease(evidence.Release{Dir: cellDir, Config: r.cfg.Raw, FramesRoot: frames, Pair: [2]string{report.JSONFile, report.TextFile}, Key: r.key}); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var cr report.CellReport
		if b, err := os.ReadFile(filepath.Join(cellDir, report.JSONFile)); err != nil || json.Unmarshal(b, &cr) != nil {
			t.Fatalf("%s: report: %v", name, err)
		}
		if cr.Outcome != rec.Outcome || cr.CountedInFrame != (rec.Outcome == outcomeCounted) || !cr.LadderStarted || cr.Tables == nil || len(cr.Tables.Cells) != 1 {
			t.Fatalf("%s: report %+v", name, cr)
		}
		for k, s := range cr.Tables.Cells[0].Steps {
			if k >= top {
				continue
			}
			inTime := -1
			for _, line := range s.Lines {
				if line.Name == report.LineInTime {
					inTime = line.Count
				}
			}
			lg := rec.Ladder.Steps[k].Gates
			if s.State != report.StepAttempted || s.Scheduled == 0 || inTime != s.Scheduled || s.G2 == nil || lg == nil || s.G2.Pass != lg.Pass || s.G6 == nil || !s.G6.Pass {
				t.Errorf("%s: step %d table %+v", name, k+1, s)
			}
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	// Concurrent runs: every cell lists only the other routes' runs of the
	// cell, and each one whose span overlaps its own by more than 1 s.
	for i, rec := range recs {
		for _, c := range rec.Concurrent {
			if j := slices.Index(runs, c); j < 0 || j == i {
				t.Errorf("%s: concurrent run %+v", names[i], c)
			}
		}
		for j, other := range recs {
			from, to := rec.StartUTC, *rec.EndUTC
			if other.StartUTC.After(from) {
				from = other.StartUTC
			}
			if other.EndUTC.Before(to) {
				to = *other.EndUTC
			}
			if j != i && to.Sub(from) > time.Second && !slices.Contains(rec.Concurrent, runs[j]) {
				t.Errorf("%s: %s ran beside it for %s and is not listed", names[i], names[j], to.Sub(from))
			}
		}
	}

	// Each cell's manifest carries the host's aggregate dispatch series
	// over its seconds, every route's sends summed, each series read final
	// or counting through the cell's end. The last second runs past the
	// end, so it holds at least the sends before the end.
	for i, r := range routes {
		m, err := evidence.ReadManifest(filepath.Join(r.cellDir(runs[i]), evidence.ManifestFile))
		if err != nil {
			t.Fatal(err)
		}
		d := m.Release.Dispatch
		if d == nil || len(d.Sources) != 1+len(recs[i].Concurrent) || len(d.Counts) == 0 {
			t.Errorf("%s: aggregate %+v with %d concurrent runs", names[i], d, len(recs[i].Concurrent))
			continue
		}
		for _, src := range d.Sources {
			if !src.Final && (src.ThroughUTC == nil || src.ThroughUTC.Before(d.EndUTC)) {
				t.Errorf("%s: %s's series counts through %v, before the cell's end %s", names[i], src.Run.Route, src.ThroughUTC, d.EndUTC)
			}
		}
		want, last := make([]int, len(d.Counts)), len(d.Counts)-1
		beforeEnd := 0
		for _, at := range sends {
			k := int(at.UTC().Truncate(time.Second).Sub(d.StartUTC) / time.Second)
			if k < 0 || k > last {
				continue
			}
			want[k]++
			if k == last && at.Before(d.EndUTC) {
				beforeEnd++
			}
		}
		if !slices.Equal(d.Counts[:last], want[:last]) || d.Counts[last] < beforeEnd || d.Counts[last] > want[last] {
			t.Errorf("%s: aggregate %v, the routes' sends per second %v", names[i], d.Counts, want)
		}
		t.Logf("%s: %d concurrent runs, at most %d dispatches a second", names[i], len(recs[i].Concurrent), d.MaxPerSecond)
	}

	// The pinned offsets keep the routes' top-step windows apart.
	type window struct {
		name     string
		from, to time.Time
	}
	var tops []window
	for i, rec := range recs {
		s := rec.Ladder.Steps[len(rec.Ladder.Steps)-1]
		tops = append(tops, window{names[i], rec.EpochUTC.Add(time.Duration(s.StartNs)), rec.EpochUTC.Add(time.Duration(s.EndNs))})
	}
	slices.SortFunc(tops, func(a, b window) int { return a.from.Compare(b.from) })
	for k := 1; k < len(tops); k++ {
		if tops[k].from.Before(tops[k-1].to) {
			t.Errorf("top steps overlap: %s from %s, %s until %s", tops[k].name, tops[k].from.Format(time.RFC3339Nano), tops[k-1].name, tops[k-1].to.Format(time.RFC3339Nano))
		}
	}

	// The host's dispatch series sums every route's sends.
	if code, err := dispatchCmd([]string{"--out", out}); err != nil || code != exitOK {
		t.Fatalf("dispatch: %d, %v", code, err)
	}
	b, err := os.ReadFile(filepath.Join(out, aggregateFile))
	if err != nil {
		t.Fatal(err)
	}
	var agg aggregateSeries
	if err := json.Unmarshal(b, &agg); err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, c := range agg.Counts {
		total += c
	}
	if len(agg.Files) != n || total != sent {
		t.Errorf("aggregate of %d files counts %d sends, the cells dispatched %d", len(agg.Files), total, sent)
	}
	t.Logf("aggregate dispatch: at most %d per second at %s", agg.MaxPerSecond, agg.MaxAtUTC.Format(time.RFC3339))

	if gateOnly && !t.Failed() {
		slices.Sort(gateParts)
		t.Skipf("client-validity gate failed in-process (%s); outcomes logged above", strings.Join(gateParts, ", "))
	}
}

// hostGateFailed takes a failed step gate on record that ended the ladder,
// with no refused write, beside the gate's own reasons.
func TestHostGateFailed(t *testing.T) {
	reasons := []string{"client-validity gate failed (§2)", "ladder ended at step 1: client_validity"}
	recOf := func(reasons []string, gs ...*loadgen.GateReport) *cellRecord {
		steps := make([]loadgen.StepRecord, len(gs))
		for i, g := range gs {
			steps[i].Gates = g
		}
		return &cellRecord{Outcome: outcomeInvalid, Reasons: reasons, Ladder: &loadgen.LadderRecord{End: loadgen.LadderEndClientValidity, Steps: steps}}
	}
	rec := func(reasons []string, g *loadgen.GateReport) *cellRecord { return recOf(reasons, g) }
	pass := loadgen.GateReport{Pass: true, SendSkewPass: true, UndispatchedPass: true, CPUMeasured: true, CPUPass: true, GCPass: true}
	undispatched := pass
	undispatched.Pass, undispatched.Undispatched, undispatched.UndispatchedPass = false, 1, false
	denied := pass
	denied.Pass, denied.WritesDenied, denied.SendSkewPass = false, 1, false
	noPart := pass
	noPart.Pass = false
	noEnd := rec(reasons, &undispatched)
	noEnd.Ladder.End = ""
	for _, tc := range []struct {
		name string
		rec  *cellRecord
		want bool
	}{
		{"undispatched", rec(reasons, &undispatched), true},
		{"first of two steps failed", recOf(reasons, &undispatched, &pass), true},
		{"failed part, then a failed gate with none", recOf(reasons, &undispatched, &noPart), true},
		{"last of two steps failed", recOf(reasons, &pass, &undispatched), true},
		{"gate reason with every gate passed", rec(reasons, &pass), false},
		{"failed gate with no failed part", rec(reasons, &noPart), false},
		{"refused write", rec(reasons, &denied), false},
		{"refused write in the first of two steps", recOf(reasons, &denied, &undispatched), false},
		{"another reason", rec([]string{"stopped: x"}, &undispatched), false},
		{"no ladder", &cellRecord{Outcome: outcomeInvalid, Reasons: reasons}, false},
		{"failed gate with no ladder end", noEnd, false},
	} {
		if got := hostGateFailed(tc.rec); got != tc.want {
			t.Errorf("%s: %t, want %t", tc.name, got, tc.want)
		}
	}
}

// skipOnHostGate skips a cell hostGateFailed takes, and a counted cell or
// any other failure goes on to the test's own checks.
func TestSkipOnHostGate(t *testing.T) {
	failed := loadgen.GateReport{Undispatched: 1, SendSkewPass: true, CPUMeasured: true, CPUPass: true, GCPass: true}
	ladder := &loadgen.LadderRecord{End: loadgen.LadderEndClientValidity, Steps: []loadgen.StepRecord{{Gates: &failed}}}
	for _, tc := range []struct {
		name string
		rec  *cellRecord
		want bool
	}{
		{"counted", &cellRecord{Outcome: outcomeCounted, Ladder: &loadgen.LadderRecord{}}, false},
		{"another reason", &cellRecord{Outcome: outcomeInvalid, Reasons: []string{"stopped: x"}, Ladder: ladder}, false},
		{"gate failed", &cellRecord{Outcome: outcomeInvalid, Reasons: []string{"client-validity gate failed (§2)", "ladder ended at step 1: client_validity"}, Ladder: ladder}, true},
	} {
		var skipped bool
		t.Run(tc.name, func(st *testing.T) {
			defer func() { skipped = st.Skipped() }()
			skipOnHostGate(st, tc.rec)
		})
		if skipped != tc.want {
			t.Errorf("%s: skipped %t, want %t", tc.name, skipped, tc.want)
		}
	}
}

// A fresh cell's sends are its schedule and probe less each step's
// undispatched requests; a re-run with no ladder is the whole count, which
// its test then refuses.
func TestFreshSends(t *testing.T) {
	two := &cellRecord{Ladder: &loadgen.LadderRecord{Steps: []loadgen.StepRecord{{Gates: &loadgen.GateReport{Undispatched: 2}}, {}, {Gates: &loadgen.GateReport{Undispatched: 1}}}}}
	if got := freshSends(30, two); got != 28 {
		t.Errorf("three steps: %d, want 28", got)
	}
	if got := freshSends(30, &cellRecord{}); got != 31 {
		t.Errorf("no ladder: %d, want 31", got)
	}
}
