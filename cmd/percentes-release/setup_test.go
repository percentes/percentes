package main

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/schedule"
)

// The route refuses to start on each defect of its inputs.
func TestNewRouteRefusals(t *testing.T) {
	ceiling := func(t *testing.T, f *fixture, recs ...probe.ReleaseRecord) {
		t.Helper()
		f.in.probeFiles = nil
		for i := range recs {
			b, _ := json.Marshal(&recs[i])
			f.in.probeFiles = append(f.in.probeFiles, writeFile(t, t.TempDir(), "p.json", b))
		}
	}
	pass := func(route string, c int, ok bool) probe.ReleaseRecord {
		return probe.ReleaseRecord{Route: route, Ceiling: probe.CeilingCheck{Ceiling: c, Pass: ok}}
	}
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, f *fixture)
		want   string
	}{
		{"few open files", func(t *testing.T, f *fixture) {
			openFilesLimit = func() (uint64, error) { return 10240, nil }
		}, "open-file limit 10240 is below 65536"},
		{"not in the schedule", func(t *testing.T, f *fixture) { f.in.cfg.Target.Name = "absent" }, `no route "absent"`},
		{"no price", func(t *testing.T, f *fixture) {
			f.in.ledgerPath = filepath.Join(t.TempDir(), "ledger.jsonl")
			if _, err := admission.Create(f.in.ledgerPath, mustCaps(t, f.in.cfg)); err != nil {
				t.Fatal(err)
			}
		}, "ledger: no price recorded for route groq"},
		{"priced at the ceiling only", func(t *testing.T, f *fixture) {
			f.in.ledgerPath = filepath.Join(t.TempDir(), "ledger.jsonl")
			l, err := admission.Create(f.in.ledgerPath, mustCaps(t, f.in.cfg))
			if err != nil {
				t.Fatal(err)
			}
			if err := l.RecordPrice("groq", admission.Price{InputUSDPerMTok: "0.15", OutputUSDPerMTok: "0.60", InputTokens: 1200, OutputTokens: 512,
				Source: "https://example.test/pricing", Retrieved: "2026-10-06"}); err != nil {
				t.Fatal(err)
			}
		}, "no price recorded for groq at 1200 input and 1024 output tokens"},
		{"fallback priced at 512 only", func(t *testing.T, f *fixture) {
			fallback(t, f.in.cfg)
			f.in.ledgerPath = filepath.Join(t.TempDir(), "ledger.jsonl")
			l, err := admission.Create(f.in.ledgerPath, mustCaps(t, f.in.cfg))
			if err != nil {
				t.Fatal(err)
			}
			if err := l.RecordPrice("groq", admission.Price{InputUSDPerMTok: "0.15", OutputUSDPerMTok: "0.60", InputTokens: 1200, OutputTokens: 512,
				Source: "https://example.test/pricing", Retrieved: "2026-10-06"}); err != nil {
				t.Fatal(err)
			}
		}, "no price recorded for groq at 1200 input and 1024 output tokens"},
		{"probe price older", func(t *testing.T, f *fixture) {
			l, err := admission.Open(f.in.ledgerPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := l.RecordPrice("groq", admission.Price{InputUSDPerMTok: "0.20", OutputUSDPerMTok: "0.80", InputTokens: 1200, OutputTokens: 512,
				Source: "https://example.test/pricing", Retrieved: "2026-10-08"}); err != nil {
				t.Fatal(err)
			}
		}, "under the ceiling rule's US$0.0010592"},
		{"model excluded", func(t *testing.T, f *fixture) {
			ceiling(t, f, pass("groq", 512, false), pass("groq", 1024, false))
		}, "the ceiling rule excludes the model"},
		{"ceiling chosen elsewhere", func(t *testing.T, f *fixture) {
			ceiling(t, f, pass("groq", 512, false), pass("groq", 1024, true))
		}, "the ceiling check chose 1024"},
		{"no own record", func(t *testing.T, f *fixture) { ceiling(t, f, pass("baseten", 512, true)) }, "no ceiling probe record of route groq at 512"},
		{"no ceiling probe", func(t *testing.T, f *fixture) { f.in.probeFiles = nil }, "--ceiling-probe is required"},
		{"byte cap", func(t *testing.T, f *fixture) { f.in.byteCap = 0 }, "--byte-cap 0"},
		{"account date", func(t *testing.T, f *fixture) { f.in.accountCreated = "15 September" }, "--account-created"},
		{"unstamped build", func(t *testing.T, f *fixture) { f.in.build = nil }, "no version-control revision"},
		{"frames in the tree", func(t *testing.T, f *fixture) { f.in.framesRoot = "." }, "frame"},
		{"no balance command", func(t *testing.T, f *fixture) { f.in.balanceCmd = " " }, "no balance command"},
		{"caps differ", func(t *testing.T, f *fixture) { f.in.cfg.Release.Caps.RouteUSD = "9" }, "ledger cap for route groq"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enoughOpenFiles(t)
			f := newFixture(t, releaseConfig(t, "https://api.example.test/openai/v1/chat/completions"), "+0.005")
			tc.mutate(t, f)
			if _, err := newRoute(f.in); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
}

// fallback sets cfg's ceiling to 1,024 tokens.
func fallback(t *testing.T, cfg *config.Config) {
	t.Helper()
	const from, to = `"max_completion_tokens": 512,`, `"max_completion_tokens": 1024,`
	if strings.Count(cfg.Target.Body, from) != 1 {
		t.Fatalf("body %q", cfg.Target.Body)
	}
	cfg.Target.Body = strings.Replace(cfg.Target.Body, from, to, 1)
	cfg.Release.Shape.CeilingTokens = 1024
}

func mustCaps(t *testing.T, cfg *config.Config) admission.Caps {
	t.Helper()
	c, err := admission.ReleaseCaps(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// The ceiling rule reads the schedule's seeded counts: each step's and
// the warm-up's largest over every cell attempt.
func TestExactPlan(t *testing.T) {
	cfg := releaseConfig(t, "https://api.example.test/openai/v1/chat/completions")
	sched := testSchedule(t, "groq")
	p, err := exactPlan(cfg, sched)
	if err != nil {
		t.Fatal(err)
	}
	want := admission.ExpectedPlan(cfg)
	if p.Cells != want.Cells || p.DayCells != want.DayCells || p.ProbeRequests != want.ProbeRequests {
		t.Fatalf("plan %+v", p)
	}
	for k, n := range p.StepRequests {
		if e := float64(want.StepRequests[k]); float64(n) < e || float64(n) > e+8*math.Sqrt(e) {
			t.Errorf("step %d: %d requests, expected the largest of 24 draws around %g", k+1, n, e)
		}
	}
	found := false
	for _, c := range sched.Cells {
		for _, a := range c.Attempts {
			sc := *cfg
			sc.Run.Seed = a.Seed
			l := loadgen.BuildLadder(&sc)
			found = found || l.Steps[4].End-l.Steps[4].First == p.StepRequests[4]
			if l.Steps[0].First > p.WarmupRequests {
				t.Fatalf("attempt warm-up %d above the plan's %d", l.Steps[0].First, p.WarmupRequests)
			}
		}
	}
	if !found {
		t.Fatal("the top step's count is no attempt's")
	}
}

// A cell's configuration carries the attempt's seed and the admitted
// steps, and its schedule is a prefix of the whole ladder's.
func TestCellConfig(t *testing.T) {
	cfg := releaseConfig(t, "https://api.example.test/openai/v1/chat/completions")
	c := cellConfig(cfg, 42, 3)
	if c.Run.Seed != 42 || !slices.Equal(c.Release.Ladder.StepsRPS, []float64{0.25, 0.5, 1}) || len(cfg.Release.Ladder.StepsRPS) != 5 {
		t.Fatalf("cell config seed %d, steps %v; route steps %v", c.Run.Seed, c.Release.Ladder.StepsRPS, cfg.Release.Ladder.StepsRPS)
	}
	full := *cfg
	full.Run.Seed = 42
	whole, cut := loadgen.BuildSchedule(&full), loadgen.BuildSchedule(c)
	if len(cut) == 0 || len(cut) >= len(whole) || !slices.Equal(whole[:len(cut)], cut) {
		t.Fatalf("cut schedule of %d is not a prefix of the whole %d", len(cut), len(whole))
	}
}

// The outcome of a run that reached the ladder.
func TestCellOutcome(t *testing.T) {
	ok := func() *loadgen.Result {
		return &loadgen.Result{Gates: loadgen.GateReport{Pass: true, CPUMeasured: true}, Ladder: &loadgen.LadderRecord{}, Tripwires: &loadgen.Tripwires{}}
	}
	clean := &evidence.ClockRecord{}
	stop := &routeStop{"HTTP 402"}
	for _, tc := range []struct {
		name    string
		res     func() *loadgen.Result
		runErr  error
		cause   error
		clock   *evidence.ClockRecord
		canary  error
		outcome string
		reasons []string
	}{
		{"clean", ok, nil, nil, clean, nil, outcomeCounted, nil},
		{"backoff", func() *loadgen.Result {
			r := ok()
			r.Ladder.End, r.Ladder.EndStep = loadgen.LadderEndBackoff, 2
			return r
		}, nil, nil, clean, nil, outcomeCounted, []string{"ladder ended at step 2: backoff"}},
		{"route stop", func() *loadgen.Result {
			r := ok()
			r.Gates.Pass = false
			r.Interrupted = &loadgen.Interruption{}
			r.Ladder.Steps = []loadgen.StepRecord{{Gates: &loadgen.GateReport{Pass: true, CPUMeasured: true}}}
			return r
		}, errors.New("aborted"), stop, clean, nil, outcomeStopped, []string{stop.Error()}},
		{"operator abort", ok, errors.New("aborted"), loadgen.ErrOperatorAbort, clean, nil, outcomeStopped, []string{"stopped: operator abort"}},
		{"emfile", func() *loadgen.Result {
			r := ok()
			r.Requests = []loadgen.Request{{ErrText: "dial tcp 192.0.2.1:443: socket: too many open files"}}
			return r
		}, nil, nil, clean, nil, outcomeInvalid, []string{errEMFILE.Error()}},
		{"gate", func() *loadgen.Result {
			r := ok()
			r.Gates.Pass = false
			r.Ladder.End, r.Ladder.EndStep = loadgen.LadderEndClientValidity, 1
			return r
		}, nil, nil, clean, nil, outcomeInvalid, []string{"client-validity gate failed (§2)", "ladder ended at step 1: client_validity"}},
		{"tripwire", func() *loadgen.Result {
			r := ok()
			r.Tripwires = &loadgen.Tripwires{Fired: true, Dispatches: 9, Scheduled: 8, MaxInFlight: 3, InFlightLimits: []int{45}}
			return r
		}, nil, nil, clean, nil, outcomeInvalid, []string{"tripwire fired (SPEC-HOSTED HA3 point 16): 9 dispatches against 8 scheduled, at most 3 in flight against limits [45]"}},
		{"protocol", ok, errors.New("loadgen: hosted target: 1 responses not over HTTP/1.1"), nil, clean, nil, outcomeInvalid, []string{"loadgen: hosted target: 1 responses not over HTTP/1.1"}},
		{"clock unread", ok, nil, nil, &evidence.ClockRecord{Flag: "clock offset: no reading"}, nil, outcomeInvalid, []string{"clock offset: no reading"}},
		{"canary", ok, nil, nil, clean, errors.New("listen"), outcomeInvalid, []string{"loopback canary did not start: listen"}},
	} {
		got, reasons := cellOutcome(tc.res(), tc.runErr, tc.cause, tc.clock, tc.canary)
		if got != tc.outcome || !slices.Equal(reasons, tc.reasons) {
			t.Errorf("%s: %s %q, want %s %q", tc.name, got, reasons, tc.outcome, tc.reasons)
		}
	}
}

// A billing refusal is matched on the target's records; EMFILE on the
// request's error text.
func TestStopSignals(t *testing.T) {
	tg := &config.Target{BillingRefusals: []config.BillingRefusal{
		{Status: 429, BodyContains: "enforced_spend_limit_reached", Meaning: "spend limit", Source: "errors.png", Retrieved: "2026-10-07"},
	}}
	if s := billingRefusal(tg, 429, []byte(`{"error":{"type":"enforced_spend_limit_reached"}}`)); s == nil || !strings.Contains(s.Error(), "HTTP 429, the documented billing refusal: spend limit (errors.png, retrieved 2026-10-07)") {
		t.Fatalf("stop %v", s)
	}
	if s := billingRefusal(tg, 429, []byte(`{"error":{"type":"rate_limit_error"}}`)); s != nil {
		t.Fatalf("a rate limit stopped the route: %v", s)
	}
	if isEMFILE(&loadgen.Request{ErrText: "dial tcp: connection refused"}) || !isEMFILE(&loadgen.Request{ErrText: "socket: too many open files"}) {
		t.Fatal("EMFILE misread")
	}
}

// The ledger gives the route's newest recorded price, whatever its token
// bounds and per-request price, and the requests it settled before the
// window.
func TestLedgerReads(t *testing.T) {
	cfg := releaseConfig(t, "https://api.example.test/openai/v1/chat/completions")
	f := newFixture(t, cfg, "+0.005")
	l, err := admission.Open(f.ledger)
	if err != nil {
		t.Fatal(err)
	}
	record := func(in, out string, ceiling int, retrieved string) {
		t.Helper()
		if err := l.RecordPrice("groq", admission.Price{InputUSDPerMTok: in, OutputUSDPerMTok: out, InputTokens: 1200, OutputTokens: ceiling, Source: "https://example.test/pricing", Retrieved: retrieved}); err != nil {
			t.Fatal(err)
		}
	}
	// US$0.0007944 a request at 1,024, read before US$0.0006496 at 512.
	record("0.15", "0.60", 1024, "2026-10-07")
	record("0.20", "0.80", 512, "2026-10-08")
	// Recorded last, read earliest.
	record("0.30", "1.20", 512, "2026-10-05")
	a, err := l.Admit("groq", admission.PurposeProbe, 10, admission.ReleaseTokens(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Settle(a, admission.Counts{Probe: 7}); err != nil {
		t.Fatal(err)
	}
	entries, err := ledgerEntries(f.ledger)
	if err != nil {
		t.Fatal(err)
	}
	p, err := routePrice(entries, "groq")
	if err != nil || p.InputUSDPerMTok != "0.20" || p.OutputUSDPerMTok != "0.80" || p.Retrieved != "2026-10-08" {
		t.Fatalf("price %+v, %v", p, err)
	}
	if _, err := routePrice(entries, "other"); err == nil {
		t.Fatal("a route with no price was priced")
	}
	if n := sentBefore(entries, "groq", time.Now().Add(time.Hour)); n != 7 {
		t.Fatalf("sent before the window: %d", n)
	}
	if n := sentBefore(entries, "groq", time.Now().Add(-time.Hour)); n != 0 {
		t.Fatalf("sent before an earlier window: %d", n)
	}
}

// The key comes from the env file into the target's variable only.
func TestLoadKey(t *testing.T) {
	tg := &config.Target{Name: "groq", APIKeyEnv: "PERCENTES_RELEASE_TEST_KEY"}
	t.Setenv(tg.APIKeyEnv, "")
	dir := t.TempDir()
	good := writeFile(t, dir, "good.env", []byte("export PERCENTES_RELEASE_TEST_KEY='abc123'\n"))
	if err := loadKey(good, tg); err != nil || os.Getenv(tg.APIKeyEnv) != "abc123" {
		t.Fatalf("%v, %q", err, os.Getenv(tg.APIKeyEnv))
	}
	other := writeFile(t, dir, "other.env", []byte("OTHER=1\n"))
	if err := loadKey(other, tg); err == nil || strings.Contains(err.Error(), "abc123") {
		t.Fatalf("missing variable: %v", err)
	}
	open := filepath.Join(dir, "open.env")
	if err := os.WriteFile(open, []byte("PERCENTES_RELEASE_TEST_KEY=x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := loadKey(open, tg); err == nil || !strings.Contains(err.Error(), "chmod 600") {
		t.Fatalf("readable file: %v", err)
	}
}

// Each cell's dispatch series counts its sends per UTC second; the
// aggregate sums every route's cells by second.
func TestDispatchSeries(t *testing.T) {
	epoch := time.Date(2026, 10, 17, 0, 10, 0, 400_000_000, time.UTC)
	res := &loadgen.Result{EpochWall: epoch, Requests: []loadgen.Request{
		{DispatchNs: 100_000_000}, {DispatchNs: 700_000_000}, {DispatchNs: 2_100_000_000}, {DispatchNs: 0},
	}}
	run := schedule.Run{Route: "groq", Label: "weekend-00", Attempt: 1}
	s := newDispatchSeries(run, res)
	if !s.StartUTC.Equal(epoch.Truncate(time.Second)) || !slices.Equal(s.Counts, []int{1, 1, 1}) {
		t.Fatalf("series %v from %s", s.Counts, s.StartUTC)
	}
	out := t.TempDir()
	if err := replaceJSON(filepath.Join(out, "groq", "weekend-00-a1", dispatchFile), s); err != nil {
		t.Fatal(err)
	}
	s2 := &dispatchSeries{Run: schedule.Run{Route: "other"}, StartUTC: epoch.Truncate(time.Second).Add(time.Second), Counts: []int{4, 0, 0, 2}}
	if err := replaceJSON(filepath.Join(out, "other", "weekend-00-a1", dispatchFile), s2); err != nil {
		t.Fatal(err)
	}
	if code, err := dispatchCmd([]string{"--out", out}); err != nil || code != exitOK {
		t.Fatalf("dispatch: %d, %v", code, err)
	}
	b, err := os.ReadFile(filepath.Join(out, aggregateFile))
	if err != nil {
		t.Fatal(err)
	}
	var a aggregateSeries
	if err := json.Unmarshal(b, &a); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(a.Counts, []int{1, 5, 1, 0, 2}) || a.MaxPerSecond != 5 || !a.MaxAtUTC.Equal(s2.StartUTC) || len(a.Files) != 2 {
		t.Fatalf("aggregate %+v", a)
	}
	if _, err := aggregate(t.TempDir()); err == nil {
		t.Fatal("an empty directory aggregated")
	}
}

// A cell's aggregate sums its own final series and each concurrent
// run's over the cell's seconds: a final series as it stands, a running
// one once it counts through the cell's end, and a run with no series as
// none. A series of another run is refused.
func TestHostDispatch(t *testing.T) {
	fastDispatch(t)
	out := t.TempDir()
	r := &route{out: out}
	start := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	run := func(route string) schedule.Run {
		return schedule.Run{Route: route, Label: "weekend-00", Attempt: 1, Date: "2026-10-17", Start: start}
	}
	path := func(run schedule.Run) string {
		return filepath.Join(runDir(filepath.Join(out, run.Route), run), dispatchFile)
	}
	own, done, live, gone := run("groq"), run("baseten"), run("google"), run("anthropic")

	from := time.Now().Add(-3 * time.Second)
	lo := from.UTC().Truncate(time.Second)
	ownSeries := &dispatchSeries{Run: own, StartUTC: lo, Counts: []int{1, 2}, Final: true}
	if err := replaceJSON(path(done), &dispatchSeries{Run: done, StartUTC: lo.Add(-time.Second), Counts: []int{5, 1, 0, 3}, Final: true}); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path(live)), 0o755); err != nil {
		t.Fatal(err)
	}
	l, err := startLiveSeries(path(live), live)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l.stop)
	l.setEpoch(lo)
	l.dispatched(&loadgen.Request{DispatchNs: int64(500 * time.Millisecond)})
	l.dispatched(&loadgen.Request{DispatchNs: int64(2500 * time.Millisecond)})

	end := time.Now()
	d, err := r.hostDispatch(from, end, ownSeries, []schedule.Run{done, live, gone})
	if err != nil {
		t.Fatal(err)
	}
	want := make([]int, int(end.Sub(lo)/time.Second)+1)
	copy(want, []int{3, 2, 4})
	if !d.StartUTC.Equal(lo) || !d.EndUTC.Equal(end) || !slices.Equal(d.Counts, want) || d.MaxPerSecond != 4 || d.MaxAtUTC == nil || !d.MaxAtUTC.Equal(lo.Add(2*time.Second)) {
		t.Fatalf("aggregate %+v, want counts %v", d, want)
	}
	if len(d.Sources) != 4 {
		t.Fatalf("sources %+v", d.Sources)
	}
	for i, w := range []struct {
		run        schedule.Run
		dispatches int
		final      bool
	}{{own, 3, true}, {done, 4, true}, {live, 2, false}, {gone, 0, false}} {
		if s := d.Sources[i]; s.Run != w.run || s.Dispatches != w.dispatches || s.Final != w.final {
			t.Errorf("source %d: %+v, want %s with %d dispatches, final %t", i, s, w.run.Route, w.dispatches, w.final)
		}
	}
	if th := d.Sources[2].ThroughUTC; th == nil || th.Before(end) {
		t.Errorf("the running series counts through %v, before the cell's end %s", th, end)
	}
	if d.Sources[1].ThroughUTC != nil || d.Sources[3].ThroughUTC != nil {
		t.Errorf("through on a final or absent series: %+v", d.Sources)
	}

	if err := replaceJSON(path(gone), &dispatchSeries{Run: done, StartUTC: lo, Counts: []int{1}, Final: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.hostDispatch(from, end, ownSeries, []schedule.Run{gone}); err == nil || !strings.Contains(err.Error(), "is the series of baseten") {
		t.Fatalf("another run's series: %v", err)
	}
}

// check-cpu takes no arguments, and the commands refuse missing flags.
func TestCommandFlags(t *testing.T) {
	if code := dispatch([]string{"check-cpu", "extra"}); code != exitError {
		t.Fatalf("check-cpu with an argument: %d", code)
	}
	if code := dispatch([]string{"run", "--config", "x.yaml"}); code != exitError {
		t.Fatalf("run without its flags: %d", code)
	}
	if code := dispatch([]string{"dispatch"}); code != exitError {
		t.Fatalf("dispatch without --out: %d", code)
	}
	if code, err := runCmd(context.Background(), nil); err == nil || code != exitError {
		t.Fatalf("run: %d, %v", code, err)
	}
}
