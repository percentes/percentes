package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/configs"
	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/report"
)

// gateReason matches a cell problem the client-validity gate alone
// gives a cell it ends: its outcome, where the ladder ended, the step G2
// failed and each step after it.
var gateReason = regexp.MustCompile(`^(outcome client_invalid: client-validity gate failed|ladder ended at step \d+: client_validity$|step \d+: G2 did not pass$|step \d+ not attempted$)`)

// shortRehearsal scales the rehearsal to shortLadder's ladder and CPU
// window, a 2 s hard timeout, ceiling checks with no spacing, the
// end-to-end test's offset minute and short mock streams whose reasoning
// still outruns Shape R's 16-token ceiling. editRoute, where not nil,
// edits each route once it is built.
func shortRehearsal(t *testing.T, editRoute func(*route)) {
	t.Helper()
	orig := settingsAt
	settingsAt = func(int) rehearsalSettings {
		return rehearsalSettings{
			ladder:     config.Ladder{StepFractions: []float64{2, 4, 8, 16, 32}, WarmupS: 1, StepS: 2, DrainS: 1},
			bindingRPS: 1,
			cpuWindowS: 1,
			timeoutS:   2,
			checkPace:  1e6,
			minute:     e2eMinute,
			ttft:       config.LatencyDist{Distribution: "fixed", FixedMs: 5},
			itl:        config.LatencyDist{Distribution: "fixed", FixedMs: 1},
			reasoning:  20,
			answer:     e2eAnswer,
			editRoute:  editRoute,
		}
	}
	t.Cleanup(func() { settingsAt = orig })
}

// Under the release's limits, caps and prices, each rehearsal route at
// the ceiling its check chooses solves to its release binding rate:
// anthropic 150.909 rps on requests_per_minute over 1 s at 10,000 RPM
// and 75.454 at 5,000, azure 0.972 on tokens_per_minute, which counts
// output at generation, azure-R 3.636 at 256 on requests_per_minute over
// 1 s, baseten 0.249 on requests_per_minute, and gemini 4.010 on
// requests_per_day where three attempts share a reset day. The 5,000
// RPM table is a copy, and any other value is refused.
func TestRehearsalRoutesSolve(t *testing.T) {
	day := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
	for _, rpm := range []int{5_000, 10_000} {
		routes, err := rehearsalRoutesAt(rpm)
		if err != nil {
			t.Fatal(err)
		}
		for i := range routes {
			sp := &routes[i]
			f := formOf(sp.form)
			cfg, err := routeConfig(configs.ReleaseReference, sp, f, sp.ceiling, "https://"+rehearsalHost+":1"+f.path, keyEnv(f), "2026-11-01")
			if err != nil {
				t.Fatalf("%s: %v", sp.name, err)
			}
			plan := admission.ExpectedPlan(cfg, 1)
			for k := 0; k < 3; k++ {
				plan.Attempts = append(plan.Attempts, admission.Attempt{Start: day.Add(time.Duration(4*k) * time.Hour)})
			}
			sol, err := admission.Solve(cfg, admission.Price{InputUSDPerMTok: sp.priceIn, OutputUSDPerMTok: sp.priceOut, Source: rehearsalURL, Retrieved: "2026-11-01"}, plan)
			if err != nil {
				t.Fatalf("%s: %v", sp.name, err)
			}
			want, binding, windowS := sp.rps, sp.binding, sp.windowS
			if sp.name == "gemini" {
				want, binding, windowS = 4.010, config.LimitRPD, 0
			}
			if sol.BindingRPS != want || sol.Binding.Dimension != binding || sol.Binding.WindowS != windowS {
				t.Errorf("%s at %d, %d RPM: %g rps on %s over %g s, want %g on %s over %g s", sp.name, sp.ceiling, rpm, sol.BindingRPS, sol.Binding.Dimension, sol.Binding.WindowS, want, binding, windowS)
			}
			if sp.name == "anthropic" && (sp.rps != rehearsalRPMs[rpm] || !slices.ContainsFunc(cfg.Target.Limits, func(l config.Limit) bool {
				return l.Dimension == config.LimitRPM && l.Value == float64(rpm)
			})) {
				t.Errorf("anthropic at %d RPM: %g rps, limits %+v", rpm, sp.rps, cfg.Target.Limits)
			}
			if cfg.Release.AccountGroup != sp.group || cfg.Release.Shape.Name != sp.shape || (sp.fallback == nil) != (sp.shape == config.ShapeS) {
				t.Errorf("%s: group %q, shape %+v", sp.name, cfg.Release.AccountGroup, cfg.Release.Shape)
			}
		}
	}
	if l := rehearsalRoutes[0].limits(); rehearsalRoutes[0].rps != 150.909 || l[0].Dimension != config.LimitRPM || l[0].Value != 10_000 {
		t.Errorf("the built-in table changed: %g rps, %+v", rehearsalRoutes[0].rps, l[0])
	}
	if _, err := rehearsalRoutesAt(7_500); err == nil || !strings.Contains(err.Error(), "want 5000 or 10000") {
		t.Errorf("7500 RPM: %v", err)
	}
	if err := checkAccountGroups(rehearsalConfigs(t)); err != nil {
		t.Errorf("account groups: %v", err)
	}
}

// rehearsalConfigs are the rehearsal routes at their chosen ceilings.
func rehearsalConfigs(t *testing.T) []*config.Config {
	t.Helper()
	var out []*config.Config
	for i := range rehearsalRoutes {
		sp := &rehearsalRoutes[i]
		f := formOf(sp.form)
		cfg, err := routeConfig(configs.ReleaseReference, sp, f, sp.ceiling, "https://"+rehearsalHost+":1"+f.path, keyEnv(f), "2026-11-01")
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, cfg)
	}
	return out
}

// The rehearsal runs its five steps against mocks served over TLS under a
// self-signed certificate. Step 1 checks each release route's ceiling,
// azure-R taking its 256 fallback, freezes each route's binding rate,
// and runs one cell of each, concurrently at the schedule's offsets, the
// two Azure routes 80 minutes or more apart; each mock finishes every
// request its routes sent. Step 2 runs an azure-R cell under a mock rate
// limit below its binding limit, and the page labels every refusal at or
// below the limit's share, while a refusal whose write time the history
// loses reads comparison unresolved. Step 3 runs one step at 1.5 times
// the rate, and step 4 a cell whose top step the throttle and
// silent-hang faults hit, each reading the host's resources. Step 5
// excludes anthropic at 16 and moves gemini to 1,024. The rehearsal
// is green, and exits 0, when every step is. On a loaded host, under the
// race detector above all, the client-validity gate can fail: a cell it
// alone made client-invalid must still pass the completeness check, keep
// its tripwires and resources within bounds, and is logged with the
// gate's own reasons excused; every other reason fails the test, and the
// test is skipped after the rest is checked. The attribution cell's
// labels are checked where it counted, the 1.5 times cell's mock answers
// whatever its outcome, and the failure-mode cell's faults where it
// attempted its top ladder step, its mock answering 200 alone where it
// did not. A second rehearsal into the same directory is refused.
func TestRehearse(t *testing.T) { rehearseChecked(t, nil, 0) }

// TestRehearse's checks with the GC pause, CPU and send-skew bounds, which
// read this test process, widened (hostGatesWide): every cell counts, every
// step attempted, the rehearsal is green and nothing is skipped. A write
// the release write limit refused fails the gate, and so the test. A
// request left undispatched does too, on the third rehearsal: one a stall
// of this process leaves past the send bound rehearses again.
func TestRehearseWideGates(t *testing.T) { rehearseChecked(t, hostGatesWide, 2) }

func rehearseChecked(t *testing.T, editRoute func(*route), retries int) {
	enoughOpenFiles(t)
	fastDispatch(t)
	shortRehearsal(t, editRoute)
	in := rehearsalInput{out: filepath.Join(t.TempDir(), "rehearsal"), frames: t.TempDir(), clockSync: fakeSNTP(t, "+0.004211"),
		masterSeed: "percentes-rehearsal-test", scale: 1, byteCap: 2000, anthropicRPM: 10_000, build: testBuild}
	code, err := rehearse(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(in.out, rehearsalFile))
	if err != nil {
		t.Fatal(err)
	}
	var sum rehearsal
	if err := json.Unmarshal(b, &sum); err != nil {
		t.Fatal(err)
	}
	if retries > 0 && sum.undispatched() > 0 {
		t.Logf("%d requests undispatched on this host; rehearsing again", sum.undispatched())
		rehearseChecked(t, editRoute, retries-1)
		return
	}
	if sum.SchemaVersion != 2 || sum.FullLength || sum.Source.Revision == "" || sum.Cell == "" || len(sum.Routes) != len(rehearsalRoutes) || sum.Dispatch == nil || sum.AnthropicRPM != 10_000 {
		t.Fatalf("summary schema %d, full length %t, source %+v, cell %q, %d routes, dispatch %v, %d RPM", sum.SchemaVersion, sum.FullLength, sum.Source, sum.Cell, len(sum.Routes), sum.Dispatch, sum.AnthropicRPM)
	}
	keyValue := regexp.MustCompile(`rehearsal-[0-9a-f]{32}`)
	for _, sp := range rehearsalRoutes {
		p := filepath.Join(in.out, "inputs", sp.name+".yaml")
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(p)
		if err != nil || fi.Mode().Perm() != 0o600 || keyValue.Match(raw) || !strings.Contains(string(raw), keyEnv(formOf(sp.form))) {
			t.Errorf("%s: replay input mode %v, %v", sp.name, fi.Mode().Perm(), err)
		}
	}
	if d := sum.Disk; d == nil || len(d.Routes) != len(rehearsalRoutes) || len(d.Missing) > 0 || d.Attempts != rehearsalAttemptsPerWindow || d.FreeOutput == 0 || d.FreeFrames == 0 {
		t.Errorf("disk projection %+v", d)
	} else {
		ceilings := map[string]int{"anthropic": 16, "azure": 1024, "azure-R": 256, "gemini": 1024, "baseten": 1024}
		for i, rd := range d.Routes {
			sp := &rehearsalRoutes[i]
			wantMult := diskMargin * max(1, float64(ceilings[sp.name])/float64(rd.MockEvents))
			if rd.Route != sp.name || rd.CeilingTokens != ceilings[sp.name] || rd.MockEvents < 1 || rd.OutputBytes == 0 || rd.FramesBytes == 0 || rd.OutputMultiple != diskMargin || rd.FramesMultiple != wantMult {
				t.Errorf("disk projection of %s: %+v, want frames multiple %g", sp.name, rd, wantMult)
			}
		}
	}

	excused := map[string]bool{}
	cellOK := func(rr *rehearsalRoute) {
		t.Helper()
		switch {
		case rr == nil:
			t.Error("no cell")
		case rr.Outcome == outcomeCounted:
			if !rr.Green || len(rr.Problems) > 0 {
				t.Errorf("step %d %s: counted, green %t, problems %q", rr.Step, rr.Route, rr.Green, rr.Problems)
			}
		case rr.Outcome == outcomeInvalid && gateReasons(rr.Reasons):
			excused[fmt.Sprintf("step %d: %s: ", rr.Step, rr.Route)] = true
			if rr.Green {
				t.Errorf("step %d %s: client-invalid and green", rr.Step, rr.Route)
			}
			var resources []string
			if rr.Resources != nil {
				resources = rr.Resources.problems()
			}
			if rr.Completeness != "pass" || rr.Manifest == nil || rr.Replay != "pass" || rr.Tripwires == nil || rr.Tripwires.Fired || len(resources) > 0 {
				t.Errorf("step %d %s: client-invalid with completeness %q, manifest %v, replay %q, tripwires %+v, resource problems %q", rr.Step, rr.Route, rr.Completeness, rr.Manifest, rr.Replay, rr.Tripwires, resources)
			}
		default:
			t.Errorf("step %d %s: outcome %s %q, problems %q", rr.Step, rr.Route, rr.Outcome, rr.Reasons, rr.Problems)
		}
	}

	offsets := map[string]int{}
	sent := 0
	for i, rr := range sum.Routes {
		sp := &rehearsalRoutes[i]
		f := formOf(sp.form)
		t.Logf("%s at %d min: %s %q", rr.Route, rr.OffsetMinutes, rr.Outcome, rr.Problems)
		for k, st := range rr.Steps {
			if st.G2 != nil && !st.G2.Pass {
				t.Logf("%s: step %d G2 %+v", rr.Route, k+1, *st.G2)
			}
		}
		if rr.Step != 1 || rr.Route != sp.name || rr.Adapter != f.adapter || rr.AuthHeader != f.auth || rr.Shape != sp.shape || rr.AccountGroup != sp.group ||
			rr.Run.Label != sum.Cell || rr.Run.Route != sp.name {
			t.Errorf("route %d: %+v", i, rr)
		}
		cellOK(&sum.Routes[i])
		offsets[rr.Route] = rr.OffsetMinutes
		if len(rr.Steps) != 5 || !rr.Steps[0].Attempted || rr.Steps[0].G2 == nil {
			t.Errorf("%s: steps %+v", rr.Route, rr.Steps)
		}
		if c := rr.Ceiling; c.Excluded || c.Ceiling != sp.ceiling {
			t.Errorf("%s: ceiling choice %+v, want %d", rr.Route, c, sp.ceiling)
		}
		pin, _ := config.LookupShape(sp.shape)
		want := []probe.CeilingCheck{{Ceiling: pin.Ceiling, LengthValue: probe.LengthValue(f.adapter), Sent: probe.CeilingRequests, Passed: probe.CeilingRequests, Pass: true}}
		if sp.name == "azure-R" {
			want = []probe.CeilingCheck{{Ceiling: 16, LengthValue: "length", Sent: probe.CeilingRequests}, {Ceiling: 256, LengthValue: "length", Sent: probe.CeilingRequests, Passed: probe.CeilingRequests, Pass: true}}
		}
		if !slices.Equal(rr.CeilingChecks, want) {
			t.Errorf("%s: ceiling checks %+v, want %+v", rr.Route, rr.CeilingChecks, want)
		}
		switch {
		case sp.rps > 0 && (rr.BindingRPS != sp.rps || rr.BindingLimit != sp.binding || rr.BindingWindowS != sp.windowS):
			t.Errorf("%s: binding %g rps on %s over %g s, want %g", rr.Route, rr.BindingRPS, rr.BindingLimit, rr.BindingWindowS, sp.rps)
		case sp.name == "gemini" && (rr.BindingRPS > 4.015 || rr.BindingLimit != config.LimitRPD && rr.BindingLimit != admission.RouteCapDimension):
			t.Errorf("gemini: binding %g rps on %s", rr.BindingRPS, rr.BindingLimit)
		}
		if rr.Sent.Probe != 1 {
			t.Errorf("%s: sent %+v", rr.Route, rr.Sent)
		}
		sent += rr.Sent.Warmup + rr.Sent.Measured
		if rr.Completeness != "pass" || rr.Manifest == nil || rr.Replay != "pass" {
			t.Errorf("%s: completeness %q, manifest %v, replay %q", rr.Route, rr.Completeness, rr.Manifest, rr.Replay)
			continue
		}
		if ref, err := evidence.HashFile(filepath.Join(in.out, filepath.FromSlash(rr.Dir), evidence.ManifestFile), evidence.ManifestFile); err != nil || ref != *rr.Manifest {
			t.Errorf("%s: manifest %+v, summary %+v, %v", rr.Route, ref, *rr.Manifest, err)
		}
	}
	var mins []int
	for _, m := range offsets {
		mins = append(mins, m)
	}
	slices.Sort(mins)
	if !slices.Equal(mins, []int{0, 40, 80, 120, 160}) {
		t.Errorf("offsets %v, want a permutation of 0 to 160 in forties", offsets)
	}
	if d := (offsets["azure"] - offsets["azure-R"] + 240) % 240; d < 80 || 240-d < 80 {
		t.Errorf("azure at %d min and azure-R at %d min, less than 80 min apart", offsets["azure"], offsets["azure-R"])
	}
	for _, m := range sum.Mocks {
		if len(m.Finished) != 1 || m.Finished["200"] != m.Sent || m.Sent == 0 {
			t.Errorf("mock %s (%v): finished %v, sent %d", m.Form, m.Routes, m.Finished, m.Sent)
		}
	}
	if len(sum.Mocks) != 4 || !slices.Equal(sum.Mocks[1].Routes, []string{"azure", "azure-R"}) {
		t.Errorf("mocks %+v", sum.Mocks)
	}
	total := 0
	for _, c := range sum.Dispatch.Counts {
		total += c
	}
	if len(sum.Dispatch.Files) != len(rehearsalRoutes) || total != sent {
		t.Errorf("dispatch series of %d files counts %d sends, the cells sent %d", len(sum.Dispatch.Files), total, sent)
	}

	a := sum.Attribution
	cellOK(a)
	if a != nil {
		if a.Step != 2 || a.Route != "azure-R" || a.RateLimit == nil || *a.RateLimit != (rehearsalRateLimit{Requests: 3, WindowS: 1}) {
			t.Errorf("step 2: %+v", a)
		}
		if got := a.Finished; got["429"] != a.Status429 || got["200"]+got["429"] != a.sent() {
			t.Errorf("step 2: the mock finished %v; sent %d", got, a.sent())
		}
		if a.Outcome == outcomeCounted && a.Status429 == 0 {
			t.Errorf("step 2: counted with no status_429, problems %q", a.Problems)
		}
		if l := a.Labels; a.Outcome == outcomeCounted && (l == nil || l.Refusals != a.Status429 || l.Page != (report.LabelCounts{AtOrBelow: a.Status429}) || l.UnwrittenAs != report.LabelUnresolved) {
			t.Errorf("step 2 labels %+v, %d status_429, problems %q", l, a.Status429, a.Problems)
		}
	}
	o := sum.Overload
	cellOK(o)
	if o != nil {
		if o.Step != 3 || o.Route != "anthropic" || len(o.Steps) != 1 || o.Steps[0].RateRPS != 1.5 || o.Resources == nil || o.Resources.PeakOpenFiles == 0 || o.Resources.CellBytes == 0 ||
			o.Resources.WriteDelay == nil || o.Resources.WriteDelay.Written == 0 {
			t.Errorf("step 3: %+v, resources %+v", o, o.Resources)
		}
		if !maps1(o.Finished, "200", o.sent()) {
			t.Errorf("step 3: the mock finished %v; the cell sent %d", o.Finished, o.sent())
		}
	}
	fm := sum.FailureMode
	cellOK(fm)
	if fm != nil {
		if fm.Step != 4 || fm.Route != "anthropic" || len(fm.Faults) != 2 || fm.Faults[0].Mode != config.MockFaultThrottle || fm.Faults[1].Mode != config.MockFaultSilentHang || fm.Resources == nil {
			t.Errorf("step 4: %+v, problems %q", fm, fm.Problems)
		} else if hang := fm.Faults[1]; (hang.PlannedEndOffsetS-hang.PlannedStartOffsetS)*config.PinnedReleaseStepS/sum.Ladder.StepS < 150-1e-6 {
			t.Errorf("step 4: the silent hang spans %g s of a %g s step, under 150 s at the pinned step", hang.PlannedEndOffsetS-hang.PlannedStartOffsetS, sum.Ladder.StepS)
		}
		top := len(fm.Steps) > 0 && fm.Steps[len(fm.Steps)-1].Attempted
		if top && (fm.Finished["429"] == 0 || fm.Finished["hung"] == 0 || fm.Status429 != fm.Finished["429"] || fm.Censored == 0) {
			t.Errorf("step 4 faults: %+v, problems %q", fm, fm.Problems)
		}
		if !top && !maps1(fm.Finished, "200", fm.sent()) {
			t.Errorf("step 4: top ladder step not attempted, the mock finished %v; the cell sent %d", fm.Finished, fm.sent())
		}
	}

	if len(sum.Checks) != 2 {
		t.Fatalf("step 5: %+v", sum.Checks)
	}
	if c := sum.Checks[0]; c.Route != "anthropic" || !c.Choice.Excluded || c.Choice.Reason == "" || len(c.Checks) != 1 || c.Checks[0].Ceiling != 16 || c.Checks[0].Pass || !c.Green {
		t.Errorf("step 5 anthropic: %+v", c)
	}
	if c := sum.Checks[1]; c.Route != "gemini" || c.Choice.Excluded || c.Choice.Ceiling != 1024 || len(c.Checks) != 2 || c.Checks[0].Pass || !c.Checks[1].Pass || !c.Green {
		t.Errorf("step 5 gemini: %+v", c)
	}
	for _, c := range sum.Checks {
		for _, f := range c.Files {
			if _, err := os.Stat(filepath.Join(in.out, filepath.FromSlash(f))); err != nil {
				t.Errorf("step 5 %s: %v", c.Route, err)
			}
		}
	}

	if green := len(sum.Reasons) == 0; sum.Green != green || sum.Green != (code == exitOK) || !sum.Green && code != exitAdverse {
		t.Errorf("green %t with reasons %q, exit %d", sum.Green, sum.Reasons, code)
	}
	for _, r := range sum.Reasons {
		ok := false
		for p := range excused {
			ok = ok || strings.HasPrefix(r, p) && gateReason.MatchString(strings.TrimPrefix(r, p))
		}
		if !ok {
			t.Errorf("not green: %s", r)
		}
	}

	if _, err := rehearse(context.Background(), in); err == nil || !strings.Contains(err.Error(), "is not empty") {
		t.Errorf("second rehearsal into %s: %v", in.out, err)
	}
	if t.Failed() {
		t.FailNow()
	}
	if len(excused) > 0 {
		if editRoute != nil {
			t.Fatalf("client-validity gate failed under wide bounds in %d cells; outcomes logged above", len(excused))
		}
		t.Skip("client-validity gate failed on this host; outcomes logged above")
	}
}

// A built rehearsal edits no route, and a rehearsal that edits one is not
// the full-length form.
func TestRehearsalEditRoute(t *testing.T) {
	set := settingsAt(1)
	if set.editRoute != nil || !set.fullLength() {
		t.Errorf("settingsAt(1): edit %t, full length %t", set.editRoute != nil, set.fullLength())
	}
	set.editRoute = hostGatesWide
	if set.fullLength() {
		t.Error("full length with a route edit")
	}
}

// undispatched is the requests r's cells left undispatched.
func (r *rehearsal) undispatched() int {
	cells := []*rehearsalRoute{r.Attribution, r.Overload, r.FailureMode}
	for i := range r.Routes {
		cells = append(cells, &r.Routes[i])
	}
	n := 0
	for _, rr := range cells {
		if rr == nil {
			continue
		}
		for _, st := range rr.Steps {
			if st.G2 != nil {
				n += st.G2.Undispatched
			}
		}
	}
	return n
}

// The resource bounds refuse a peak resident set size above half the
// host's memory, open descriptors at the open-file floor and sockets to
// one destination filling the local port range; readings within them
// pass.
func TestRehearsalResourceBounds(t *testing.T) {
	ok := rehearsalResources{PeakRSSBytes: 1 << 30, MemoryBytes: 8 << 30, PeakOpenFiles: 7000, PeakSockets: 7000, LocalPortRange: []int{32768, 60999},
		CellBytes: 200 << 20}
	if p := ok.problems(); len(p) != 0 {
		t.Errorf("within bounds: %q", p)
	}
	for _, tc := range []struct {
		edit func(r *rehearsalResources)
		want string
	}{
		{func(r *rehearsalResources) { r.PeakRSSBytes = 4<<30 + 1 }, "above half the host's"},
		{func(r *rehearsalResources) { r.PeakOpenFiles = minOpenFiles }, "open descriptors, at or above"},
		{func(r *rehearsalResources) { r.PeakSockets = 60999 - 32768 + 1 }, "at or above the local port range"},
	} {
		r := ok
		tc.edit(&r)
		if p := strings.Join(r.problems(), "; "); !strings.Contains(p, tc.want) {
			t.Errorf("%+v: problems %q, want %q", r, p, tc.want)
		}
	}
}

// A window's projected disk sums every route's attempts at their
// multiples: on one filesystem output and frames together against its
// free disk, on two each against its own. A route with no cell is a
// problem.
func TestRehearsalWindowDisk(t *testing.T) {
	routes := []routeDisk{{Route: "a", OutputBytes: 4, FramesBytes: 2, OutputMultiple: 1, FramesMultiple: 1}, {Route: "b", OutputBytes: 4, FramesBytes: 1, OutputMultiple: 1, FramesMultiple: 2}}
	for _, tc := range []struct {
		shared              bool
		freeOut, freeFrames uint64
		want                string
	}{
		{true, 169, 169, ""},
		{true, 168, 500, "112 bytes of output and 56 of frames on one filesystem with 168 free"},
		{false, 113, 57, ""},
		{false, 112, 500, "112 bytes of output with 112 free"},
		{false, 200, 56, "56 bytes of frames with 56 free"},
	} {
		w := &windowDisk{Routes: routes, Shared: tc.shared, FreeOutput: tc.freeOut, FreeFrames: tc.freeFrames}
		w.project()
		got := strings.Join(w.problems(), "; ")
		if w.Output != 112 || w.Frames != 56 || (tc.want == "") != (got == "") || !strings.Contains(got, tc.want) {
			t.Errorf("shared %t, free %d and %d: projected %d and %d, problems %q, want %q", tc.shared, tc.freeOut, tc.freeFrames, w.Output, w.Frames, got, tc.want)
		}
	}
	w := &windowDisk{Routes: routes, FreeOutput: 1 << 30, FreeFrames: 1 << 30, Missing: []string{"c"}}
	w.project()
	if p := w.problems(); !slices.Equal(p, []string{"no rehearsal cell of c to project"}) {
		t.Errorf("unsampled route: %q", p)
	}
}

// At full length step 1's mocks stream anthropic and azure-R to their
// ceilings, and the Shape S routes fewer events than their 1,024-token
// fallback, whose frames the projection scales up to it.
func TestRehearsalMockEvents(t *testing.T) {
	h := &rehearsalRun{specs: rehearsalRoutes, streams: stepOneStreams(settingsAt(1))}
	for _, sp := range rehearsalRoutes {
		h.checks = append(h.checks, &routeCheck{choice: probe.CeilingChoice{Ceiling: sp.ceiling}})
	}
	want := map[string]int{"anthropic": 16, "azure": 326, "azure-R": 256, "gemini": 126, "baseten": 326}
	for i, sp := range rehearsalRoutes {
		if got := h.mockEvents(i); got != want[sp.name] {
			t.Errorf("%s: %d events, want %d", sp.name, got, want[sp.name])
		}
	}
}

// The rehearse command refuses a missing flag, a scale whose step is
// shorter than the CPU window, an anthropic route limit other than
// 5,000 or 10,000 RPM, a missing byte cap, a frame store in the
// source tree, an empty master seed, a clock-sync command it cannot
// parse, an --out path holding white space, too few open files and a
// build from a modified tree without its diff, and writes nothing.
func TestRehearseRefusals(t *testing.T) {
	enoughOpenFiles(t)
	out := filepath.Join(t.TempDir(), "rehearsal")
	frames := t.TempDir()
	set := []string{"--out", out, "--frames", frames, "--clock-sync", "chronyc tracking"}
	with := func(args ...string) []string { return append(slices.Clone(set), args...) }
	for _, tc := range []struct {
		args  []string
		files uint64
		want  string
	}{
		{[]string{"--frames", frames, "--clock-sync", "chronyc tracking"}, minOpenFiles, "--out is required"},
		{[]string{"--out", out, "--clock-sync", "chronyc tracking"}, minOpenFiles, "--frames is required"},
		{[]string{"--out", out, "--frames", frames}, minOpenFiles, "--clock-sync is required"},
		{with("--scale", "0"), minOpenFiles, "--scale 0"},
		{with("--anthropic-rpm", "5000", "--scale", "0"), minOpenFiles, "--scale 0"},
		{with("--anthropic-rpm", "7500", "--byte-cap", "2000"), minOpenFiles, "--anthropic-rpm 7500: want 5000 or 10000"},
		{with("--scale", "49", "--byte-cap", "2000"), minOpenFiles, "shorter than the 5 s CPU window"},
		{with("--scale", "48"), minOpenFiles, "--byte-cap 0"},
		{[]string{"--out", out, "--frames", ".", "--clock-sync", "chronyc tracking", "--byte-cap", "2000"}, minOpenFiles, "inside the instrument source tree"},
		{[]string{"--out", out, "--frames", frames, "--clock-sync", "ntpdate -q pool", "--byte-cap", "2000"}, minOpenFiles, "clock-sync command"},
		{with("--byte-cap", "2000", "--master-seed", ""), minOpenFiles, "--master-seed is empty"},
		{[]string{"--out", out + " two", "--frames", frames, "--clock-sync", "chronyc tracking", "--byte-cap", "2000"}, minOpenFiles, "holds white space"},
		{with("--byte-cap", "2000"), 10240, "open-file limit 10240"},
	} {
		openFilesLimit = func() (uint64, error) { return tc.files, nil }
		code, err := rehearseCmd(context.Background(), tc.args)
		if code != exitError || err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%q: exit %d, %v; want %q", tc.args, code, err, tc.want)
		}
	}
	modified := &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "0123456789abcdef0123456789abcdef01234567"}, {Key: "vcs.modified", Value: "true"}}}
	openFilesLimit = func() (uint64, error) { return minOpenFiles, nil }
	in := rehearsalInput{out: out, frames: frames, clockSync: "chronyc tracking", masterSeed: "percentes-rehearsal-test", scale: 1, byteCap: 2000, anthropicRPM: 10_000, build: modified}
	if _, err := rehearse(context.Background(), in); err == nil || !strings.Contains(err.Error(), "--source-diff is required") {
		t.Errorf("modified build: %v", err)
	}
	for _, p := range []string{out, out + " two"} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s: %v, want nothing written", p, err)
		}
	}
}
