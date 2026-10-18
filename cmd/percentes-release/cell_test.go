package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
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

// chatStream is a complete chat-completions stream of two content
// events.
const chatStream = "data: {\"model\":\"openai/gpt-oss-120b\",\"choices\":[{\"delta\":{\"content\":\"Two \"}}]}\n\n" +
	"data: {\"model\":\"openai/gpt-oss-120b\",\"choices\":[{\"delta\":{\"content\":\"sentences.\"}}]}\n\n" +
	"data: {\"model\":\"openai/gpt-oss-120b\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
	"data: [DONE]\n\n"

// endpoint serves the route: status(n, body) answers the nth request,
// 1-based, with a status and an error body, or 200 and chatStream. It
// records every request body.
type endpoint struct {
	srv    *httptest.Server
	hits   atomic.Int64
	mu     sync.Mutex
	bodies [][]byte
}

func newEndpoint(t *testing.T, status func(n int64) (int, string)) *endpoint {
	t.Helper()
	e := &endpoint{}
	e.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		e.mu.Lock()
		e.bodies = append(e.bodies, b)
		e.mu.Unlock()
		if s, body := status(e.hits.Add(1)); s != http.StatusOK {
			w.WriteHeader(s)
			io.WriteString(w, body) //nolint:errcheck
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, chatStream) //nolint:errcheck
	}))
	t.Cleanup(e.srv.Close)
	return e
}

func (e *endpoint) url() string { return e.srv.URL + "/openai/v1/chat/completions" }

func always200(int64) (int, string) { return http.StatusOK, "" }

// newTestRoute is cfg's route over a fixture with the clock at offset.
func newTestRoute(t *testing.T, cfg *config.Config, offset string) (*route, *fixture) {
	t.Helper()
	enoughOpenFiles(t)
	f := newFixture(t, cfg, offset)
	r, err := newRoute(f.in)
	if err != nil {
		t.Fatal(err)
	}
	return r, f
}

// checkRetained runs the completeness check over run's directory,
// requires its report pair to replay byte for byte, and returns its
// manifest and report text.
func checkRetained(t *testing.T, r *route, run schedule.Run) (*evidence.Manifest, string) {
	t.Helper()
	dir := r.cellDir(run)
	m, err := evidence.CheckRelease(evidence.Release{Dir: dir, Config: r.cfg.Raw, FramesRoot: r.framesRoot, Pair: [2]string{report.JSONFile, report.TextFile}})
	if err != nil {
		t.Fatal(err)
	}
	raw, text, err := report.FromRun(dir, r.cfg.Raw)
	if err != nil {
		t.Fatal(err)
	}
	for name, b := range map[string][]byte{report.JSONFile: raw, report.TextFile: []byte(text)} {
		if got, err := os.ReadFile(filepath.Join(dir, name)); err != nil || !bytes.Equal(got, b) {
			t.Fatalf("%s does not replay byte for byte: %v", name, err)
		}
	}
	return m, text
}

func ledgerState(t *testing.T, path string) *admission.State {
	t.Helper()
	l, err := admission.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	st, err := l.State()
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// A cell runs the ladder at the admitted steps only, with Shape S texts
// under the run's seed, settles what it sent, and writes its evidence:
// the attempts without answer text, the probe, the final dispatch
// series, a manifest carrying the cell record, its label, the host's
// aggregate dispatch series and the inter-chunk gap histograms, and the
// report pair.
func TestCellRunsAdmittedSteps(t *testing.T) {
	e := newEndpoint(t, always200)
	r, f := newTestRoute(t, releaseConfig(t, e.url()), "+0.005032")
	if got := r.verdict.Admitted(); len(got) != 3 || r.steps != 3 {
		t.Fatalf("admitted %v, steps %d; the reference limits admit 0.25, 0.5 and 1 rps", got, r.steps)
	}
	shortLadder(r)
	run := r.runs[0]
	rec, err := r.runCell(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("outcome %s %q", rec.Outcome, rec.Reasons)
	if rec.Outcome != outcomeCounted {
		for _, reason := range rec.Reasons {
			if !strings.HasPrefix(reason, "client-validity gate failed") && !strings.HasPrefix(reason, "ladder ended at step") {
				t.Errorf("reason %q", reason)
			}
		}
	}
	if got := readCell(t, r, run); got.Outcome != rec.Outcome || got.EndUTC == nil || got.Run != run {
		t.Fatalf("cell record %+v", got)
	}
	if l := rec.Ladder; l == nil || len(l.Steps) != 3 || (rec.Outcome == outcomeCounted && !l.Steps[2].Attempted) {
		t.Fatalf("ladder %+v", l)
	}
	dir := r.cellDir(run)
	for _, name := range []string{cellFile, evidence.AttemptsFile, evidence.ManifestFile, probe.File, dispatchFile, evidence.ClockFile, evidence.StatusStartFile, evidence.StatusEndFile,
		evidence.BalanceStartFile, evidence.BalanceEndFile, "ceiling-probe-groq-512.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Error(err)
		}
	}

	b, err := os.ReadFile(filepath.Join(dir, evidence.AttemptsFile))
	if err != nil {
		t.Fatal(err)
	}
	recs, err := evidence.ReadRecords(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	sent := 0
	for _, a := range recs {
		if a.Step < 1 || a.Step > 3 || a.Answer != nil {
			t.Fatalf("record %d: step %d, answer %v", a.Index, a.Step, a.Answer)
		}
		if a.DispatchNs != 0 {
			sent++
		}
	}
	if rec.Sent.Probe != 1 || rec.Sent.Warmup+rec.Sent.Measured != sent || int(e.hits.Load()) != sent+1 {
		t.Fatalf("sent %+v, %d dispatched records, server saw %d", rec.Sent, sent, e.hits.Load())
	}
	if u := ledgerState(t, f.ledger).Routes["groq"]; u == nil || u.Reserved != 0 || u.Probe+u.Warmup+u.Measured != sent+1 {
		t.Fatalf("ledger usage %+v", u)
	}
	e.mu.Lock()
	for i, body := range e.bodies {
		if prefix := fmt.Sprintf("cs-%d-", run.Seed); !bytes.Contains(body, []byte(prefix)) {
			t.Fatalf("request %d does not carry %s: %.120s", i, prefix, body)
		}
	}
	e.mu.Unlock()

	m, _ := checkRetained(t, r, run)
	c := m.Release
	if c == nil || c.Run != run || c.OffsetMinutes != r.offsetMinutes || c.AccountCreated != "2026-09-15" || len(c.Probes) != 1 || c.Admission.TopRPS != 1 || c.CeilingTokens != 512 {
		t.Fatalf("cell record %+v", c)
	}
	if m.Counted() != (rec.Outcome == outcomeCounted) || c.Outcome != rec.Outcome || !c.LadderStarted || !slices.Equal(c.Reasons, rec.Reasons) {
		t.Fatalf("manifest: counted %t, cell %+v", m.Counted(), c)
	}
	if m.Seed != run.Seed || m.RunValid != (rec.Outcome == outcomeCounted) || m.Artifact(dispatchFile) == nil {
		t.Fatalf("manifest seed %d, valid %t, artifacts %+v", m.Seed, m.RunValid, m.Artifacts)
	}
	var ds dispatchSeries
	if raw, err := os.ReadFile(filepath.Join(dir, dispatchFile)); err != nil || json.Unmarshal(raw, &ds) != nil {
		t.Fatalf("dispatch series: %v", err)
	}
	total := 0
	for _, n := range ds.Counts {
		total += n
	}
	if total != sent || ds.Run != run || !ds.Final {
		t.Fatalf("dispatch series counts %d of %d sent, final %t", total, sent, ds.Final)
	}
	if d := c.Dispatch; d == nil || len(d.Sources) != 1 || d.Sources[0].Run != run || d.Sources[0].Dispatches != sent || !d.Sources[0].Final {
		t.Fatalf("aggregate dispatch series %+v", d)
	}
	completed := map[int]int64{}
	for _, a := range recs {
		if a.Outcome == loadgen.OutcomeCompleted && !a.Warmup {
			completed[a.Step]++
		}
	}
	var gaps int64
	for _, g := range c.Gaps {
		if g.Gaps != completed[g.Step] {
			t.Errorf("step %d: %d gaps over %d completions of two content events", g.Step, g.Gaps, completed[g.Step])
		}
		gaps += g.Gaps
	}
	if gaps == 0 || len(c.Gaps) != len(completed) {
		t.Fatalf("gap histograms %+v for completions per step %v", c.Gaps, completed)
	}
}

// A documented billing refusal inside the ladder stops the route: the
// cell keeps its evidence, labelled with the refusal and outside the
// route's frame, and the loop ends with the stop code.
func TestBillingRefusalStopsRoute(t *testing.T) {
	e := newEndpoint(t, func(n int64) (int, string) {
		if n >= 3 {
			return http.StatusBadRequest, `{"error":{"message":"Organization has been restricted.","code":"blocked_api_access"}}`
		}
		return http.StatusOK, ""
	})
	cfg := releaseConfig(t, e.url())
	cfg.Target.BillingRefusals = []config.BillingRefusal{{Status: 400, BodyContains: "blocked_api_access", Meaning: "spend limit reached", Source: "errors.png", Retrieved: "2026-10-07"}}
	r, _ := newTestRoute(t, cfg, "+0.005032")
	shortLadder(r)
	run := r.runs[0]
	r.now, r.wait = func() time.Time { return run.Start }, noWait
	code, err := r.loop(context.Background())
	if err != nil || code != exitStopped {
		t.Fatalf("loop: code %d, %v", code, err)
	}
	rec := readCell(t, r, run)
	if rec.Outcome != outcomeStopped || len(rec.Reasons) == 0 || !strings.Contains(rec.Reasons[0], "route stop: HTTP 400, the documented billing refusal: spend limit reached") {
		t.Fatalf("cell %+v", rec)
	}
	if rec.Interrupted == nil || rec.Interrupted.Unsent == 0 {
		t.Fatalf("interruption %+v", rec.Interrupted)
	}
	if n := e.hits.Load(); n < 3 {
		t.Fatalf("server saw %d requests", n)
	}
	m, _ := checkRetained(t, r, run)
	if m.RunValid || len(m.InvalidReasons) == 0 || m.InvalidReasons[0] != rec.Reasons[0] {
		t.Fatalf("manifest %+v", m)
	}
	if c := m.Release; m.Counted() || c.Outcome != outcomeStopped || !c.LadderStarted || !slices.Equal(c.Reasons, rec.Reasons) {
		t.Fatalf("cell label %+v", c)
	}
	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	if len(st.runs) != len(r.runs)+1 || st.next == nil || st.next.Attempt != 1 {
		t.Fatalf("after the stop: %d runs, next %+v; the stopped cell takes a re-run", len(st.runs), st.next)
	}
}

// A probe 401 after a cell whose ladder met 401s stops the route: the
// probed cell keeps a labelled manifest and report pair naming the
// earlier cell, the earlier cell keeps its record and its count, and the
// loop ends with the stop code.
func TestAuthFaultStopsRoute(t *testing.T) {
	e := newEndpoint(t, func(n int64) (int, string) {
		if n >= 3 {
			return http.StatusUnauthorized, `{"error":{"message":"Invalid API Key","code":"invalid_api_key"}}`
		}
		return http.StatusOK, ""
	})
	r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+0.005032")
	shortLadder(r)
	now := r.runs[0].Start
	r.now = func() time.Time { return now }
	r.wait = func(_ context.Context, t time.Time) error {
		now = t
		return nil
	}
	code, err := r.loop(context.Background())
	if err != nil || code != exitStopped {
		t.Fatalf("loop: code %d, %v", code, err)
	}
	first := readCell(t, r, r.runs[0])
	if first.Outcome == outcomeStopped || first.Outcome == outcomeBlocked || first.ProbeStatus != http.StatusOK {
		t.Fatalf("first cell %+v", first)
	}
	b, err := os.ReadFile(filepath.Join(r.cellDir(r.runs[0]), evidence.AttemptsFile))
	if err != nil {
		t.Fatal(err)
	}
	recs, err := evidence.ReadRecords(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	refused := 0
	for _, a := range recs {
		if a.Status == http.StatusUnauthorized {
			refused++
		}
	}
	if refused == 0 || len(first.AuthStatuses) != 1 || first.AuthStatuses[http.StatusUnauthorized] != refused {
		t.Fatalf("first cell counts %v; attempts file holds %d 401s", first.AuthStatuses, refused)
	}

	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	var second *cellRecord
	for _, run := range st.runs {
		if c := st.records[runKey(run)]; c != nil && run != r.runs[0] {
			if second != nil {
				t.Fatalf("cells %+v and %+v after the first", second.Run, c.Run)
			}
			second = c
		}
	}
	if second == nil {
		t.Fatal("no second cell")
	}
	want := fmt.Sprintf("route stop: probe request returned 401, as did %d requests inside its ladder at %s attempt 1: independent evidence of a credential or account fault (SPEC-HOSTED HA3 point 16)",
		refused, r.runs[0].Label)
	if second.Outcome != outcomeStopped || !slices.Equal(second.Reasons, []string{want}) || second.ProbeStatus != http.StatusUnauthorized || second.Sent != (admission.Counts{Probe: 1}) {
		t.Fatalf("second cell %+v", second)
	}
	m, text := checkRetained(t, r, second.Run)
	if c := m.Release; m.Counted() || c.Outcome != outcomeStopped || c.LadderStarted || !slices.Equal(c.Reasons, second.Reasons) {
		t.Fatalf("manifest: counted %t, cell %+v", m.Counted(), c)
	}
	if !strings.Contains(text, "\n  "+want+"\n") {
		t.Fatalf("report text:\n%s", text)
	}
	if again := readCell(t, r, r.runs[0]); again.Outcome != first.Outcome || !slices.Equal(again.Reasons, first.Reasons) {
		t.Fatalf("first cell rewritten: %+v", again)
	}
}

// A probe's 401 or 403 stops the route only when the route's previous
// cell to send its probe saw the same status: cells that sent nothing are
// passed over, and an interrupted cell with no probe recorded ends the
// search.
func TestAuthFaultEvidence(t *testing.T) {
	base := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	cell := func(i int, outcome string, probeStatus int, auth map[int]int) *cellRecord {
		c := &cellRecord{Run: schedule.Run{Route: "groq", Label: fmt.Sprintf("weekday-%02d", i), Attempt: 1}, Outcome: outcome, StartUTC: base.Add(time.Duration(i) * time.Hour),
			ProbeStatus: probeStatus, AuthStatuses: auth}
		if probeStatus != 0 {
			c.Sent.Probe = 1
		}
		return c
	}
	cur := cell(10, outcomeRunning, 0, nil)
	for _, tc := range []struct {
		name    string
		status  int
		earlier []*cellRecord
		want    string
	}{
		{"no earlier cell", 401, nil, ""},
		{"probe 401 again", 401, []*cellRecord{cell(1, outcomeBlocked, 401, nil)}, "probe request returned 401, as did its probe at weekday-01 attempt 1"},
		{"probe 403 again", 403, []*cellRecord{cell(1, outcomeBlocked, 403, nil)}, "probe request returned 403, as did its probe at weekday-01 attempt 1"},
		{"ladder 403s", 403, []*cellRecord{cell(1, outcomeCounted, 200, map[int]int{403: 5})}, "probe request returned 403, as did 5 requests inside its ladder at weekday-01 attempt 1"},
		{"another status", 401, []*cellRecord{cell(1, outcomeBlocked, 403, map[int]int{403: 2})}, ""},
		{"not an auth status", 404, []*cellRecord{cell(1, outcomeBlocked, 404, nil)}, ""},
		{"a clean cell between", 401, []*cellRecord{cell(1, outcomeCounted, 200, map[int]int{401: 3}), cell(2, outcomeCounted, 200, nil)}, ""},
		{"a 5xx probe between", 401, []*cellRecord{cell(1, outcomeBlocked, 401, nil), cell(2, outcomeCounted, 503, nil)}, ""},
		{"cells that sent nothing between", 401, []*cellRecord{cell(1, outcomeCounted, 200, map[int]int{401: 3}), cell(2, outcomeMissed, 0, nil), cell(3, outcomeStopped, 0, nil)},
			"probe request returned 401, as did 3 requests inside its ladder at weekday-01 attempt 1"},
		{"an interrupted cell between", 401, []*cellRecord{cell(1, outcomeBlocked, 401, nil), cell(2, outcomeInterrupted, 0, nil)}, ""},
		{"a later cell only", 401, []*cellRecord{cell(11, outcomeBlocked, 401, nil)}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &route{dir: t.TempDir()}
			for _, c := range append(tc.earlier, cur) {
				if err := replaceJSON(filepath.Join(runDir(r.dir, c.Run), cellFile), c); err != nil {
					t.Fatal(err)
				}
			}
			stop, err := r.authFault(cur, tc.status)
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case tc.want == "" && stop != nil:
				t.Fatalf("stop %q", stop.Error())
			case tc.want != "" && (stop == nil || stop.Error() != "route stop: "+tc.want+": independent evidence of a credential or account fault (SPEC-HOSTED HA3 point 16)"):
				t.Fatalf("stop %v, want %q", stop, tc.want)
			}
		})
	}
}

// Before the ladder: a clock offset above the bound or a ledger refusal
// stops the route with nothing sent; a probe the start gate blocks ends
// the cell after one request; a probe answered with a billing refusal
// stops the route. Each cell keeps a labelled manifest and report pair
// that pass the completeness check outside the route's frame.
func TestCellStopsAndBlocks(t *testing.T) {
	for _, tc := range []struct {
		name    string
		offset  string
		status  int
		body    string
		hosting string
		outcome string
		reason  string
		probes  int
	}{
		{"clock offset", "+75.5", 200, "", "", outcomeStopped, "route stop: clock offset: 75.5 s", 0},
		{"ledger refusal", "+0.005", 200, "", "1299.99", outcomeStopped, "route stop: admission: cell for groq refused", 0},
		{"probe 401", "+0.005", 401, "", "", outcomeBlocked, "probe request returned 401", 1},
		{"probe billing refusal", "+0.005", 402, "", "", outcomeStopped, "route stop: HTTP 402, the documented billing refusal", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEndpoint(t, func(int64) (int, string) { return tc.status, tc.body })
			cfg := releaseConfig(t, e.url())
			cfg.Target.BillingRefusals = []config.BillingRefusal{{Status: 402, Meaning: "balance exhausted", Source: "errors.png", Retrieved: "2026-10-07"}}
			r, f := newTestRoute(t, cfg, tc.offset)
			shortLadder(r)
			if tc.hosting != "" {
				l, err := admission.Open(f.ledger)
				if err != nil {
					t.Fatal(err)
				}
				if err := l.RecordHosting(tc.hosting, "client host"); err != nil {
					t.Fatal(err)
				}
			}
			run := r.runs[0]
			rec, err := r.runCell(context.Background(), run)
			if err != nil {
				t.Fatal(err)
			}
			if rec.Outcome != tc.outcome || len(rec.Reasons) != 1 || !strings.Contains(rec.Reasons[0], tc.reason) {
				t.Fatalf("outcome %s, reasons %q", rec.Outcome, rec.Reasons)
			}
			if n := e.hits.Load(); n != int64(tc.probes) || rec.Sent != (admission.Counts{Probe: tc.probes}) {
				t.Fatalf("server saw %d requests; sent %+v", n, rec.Sent)
			}
			m, text := checkRetained(t, r, run)
			c := m.Release
			if m.Counted() || c.Outcome != tc.outcome || c.LadderStarted || !slices.Equal(c.Reasons, rec.Reasons) || (m.Artifact(probe.File) != nil) != (tc.probes == 1) {
				t.Fatalf("manifest: counted %t, cell %+v, artifacts %+v", m.Counted(), c, m.Artifacts)
			}
			if b, err := os.ReadFile(filepath.Join(r.cellDir(run), evidence.AttemptsFile)); err != nil || len(b) != 0 {
				t.Fatalf("attempts file %q: %v", b, err)
			}
			if !strings.Contains(text, "Outcome: "+tc.outcome+"; a labelled record, outside the route's frame of 12 cells\n  "+rec.Reasons[0]+"\n") {
				t.Fatalf("report text:\n%s", text)
			}
			if u := ledgerState(t, f.ledger).Routes["groq"]; u != nil && u.Reserved != 0 {
				t.Fatalf("reservation left open: %+v", u)
			}
		})
	}
}

// Each cell reads the account balance at its start and at its end, also
// a cell stopped before its ladder, and its manifest names both records.
func TestCellBalanceRecords(t *testing.T) {
	e := newEndpoint(t, always200)
	r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+75.5")
	for i, run := range r.runs[:2] {
		rec, err := r.runCell(context.Background(), run)
		if err != nil {
			t.Fatal(err)
		}
		if rec.Outcome != outcomeStopped {
			t.Fatalf("cell %d: outcome %s %q", i, rec.Outcome, rec.Reasons)
		}
		m, _ := checkRetained(t, r, run)
		for j, b := range []struct {
			name string
			ref  *evidence.FileRef
		}{{evidence.BalanceStartFile, m.BalanceStart}, {evidence.BalanceEndFile, m.BalanceEnd}} {
			if b.ref == nil || b.ref.Name != b.name {
				t.Fatalf("cell %d: manifest names %+v for %s", i, b.ref, b.name)
			}
			raw, err := os.ReadFile(filepath.Join(r.cellDir(run), b.name))
			if err != nil {
				t.Fatal(err)
			}
			var got evidence.StatusRecord
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			want := fmt.Sprintf("balance USD 30.00, read %d\n", 2*i+j+1)
			if got.Stdout.Text != want || !slices.Equal(got.Command, []string{"balance", "--account", "test"}) || got.Error != "" {
				t.Errorf("cell %d %s: command %q, stdout %q, error %q; want stdout %q", i, b.name, got.Command, got.Stdout.Text, got.Error, want)
			}
		}
	}
	if n := e.hits.Load(); n != 0 {
		t.Fatalf("server saw %d requests", n)
	}
}

// A missed cell and a cell whose process ended or failed during it each
// get a labelled manifest and report pair, without a request, that pass
// the completeness check outside the route's frame. A cell left running
// mid-ladder keeps the start records, probe record and running series it
// wrote, reads
// its end records, and has empty attempts and frame files; a cell failed
// before its records reads them when labelled, with its error as its
// reason; a missed cell reads its start records when labelled, with no
// end records and no concurrent runs, while the interrupted cells list
// another route's cell running over them. No cell is labelled twice.
func TestLabelMissedAndInterrupted(t *testing.T) {
	fastDispatch(t)
	e := newEndpoint(t, always200)
	r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+0.005032")
	missed, gone, failed := r.runs[0], r.runs[1], r.runs[2]
	now := gone.Start.Add(time.Minute)
	r.now = func() time.Time {
		now = now.Add(time.Second)
		return now
	}
	r.wait = func(context.Context, time.Time) error { return errors.New("operator abort") }
	ctx := context.Background()

	other := schedule.Run{Route: "other", Label: gone.Label, Attempt: 1, Date: gone.Date, Start: gone.Start.Add(-time.Minute)}
	otherEnd := gone.Start.Add(time.Hour)
	if err := replaceJSON(filepath.Join(r.out, "other", cellLabel(other), cellFile),
		&cellRecord{Run: other, Outcome: outcomeCounted, StartUTC: other.Start, EndUTC: &otherEnd}); err != nil {
		t.Fatal(err)
	}

	rec := r.newRecord(gone)
	rec.Outcome, rec.StartUTC = outcomeRunning, gone.Start
	if err := r.writeNew(gone, rec); err != nil {
		t.Fatal(err)
	}
	dir := r.cellDir(gone)
	if _, err := evidence.WriteClock(dir, evidence.ReadClock(ctx, r.clock)); err != nil {
		t.Fatal(err)
	}
	if _, err := evidence.WriteStatus(dir, evidence.StatusStartFile, evidence.ReadStatus(ctx, r.status)); err != nil {
		t.Fatal(err)
	}
	if _, err := evidence.WriteStatus(dir, evidence.BalanceStartFile, evidence.ReadBalance(ctx, r.balance)); err != nil {
		t.Fatal(err)
	}
	if _, err := probe.Write(dir, &probe.Record{}); err != nil {
		t.Fatal(err)
	}
	through := gone.Start.Add(3 * time.Second)
	if err := replaceJSON(filepath.Join(dir, dispatchFile), &dispatchSeries{Run: gone, StartUTC: gone.Start, Counts: []int{0, 3, 2}, ThroughUTC: &through}); err != nil {
		t.Fatal(err)
	}
	rec = r.newRecord(failed)
	rec.Outcome, rec.Error = outcomeInterrupted, "probe: connection refused"
	if err := r.writeNew(failed, rec); err != nil {
		t.Fatal(err)
	}

	code, err := r.loop(ctx)
	if err != nil || code != exitStopped {
		t.Fatalf("loop: %d, %v", code, err)
	}
	if n := e.hits.Load(); n != 0 {
		t.Fatalf("server saw %d requests", n)
	}
	balance := func(t *testing.T, run schedule.Run, ref *evidence.FileRef, read int) {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(r.cellDir(run), ref.Name))
		if err != nil {
			t.Fatal(err)
		}
		var got evidence.StatusRecord
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if want := fmt.Sprintf("balance USD 30.00, read %d\n", read); got.Stdout.Text != want {
			t.Errorf("%s %s: stdout %q, want %q", run.Label, ref.Name, got.Stdout.Text, want)
		}
	}
	for _, tc := range []struct {
		run             schedule.Run
		outcome, reason string
		ladder, end     bool
		concurrent      bool
		dispatches      int
		balances        []int
		text            string
	}{
		{gone, outcomeInterrupted, "the process ended during the cell", true, true, true, 5, []int{1, 2},
			"The ladder started and its requests ended with the process; the attempts file is empty.\n"},
		{failed, outcomeInterrupted, "execution error: probe: connection refused", false, true, true, 0, []int{3, 4},
			"The ladder did not start; the attempts file is empty.\n"},
		{missed, outcomeMissed, "not started: ", false, false, false, 0, []int{5},
			"The ladder did not start; the attempts file is empty.\n"},
	} {
		got := readCell(t, r, tc.run)
		if got.Outcome != tc.outcome || len(got.Reasons) != 1 || !strings.HasPrefix(got.Reasons[0], tc.reason) {
			t.Fatalf("%s: cell record %s %q", tc.run.Label, got.Outcome, got.Reasons)
		}
		m, text := checkRetained(t, r, tc.run)
		c := m.Release
		if m.Counted() || m.RunValid || c.Outcome != tc.outcome || c.LadderStarted != tc.ladder || !slices.Equal(c.Reasons, got.Reasons) || !slices.Equal(m.InvalidReasons, got.Reasons) {
			t.Fatalf("%s: manifest valid %t %q, cell %+v", tc.run.Label, m.RunValid, m.InvalidReasons, c)
		}
		if !strings.Contains(text, "Outcome: "+tc.outcome+"; a labelled record, outside the route's frame of 12 cells\n  "+got.Reasons[0]+"\n"+tc.text) {
			t.Errorf("%s: report text:\n%s", tc.run.Label, text)
		}
		if b, err := os.ReadFile(filepath.Join(r.cellDir(tc.run), evidence.AttemptsFile)); err != nil || len(b) != 0 {
			t.Fatalf("%s: attempts file %q: %v", tc.run.Label, b, err)
		}
		if (m.Artifact(probe.File) != nil) != (tc.run == gone) {
			t.Fatalf("%s: artifacts %+v", tc.run.Label, m.Artifacts)
		}
		if m.Frames == nil || filepath.Dir(filepath.Join(r.framesRoot, m.Frames.Name)) != evidence.FramesDir(r.framesRoot, got.StartUTC) || m.Frames.Bytes != 0 {
			t.Fatalf("%s: frame store %+v", tc.run.Label, m.Frames)
		}
		var concurrent []schedule.Run
		if tc.concurrent {
			concurrent = []schedule.Run{other}
		}
		if d := c.Dispatch; len(d.Sources) != 1+len(concurrent) || d.Sources[0].Dispatches != tc.dispatches || d.Sources[0].Final != (tc.dispatches == 0) ||
			!slices.Equal(c.Concurrent, concurrent) || !slices.Equal(got.Concurrent, concurrent) {
			t.Fatalf("%s: aggregate dispatch series %+v, concurrent %v", tc.run.Label, d, c.Concurrent)
		}
		if d := c.Dispatch; tc.dispatches > 0 && (d.Sources[0].ThroughUTC == nil || !d.Sources[0].ThroughUTC.Equal(through) || !d.EndUTC.Equal(through)) {
			t.Fatalf("%s: aggregate dispatch series %+v", tc.run.Label, d)
		}
		if (m.StatusEnd != nil) != tc.end || (m.BalanceEnd != nil) != tc.end || m.BalanceStart == nil {
			t.Fatalf("%s: end status %v, balances %v and %v", tc.run.Label, m.StatusEnd, m.BalanceStart, m.BalanceEnd)
		}
		balance(t, tc.run, m.BalanceStart, tc.balances[0])
		if tc.end {
			balance(t, tc.run, m.BalanceEnd, tc.balances[1])
		}
	}

	before := map[string][]byte{}
	for _, run := range []schedule.Run{missed, gone, failed} {
		b, err := os.ReadFile(filepath.Join(r.cellDir(run), evidence.ManifestFile))
		if err != nil {
			t.Fatal(err)
		}
		before[run.Label] = b
	}
	if code, err := r.loop(ctx); err != nil || code != exitStopped {
		t.Fatalf("loop again: %d, %v", code, err)
	}
	for _, run := range []schedule.Run{missed, gone, failed} {
		if b, err := os.ReadFile(filepath.Join(r.cellDir(run), evidence.ManifestFile)); err != nil || !bytes.Equal(b, before[run.Label]) {
			t.Fatalf("%s labelled again: %v", run.Label, err)
		}
	}
}
