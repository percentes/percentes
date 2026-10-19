package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
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
	"github.com/percentes/percentes/internal/validity"
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
	// The cell is admitted at its configured ceiling.
	if b, _ := os.ReadFile(f.ledger); !strings.Contains(string(b), `"kind":"admit","route":"groq","input_tokens":1200,"output_tokens":512,"per_request_usd":"0.0004872","purpose":"cell"`) {
		t.Fatalf("ledger %s", b)
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
	if c == nil || c.Run != run || c.OffsetMinutes != r.offsetMinutes || c.AccountCreated != "2026-09-15" || len(c.Probes) != 2 || c.Admission.TopRPS != 1 || c.CeilingTokens != 512 {
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
	// The manifest carries G2 over the whole ladder and at each attempted
	// step, as the cell record has them, then hosted-G6 per step.
	var g2, g6 []validity.Gate
	for _, g := range m.Gates {
		switch g.ID {
		case "G2":
			g2 = append(g2, g)
		case "G6":
			g6 = append(g6, g)
		default:
			t.Errorf("gate %+v", g)
		}
	}
	want := []validity.Gate{validity.G2(0, nil)}
	for k, s := range rec.Ladder.Steps {
		if s.Attempted {
			want = append(want, validity.G2(k+1, s.Gates))
		}
	}
	if len(g2) != len(want) {
		t.Fatalf("G2 results %+v for %d attempted steps", g2, len(want)-1)
	}
	for i := range want {
		if g2[i].Step != want[i].Step || i > 0 && g2[i] != want[i] {
			t.Errorf("G2 %+v, want %+v", g2[i], want[i])
		}
	}
	if g := g2[0]; !g.Observed || rec.Outcome == outcomeCounted && !g.Pass {
		t.Errorf("%s cell, whole-ladder G2 %+v", rec.Outcome, g)
	}
	inTime, scheduled := map[int]int{}, map[int]int{}
	for _, a := range recs {
		if !a.Warmup {
			scheduled[a.Step]++
			if a.InTime {
				inTime[a.Step]++
			}
		}
	}
	if len(g6) == 0 {
		t.Fatal("no hosted-G6 result")
	}
	for k, g := range g6 {
		if w := validity.ReleaseG6(k+1, inTime[k+1], scheduled[k+1], r.cfg.Release.Shape.DeadlineS); g != w {
			t.Errorf("hosted-G6 %+v, want %+v", g, w)
		}
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

// A cell's probe withholds what the route retains: no file under the
// release output directory carries a retained header value or the
// retained limits body, and the probe's retained file in the run's frame
// directory, readable by the owner only, holds them.
func TestCellProbeWithholds(t *testing.T) {
	const limitsBody = `{"rate_limits":{"rpm":"qz104729"}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Ratelimit-Limit-Requests", "1000")
		w.Header().Set("X-Ratelimit-Remaining-Requests", "qz7919")
		if r.Method == http.MethodGet {
			io.WriteString(w, limitsBody) //nolint:errcheck
			return
		}
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, `{"error":{"message":"model not found"}}`) //nolint:errcheck
	}))
	t.Cleanup(srv.Close)
	cfg := releaseConfig(t, srv.URL+"/openai/v1/chat/completions")
	cfg.Target.RetainedHeaders = []string{"x-ratelimit-remaining-*"}
	cfg.Target.Probe = &config.TargetProbe{LimitsURL: srv.URL + "/openai/v1/models", LimitsAuthScheme: "Bearer", LimitsRetained: true}
	r, f := newTestRoute(t, cfg, "+0.005")
	run := r.runs[0]
	rec, err := r.runCell(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Outcome != outcomeBlocked || rec.ProbeStatus != http.StatusNotFound {
		t.Fatalf("outcome %s, probe status %d", rec.Outcome, rec.ProbeStatus)
	}
	m, _ := checkRetained(t, r, run)
	if m.Artifact(probe.File) == nil {
		t.Fatalf("artifacts %+v", m.Artifacts)
	}
	var published int
	if err := filepath.WalkDir(f.in.out, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		published++
		for _, v := range []string{"qz7919", "qz104729"} {
			if bytes.Contains(b, []byte(v)) {
				t.Errorf("%s carries the withheld %s", p, v)
			}
		}
		return nil
	}); err != nil || published == 0 {
		t.Fatalf("walk: %d files, %v", published, err)
	}
	b, err := os.ReadFile(filepath.Join(r.cellDir(run), probe.File))
	if err != nil || !bytes.Contains(b, []byte(`"[retained]"`)) {
		t.Fatalf("%s: %v\n%s", probe.File, err, b)
	}
	held := filepath.Join(r.framesRoot, filepath.Dir(m.Frames.Name), probe.RetainedFile)
	raw, err := os.ReadFile(held)
	if err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(held); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("retained file mode %v, %v", fi, err)
	}
	var back probe.RetainedRecord
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	const kept = "map[X-Ratelimit-Remaining-Requests:[qz7919]]"
	if q, l := back.Exchanges["request"], back.Exchanges["limits"]; q == nil || fmt.Sprint(q.Headers) != kept || l == nil || fmt.Sprint(l.Headers) != kept ||
		l.Body == nil || l.Body.Text == nil || *l.Body.Text != limitsBody {
		t.Fatalf("retained record %s", raw)
	}
}

// confidentialConfig is releaseConfig with its tokens_per_minute limit
// at 250,013.5, marked confidential in the file as loaded, and its
// rate-limit headers retained.
func confidentialConfig(t *testing.T, endpoint string) *config.Config {
	t.Helper()
	raw := releaseConfig(t, endpoint).Raw
	const public = "      value: 250000\n      marking: public\n      counting: input_and_output\n      source: groq-rate-limits-page.png\n      retrieved: 2026-10-07\n      sha256:"
	if !bytes.Contains(raw, []byte(public)) || !bytes.Contains(raw, []byte(rateLimitHeaders)) {
		t.Fatal("reference no longer carries the lines this test edits")
	}
	raw = bytes.Replace(raw, []byte(rateLimitHeaders), []byte(rateLimitHeaders+"  retained_headers: [x-ratelimit-*]\n"), 1)
	cfg, err := config.Parse(bytes.Replace(raw, []byte(public), []byte("      value: 250013.5\n      marking: confidential\n      counting: input_and_output\n      source: groq-rate-limits-page.png\n      retrieved: 2026-10-07\n      hmac_sha256:"), 1))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Target.EndpointURL = endpoint
	return cfg
}

// rulingConfig is releaseConfig with a postpaid balance_unavailable
// ruling and the spend_per_month_usd limit that covers it, in the file as
// loaded.
func rulingConfig(t *testing.T, endpoint string) *config.Config {
	t.Helper()
	raw := releaseConfig(t, endpoint).Raw
	const limits, naming = "  limits:\n", "  naming:\n"
	if bytes.Count(raw, []byte(limits)) != 1 || bytes.Count(raw, []byte(naming)) != 1 {
		t.Fatal("reference no longer carries the lines this test edits")
	}
	limit := "    - dimension: spend_per_month_usd\n      value: 10\n      marking: public\n      operator_set: true\n" +
		"      source: spend-limit.png\n      retrieved: 2026-10-07\n      sha256: \"" + strings.Repeat("0", 64) + "\"\n"
	ruling := "  balance_unavailable:\n    kind: postpaid\n    reason: postpaid account\n    ruled: 2026-10-07\n    ruling: rulings.md\n"
	raw = bytes.Replace(raw, []byte(limits), []byte(limits+limit), 1)
	cfg, err := config.Parse(bytes.Replace(raw, []byte(naming), []byte(ruling+naming), 1))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Target.EndpointURL = endpoint
	return cfg
}

// A cell of a route with a confidential limit publishes no file that
// carries the value, which the endpoint echoes in a rate-limit header:
// its manifest names the file as published, its verdict and report pair
// name the limit by source and keyed hash, its attempts carry the
// header's stand-in, and the run's frame directory keeps the file as
// loaded, from which the completeness check and the replay work. No file
// under the output directory or the frame store carries the release key.
func TestCellConfidentialLimit(t *testing.T) {
	e := newEndpoint(t, always200)
	endpoint, _ := withHeaders(t, e.url(), map[string]string{"X-Ratelimit-Limit-Tokens": "250013.5"})
	cfg := confidentialConfig(t, endpoint)
	r, f := newTestRoute(t, cfg, "+0.005032")
	if got := r.verdict.Admitted(); len(got) != 3 {
		t.Fatalf("admitted %v", got)
	}
	shortLadder(r)
	run := r.runs[0]
	rec, err := r.runCell(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Outcome != outcomeCounted {
		t.Fatalf("outcome %s %q", rec.Outcome, rec.Reasons)
	}
	m, text := checkRetained(t, r, run)
	published, err := cfg.Published()
	if err != nil {
		t.Fatal(err)
	}
	if sum := sha256.Sum256(published); m.Config.SHA256 != hex.EncodeToString(sum[:]) {
		t.Errorf("manifest config %+v, not the file as published", m.Config)
	}
	l := cfg.Target.Limits[1]
	if want := "tokens_per_minute confidential (" + l.Source + ", hmac-sha256 " + l.HMACSHA256 + "): nominal demand at most half of it (half test: pass)"; !strings.Contains(text, want) {
		t.Errorf("report lacks %q:\n%s", want, text)
	}
	if b := m.Release.Admission.Steps[3].Binding; b == nil || !b.Confidential || b.Value != 0 || b.HMACSHA256 != l.HMACSHA256 {
		t.Errorf("binding limit %+v", b)
	}
	noReleaseKey(t, f.in.out, r.framesRoot)
	files, withheld := 0, 0
	if err := filepath.WalkDir(f.in.out, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		files++
		if bytes.Contains(b, []byte("250013.5")) || bytes.Contains(b, []byte("2.500135e+05")) {
			t.Errorf("%s carries the confidential value", p)
		}
		if d.Name() == evidence.AttemptsFile && bytes.Contains(b, []byte(`"X-Ratelimit-Limit-Tokens":["[retained]"]`)) {
			withheld++
		}
		return nil
	}); err != nil || files == 0 || withheld == 0 {
		t.Fatalf("walk: %d files, %d attempts files with the header withheld, %v", files, withheld, err)
	}
	held := filepath.Join(r.framesRoot, filepath.Dir(m.Frames.Name), evidence.ConfigCopyFile)
	if b, err := os.ReadFile(held); err != nil || !bytes.Equal(b, cfg.Raw) {
		t.Fatalf("config copy: %v", err)
	}
	if fi, err := os.Stat(held); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("config copy mode %v, %v", fi, err)
	}
}

// A probe answered with a documented billing refusal that names body
// text stops a route with a confidential limit after that one request,
// whether or not the start gate blocks the status, and probe.json still
// withholds the body.
func TestCellConfidentialBillingRefusal(t *testing.T) {
	const body = `{"error":{"message":"Organization has been restricted.","code":"blocked_api_access"}}`
	for _, status := range []int{http.StatusBadRequest, http.StatusPaymentRequired} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			e := newEndpoint(t, func(int64) (int, string) { return status, body })
			cfg := confidentialConfig(t, e.url())
			cfg.Target.BillingRefusals = []config.BillingRefusal{{Status: status, BodyContains: "blocked_api_access", Meaning: "spend limit reached", Source: "errors.png", Retrieved: "2026-10-07"}}
			r, _ := newTestRoute(t, cfg, "+0.005")
			shortLadder(r)
			run := r.runs[0]
			rec, err := r.runCell(context.Background(), run)
			if err != nil {
				t.Fatal(err)
			}
			want := fmt.Sprintf("route stop: HTTP %d, the documented billing refusal: spend limit reached", status)
			if rec.Outcome != outcomeStopped || len(rec.Reasons) != 1 || !strings.Contains(rec.Reasons[0], want) {
				t.Fatalf("outcome %s, reasons %q", rec.Outcome, rec.Reasons)
			}
			if n := e.hits.Load(); n != 1 || rec.Sent != (admission.Counts{Probe: 1}) {
				t.Fatalf("server saw %d requests; sent %+v", n, rec.Sent)
			}
			b, err := os.ReadFile(filepath.Join(r.cellDir(run), probe.File))
			if err != nil || bytes.Contains(b, []byte("blocked_api_access")) || !bytes.Contains(b, []byte(`"retained": true`)) {
				t.Fatalf("%s: %v\n%s", probe.File, err, b)
			}
		})
	}
}

// A route whose retention keeps no model output runs its cell with a
// frame store that counts each answer's bytes and keeps none of them.
func TestCellRetainsNoModelOutput(t *testing.T) {
	e := newEndpoint(t, always200)
	cfg := releaseConfig(t, e.url())
	cfg.Target.Retention.ModelOutput = config.RetainNone
	r, _ := newTestRoute(t, cfg, "+0.005032")
	shortLadder(r)
	run := r.runs[0]
	if _, err := r.runCell(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	m, _ := checkRetained(t, r, run)
	b, err := os.ReadFile(filepath.Join(r.framesRoot, filepath.FromSlash(m.Frames.Name)))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(b, []byte("sentences.")) {
		t.Fatal("frame store keeps the answer")
	}
	answered := 0
	for _, line := range bytes.Split(bytes.TrimSuffix(b, []byte("\n")), []byte("\n")) {
		var rec evidence.FrameRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatal(err)
		}
		if rec.Status != http.StatusOK {
			continue
		}
		answered++
		if rec.Frames != nil || rec.DroppedBytes != int64(len(chatStream)) {
			t.Fatalf("request %d: %d frames, dropped %d of %d", rec.Index, len(rec.Frames), rec.DroppedBytes, len(chatStream))
		}
	}
	if answered == 0 {
		t.Fatalf("no answered request in the frame store:\n%s", b)
	}
}

// A cell whose responses report a model id other than the expected one is
// labelled model_mismatch with the expected and every reported id: it
// passes the completeness check and the replay, stays out of the frame,
// and takes a re-run within the route's bound.
func TestCellModelMismatch(t *testing.T) {
	stream := strings.ReplaceAll(chatStream, `"model":"openai/gpt-oss-120b"`, `"model":"openai/gpt-oss-120b-v2"`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body) //nolint:errcheck
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, stream) //nolint:errcheck
	}))
	t.Cleanup(srv.Close)
	r, _ := newTestRoute(t, releaseConfig(t, srv.URL+"/openai/v1/chat/completions"), "+0.005032")
	shortLadder(r)
	var run schedule.Run
	for _, a := range r.runs {
		if r.hasAttempt2(a.Label) {
			run = a
			break
		}
	}
	if run.Label == "" {
		t.Fatal("the test schedule has no cell with an attempt 2")
	}
	rec, err := r.runCell(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	want := `model mismatch (SPEC-HOSTED §2): %d requests reported a model other than the expected "openai/gpt-oss-120b"; reported ["openai/gpt-oss-120b-v2"]`
	if rec.Outcome != outcomeMismatch || len(rec.Reasons) == 0 || !strings.HasPrefix(rec.Reasons[0], "model mismatch (SPEC-HOSTED §2): ") ||
		rec.Reasons[0] != fmt.Sprintf(want, rec.Sent.Warmup+rec.Sent.Measured) {
		t.Fatalf("outcome %s %q", rec.Outcome, rec.Reasons)
	}
	m, text := checkRetained(t, r, run)
	if m.Counted() || m.RunValid || m.Release.Outcome != evidence.CellModelMismatch || m.Endpoint.ExpectedModel != "openai/gpt-oss-120b" ||
		!slices.Equal(m.Endpoint.ReportedModels, []string{"openai/gpt-oss-120b-v2"}) || m.Endpoint.ModelMismatches != rec.Sent.Warmup+rec.Sent.Measured {
		t.Fatalf("manifest: counted %t, valid %t, outcome %s, endpoint %+v", m.Counted(), m.RunValid, m.Release.Outcome, m.Endpoint)
	}
	if !strings.Contains(text, "Outcome: model_mismatch; a labelled record, outside the route's frame") {
		t.Errorf("report text:\n%s", text)
	}
	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	reran := false
	for _, rr := range st.runs {
		reran = reran || rr.Label == run.Label && rr.Attempt == 2
	}
	if !reran {
		t.Errorf("no re-run of %s in %v", run.Label, st.runs)
	}
}

// A failed balance check, a balance below the cell's reservation, and a
// balance unavailable where the route records no balance_unavailable
// ruling each block the cell before admission: the ledger gains no line
// and the endpoint no request, and the cell keeps its evidence, its start
// balance record carrying the check. A balance at the reservation, and
// one unavailable under the ruling, let the cell start: it is admitted
// and its probe sent, which the endpoint answers with a 401.
func TestCellBalanceGate(t *testing.T) {
	below := func(need string) string {
		r, _ := new(big.Rat).SetString(need)
		return r.Sub(r, big.NewRat(1, 10_000_000)).FloatString(7)
	}
	amount := func(usd string) []string { return []string{"/bin/echo", `{"amount_usd": "` + usd + `"}`} }
	unavailable := []string{"/bin/echo", `{"unavailable": "shown in the console only"}`}
	for _, tc := range []struct {
		name    string
		balance func(need string) []string
		ruling  bool
		status  string
		reason  string
	}{
		{"command fails", func(string) []string { return []string{"/bin/sh", "-c", `echo '{"amount_usd": "30.00"}'; exit 3`} }, true,
			evidence.BalanceCheckFailed, "balance check failed: the command failed: exit status 3"},
		{"output not JSON", func(string) []string { return []string{"/bin/echo", "balance USD 30.00"} }, true,
			evidence.BalanceCheckFailed, "balance check failed: the output is not one JSON object with amount_usd or unavailable"},
		{"below the reservation", func(need string) []string { return amount(below(need)) }, true,
			evidence.BalanceInsufficient, "is below the cell's reservation of US$"},
		{"unavailable without a ruling", func(string) []string { return unavailable }, false,
			evidence.BalanceUnavailable, "balance unavailable (shown in the console only), and the route records no balance_unavailable ruling"},
		{"at the reservation", amount, false, evidence.BalanceSufficient, ""},
		{"unavailable under a ruling", func(string) []string { return unavailable }, true, evidence.BalanceUnavailable, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEndpoint(t, func(int64) (int, string) { return http.StatusUnauthorized, "" })
			cfg := releaseConfig(t, e.url())
			if tc.ruling {
				cfg = rulingConfig(t, e.url())
			}
			r, f := newTestRoute(t, cfg, "+0.005")
			run := r.runs[0]
			need, err := ledgerState(t, f.ledger).ReservationUSD("groq", 1+len(loadgen.BuildSchedule(cellConfig(r.cfg, run.Seed, r.steps))), admission.ReleaseTokens(r.cfg))
			if err != nil {
				t.Fatal(err)
			}
			r.balance = tc.balance(need)
			before, err := ledgerEntries(f.ledger, "")
			if err != nil {
				t.Fatal(err)
			}
			rec, err := r.runCell(context.Background(), run)
			if err != nil {
				t.Fatal(err)
			}
			starts := tc.reason == ""
			reason, probes := tc.reason, 0
			if starts {
				reason, probes = "probe request returned 401", 1
			}
			if rec.Outcome != outcomeBlocked || len(rec.Reasons) != 1 || !strings.Contains(rec.Reasons[0], reason) {
				t.Fatalf("outcome %s, reasons %q; want blocked, %q", rec.Outcome, rec.Reasons, reason)
			}
			if n := e.hits.Load(); n != int64(probes) || rec.Sent != (admission.Counts{Probe: probes}) {
				t.Fatalf("server saw %d requests; sent %+v", n, rec.Sent)
			}
			after, err := ledgerEntries(f.ledger, "")
			if err != nil {
				t.Fatal(err)
			}
			if added := after[len(before):]; (len(added) == 0) == starts {
				t.Fatalf("ledger lines added %+v", added)
			}
			if u := ledgerState(t, f.ledger).Routes["groq"]; u.Reserved != 0 || u.Probe != probes || u.Charged() != probes {
				t.Fatalf("ledger usage %+v", u)
			}
			raw, err := os.ReadFile(filepath.Join(r.cellDir(run), evidence.BalanceStartFile))
			if err != nil {
				t.Fatal(err)
			}
			var got evidence.StatusRecord
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			if c := got.Check; c == nil || c.Status != tc.status || c.RequiredUSD != need || (c.Ruling != nil) != (tc.status == evidence.BalanceUnavailable && tc.ruling) {
				t.Fatalf("start balance check %+v, want %s against US$%s", c, tc.status, need)
			}
			m, text := checkRetained(t, r, run)
			if c := m.Release; m.Counted() || c.Outcome != outcomeBlocked || c.LadderStarted || !slices.Equal(c.Reasons, rec.Reasons) || (m.Artifact(probe.File) != nil) != starts {
				t.Fatalf("manifest: counted %t, cell %+v, artifacts %+v", m.Counted(), c, m.Artifacts)
			}
			if !strings.Contains(text, "Outcome: blocked; a labelled record, outside the route's frame of 12 cells\n  "+rec.Reasons[0]+"\n") {
				t.Fatalf("report text:\n%s", text)
			}
		})
	}
}

// Each cell reads the account balance at its start and at its end, also
// a cell stopped before its ladder, and its manifest names both records.
// The start record carries the check against the cell's probe and
// scheduled requests at the ledger's price.
func TestCellBalanceRecords(t *testing.T) {
	e := newEndpoint(t, always200)
	r, f := newTestRoute(t, releaseConfig(t, e.url()), "+75.5")
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
			want := fmt.Sprintf("read %d\n", 2*i+j+1)
			if got.Stdout.Text != fakeBalanceOut+"\n" || got.Stderr.Text != want || !slices.Equal(got.Command, []string{"balance", "--account", "test"}) || got.Error != "" {
				t.Errorf("cell %d %s: command %q, stdout %q, stderr %q, error %q; want stderr %q", i, b.name, got.Command, got.Stdout.Text, got.Stderr.Text, got.Error, want)
			}
			if b.name == evidence.BalanceEndFile {
				if got.Check != nil {
					t.Errorf("cell %d: end balance checked: %+v", i, got.Check)
				}
				continue
			}
			need, err := ledgerState(t, f.ledger).ReservationUSD("groq", 1+len(loadgen.BuildSchedule(cellConfig(r.cfg, run.Seed, r.steps))), admission.ReleaseTokens(r.cfg))
			if err != nil {
				t.Fatal(err)
			}
			if c := got.Check; c == nil || *c != (evidence.BalanceCheck{Status: evidence.BalanceSufficient, AmountUSD: "30.00", RequiredUSD: need}) {
				t.Errorf("cell %d: start balance check %+v, want sufficient against US$%s", i, c, need)
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
		if want := fmt.Sprintf("read %d\n", read); got.Stderr.Text != want || got.Stdout.Text != fakeBalanceOut+"\n" {
			t.Errorf("%s %s: stdout %q, stderr %q, want stderr %q", run.Label, ref.Name, got.Stdout.Text, got.Stderr.Text, want)
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

// refreshFixture gives f's operator file a limits record of its own,
// limits.txt, and a refresh command that copies page over it, touches
// the file of the other records and prints the recorded limits, and
// returns the operator file's directory.
func refreshFixture(t *testing.T, f *fixture, page string) string {
	t.Helper()
	dir := filepath.Dir(f.in.opPath)
	old := `{"kind": "limits", "path": "r.txt"`
	if strings.Count(operatorJSON, old) != 1 {
		t.Fatalf("operator fixture: %q not found once", old)
	}
	writeFile(t, dir, "operator.json", []byte(strings.Replace(operatorJSON, old, `{"kind": "limits", "path": "limits.txt"`, 1)))
	writeFile(t, dir, "limits.txt", []byte("limits at the freeze"))
	limits := filepath.Join(t.TempDir(), "limits")
	if err := writeMockRefresh(limits, f.in.cfg); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "refresh")
	body := "#!/bin/sh\ncat '" + page + "' > '" + filepath.Join(dir, "limits.txt") + "' && '" + limits + "' '" + filepath.Join(dir, "r.txt") + "'\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	f.in.refreshCmd = script
	return dir
}

// limitsRecord is m's limits record.
func limitsRecord(t *testing.T, m *evidence.Manifest) evidence.OperatorRecord {
	t.Helper()
	for _, rec := range m.Records {
		if rec.Kind == evidence.RecordLimits {
			return rec
		}
	}
	t.Fatalf("manifest records %+v: no limits record", m.Records)
	return evidence.OperatorRecord{}
}

func sha(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// Each cell runs the refresh command and reads the operator file and its
// records again: a limits page that changes between two cells of one
// process reaches each cell's manifest and its record copies, dated when
// the cell's refresh wrote it, and the route page names the change at the
// second cell. A missed cell labelled later reads the file as it stands.
func TestCellOperatorRefreshed(t *testing.T) {
	fastDispatch(t)
	enoughOpenFiles(t)
	e := newEndpoint(t, always200)
	cfgPath := routeConfigFile(t, "groq", e.url(), "10")
	cfg, err := config.LoadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(cfg.Target.APIKeyEnv, testKey)
	f := newFixture(t, cfg, "+0.005032")
	page := writeFile(t, t.TempDir(), "page", []byte("limits v1"))
	dir := refreshFixture(t, f, page)
	r, err := newRoute(f.in)
	if err != nil {
		t.Fatal(err)
	}
	shortLadder(r)
	first, second, missed := r.runs[0], r.runs[1], r.runs[2]
	var notCounted []string
	for _, c := range []struct {
		run    schedule.Run
		limits string
	}{{first, "limits v1"}, {second, "limits v2"}} {
		writeFile(t, filepath.Dir(page), "page", []byte(c.limits))
		begun := time.Now().Add(-2 * time.Second)
		rec := runCellOK(t, r, c.run)
		if rec.Outcome != outcomeCounted {
			for _, reason := range rec.Reasons {
				if !strings.HasPrefix(reason, "client-validity gate failed") && !strings.HasPrefix(reason, "ladder ended at step") {
					t.Fatalf("%s: outcome %s, reason %q", c.run.Label, rec.Outcome, reason)
				}
			}
			notCounted = append(notCounted, fmt.Sprintf("%s %q", c.run.Label, rec.Reasons))
		}
		m, _ := checkRetained(t, r, c.run)
		lim := limitsRecord(t, m)
		at, err := time.Parse(time.RFC3339, lim.Retrieved)
		if lim.Name != "limits.txt" || lim.SHA256 != sha(c.limits) || err != nil || at.Before(begun) || at.After(time.Now().Add(2*time.Second)) {
			t.Fatalf("%s: limits record %+v, want %s retrieved during the cell", c.run.Label, lim, sha(c.limits))
		}
		copied, err := os.ReadFile(filepath.Join(r.framesRoot, filepath.Dir(filepath.FromSlash(m.Frames.Name)), evidence.RecordsCopyDir, "limits.txt"))
		if err != nil || string(copied) != c.limits {
			t.Fatalf("%s: limits copy %q: %v", c.run.Label, copied, err)
		}
		if m.Artifact(evidence.RefreshFile) == nil {
			t.Fatalf("%s: artifacts %+v", c.run.Label, m.Artifacts)
		}
	}

	writeFile(t, dir, "limits.txt", []byte("limits v3"))
	rec := r.newRecord(missed)
	rec.Outcome, rec.Reasons = outcomeMissed, []string{"not started"}
	if err := r.writeNew(missed, rec); err != nil {
		t.Fatal(err)
	}
	if err := r.labelCell(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	m, _ := checkRetained(t, r, missed)
	if lim := limitsRecord(t, m); lim.SHA256 != sha("limits v3") || !slices.Equal(m.Release.Reasons, []string{"not started"}) {
		t.Fatalf("missed cell: limits record %+v, reasons %q", lim, m.Release.Reasons)
	}

	if len(notCounted) > 0 {
		t.Skipf("client-validity gate failed (%s); no two counted cells to page", strings.Join(notCounted, "; "))
	}
	pages := t.TempDir()
	if code, err := pagesCmd([]string{"--out", r.out, "--frames", r.framesRoot, "--config", cfgPath, "--pages", pages}); code != exitOK || err != nil {
		t.Fatalf("pages: exit %d: %v", code, err)
	}
	b, err := os.ReadFile(filepath.Join(pages, routesDir, "groq.json"))
	if err != nil {
		t.Fatal(err)
	}
	var st report.StepTables
	if err := json.Unmarshal(b, &st); err != nil {
		t.Fatal(err)
	}
	doc := func(limits string) []report.DocRecord {
		return []report.DocRecord{{Kind: evidence.RecordLimits, URL: "https://example.test/limits", SHA256: sha(limits)}}
	}
	want := []report.DriftChange{{Cell: cellLabel(second), StartUTC: second.Start.UTC(), Removed: doc("limits v1"), Added: doc("limits v2")}}
	if !reflect.DeepEqual(st.DriftChanges, want) {
		t.Fatalf("drift changes %+v, want %+v", st.DriftChanges, want)
	}
}

// A refresh command that fails, one that leaves a limits or eligibility
// file unwritten, one that leaves the operator file unreadable and one
// that prints no limits each block the cell before admission: no ledger line, no request, and a
// manifest carrying the records read when the route started. The
// endpoint answers 401, so a cell let through ends at its probe.
func TestCellRefreshBlocks(t *testing.T) {
	for _, tc := range []struct {
		name   string
		script func(opPath string) string
		reason string
	}{
		{"command fails", func(string) string { return "exit 4" }, "refresh command: exit status 4"},
		{"records not rewritten", func(string) string { return "exit 0" }, "not rewritten since"},
		{"operator file unreadable", func(op string) string { return "echo '{' > '" + op + "'" }, "evidence: operator file"},
		{"no limits printed", func(op string) string { return "touch '" + filepath.Join(filepath.Dir(op), "r.txt") + "'" },
			"refreshed limits: the output is not one JSON object with limits"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEndpoint(t, func(int64) (int, string) { return http.StatusUnauthorized, "" })
			enoughOpenFiles(t)
			f := newFixture(t, releaseConfig(t, e.url()), "+0.005")
			script := filepath.Join(t.TempDir(), "refresh")
			if err := os.WriteFile(script, []byte("#!/bin/sh\n"+tc.script(f.in.opPath)+"\n"), 0o755); err != nil {
				t.Fatal(err)
			}
			f.in.refreshCmd = script
			r, err := newRoute(f.in)
			if err != nil {
				t.Fatal(err)
			}
			old := time.Now().Add(-time.Minute)
			if err := os.Chtimes(filepath.Join(filepath.Dir(f.in.opPath), "r.txt"), old, old); err != nil {
				t.Fatal(err)
			}
			before, err := ledgerEntries(f.ledger, "")
			if err != nil {
				t.Fatal(err)
			}
			run := r.runs[0]
			rec, err := r.runCell(context.Background(), run)
			if err != nil {
				t.Fatal(err)
			}
			if rec.Outcome != outcomeBlocked || len(rec.Reasons) != 1 || !strings.HasPrefix(rec.Reasons[0], "operator records not refreshed at the cell's start (SPEC-HOSTED §4): ") ||
				!strings.Contains(rec.Reasons[0], tc.reason) || !strings.HasSuffix(rec.Reasons[0], "; the manifest carries the records read when the route started") {
				t.Fatalf("outcome %s, reasons %q; want blocked, %q", rec.Outcome, rec.Reasons, tc.reason)
			}
			if n := e.hits.Load(); n != 0 || rec.Sent != (admission.Counts{}) {
				t.Fatalf("server saw %d requests; sent %+v", n, rec.Sent)
			}
			if after, err := ledgerEntries(f.ledger, ""); err != nil || len(after) != len(before) {
				t.Fatalf("ledger lines %d, were %d: %v", len(after), len(before), err)
			}
			m, _ := checkRetained(t, r, run)
			if m.Counted() || m.Release.Outcome != outcomeBlocked || m.Operator != r.op.File ||
				!slices.EqualFunc(m.Records, r.op.Records, func(a, b evidence.OperatorRecord) bool {
					return a.Kind == b.Kind && a.FileRef == b.FileRef && a.URL == b.URL && a.Retrieved == b.Retrieved && a.Note == b.Note
				}) {
				t.Fatalf("manifest: outcome %s, operator %+v, records %+v", m.Release.Outcome, m.Operator, m.Records)
			}
			if lim := limitsRecord(t, m); lim.Retrieved != "2026-10-07" {
				t.Fatalf("limits record %+v", lim)
			}
			if m.Artifact(evidence.RefreshFile) == nil {
				t.Fatalf("artifacts %+v", m.Artifacts)
			}
		})
	}
}

// A cell whose ledger path holds another ledger than the one the route
// started on ends in an error before any request, appending nothing to
// that file: at the limits re-check, and at the reservation's read where
// the refresh failed and the re-check does not run.
func TestCellLedgerSwapped(t *testing.T) {
	for _, tc := range []struct {
		name, script, want string
	}{
		{"refreshed", "", "ledger: open line sha256 %s; the route is bound to %s"},
		{"not refreshed", "exit 4", "admission: ledger open line sha256 %s; bound to %s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEndpoint(t, func(int64) (int, string) { return http.StatusUnauthorized, "" })
			enoughOpenFiles(t)
			f := newFixture(t, releaseConfig(t, e.url()), "+0.005")
			if tc.script != "" {
				f.in.refreshCmd = writeFile(t, t.TempDir(), "refresh", []byte("#!/bin/sh\n"+tc.script+"\n"))
				if err := os.Chmod(f.in.refreshCmd, 0o755); err != nil {
					t.Fatal(err)
				}
			}
			r, err := newRoute(f.in)
			if err != nil {
				t.Fatal(err)
			}
			other := filepath.Join(t.TempDir(), "ledger.jsonl")
			l, err := admission.Create(other, mustCaps(t, f.in.cfg))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := recordPrice(l, f.in.cfg, "0.15", "0.60", "https://example.test/pricing", "2026-10-06"); err != nil {
				t.Fatal(err)
			}
			swapped := ledgerOpenSHA256(t, other)
			if swapped == f.open {
				t.Fatalf("two ledgers open with sha256 %s", swapped)
			}
			if err := os.Rename(other, f.ledger); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(f.ledger)
			if err != nil {
				t.Fatal(err)
			}
			rec, err := r.runCell(context.Background(), r.runs[0])
			if want := fmt.Sprintf(tc.want, swapped, f.open); err == nil || !strings.Contains(err.Error(), want) || rec.Outcome != outcomeInterrupted {
				t.Fatalf("outcome %s: %v; want interrupted, %q", rec.Outcome, err, want)
			}
			if n := e.hits.Load(); n != 0 {
				t.Fatalf("server saw %d requests", n)
			}
			if after, err := os.ReadFile(f.ledger); err != nil || !bytes.Equal(after, before) {
				t.Fatalf("the other ledger changed: %v", err)
			}
		})
	}
}
