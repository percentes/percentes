package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/probe"
)

// chatStream is a complete chat-completions stream.
const chatStream = "data: {\"model\":\"openai/gpt-oss-120b\",\"choices\":[{\"delta\":{\"content\":\"Two sentences.\"}}]}\n\n" +
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
// the attempts without answer text, the probe, the dispatch series and a
// manifest carrying the cell record.
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
	for _, name := range []string{cellFile, evidence.AttemptsFile, evidence.ManifestFile, probe.File, dispatchFile, evidence.ClockFile, evidence.StatusStartFile, evidence.StatusEndFile, "ceiling-probe-groq-512.json"} {
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

	m, err := evidence.ReadManifest(filepath.Join(dir, evidence.ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	c := m.Release
	if c == nil || c.Run != run || c.OffsetMinutes != r.offsetMinutes || c.AccountCreated != "2026-09-15" || len(c.Probes) != 1 || c.Admission.TopRPS != 1 || c.CeilingTokens != 512 {
		t.Fatalf("cell record %+v", c)
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
	if total != sent || ds.Run != run {
		t.Fatalf("dispatch series counts %d of %d sent", total, sent)
	}
}

// A documented billing refusal inside the ladder stops the route: the
// cell keeps its evidence, labelled with the refusal, and the loop ends
// with the stop code.
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
	m, err := evidence.ReadManifest(filepath.Join(r.cellDir(run), evidence.ManifestFile))
	if err != nil || m.RunValid || len(m.InvalidReasons) == 0 || m.InvalidReasons[0] != rec.Reasons[0] {
		t.Fatalf("manifest: %v, %+v", err, m)
	}
	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	if len(st.runs) != len(r.runs)+1 || st.next == nil || st.next.Attempt != 1 {
		t.Fatalf("after the stop: %d runs, next %+v; the stopped cell takes a re-run", len(st.runs), st.next)
	}
}

// Before the ladder: a clock offset above the bound or a ledger refusal
// stops the route with nothing sent; a probe the start gate blocks ends
// the cell after one request; a probe answered with a billing refusal
// stops the route.
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
			if _, err := os.Stat(filepath.Join(r.cellDir(run), evidence.ManifestFile)); err == nil {
				t.Fatal("a cell that never reached the ladder wrote a manifest")
			}
			if u := ledgerState(t, f.ledger).Routes["groq"]; u != nil && u.Reserved != 0 {
				t.Fatalf("reservation left open: %+v", u)
			}
		})
	}
}
