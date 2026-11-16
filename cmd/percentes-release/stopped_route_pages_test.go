package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/report"
)

// stoppedOnly runs a route whose probe a billing refusal with meaning
// stops, leaving a stopped cell and no manifest.
func stoppedOnly(t *testing.T, meaning string) (*route, *fixture) {
	t.Helper()
	e := newEndpoint(t, func(int64) (int, string) { return 402, "no credit" })
	cfg := releaseConfig(t, e.url())
	cfg.Target.BillingRefusals = []config.BillingRefusal{{Status: 402, Meaning: meaning, Source: "errors.png", Retrieved: "2026-10-07"}}
	r, f := newTestRoute(t, cfg, "+0.005")
	run := r.runs[0]
	r.now = func() time.Time { return run.Start }
	frames := filepath.Join(evidence.FramesDir(r.framesRoot, run.Start), evidence.FramesFile)
	if err := os.MkdirAll(filepath.Dir(frames), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(frames, []byte("{\"index\":0}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	r.wait = noWait
	if code, err := r.loop(context.Background()); err != nil || code != exitStopped {
		t.Fatalf("loop: exit %d, %v", code, err)
	}
	return r, f
}

// A stopped-only route whose stop reasons the index cannot print, here
// for a table delimiter, gets an index line that sends the reader to its
// page, which states them.
func TestStoppedRouteIndexLineFallsBackToPage(t *testing.T) {
	r, f := stoppedOnly(t, "balance | exhausted")
	cfgs := append([]*config.Config{r.cfg}, f.in.hostCfgs...)
	rp, err := buildPages(r.out, r.framesRoot, r.sched, r.scheduleSHA256, r.ledgerPath, f.in.probeFiles, cfgs, r.key, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(rp.index.Stopped) != 1 || rp.index.Stopped[0].Reason != "no cell manifest; the stopped cells and their reasons are on the route page" {
		t.Fatalf("index %+v", rp.index.Stopped)
	}
	if page := string(rp.files[rp.index.Stopped[0].Page]); !strings.Contains(page, "balance | exhausted") {
		t.Fatalf("route page %q", page)
	}
}

// The pages refuse a stopped-only route given no ceiling-check record of
// its host group.
func TestStoppedRoutePagesNeedHostGroupCheckRecord(t *testing.T) {
	r, _ := stoppedOnly(t, "balance exhausted")
	_, err := buildPages(r.out, r.framesRoot, r.sched, r.scheduleSHA256, r.ledgerPath, nil, []*config.Config{r.cfg}, r.key, "")
	if want := "pages: route groq has a stopped cell and no --check-record of its host group"; err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("buildPages: %v; want %q", err, want)
	}
}

// billingStopFixture is billingStopRoute with its fixture.
func billingStopFixture(t *testing.T) (*route, *endpoint, *fixture) {
	t.Helper()
	e := newEndpoint(t, func(int64) (int, string) { return 402, "no credit" })
	cfg := releaseConfig(t, e.url())
	cfg.Target.BillingRefusals = []config.BillingRefusal{{Status: 402, Meaning: "balance exhausted", Source: "errors.png", Retrieved: "2026-10-07"}}
	r, f := newTestRoute(t, cfg, "+0.005")
	run := r.runs[0]
	r.now = func() time.Time { return run.Start }
	return r, e, f
}

// stopsPages builds r's pages over its fixture f.
func stopsPages(t *testing.T, r *route, f *fixture) *releasePages {
	t.Helper()
	cfgs := append([]*config.Config{r.cfg}, f.in.hostCfgs...)
	rp, err := buildPages(r.out, r.framesRoot, r.sched, r.scheduleSHA256, r.ledgerPath, f.in.probeFiles, cfgs, r.key, "")
	if err != nil {
		t.Fatal(err)
	}
	return rp
}

// A stopped cell's page text and JSON give its billing refusal count and
// a fixed execution error; its run record keeps the raw error. Its
// evidence pack holds its page files and its host group's ceiling-check
// records, which pages copies under --out.
func TestStoppedCellPageAndEvidencePack(t *testing.T) {
	r, _, f := billingStopFixture(t)
	run := r.runs[0]
	frames := filepath.Join(evidence.FramesDir(r.framesRoot, run.Start), evidence.FramesFile)
	if err := os.MkdirAll(filepath.Dir(frames), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(frames, []byte("{\"index\":0}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	r.wait = noWait
	if code, err := r.loop(context.Background()); err != nil || code != exitStopped {
		t.Fatalf("loop: exit %d, %v", code, err)
	}
	if rec := readCell(t, r, run); !strings.Contains(rec.Error, frames) {
		t.Fatalf("run record error %q", rec.Error)
	}
	rp := stopsPages(t, r, f)
	name := r.cfg.Target.Name
	page := string(rp.files[path.Join(routesDir, name+".txt")])
	if !strings.Contains(page, "; billing refusals: 1; execution error: "+stoppedError+"\n") || strings.Contains(page, frames) || strings.Contains(page, r.framesRoot) {
		t.Fatalf("route page %q", page)
	}
	var p stoppedPage
	if err := json.Unmarshal(rp.files[path.Join(routesDir, name+".json")], &p); err != nil {
		t.Fatal(err)
	}
	if len(p.Stops) != 1 || p.Stops[0].BillingRefusals != 1 || p.Stops[0].Error != stoppedError {
		t.Fatalf("route page JSON %+v", p)
	}
	var copies []string
	for k := range rp.out {
		if strings.HasPrefix(k, name+"/ceiling-probe-") {
			copies = append(copies, k)
		}
	}
	if len(copies) == 0 {
		t.Fatalf("ceiling-check copies %v", keys(rp.out))
	}
	manifests, err := releaseFiles(r.out, "*", evidence.ManifestFile)
	if err != nil {
		t.Fatal(err)
	}
	packs, err := routePacks(rp, manifests)
	if err != nil {
		t.Fatal(err)
	}
	files, err := evidence.ReadPack(packs[name])
	if err != nil {
		t.Fatal(err)
	}
	var inv evidence.PackInventory
	if err := json.Unmarshal(files[evidence.PackInventoryFile], &inv); err != nil {
		t.Fatal(err)
	}
	want := []string{path.Join(packPagesDir, routesDir, name+".json"), path.Join(packPagesDir, routesDir, name+".txt")}
	for _, c := range copies {
		want = append(want, path.Join(packOutDir, c))
	}
	var got []string
	for _, pf := range inv.Files {
		got = append(got, pf.Path)
	}
	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("pack inventory %q, want %q", got, want)
	}
	for _, c := range copies {
		if string(files[path.Join(packOutDir, c)]) != string(rp.out[c]) {
			t.Errorf("pack copy %s differs from the one pages writes", c)
		}
	}
}

// pages refuses a route with no manifest and a stopped attempt while a
// ceiling-check admission of its host group has no record given, naming
// the ledger lines.
func TestPagesRefuseUnnamedCheckAdmissions(t *testing.T) {
	cfg := releaseConfig(t, "https://api.example.test/openai/v1/chat/completions")
	name := cfg.Target.Name
	b, err := probe.Encode(&probe.ReleaseRecord{Route: name, LedgerSeq: 2})
	if err != nil {
		t.Fatal(err)
	}
	rec := writeFile(t, t.TempDir(), "record.json", b)
	frozen := []admission.Entry{{Seq: 1}, {Seq: 2, Kind: admission.KindAdmit, Purpose: admission.PurposeProbe, Route: name},
		{Seq: 5, Kind: admission.KindAdmit, Purpose: admission.PurposeProbe, Route: name}}
	rp := &releasePages{routes: []report.IndexRoute{{Config: cfg}}, recs: []*routeRecords{{stops: []*cellRecord{{}}}}}
	want := fmt.Sprintf("route %s has no cell manifest to carry its host group's ceiling-check records; give with --check-record the record of ledger line 5", name)
	if err := unnamedChecks(rp, []string{rec}, frozen); err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("got %v, want %q", err, want)
	}
	if err := unnamedChecks(rp, []string{rec}, frozen[:2]); err != nil {
		t.Fatalf("every check given: %v", err)
	}
	rp.recs[0].verdict = &admission.Verdict{}
	if err := unnamedChecks(rp, []string{rec}, frozen); err != nil {
		t.Fatalf("a route with a manifest: %v", err)
	}
}

// missedAndStopped is route groq with its first cell missed and labelled,
// with a manifest, and its second stopped with no manifest, with the
// release flags pages takes and no --pages.
func missedAndStopped(t *testing.T) (*route, []string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body) //nolint:errcheck
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, chatStream) //nolint:errcheck
	}))
	t.Cleanup(srv.Close)
	var sched string
	groq, groqCfg := pagesRoute(t, "groq", srv.URL+"/openai/v1/chat/completions", "", "", scheduleOf(&sched))
	missed := groq.newRecord(groq.runs[0])
	missed.Outcome, missed.Reasons = outcomeMissed, []string{"not started"}
	if err := groq.writeNew(groq.runs[0], missed); err != nil {
		t.Fatal(err)
	}
	if err := groq.labelCell(context.Background(), missed); err != nil {
		t.Fatal(err)
	}
	stopped := groq.newRecord(groq.runs[1])
	stopped.Outcome, stopped.Reasons, stopped.Error = outcomeStopped, []string{"route stop: a clock offset above the pinned bound"}, "open /opt/frames/x: file exists"
	if err := replaceJSON(filepath.Join(groq.cellDir(groq.runs[1]), cellFile), stopped); err != nil {
		t.Fatal(err)
	}
	return groq, []string{"--out", groq.out, "--ledger", groq.ledgerPath, "--frames", groq.framesRoot, "--config", groqCfg, "--schedule", sched}
}

// A route page states each stopped attempt with no manifest, its reasons
// and billing refusals, in its text and its JSON.
func TestRoutePageStatesStoppedAttempts(t *testing.T) {
	groq, release := missedAndStopped(t)
	pages := t.TempDir()
	if code, err := pagesCmd(append(release, "--pages", pages)); code != exitOK || err != nil {
		t.Fatalf("exit %d: %v", code, err)
	}
	files := readPages(t, pages)
	page := string(files["routes/groq.txt"])
	line := fmt.Sprintf("- %s stopped: route stop: a clock offset above the pinned bound; billing refusals: 0; execution error: %s\n", cellLabel(groq.runs[1]), stoppedError)
	if !strings.Contains(page, " --\n\n-- Stopped cells with no "+evidence.ManifestFile+", from their run records --\n"+line) || strings.Contains(page, "/opt/frames") {
		t.Fatalf("groq page lacks %q:\n%s", line, page)
	}
	var p struct {
		Route string        `json:"route"`
		Stops []stoppedCell `json:"stopped_no_manifest"`
	}
	if err := json.Unmarshal(files["routes/groq.json"], &p); err != nil {
		t.Fatal(err)
	}
	if p.Route != "groq" || len(p.Stops) != 1 || p.Stops[0].Cell != cellLabel(groq.runs[1]) || p.Stops[0].Error != stoppedError {
		t.Fatalf("groq page JSON %+v", p)
	}
}
