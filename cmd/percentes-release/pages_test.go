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
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/report"
	"github.com/percentes/percentes/internal/schedule"
)

// pagesRoute is route name at endpoint over a fixture, writing under out
// and frames when they are given, with the path of its configuration
// file. Each edit changes the fixture before the route starts.
func pagesRoute(t *testing.T, name, endpoint, out, frames string, edits ...func(*fixture)) (*route, string) {
	t.Helper()
	enoughOpenFiles(t)
	path := routeConfigFile(t, name, endpoint, "10")
	cfg, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(cfg.Target.APIKeyEnv, testKey)
	f := newFixture(t, cfg, "+0.005032")
	if out != "" {
		f.in.out, f.in.framesRoot = out, frames
	}
	for _, edit := range edits {
		edit(f)
	}
	r, err := newRoute(f.in)
	if err != nil {
		t.Fatal(err)
	}
	return r, path
}

// scheduleOf is an edit that keeps the fixture's schedule path in p.
func scheduleOf(p *string) func(*fixture) {
	return func(f *fixture) { *p = f.in.schedulePath }
}

// oneRelease is an edit that runs a fixture's route on one ledger capping
// and pricing the routes names at endpoint and on one schedule, at the
// path returned, freezing their configurations and that ledger, with
// ceiling records of the route's model admitted there.
func oneRelease(t *testing.T, endpoint string, names ...string) (func(*fixture), string) {
	t.Helper()
	var cfgs []*config.Config
	for _, n := range names {
		cfg, err := config.LoadFile(routeConfigFile(t, n, endpoint, "10"))
		if err != nil {
			t.Fatal(err)
		}
		cfgs = append(cfgs, cfg)
	}
	caps, err := admission.ReleaseCaps(cfgs...)
	if err != nil {
		t.Fatal(err)
	}
	ledger := filepath.Join(t.TempDir(), "ledger.jsonl")
	l, err := admission.Create(ledger, caps)
	if err != nil {
		t.Fatal(err)
	}
	for _, cfg := range cfgs {
		if _, err := recordPrice(l, cfg, "0.15", "0.60", "https://example.test/pricing", "2026-10-06"); err != nil {
			t.Fatal(err)
		}
	}
	sched := writeFile(t, t.TempDir(), "schedule.json", ledgerSchedule(t, ledger, cfgs, names...))
	open := ledgerOpenSHA256(t, ledger)
	return func(f *fixture) {
		f.ledger, f.open, f.in.ledgerPath, f.in.schedulePath = ledger, open, ledger, sched
		f.in.probeFiles = nil
		for _, h := range append([]string{f.in.cfg.Target.Name}, otherHosts(f.in.cfg)...) {
			f.in.probeFiles = append(f.in.probeFiles, writeCeiling(t, hostCeiling(f.in.cfg, f.in.byteCap, h, 512, true, open)))
		}
	}, sched
}

func runCellOK(t *testing.T, r *route, run schedule.Run) *cellRecord {
	t.Helper()
	rec, err := r.runCell(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

// readPages reads every file under dir by its slash path.
func readPages(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	files := map[string][]byte{}
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		rel, _ := filepath.Rel(dir, p)
		files[filepath.ToSlash(rel)] = b
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// editJSON rewrites the JSON file at path through edit, as type T.
func editJSON[T any](t *testing.T, path string, edit func(*T)) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	edit(&v)
	if err := replaceJSON(path, &v); err != nil {
		t.Fatal(err)
	}
}

// The index page and each route's page are written from the cells'
// records: a counted cell enters its route's page and the index counts;
// a labelled or missed attempt does not; the index carries no forbidden
// string; the pages regenerate byte for byte; a second counted attempt
// of one cell is refused; and a later cell whose manifest records
// another account tier is named on the route page as a change.
func TestPagesCmd(t *testing.T) {
	var block atomic.Bool
	e := newEndpoint(t, func(int64) (int, string) {
		if block.Load() {
			return http.StatusUnauthorized, `{"error":{"message":"invalid key"}}`
		}
		return http.StatusOK, ""
	})
	release, sched := oneRelease(t, e.url(), "groq", "baseten")
	groq, groqCfg := pagesRoute(t, "groq", e.url(), "", "", release)
	shortLadder(groq)
	counted, blocked := groq.runs[0], groq.runs[1]
	if rec := runCellOK(t, groq, counted); rec.Outcome != outcomeCounted {
		for _, reason := range rec.Reasons {
			if !strings.HasPrefix(reason, "client-validity gate failed") && !strings.HasPrefix(reason, "ladder ended at step") {
				t.Fatalf("outcome %s, reason %q", rec.Outcome, reason)
			}
		}
		t.Skipf("client-validity gate failed (%q); no counted cell to page", rec.Reasons)
	}
	block.Store(true)
	if rec := runCellOK(t, groq, blocked); rec.Outcome != outcomeBlocked {
		t.Fatalf("outcome %s %q", rec.Outcome, rec.Reasons)
	}
	baseten, basetenCfg := pagesRoute(t, "baseten", e.url(), groq.out, groq.framesRoot, release)
	if rec := runCellOK(t, baseten, baseten.runs[0]); rec.Outcome != outcomeBlocked {
		t.Fatalf("outcome %s %q", rec.Outcome, rec.Reasons)
	}
	missed := baseten.newRecord(baseten.runs[1])
	missed.Outcome, missed.Reasons = outcomeMissed, []string{"not started"}
	if err := baseten.writeNew(baseten.runs[1], missed); err != nil {
		t.Fatal(err)
	}

	pages := t.TempDir()
	args := []string{"--out", groq.out, "--frames", groq.framesRoot, "--config", groqCfg, "--config", basetenCfg, "--schedule", sched, "--pages", pages}
	if code, err := pagesCmd(args); code != exitOK || err != nil {
		t.Fatalf("exit %d: %v", code, err)
	}
	files := readPages(t, pages)
	if len(files) != 6 {
		t.Fatalf("files %v", keys(files))
	}
	index := string(files[indexPage])
	for _, frag := range []string{
		"| Groq | baseten | openai/gpt-oss-120b | 1 | 0.25 | admitted | none | 512 | incomplete frame, 0 of 12 cells recorded |",
		"| Groq | groq | openai/gpt-oss-120b | 1 | 0.25 | admitted | 3 | 512 | incomplete frame, 1 of 12 cells recorded |",
		"| [groq](routes/groq.txt) |\n",
		"| [baseten](routes/baseten.txt) |\n",
	} {
		if !strings.Contains(index, frag) {
			t.Errorf("index lacks %q:\n%s", frag, index)
		}
	}
	if strings.Index(index, "| Groq | baseten |") > strings.Index(index, "| Groq | groq |") {
		t.Errorf("baseten's rows follow groq's:\n%s", index)
	}
	for _, name := range []string{indexPage, indexJSON} {
		low := strings.ToLower(string(files[name]))
		for _, f := range report.IndexForbidden {
			if strings.Contains(low, f) {
				t.Errorf("%s carries %q", name, f)
			}
		}
	}

	page := string(files["routes/groq.txt"])
	if !strings.Contains(page, "-- Cell "+cellLabel(counted)+":") || strings.Contains(page, cellLabel(blocked)) ||
		!strings.Contains(page, "-- Incomplete frame: 1 of 12 cells; no pooled figures --") {
		t.Errorf("groq page:\n%s", page)
	}
	var st report.StepTables
	if err := json.Unmarshal(files["routes/groq.json"], &st); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(groq.cellDir(counted), evidence.AttemptsFile))
	if err != nil {
		t.Fatal(err)
	}
	recs, err := evidence.ReadRecords(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	measured, scheduled := 0, 0
	for i := range recs {
		if !recs[i].Warmup {
			measured++
		}
	}
	if len(st.Cells) == 1 {
		for _, s := range st.Cells[0].Steps {
			scheduled += s.Scheduled
		}
	}
	if len(st.Cells) != 1 || st.Cells[0].Cell != cellLabel(counted) || scheduled != measured || measured == 0 {
		t.Fatalf("groq tables: %d cells, %d scheduled, the attempts file %d measured", len(st.Cells), scheduled, measured)
	}
	// Each completion of the endpoint's two content events gives one gap.
	var completions, gaps int64
	perStep := map[int]int{}
	for i := range recs {
		if !recs[i].Warmup && recs[i].Outcome == loadgen.OutcomeCompleted {
			completions++
			perStep[recs[i].Step]++
		}
	}
	// Throughput is over the configuration file's step window, the pinned
	// one, whatever ladder the test ran.
	for _, s := range st.Cells[0].Steps {
		if s.State != report.StepAttempted || s.Scheduled == 0 {
			continue
		}
		line := fmt.Sprintf("  throughput over the step's configured %d s window: %d completions, %.3f per second; no completion carried provider-reported completion tokens\n",
			config.PinnedReleaseStepS, perStep[s.Step], float64(perStep[s.Step])/config.PinnedReleaseStepS)
		if s.Throughput == nil || s.Throughput.Completions != perStep[s.Step] || !strings.Contains(page, line) {
			t.Errorf("step %d: throughput %+v; groq page lacks %q", s.Step, s.Throughput, line)
		}
	}
	for _, s := range st.Cells[0].Steps {
		if s.State == report.StepAttempted && s.Scheduled > 0 && (s.Incidence == nil || s.Signals == nil) {
			t.Errorf("step %d: incidence quantiles %v, signals %v", s.Step, s.Incidence, s.Signals)
		}
		if s.Signals != nil {
			gaps += s.Signals.Gaps.N
			if line := fmt.Sprintf("  inter-chunk gaps between successive content events, over completions: n %d\n", s.Signals.Gaps.N); !strings.Contains(page, line) {
				t.Errorf("groq page lacks %q", line)
			}
		}
	}
	if completions == 0 || gaps != completions || !strings.Contains(page, "  completion-incidence quantiles, Aalen-Johansen over scheduled, inside the 90 s hard timeout:\n") {
		t.Errorf("groq page: %d gaps over %d completions:\n%s", gaps, completions, page)
	}
	var basetenTables report.StepTables
	if err := json.Unmarshal(files["routes/baseten.json"], &basetenTables); err != nil || len(basetenTables.Cells) != 0 {
		t.Errorf("baseten tables: %d cells: %v", len(basetenTables.Cells), err)
	}

	if code := dispatch(append([]string{"pages"}, args...)); code != exitOK {
		t.Fatalf("again: exit %d", code)
	}
	for name, b := range readPages(t, pages) {
		if !bytes.Equal(b, files[name]) {
			t.Errorf("%s changed when written again", name)
		}
	}

	second := counted
	second.Attempt = 2
	dir := groq.cellDir(second)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	ents, err := os.ReadDir(groq.cellDir(counted))
	if err != nil {
		t.Fatal(err)
	}
	for _, ent := range ents {
		b, err := os.ReadFile(filepath.Join(groq.cellDir(counted), ent.Name()))
		if err != nil {
			t.Fatal(err)
		}
		writeFile(t, dir, ent.Name(), b)
	}
	editJSON(t, filepath.Join(dir, cellFile), func(c *cellRecord) { c.Run.Attempt = 2 })
	editJSON(t, filepath.Join(dir, evidence.ManifestFile), func(m *evidence.Manifest) {
		m.Release.Run.Attempt = 2
		m.Release.Dispatch.Sources[0].Run.Attempt = 2
	})
	code, err := pagesCmd(args)
	if code != exitError || err == nil || !strings.Contains(err.Error(), "cell "+counted.Label+" of route groq counted twice") {
		t.Fatalf("exit %d: %v", code, err)
	}

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	later := groq.runs[2]
	if !later.Start.After(counted.Start) {
		t.Fatalf("run %s starts %s, before %s", later.Label, later.Start, counted.Start)
	}
	dir = groq.cellDir(later)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, ent := range ents {
		b, err := os.ReadFile(filepath.Join(groq.cellDir(counted), ent.Name()))
		if err != nil {
			t.Fatal(err)
		}
		writeFile(t, dir, ent.Name(), b)
	}
	editJSON(t, filepath.Join(dir, cellFile), func(c *cellRecord) { c.Run = later })
	editJSON(t, filepath.Join(dir, evidence.ManifestFile), func(m *evidence.Manifest) {
		m.Release.Run, m.Release.Dispatch.Sources[0].Run = later, later
		m.Endpoint.AccountTier = "Scale"
	})
	if code, err := pagesCmd(args); code != exitOK || err != nil {
		t.Fatalf("exit %d: %v", code, err)
	}
	files = readPages(t, pages)
	var split report.StepTables
	if err := json.Unmarshal(files["routes/groq.json"], &split); err != nil {
		t.Fatal(err)
	}
	want := []report.DriftChange{{Cell: cellLabel(later), StartUTC: later.Start.UTC(),
		Removed: []report.DocRecord{{Kind: report.DocTier, Tier: "Developer"}}, Added: []report.DocRecord{{Kind: report.DocTier, Tier: "Scale"}}}}
	if !reflect.DeepEqual(split.DriftChanges, want) || len(split.Cells) != 2 {
		t.Errorf("groq tables: %d cells, changes %+v", len(split.Cells), split.DriftChanges)
	}
	page = string(files["routes/groq.txt"])
	if line := "-- Change of limits or documentation, first observed at cell " + cellLabel(later) + ", started " + later.Start.UTC().Format(time.DateOnly) +
		" --\n  removed: account tier Developer\n  added: account tier Scale\n"; !strings.Contains(page, line) {
		t.Errorf("groq page lacks %q:\n%s", line, page)
	}
}

// A route's page lists each model_mismatch cell, outside the frame, with
// its expected and reported model ids and its mismatch count, as its
// JSON does.
func TestPagesModelMismatch(t *testing.T) {
	stream := strings.ReplaceAll(chatStream, `"model":"openai/gpt-oss-120b"`, `"model":"openai/gpt-oss-120b-v2"`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body) //nolint:errcheck
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, stream) //nolint:errcheck
	}))
	t.Cleanup(srv.Close)
	var sched string
	groq, groqCfg := pagesRoute(t, "groq", srv.URL+"/openai/v1/chat/completions", "", "", scheduleOf(&sched))
	shortLadder(groq)
	run := groq.runs[0]
	rec := runCellOK(t, groq, run)
	if rec.Outcome != outcomeMismatch {
		t.Fatalf("outcome %s %q", rec.Outcome, rec.Reasons)
	}
	n := rec.Sent.Warmup + rec.Sent.Measured
	pages := t.TempDir()
	if code, err := pagesCmd([]string{"--out", groq.out, "--frames", groq.framesRoot, "--config", groqCfg, "--schedule", sched, "--pages", pages}); code != exitOK || err != nil {
		t.Fatalf("exit %d: %v", code, err)
	}
	files := readPages(t, pages)
	page := string(files["routes/groq.txt"])
	line := fmt.Sprintf("-- Cell %s, model_mismatch, outside the frame (SPEC-HOSTED §2): %d requests reported a model other than the expected \"openai/gpt-oss-120b\"; reported [\"openai/gpt-oss-120b-v2\"] --\n", cellLabel(run), n)
	if !strings.Contains(page, line) || strings.Contains(page, "-- Cell "+cellLabel(run)+":") {
		t.Errorf("groq page lacks %q or tables the cell:\n%s", line, page)
	}
	var st report.StepTables
	if err := json.Unmarshal(files["routes/groq.json"], &st); err != nil {
		t.Fatal(err)
	}
	want := []report.ModelMismatch{{Cell: cellLabel(run), Expected: "openai/gpt-oss-120b", Reported: []string{"openai/gpt-oss-120b-v2"}, Requests: n}}
	if !reflect.DeepEqual(st.ModelMismatches, want) || len(st.Cells) != 0 {
		t.Errorf("groq tables: %d cells, mismatches %+v", len(st.Cells), st.ModelMismatches)
	}
}

func keys(m map[string][]byte) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// pages refuses, writing nothing, a missing flag, a route with no cell
// record or with cells and no configuration, a configuration other than
// the one the cells ran, a schedule other than the one the cells ran
// under, a running cell, a labelled cell without its manifest, a run
// record that disagrees with its directory or manifest, a schedule hash
// that differs between cells or between a run record and its manifest,
// and a ceiling choice or a verdict that differs between cells.
func TestPagesCmdRefusals(t *testing.T) {
	e := newEndpoint(t, func(int64) (int, string) { return http.StatusNotFound, "" })
	other, err := testSchedule(t, nil, "groq", "other").JSON()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		// edit changes the release after groq's first two cells are
		// blocked, and returns the arguments that replace the defaults.
		edit func(t *testing.T, groq *route, cfg, sched, pages string) []string
		want string
	}{
		{"no pages", func(_ *testing.T, g *route, cfg, sched, _ string) []string {
			return []string{"--out", g.out, "--frames", g.framesRoot, "--config", cfg, "--schedule", sched}
		}, "--pages is required"},
		{"no config", func(_ *testing.T, g *route, _, sched, pages string) []string {
			return []string{"--out", g.out, "--frames", g.framesRoot, "--schedule", sched, "--pages", pages}
		}, "--config is required"},
		{"no schedule", func(_ *testing.T, g *route, cfg, _, pages string) []string {
			return []string{"--out", g.out, "--frames", g.framesRoot, "--config", cfg, "--pages", pages}
		}, "--schedule is required"},
		{"route without cells", func(t *testing.T, g *route, cfg, sched, pages string) []string {
			anth := routeConfigFile(t, "anth", e.url(), "10")
			return []string{"--out", g.out, "--frames", g.framesRoot, "--config", cfg, "--config", anth, "--schedule", sched, "--pages", pages}
		}, "route anth has no cell with a manifest.json"},
		{"cells without config", func(t *testing.T, g *route, _, _, pages string) []string {
			var sched string
			anth, cfg := pagesRoute(t, "anth", e.url(), g.out, g.framesRoot, scheduleOf(&sched))
			runCellOK(t, anth, anth.runs[0])
			return []string{"--out", g.out, "--frames", g.framesRoot, "--config", cfg, "--schedule", sched, "--pages", pages}
		}, "route groq has cells under"},
		{"other config", func(t *testing.T, g *route, _, sched, pages string) []string {
			changed := writeFile(t, t.TempDir(), "groq.yaml", append(append([]byte{}, g.cfg.Raw...), "# changed\n"...))
			return []string{"--out", g.out, "--frames", g.framesRoot, "--config", changed, "--schedule", sched, "--pages", pages}
		}, "config: not groq.yaml"},
		{"other schedule", func(t *testing.T, g *route, cfg, _, pages string) []string {
			path := writeFile(t, t.TempDir(), "schedule.json", other)
			return []string{"--out", g.out, "--frames", g.framesRoot, "--config", cfg, "--schedule", path, "--pages", pages}
		}, "\"; --schedule has sha256 " + sha(string(other))},
		{"running", func(t *testing.T, g *route, _, _, _ string) []string {
			rec := g.newRecord(g.runs[2])
			rec.Outcome = outcomeRunning
			if err := g.writeNew(g.runs[2], rec); err != nil {
				t.Fatal(err)
			}
			return nil
		}, "the cell is running"},
		{"no manifest", func(t *testing.T, g *route, _, _, _ string) []string {
			if err := os.Remove(filepath.Join(g.cellDir(g.runs[1]), evidence.ManifestFile)); err != nil {
				t.Fatal(err)
			}
			return nil
		}, "blocked with no manifest.json"},
		{"directory", func(t *testing.T, g *route, _, _, _ string) []string {
			if err := os.Rename(g.cellDir(g.runs[1]), filepath.Join(g.dir, "moved-a1")); err != nil {
				t.Fatal(err)
			}
			return nil
		}, "moved-a1: the run record is route groq"},
		{"run differs", func(t *testing.T, g *route, _, _, _ string) []string {
			editJSON(t, filepath.Join(g.cellDir(g.runs[1]), cellFile), func(c *cellRecord) { c.Run.Seed++ })
			return nil
		}, "the manifest is "},
		{"schedule differs between cells", func(t *testing.T, g *route, _, _, _ string) []string {
			editJSON(t, filepath.Join(g.cellDir(g.runs[1]), cellFile), func(c *cellRecord) { c.ScheduleSHA256 = sha(string(other)) })
			return nil
		}, "the cell ran under schedule sha256 \"" + sha(string(other)) + "\", the route's other cells under"},
		{"schedule differs from the manifest", func(t *testing.T, g *route, _, _, _ string) []string {
			editJSON(t, filepath.Join(g.cellDir(g.runs[1]), evidence.ManifestFile), func(m *evidence.Manifest) { m.Release.ScheduleSHA256 = sha(string(other)) })
			return nil
		}, "the manifest's schedule sha256 \"" + sha(string(other)) + "\", the run record's"},
		{"counted in the run record", func(t *testing.T, g *route, _, _, _ string) []string {
			editJSON(t, filepath.Join(g.cellDir(g.runs[1]), cellFile), func(c *cellRecord) { c.Outcome = outcomeCounted })
			return nil
		}, "counted in the run record, blocked in the manifest"},
		{"ceiling choice differs", func(t *testing.T, g *route, _, _, _ string) []string {
			editJSON(t, filepath.Join(g.cellDir(g.runs[1]), evidence.ManifestFile), func(m *evidence.Manifest) {
				m.Release.Ceiling.Reason = "groq failed at 512; every host passed at 1024"
			})
			return nil
		}, "the ceiling choice or its probe records differ from the route's other cells'"},
		{"verdict differs", func(t *testing.T, g *route, _, _, _ string) []string {
			editJSON(t, filepath.Join(g.cellDir(g.runs[1]), evidence.ManifestFile), func(m *evidence.Manifest) {
				m.Release.Admission.Steps[0].Demands[0].Fraction += 0.01
			})
			return nil
		}, "the ceiling-rule verdict differs"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var sched string
			groq, cfg := pagesRoute(t, "groq", e.url(), "", "", scheduleOf(&sched))
			shortLadder(groq)
			for _, run := range groq.runs[:2] {
				if rec := runCellOK(t, groq, run); rec.Outcome != outcomeBlocked {
					t.Fatalf("outcome %s %q", rec.Outcome, rec.Reasons)
				}
			}
			pages := t.TempDir()
			args := []string{"--out", groq.out, "--frames", groq.framesRoot, "--config", cfg, "--schedule", sched, "--pages", pages}
			if code, err := pagesCmd(args); code != exitOK || err != nil {
				t.Fatalf("before the edit: exit %d: %v", code, err)
			}
			if err := os.RemoveAll(pages); err != nil {
				t.Fatal(err)
			}
			if a := c.edit(t, groq, cfg, sched, pages); a != nil {
				args = a
			}
			code, err := pagesCmd(args)
			if code != exitError || err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("exit %d: %v, want %q", code, err, c.want)
			}
			if _, err := os.Stat(pages); !os.IsNotExist(err) {
				t.Errorf("pages written on a refusal: %v", err)
			}
		})
	}
}

// The hosts of one model page when their manifests carry one ceiling
// choice and one set of probe records, and are refused, writing nothing,
// when a host's process read another set of probe records or its
// manifest records another choice.
func TestPagesHostGroup(t *testing.T) {
	e := newEndpoint(t, func(int64) (int, string) { return http.StatusNotFound, "" })
	extra := func(f *fixture) {
		for _, h := range []string{"baseten", "groq"} {
			f.in.probeFiles = append(f.in.probeFiles, writeCeiling(t, hostCeiling(f.in.cfg, f.in.byteCap, h, 1024, true, f.open)))
		}
	}
	for _, c := range []struct {
		name     string
		edit     func(*fixture)
		manifest func(*evidence.Manifest)
		want     string
	}{
		{"one choice", func(*fixture) {}, nil, ""},
		{"other probe records", extra, nil, "pages: routes groq and baseten, hosts of one model, carry different ceiling probe records"},
		{"other choice", func(*fixture) {}, func(m *evidence.Manifest) {
			m.Release.Ceiling.Reason = "groq failed at 512; every host passed at 1024"
		}, "pages: routes groq and baseten, hosts of one model, ran at different ceiling choices"},
	} {
		t.Run(c.name, func(t *testing.T) {
			share, sched := oneRelease(t, e.url(), "groq", "baseten")
			groq, groqCfg := pagesRoute(t, "groq", e.url(), "", "", share)
			baseten, basetenCfg := pagesRoute(t, "baseten", e.url(), groq.out, groq.framesRoot, share, c.edit)
			for _, r := range []*route{groq, baseten} {
				shortLadder(r)
				if rec := runCellOK(t, r, r.runs[0]); rec.Outcome != outcomeBlocked {
					t.Fatalf("%s: outcome %s %q", r.cfg.Target.Name, rec.Outcome, rec.Reasons)
				}
			}
			if c.manifest != nil {
				editJSON(t, filepath.Join(baseten.cellDir(baseten.runs[0]), evidence.ManifestFile), c.manifest)
			}
			pages := filepath.Join(t.TempDir(), "pages")
			code, err := pagesCmd([]string{"--out", groq.out, "--frames", groq.framesRoot, "--config", groqCfg, "--config", basetenCfg, "--schedule", sched, "--pages", pages})
			if c.want == "" {
				if code != exitOK || err != nil {
					t.Fatalf("exit %d: %v", code, err)
				}
				return
			}
			if code != exitError || err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("exit %d: %v, want %q", code, err, c.want)
			}
			if _, err := os.Stat(pages); !os.IsNotExist(err) {
				t.Errorf("pages written on a refusal: %v", err)
			}
		})
	}
}

// A host whose attempts are all missed or interrupted gets no page and a
// pack whose inventory names no file, needs no window-close record, and
// leaves its sibling's pages and publication record to pass while its
// configuration runs at the sibling's ceiling;
// at another ceiling pages and publish-check refuse it, writing nothing.
func TestPagesMissedHost(t *testing.T) {
	e := newEndpoint(t, func(int64) (int, string) { return http.StatusNotFound, "" })
	release, sched := oneRelease(t, e.url(), "groq", "baseten")
	groq, groqCfg := pagesRoute(t, "groq", e.url(), "", "", release)
	baseten, basetenCfg := pagesRoute(t, "baseten", e.url(), groq.out, groq.framesRoot, release)
	shortLadder(groq)
	if rec := runCellOK(t, groq, groq.runs[0]); rec.Outcome != outcomeBlocked {
		t.Fatalf("outcome %s %q", rec.Outcome, rec.Reasons)
	}
	for i, outcome := range []string{outcomeMissed, outcomeInterrupted} {
		rec := baseten.newRecord(baseten.runs[i])
		rec.Outcome = outcome
		if err := baseten.writeNew(baseten.runs[i], rec); err != nil {
			t.Fatal(err)
		}
	}
	st, err := groq.state()
	if err != nil {
		t.Fatal(err)
	}
	stdoutOf(t, func() {
		if err := groq.windowClose(context.Background(), st); err != nil {
			t.Error(err)
		}
	})
	pages := t.TempDir()
	args := []string{"--out", groq.out, "--frames", groq.framesRoot, "--config", groqCfg, "--config", basetenCfg, "--schedule", sched, "--pages", pages}
	var code int
	text := stdoutOf(t, func() { code, err = pagesCmd(args) })
	if code != exitOK || err != nil || !strings.Contains(text, "baseten: no cell with a manifest.json, 2 with a run record only; no page\n") {
		t.Fatalf("exit %d: %v:\n%s", code, err, text)
	}
	files := readPages(t, pages)
	if index := string(files[indexPage]); len(files) != 4 || files["routes/groq.txt"] == nil || !strings.Contains(index, "| Groq | groq |") || strings.Contains(index, "| Groq | baseten |") {
		t.Fatalf("files %v:\n%s", keys(files), index)
	}
	packs := map[string][]byte{"groq": packOf(t, args[:len(args)-2], "groq"), "baseten": packOf(t, args[:len(args)-2], "baseten")}
	if packed, err := evidence.ReadPack(packs["baseten"]); err != nil || len(packed) != 1 || !strings.Contains(string(packed[evidence.PackInventoryFile]), `"files": []`) {
		t.Fatalf("baseten's pack: %v: %q", err, packed)
	}
	record := publication(t, groq.out, pages, packs, func(p *evidence.Publication) {
		r := p.Routes[0]
		n := *r.Notice
		n.Pack = &evidence.PublishedFile{Path: "baseten-pack.tar", SHA256: sha(string(packs["baseten"]))}
		r.Route, r.Notice, r.Permission = "baseten", &n, nil
		p.Routes = append(p.Routes, r)
	})
	text = stdoutOf(t, func() {
		code, err = publishCheckCmd(append(args[:len(args):len(args)], "--write-page", "--record", record))
	})
	if code != exitOK || err != nil || !strings.Contains(text, "publication record complete: 1 manifests, 4 pages, 2 routes\n") {
		t.Fatalf("publish-check: exit %d: %v:\n%s", code, err, text)
	}

	other := filepath.Join(t.TempDir(), "pages")
	args = []string{"--out", groq.out, "--frames", groq.framesRoot, "--config", groqCfg, "--config", fallbackFile(t, basetenCfg), "--schedule", sched, "--pages", other}
	const want = "pages: routes groq and baseten, hosts of one model, ran at ceilings 512 and 1024"
	if code, err := pagesCmd(args); code != exitError || err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("exit %d: %v, want %q", code, err, want)
	}
	if _, err := os.Stat(other); !os.IsNotExist(err) {
		t.Errorf("pages written on a refusal: %v", err)
	}
	if code, err := publishCheckCmd(append(args, "--record", record)); code != exitError || err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("publish-check: exit %d: %v, want %q", code, err, want)
	}
}
