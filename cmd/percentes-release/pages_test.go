package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/report"
	"github.com/percentes/percentes/internal/schedule"
)

// pagesRoute is route name at endpoint over a fixture, writing under out
// and frames when they are given, with the path of its configuration
// file.
func pagesRoute(t *testing.T, name, endpoint, out, frames string) (*route, string) {
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
	r, err := newRoute(f.in)
	if err != nil {
		t.Fatal(err)
	}
	return r, path
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
// string; the pages regenerate byte for byte; and a second counted
// attempt of one cell is refused.
func TestPagesCmd(t *testing.T) {
	var block atomic.Bool
	e := newEndpoint(t, func(int64) (int, string) {
		if block.Load() {
			return http.StatusUnauthorized, `{"error":{"message":"invalid key"}}`
		}
		return http.StatusOK, ""
	})
	groq, groqCfg := pagesRoute(t, "groq", e.url(), "", "")
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
	anth, anthCfg := pagesRoute(t, "anth", e.url(), groq.out, groq.framesRoot)
	if rec := runCellOK(t, anth, anth.runs[0]); rec.Outcome != outcomeBlocked {
		t.Fatalf("outcome %s %q", rec.Outcome, rec.Reasons)
	}
	missed := anth.newRecord(anth.runs[1])
	missed.Outcome, missed.Reasons = outcomeMissed, []string{"not started"}
	if err := anth.writeNew(anth.runs[1], missed); err != nil {
		t.Fatal(err)
	}

	pages := t.TempDir()
	args := []string{"--out", groq.out, "--frames", groq.framesRoot, "--config", groqCfg, "--config", anthCfg, "--pages", pages}
	if code, err := pagesCmd(args); code != exitOK || err != nil {
		t.Fatalf("exit %d: %v", code, err)
	}
	files := readPages(t, pages)
	if len(files) != 6 {
		t.Fatalf("files %v", keys(files))
	}
	index := string(files[indexPage])
	for _, frag := range []string{
		"| Groq | anth | openai/gpt-oss-120b | 1 | 0.25 | admitted | none | 512 | incomplete frame, 0 of 12 cells recorded |",
		"| Groq | groq | openai/gpt-oss-120b | 1 | 0.25 | admitted | 3 | 512 | incomplete frame, 1 of 12 cells recorded |",
		"| [groq](routes/groq.txt) |\n",
		"| [anth](routes/anth.txt) |\n",
	} {
		if !strings.Contains(index, frag) {
			t.Errorf("index lacks %q:\n%s", frag, index)
		}
	}
	if strings.Index(index, "| Groq | anth |") > strings.Index(index, "| Groq | groq |") {
		t.Errorf("anth's rows follow groq's:\n%s", index)
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
		t.Errorf("groq tables: %d cells, %d scheduled, the attempts file %d measured", len(st.Cells), scheduled, measured)
	}
	var anthTables report.StepTables
	if err := json.Unmarshal(files["routes/anth.json"], &anthTables); err != nil || len(anthTables.Cells) != 0 {
		t.Errorf("anth tables: %d cells: %v", len(anthTables.Cells), err)
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
	editJSON(t, filepath.Join(dir, evidence.ManifestFile), func(m *evidence.Manifest) { m.Release.Run.Attempt = 2 })
	code, err := pagesCmd(args)
	if code != exitError || err == nil || !strings.Contains(err.Error(), "cell "+counted.Label+" of route groq counted twice") {
		t.Fatalf("exit %d: %v", code, err)
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
// the one the cells ran, a running cell, a labelled cell without its
// manifest, a run record that disagrees with its directory or manifest,
// and a verdict that differs between cells.
func TestPagesCmdRefusals(t *testing.T) {
	e := newEndpoint(t, func(int64) (int, string) { return http.StatusUnauthorized, "" })
	for _, c := range []struct {
		name string
		// edit changes the release after groq's first two cells are
		// blocked, and returns the arguments that replace the defaults.
		edit func(t *testing.T, groq *route, cfg, pages string) []string
		want string
	}{
		{"no pages", func(_ *testing.T, g *route, cfg, _ string) []string {
			return []string{"--out", g.out, "--frames", g.framesRoot, "--config", cfg}
		}, "--pages is required"},
		{"no config", func(_ *testing.T, g *route, _, pages string) []string {
			return []string{"--out", g.out, "--frames", g.framesRoot, "--pages", pages}
		}, "--config is required"},
		{"route without cells", func(t *testing.T, g *route, cfg, pages string) []string {
			anth := routeConfigFile(t, "anth", e.url(), "10")
			return []string{"--out", g.out, "--frames", g.framesRoot, "--config", cfg, "--config", anth, "--pages", pages}
		}, "route anth has no cell with a manifest.json"},
		{"cells without config", func(t *testing.T, g *route, _, pages string) []string {
			anth, cfg := pagesRoute(t, "anth", e.url(), g.out, g.framesRoot)
			runCellOK(t, anth, anth.runs[0])
			return []string{"--out", g.out, "--frames", g.framesRoot, "--config", cfg, "--pages", pages}
		}, "route groq has cells under"},
		{"other config", func(t *testing.T, g *route, _, pages string) []string {
			changed := writeFile(t, t.TempDir(), "groq.yaml", append(append([]byte{}, g.cfg.Raw...), "# changed\n"...))
			return []string{"--out", g.out, "--frames", g.framesRoot, "--config", changed, "--pages", pages}
		}, "config: not groq.yaml"},
		{"running", func(t *testing.T, g *route, _, _ string) []string {
			rec := g.newRecord(g.runs[2])
			rec.Outcome = outcomeRunning
			if err := g.writeNew(g.runs[2], rec); err != nil {
				t.Fatal(err)
			}
			return nil
		}, "the cell is running"},
		{"no manifest", func(t *testing.T, g *route, _, _ string) []string {
			if err := os.Remove(filepath.Join(g.cellDir(g.runs[1]), evidence.ManifestFile)); err != nil {
				t.Fatal(err)
			}
			return nil
		}, "blocked with no manifest.json"},
		{"directory", func(t *testing.T, g *route, _, _ string) []string {
			if err := os.Rename(g.cellDir(g.runs[1]), filepath.Join(g.dir, "moved-a1")); err != nil {
				t.Fatal(err)
			}
			return nil
		}, "moved-a1: the run record is route groq"},
		{"run differs", func(t *testing.T, g *route, _, _ string) []string {
			editJSON(t, filepath.Join(g.cellDir(g.runs[1]), cellFile), func(c *cellRecord) { c.Run.Seed++ })
			return nil
		}, "the manifest is "},
		{"counted in the run record", func(t *testing.T, g *route, _, _ string) []string {
			editJSON(t, filepath.Join(g.cellDir(g.runs[1]), cellFile), func(c *cellRecord) { c.Outcome = outcomeCounted })
			return nil
		}, "counted in the run record, blocked in the manifest"},
		{"verdict differs", func(t *testing.T, g *route, _, _ string) []string {
			editJSON(t, filepath.Join(g.cellDir(g.runs[1]), evidence.ManifestFile), func(m *evidence.Manifest) {
				m.Release.Admission.Steps[0].Demands[0].Fraction += 0.01
			})
			return nil
		}, "the ceiling-rule verdict differs"},
	} {
		t.Run(c.name, func(t *testing.T) {
			groq, cfg := pagesRoute(t, "groq", e.url(), "", "")
			shortLadder(groq)
			for _, run := range groq.runs[:2] {
				if rec := runCellOK(t, groq, run); rec.Outcome != outcomeBlocked {
					t.Fatalf("outcome %s %q", rec.Outcome, rec.Reasons)
				}
			}
			pages := t.TempDir()
			args := []string{"--out", groq.out, "--frames", groq.framesRoot, "--config", cfg, "--pages", pages}
			if code, err := pagesCmd(args); code != exitOK || err != nil {
				t.Fatalf("before the edit: exit %d: %v", code, err)
			}
			if err := os.RemoveAll(pages); err != nil {
				t.Fatal(err)
			}
			if a := c.edit(t, groq, cfg, pages); a != nil {
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
