package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/schedule"
)

// limitsRefresh gives f a refresh command that touches the record file
// and prints out.
func limitsRefresh(t *testing.T, f *fixture, out string) {
	t.Helper()
	script := filepath.Join(t.TempDir(), "refresh")
	body := "#!/bin/sh\ntouch '" + filepath.Join(filepath.Dir(f.in.opPath), "r.txt") + "' && echo '" + out + "'\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	f.in.refreshCmd = script
}

// tpmRefresh is a refresh output with the reference's requests_per_minute
// limit as recorded and its tokens_per_minute limit at tpm.
func tpmRefresh(tpm string) string {
	return `{"limits":[{"dimension":"requests_per_minute","value":1000},{"dimension":"tokens_per_minute",` + tpm + `}]}`
}

// SPEC-HOSTED §4: limits read at a cell's start that no longer admit a
// step the route runs stop the route before admission, naming the step
// and the limit, with no ledger line and no request; limits that still
// admit it, or that could not be read, let the cell go on (to its probe,
// which the endpoint answers 401). A confidential limit is named by its
// source and hash, and neither its recorded value nor the value read
// reaches a file under the release output directory; the refresh record
// as run is kept in the run's frame directory and, at window close,
// beside the record's copies, and the window-close record names the
// operator file by keyed hash.
func TestCellLimitsStop(t *testing.T) {
	const recorded = "250013.5"
	for _, tc := range []struct {
		name         string
		confidential bool
		tpm          string
		stop         string
	}{
		{"lowered", false, `"value":200000`, "route stop: the limits read at the cell's start, at the price retrieved 2026-10-06, no longer admit step 3 (1 rps) of the 3 the route runs: " +
			"tokens_per_minute recorded 250000, read 200000: nominal demand 102720 above half of it; the route keeps its steps (SPEC-HOSTED §4)"},
		{"raised", false, `"value":300000`, ""},
		{"unavailable", false, `"unavailable":"console only"`, ""},
		{"confidential lowered", true, `"value":200000`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEndpoint(t, func(int64) (int, string) { return http.StatusUnauthorized, "" })
			enoughOpenFiles(t)
			cfg := releaseConfig(t, e.url())
			if tc.confidential {
				cfg = confidentialConfig(t, e.url())
			}
			f := newFixture(t, cfg, "+0.005")
			limitsRefresh(t, f, tpmRefresh(tc.tpm))
			r, err := newRoute(f.in)
			if err != nil {
				t.Fatal(err)
			}
			if got := r.verdict.Admitted(); len(got) != 3 {
				t.Fatalf("admitted %v", got)
			}
			shortLadder(r)
			want := tc.stop
			if tc.confidential {
				l := cfg.Target.Limits[1]
				want = "route stop: the limits read at the cell's start, at the price retrieved 2026-10-06, no longer admit step 3 (1 rps) of the 3 the route runs: " +
					"tokens_per_minute confidential (" + l.Source + ", hmac-sha256 " + l.HMACSHA256 + "): nominal demand 102720 above half of it; the route keeps its steps (SPEC-HOSTED §4)"
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
			m, _ := checkRetained(t, r, run)
			if want == "" {
				if rec.Outcome != outcomeBlocked || len(rec.Reasons) != 1 || !strings.Contains(rec.Reasons[0], "probe request returned 401") || e.hits.Load() != 1 {
					t.Fatalf("outcome %s, reasons %q, %d requests; want blocked at the probe", rec.Outcome, rec.Reasons, e.hits.Load())
				}
				return
			}
			if rec.Outcome != outcomeStopped || len(rec.Reasons) != 1 || rec.Reasons[0] != want {
				t.Fatalf("outcome %s, reasons %q; want stopped, %q", rec.Outcome, rec.Reasons, want)
			}
			if n := e.hits.Load(); n != 0 || rec.Sent != (admission.Counts{}) {
				t.Fatalf("server saw %d requests; sent %+v", n, rec.Sent)
			}
			if after, err := ledgerEntries(f.ledger, ""); err != nil || len(after) != len(before) {
				t.Fatalf("ledger lines %d, were %d: %v", len(after), len(before), err)
			}
			if m.Counted() || m.Release.Outcome != outcomeStopped || m.Release.Reasons[0] != want || m.Artifact(evidence.RefreshFile) == nil {
				t.Fatalf("manifest: outcome %s, reasons %q, artifacts %+v", m.Release.Outcome, m.Release.Reasons, m.Artifacts)
			}
			pub, err := os.ReadFile(filepath.Join(r.cellDir(run), evidence.RefreshFile))
			if err != nil {
				t.Fatal(err)
			}
			var published evidence.StatusRecord
			if err := json.Unmarshal(pub, &published); err != nil {
				t.Fatal(err)
			}
			if len(published.Limits) != 2 || published.Limits[1].Confidential != tc.confidential {
				t.Fatalf("published limits %+v", published.Limits)
			}
			if !tc.confidential {
				if published.Limits[1].Value != 200000 || published.Stdout.Text != tpmRefresh(tc.tpm)+"\n" {
					t.Fatalf("published refresh record %s", pub)
				}
				return
			}

			held := filepath.Join(r.framesRoot, filepath.Dir(m.Frames.Name), evidence.RefreshFile)
			if b, err := os.ReadFile(held); err != nil || !bytes.Contains(b, []byte("200000")) {
				t.Fatalf("retained refresh record %s: %v", b, err)
			}
			st, err := r.state()
			if err != nil {
				t.Fatal(err)
			}
			r.now = func() time.Time { return time.Date(2026, 10, 27, 1, 0, 0, 0, time.UTC) }
			if err := r.windowClose(context.Background(), st); err != nil {
				t.Fatal(err)
			}
			w, err := evidence.ReadWindowClose(filepath.Join(r.dir, evidence.WindowCloseFile))
			if err != nil {
				t.Fatal(err)
			}
			if b, err := os.ReadFile(filepath.Join(r.framesRoot, w.Copies, evidence.RefreshFile)); err != nil || !bytes.Contains(b, []byte("200000")) {
				t.Fatalf("retained window-close refresh record %s: %v", b, err)
			}
			opCopy, err := os.ReadFile(filepath.Join(r.framesRoot, w.Copies, evidence.OperatorCopyFile))
			if err != nil || w.Operator != (evidence.FileRef{Name: filepath.Base(f.in.opPath), HMACSHA256: r.key.Sum(opCopy)}) {
				t.Fatalf("window-close operator file %+v: %v", w.Operator, err)
			}
			if err := filepath.WalkDir(f.in.out, func(p string, d os.DirEntry, err error) error {
				if err != nil || d.IsDir() {
					return err
				}
				b, err := os.ReadFile(p)
				if err != nil {
					return err
				}
				for _, v := range []string{recorded, "200000", "2e+05"} {
					if bytes.Contains(b, []byte(v)) {
						t.Errorf("%s carries %s", p, v)
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// SPEC-HOSTED §4: limitsStop tests the limits at the route's newest
// recorded price, so a price recorded during the window that its
// spend_per_month_usd limit no longer covers stops the route at the next
// cell's start, naming the price's date, with no ledger line and no
// request; a price that the limit still covers lets the cell go on.
func TestCellPriceStop(t *testing.T) {
	for _, tc := range []struct {
		name, in, out, stop string
	}{
		{"raised", "0.30", "1.20", "route stop: the limits read at the cell's start, at the price retrieved 2026-10-08, no longer admit step 3 (1 rps) of the 3 the route runs: " +
			"spend_per_month_usd recorded 10, read 10: nominal demand 13.3093824 above it; the route keeps its steps (SPEC-HOSTED §4)"},
		{"covered", "0.20", "0.80", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEndpoint(t, func(int64) (int, string) { return http.StatusUnauthorized, "" })
			enoughOpenFiles(t)
			f := newFixture(t, rulingConfig(t, e.url()), "+0.005")
			r, err := newRoute(f.in)
			if err != nil {
				t.Fatal(err)
			}
			if got := r.verdict.Admitted(); len(got) != 3 {
				t.Fatalf("admitted %v", got)
			}
			shortLadder(r)
			l, err := admission.Open(f.ledger)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := recordPrice(l, r.rule.cfg, tc.in, tc.out, "https://example.test/pricing", "2026-10-08"); err != nil {
				t.Fatal(err)
			}
			before, err := ledgerEntries(f.ledger, "")
			if err != nil {
				t.Fatal(err)
			}
			rec, err := r.runCell(context.Background(), r.runs[0])
			if err != nil {
				t.Fatal(err)
			}
			if tc.stop == "" {
				if rec.Outcome != outcomeBlocked || len(rec.Reasons) != 1 || !strings.Contains(rec.Reasons[0], "probe request returned 401") || e.hits.Load() != 1 {
					t.Fatalf("outcome %s, reasons %q, %d requests; want blocked at the probe", rec.Outcome, rec.Reasons, e.hits.Load())
				}
				return
			}
			if rec.Outcome != outcomeStopped || len(rec.Reasons) != 1 || rec.Reasons[0] != tc.stop {
				t.Fatalf("outcome %s, reasons %q; want stopped, %q", rec.Outcome, rec.Reasons, tc.stop)
			}
			if n := e.hits.Load(); n != 0 || rec.Sent != (admission.Counts{}) {
				t.Fatalf("server saw %d requests; sent %+v", n, rec.Sent)
			}
			if after, err := ledgerEntries(f.ledger, ""); err != nil || len(after) != len(before) {
				t.Fatalf("ledger lines %d, were %d: %v", len(after), len(before), err)
			}
		})
	}
}

// A route started again after a price is recorded takes its verdict at
// the price the schedule froze, so its admitted steps and verdict are the
// first start's: under a raised price limitsStop stops its next cell
// with no ledger line and no request, and under a lowered price the cell
// goes on.
func TestRestartKeepsFrozenVerdict(t *testing.T) {
	for _, tc := range []struct {
		name, in, out, stop string
	}{
		{"raised", "0.30", "1.20", "route stop: the limits read at the cell's start, at the price retrieved 2026-10-08, no longer admit step 3 (1 rps) of the 3 the route runs: " +
			"spend_per_month_usd recorded 10, read 10: nominal demand 13.3093824 above it; the route keeps its steps (SPEC-HOSTED §4)"},
		{"lowered", "0.05", "0.20", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEndpoint(t, func(int64) (int, string) { return http.StatusUnauthorized, "" })
			enoughOpenFiles(t)
			f := newFixture(t, rulingConfig(t, e.url()), "+0.005")
			first, err := newRoute(f.in)
			if err != nil {
				t.Fatal(err)
			}
			l, err := admission.Open(f.ledger)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := recordPrice(l, first.rule.cfg, tc.in, tc.out, "https://example.test/pricing", "2026-10-08"); err != nil {
				t.Fatal(err)
			}
			in := f.in
			in.cfg = rulingConfig(t, e.url())
			r, err := newRoute(in)
			if err != nil {
				t.Fatal(err)
			}
			if got := r.verdict.Admitted(); len(got) != 3 || !reflect.DeepEqual(r.verdict, first.verdict) {
				t.Fatalf("admitted %v at US$%s a request; the first start admitted %v at US$%s", got, r.verdict.PerRequestUSD, first.verdict.Admitted(), first.verdict.PerRequestUSD)
			}
			shortLadder(r)
			before, err := ledgerEntries(f.ledger, "")
			if err != nil {
				t.Fatal(err)
			}
			rec, err := r.runCell(context.Background(), r.runs[0])
			if err != nil {
				t.Fatal(err)
			}
			if tc.stop == "" {
				if rec.Outcome != outcomeBlocked || len(rec.Reasons) != 1 || !strings.Contains(rec.Reasons[0], "probe request returned 401") || e.hits.Load() != 1 {
					t.Fatalf("outcome %s, reasons %q, %d requests; want blocked at the probe", rec.Outcome, rec.Reasons, e.hits.Load())
				}
				return
			}
			if rec.Outcome != outcomeStopped || len(rec.Reasons) != 1 || rec.Reasons[0] != tc.stop {
				t.Fatalf("outcome %s, reasons %q; want stopped, %q", rec.Outcome, rec.Reasons, tc.stop)
			}
			if n := e.hits.Load(); n != 0 || rec.Sent != (admission.Counts{}) {
				t.Fatalf("server saw %d requests; sent %+v", n, rec.Sent)
			}
			if after, err := ledgerEntries(f.ledger, ""); err != nil || len(after) != len(before) {
				t.Fatalf("ledger lines %d, were %d: %v", len(after), len(before), err)
			}
		})
	}
}

// Each cell's run record and manifest carry the SHA-256 of the schedule
// file it ran under. A route started again on a schedule written afresh
// from the same inputs after a price is recorded, at the ledger's new
// position, is refused while its output directory holds a cell written
// under the first schedule; started again on the first schedule it runs.
func TestRestartRefusesOtherSchedule(t *testing.T) {
	e := newEndpoint(t, func(int64) (int, string) { return http.StatusUnauthorized, "" })
	enoughOpenFiles(t)
	f := newFixture(t, rulingConfig(t, e.url()), "+0.005")
	first, err := newRoute(f.in)
	if err != nil {
		t.Fatal(err)
	}
	shortLadder(first)
	raw, err := os.ReadFile(f.in.schedulePath)
	if err != nil {
		t.Fatal(err)
	}
	want := sha(string(raw))
	l, err := admission.Open(f.ledger)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recordPrice(l, first.rule.cfg, "0.30", "1.20", "https://example.test/pricing", "2026-10-08"); err != nil {
		t.Fatal(err)
	}
	run := first.runs[1]
	rec, err := first.runCell(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Outcome != outcomeStopped || readCell(t, first, run).ScheduleSHA256 != want {
		t.Fatalf("outcome %s %q, schedule sha256 %q; want stopped under %s", rec.Outcome, rec.Reasons, readCell(t, first, run).ScheduleSHA256, want)
	}
	m, err := evidence.ReadManifest(filepath.Join(first.cellDir(run), evidence.ManifestFile))
	if err != nil || m.Release.ScheduleSHA256 != want {
		t.Fatalf("manifest: %v; want schedule sha256 %s", err, want)
	}

	in := f.in
	in.cfg = rulingConfig(t, e.url())
	fresh := frozenSchedule(t, f.ledger, in.cfg, in.hostCfgs)
	in.schedulePath = writeFile(t, t.TempDir(), "schedule.json", fresh)
	msg := "the cell ran under schedule sha256 \"" + want + "\"; --schedule has sha256 " + sha(string(fresh))
	if _, err := newRoute(in); err == nil || !strings.Contains(err.Error(), msg) {
		t.Fatalf("restart on a fresh schedule: %v; want %q", err, msg)
	}
	in.schedulePath = f.in.schedulePath
	r, err := newRoute(in)
	if err != nil {
		t.Fatalf("restart on the first schedule: %v", err)
	}
	if !reflect.DeepEqual(r.verdict, first.verdict) {
		t.Fatalf("admitted %v; the first start admitted %v", r.verdict.Admitted(), first.verdict.Admitted())
	}
}

// After the last cell the route records its operator file and records
// once more after the refresh command, keeps their copies under the
// frame store, and never writes the record again; the route page names
// the record. A refresh fault writes no record and returns an error.
// pages refuses the record once a retained copy is edited, and once a
// cell has run after its refresh.
func TestWindowCloseRecord(t *testing.T) {
	enoughOpenFiles(t)
	e := newEndpoint(t, func(int64) (int, string) { return http.StatusUnauthorized, "" })
	cfgPath := routeConfigFile(t, "groq", e.url(), "10")
	cfg, err := config.LoadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(cfg.Target.APIKeyEnv, testKey)
	f := newFixture(t, cfg, "+0.005")
	page := writeFile(t, t.TempDir(), "page", []byte("limits v1"))
	dir := refreshFixture(t, f, page)
	r, err := newRoute(f.in)
	if err != nil {
		t.Fatal(err)
	}
	run := r.runs[0]
	if rec := runCellOK(t, r, run); rec.Outcome != outcomeBlocked {
		t.Fatalf("outcome %s %q", rec.Outcome, rec.Reasons)
	}
	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	closed := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	r.now = func() time.Time { return closed }
	path := filepath.Join(r.dir, evidence.WindowCloseFile)

	for _, tc := range []struct {
		name    string
		refresh []string
		want    string
	}{
		{"command fails", []string{"false"}, "groq: window close: the records were not refreshed (SPEC-HOSTED §4): refresh command: exit status 1"},
		{"no limits printed", []string{"touch", filepath.Join(dir, "r.txt"), filepath.Join(dir, "limits.txt")}, "refreshed limits: the output is not one JSON object with limits"},
	} {
		refresh := r.refresh
		r.refresh = tc.refresh
		if err := r.windowClose(context.Background(), st); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: %v", tc.name, err)
		}
		r.refresh = refresh
		if fileExists(path) || fileExists(filepath.Join(r.framesRoot, "window-close")) {
			t.Fatalf("%s: a record was written", tc.name)
		}
	}

	writeFile(t, filepath.Dir(page), "page", []byte("limits v2"))
	begun := time.Now().Add(-2 * time.Second)
	printed := stdoutOf(t, func() {
		if err := r.windowClose(context.Background(), st); err != nil {
			t.Error(err)
		}
	})
	if printed != "groq: window close recorded "+closed.Format(time.RFC3339)+"; no counted cell to compare\n" {
		t.Fatalf("printed %q", printed)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := evidence.ReadWindowClose(path)
	if err != nil {
		t.Fatal(err)
	}
	var lim evidence.OperatorRecord
	for _, rec := range w.Records {
		if rec.Kind == evidence.RecordLimits {
			lim = rec
		}
	}
	at, err := time.Parse(time.RFC3339, lim.Retrieved)
	if w.Route != "groq" || !w.RecordedUTC.Equal(closed) || w.AccountTier != "Developer" || lim.SHA256 != sha("limits v2") || err != nil ||
		at.Before(begun) || w.Refresh == nil || len(w.Refresh.Limits) != 2 || w.Refresh.Limits[1].Value != 250000 {
		t.Fatalf("record %s", b)
	}
	held := filepath.Join(r.framesRoot, w.Copies)
	if held != evidence.WindowCloseDir(r.framesRoot, "groq", closed) {
		t.Fatalf("copies %s", w.Copies)
	}
	if c, err := os.ReadFile(filepath.Join(held, evidence.RecordsCopyDir, "limits.txt")); err != nil || string(c) != "limits v2" {
		t.Fatalf("limits copy %q: %v", c, err)
	}
	if fi, err := os.Stat(filepath.Join(held, evidence.OperatorCopyFile)); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("operator copy %v: %v", fi, err)
	}
	if fileExists(filepath.Join(held, evidence.RefreshFile)) {
		t.Fatal("a route with public limits keeps a refresh record as run")
	}

	writeFile(t, filepath.Dir(page), "page", []byte("limits v3"))
	r.now = func() time.Time { return closed.Add(time.Hour) }
	if err := r.windowClose(context.Background(), st); err != nil {
		t.Fatal(err)
	}
	if again, err := os.ReadFile(path); err != nil || !bytes.Equal(again, b) {
		t.Fatalf("the record was written again: %v", err)
	}

	pages := t.TempDir()
	args := []string{"--out", r.out, "--frames", r.framesRoot, "--config", cfgPath, "--schedule", f.in.schedulePath, "--pages", pages}
	if code, err := pagesCmd(args); code != exitOK || err != nil {
		t.Fatalf("pages: exit %d: %v", code, err)
	}
	text, err := os.ReadFile(filepath.Join(pages, routesDir, "groq.txt"))
	if err != nil || !strings.Contains(string(text), "-- Window close, recorded "+closed.Format(time.DateOnly)+": no counted cell to compare --\n") {
		t.Fatalf("route page: %v\n%s", err, text)
	}

	limitsCopy := filepath.Join(held, evidence.RecordsCopyDir, "limits.txt")
	if err := os.WriteFile(limitsCopy, []byte("limits v9"), 0o600); err != nil {
		t.Fatal(err)
	}
	want := "pages: route groq: evidence: window-close record refused: limits record copy: limits.txt has SHA-256 " + sha("limits v9")
	if code, err := pagesCmd(args); code != exitError || err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("edited copy: exit %d: %v; want %q", code, err, want)
	}
	if err := os.WriteFile(limitsCopy, []byte("limits v2"), 0o600); err != nil {
		t.Fatal(err)
	}
	later := runCellOK(t, r, r.runs[1])
	ended := ", not after the route's last cell ended at " + later.EndUTC.UTC().Format(time.RFC3339Nano)
	want = "pages: route groq: evidence: window-close record refused: recorded " + closed.Format(time.RFC3339Nano) + ended +
		"; refresh: started " + w.Refresh.StartUTC.UTC().Format(time.RFC3339Nano) + ended
	if code, err := pagesCmd(args); code != exitError || err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("a cell after the record: exit %d: %v; want %q", code, err, want)
	}
}

// The window close compares its record with the last counted cell by
// start and prints what it finds.
func TestWindowCloseCompares(t *testing.T) {
	enoughOpenFiles(t)
	e := newEndpoint(t, func(int64) (int, string) { return http.StatusUnauthorized, "" })
	f := newFixture(t, releaseConfig(t, e.url()), "+0.005")
	page := writeFile(t, t.TempDir(), "page", []byte("limits v1"))
	refreshFixture(t, f, page)
	r, err := newRoute(f.in)
	if err != nil {
		t.Fatal(err)
	}
	first, second := r.runs[0], r.runs[1]
	runCellOK(t, r, second)
	writeFile(t, filepath.Dir(page), "page", []byte("limits v0"))
	runCellOK(t, r, first)
	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range []schedule.Run{first, second} {
		st.records[runKey(run)].Outcome = outcomeCounted
	}
	writeFile(t, filepath.Dir(page), "page", []byte("limits v1"))
	r.now = func() time.Time { return time.Date(2026, 10, 27, 1, 0, 0, 0, time.UTC) }
	same := stdoutOf(t, func() {
		if err := r.windowClose(context.Background(), st); err != nil {
			t.Error(err)
		}
	})
	if want := "groq: window close recorded 2026-10-27T01:00:00Z: limits and documentation as at " + cellLabel(second) + "\n"; same != want {
		t.Fatalf("printed %q, want %q", same, want)
	}

	if err := os.Remove(filepath.Join(r.dir, evidence.WindowCloseFile)); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Dir(page), "page", []byte("limits v2"))
	r.now = func() time.Time { return time.Date(2026, 10, 27, 2, 0, 0, 0, time.UTC) }
	changed := stdoutOf(t, func() {
		if err := r.windowClose(context.Background(), st); err != nil {
			t.Error(err)
		}
	})
	if want := "groq: window close recorded 2026-10-27T02:00:00Z: limits or documentation changed since " + cellLabel(second) +
		": removed [limits https://example.test/limits sha256 " + sha("limits v1") + "]; added [limits https://example.test/limits sha256 " + sha("limits v2") + "]\n"; changed != want {
		t.Fatalf("printed %q, want %q", changed, want)
	}
}
