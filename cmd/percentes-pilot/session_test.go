package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/pilot"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/score"
)

func readSession(t *testing.T, dir string) sessionRecord {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, sessionFile))
	if err != nil {
		t.Fatal(err)
	}
	var rec sessionRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatal(err)
	}
	return rec
}

// A session runs each endpoint's window in the frozen order, admitted and
// probed first, the schedule starting one session interval after the
// probe request and the second window as the first drains. Each window
// keeps its probe, scored attempts and manifest, the manifest carries the
// loopback canary's summary, and the ledger holds what was sent.
func TestSessionRunsBothWindows(t *testing.T) {
	cfg := testConfig(t, [2]string{routeMock(t), routeMock(t)}, 5, 0.4, 0.8)
	f := newSessionFixture(t, cfg, "weekday-1")
	sess, _, err := pick(f.in.schedule, "weekday-1")
	if err != nil {
		t.Fatal(err)
	}
	code, err := runSession(context.Background(), f.in)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(f.in.out, "weekday-1")
	rec := readSession(t, dir)
	if len(rec.Windows) != 2 || rec.Windows[0].Endpoint != sess.Order[0] || rec.Windows[1].Endpoint != sess.Order[1] {
		t.Fatalf("windows %+v, order %v", rec.Windows, sess.Order)
	}
	if rec.GapS == nil || rec.GapExceeded || *rec.GapS < 0 || *rec.GapS > 60 {
		t.Errorf("gap %v exceeded %t", rec.GapS, rec.GapExceeded)
	}
	allValid := true
	for _, w := range rec.Windows {
		if w.Blocked != "" || w.EpochUTC == nil || w.DrainedUTC == nil {
			t.Fatalf("window %+v did not run", w)
		}
		for _, r := range w.InvalidReasons {
			if !strings.HasPrefix(r, "client-validity gate failed") {
				t.Errorf("%s: invalid reason %q", w.Endpoint, r)
			}
		}
		allValid = allValid && w.ClientValid
		if w.Sent != (admission.Counts{Probe: 1, Warmup: 2, Measured: 4}) {
			t.Errorf("%s: sent %+v", w.Endpoint, w.Sent)
		}
		wdir := filepath.Join(dir, w.Dir)
		for _, name := range []string{probe.File, evidence.AttemptsFile, evidence.ManifestFile, evidence.ClockFile, evidence.StatusStartFile, evidence.StatusEndFile} {
			if _, err := os.Stat(filepath.Join(wdir, name)); err != nil {
				t.Errorf("%s: %v", w.Endpoint, err)
			}
		}
		var pr probe.Record
		if raw, err := os.ReadFile(filepath.Join(wdir, probe.File)); err != nil {
			t.Fatal(err)
		} else if err := json.Unmarshal(raw, &pr); err != nil {
			t.Fatal(err)
		}
		if pr.Request.Status != http.StatusOK || pr.WrongModel != nil || w.EpochUTC.Sub(pr.Request.DispatchUTC) < interval(cfg) {
			t.Errorf("%s: probe status %d sent at %s, window epoch %s", w.Endpoint, pr.Request.Status, pr.Request.DispatchUTC, w.EpochUTC)
		}
		m, err := evidence.ReadManifest(filepath.Join(wdir, evidence.ManifestFile))
		if err != nil {
			t.Fatal(err)
		}
		if m.Replay == nil || len(m.Replay.Windows) != 1 || m.Replay.Windows[0].Canary == nil || m.Replay.Windows[0].Canary.Streams == 0 || m.Replay.CanaryError != "" {
			t.Errorf("%s: canary not recorded: %+v", w.Endpoint, m.Replay)
		}
		if m.Artifact(probe.File) == nil || m.Artifact(evidence.AttemptsFile) == nil || m.Frames == nil {
			t.Errorf("%s: manifest artifacts %+v frames %+v", w.Endpoint, m.Artifacts, m.Frames)
		}
		fh, err := os.Open(filepath.Join(wdir, evidence.AttemptsFile))
		if err != nil {
			t.Fatal(err)
		}
		recs, err := evidence.ReadRecords(fh)
		fh.Close()
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) != 6 {
			t.Fatalf("%s: %d records", w.Endpoint, len(recs))
		}
		for i, r := range recs {
			switch {
			case i < 2 && r.Score != nil:
				t.Errorf("%s: warm-up record %d scored", w.Endpoint, i)
			case i >= 2 && (r.Score == nil || r.Score.Class != score.Success):
				t.Errorf("%s: measured record %d scored %+v", w.Endpoint, i, r.Score)
			}
		}
		u := ledgerUsage(t, f.ledger, cfg.Pilot.Targets[indexOf(t, cfg.Pilot.Targets, w.Endpoint)].EndpointURL)
		if u.Probe != 1 || u.Warmup != 2 || u.Measured != 4 || u.Reserved != 0 {
			t.Errorf("%s: ledger %+v", w.Endpoint, u)
		}
	}
	if want := exitAdverse; allValid {
		want = exitOK
		if code != want {
			t.Errorf("exit %d, want %d", code, want)
		}
	} else if code != want {
		t.Errorf("exit %d, want %d", code, want)
	}
	if _, err := runSession(context.Background(), f.in); err == nil || !strings.Contains(err.Error(), "never re-run") {
		t.Errorf("second run of the session: %v", err)
	}
}

func indexOf(t *testing.T, ts []config.Target, name string) int {
	t.Helper()
	for i := range ts {
		if ts[i].Name == name {
			return i
		}
	}
	t.Fatalf("no target %s", name)
	return -1
}

// A probe status that blocks the start stops that window only: the other
// window runs, and the blocked one's admission settles with the probe
// request alone.
func TestSessionProbeBlocksOneWindow(t *testing.T) {
	cfg := testConfig(t, [2]string{routeMock(t), statusServer(t, http.StatusUnauthorized)}, 5, 0.2, 0.4)
	f := newSessionFixture(t, cfg, "weekday-1")
	code, err := runSession(context.Background(), f.in)
	if err != nil {
		t.Fatal(err)
	}
	if code != exitAdverse {
		t.Errorf("exit %d", code)
	}
	rec := readSession(t, filepath.Join(f.in.out, "weekday-1"))
	if len(rec.Windows) != 2 {
		t.Fatalf("%d windows", len(rec.Windows))
	}
	for _, w := range rec.Windows {
		switch w.Endpoint {
		case cfg.Pilot.Targets[1].Name:
			if !strings.Contains(w.Blocked, "401") || w.ProbeStatus != 401 || w.EpochUTC != nil || w.Sent != (admission.Counts{Probe: 1}) {
				t.Errorf("blocked window %+v", w)
			}
			if u := ledgerUsage(t, f.ledger, cfg.Pilot.Targets[1].EndpointURL); u.Probe != 1 || u.Warmup+u.Measured+u.Reserved != 0 {
				t.Errorf("blocked window's ledger %+v", u)
			}
		default:
			if w.Blocked != "" || w.EpochUTC == nil {
				t.Errorf("the other window did not run: %+v", w)
			}
		}
	}
	if rec.GapS != nil {
		t.Errorf("gap %v recorded with one window", *rec.GapS)
	}
}

// A session refuses to start more than 15 minutes after its slot, and
// --local refuses an endpoint off this host.
func TestSessionStartRules(t *testing.T) {
	cfg := testConfig(t, [2]string{routeMock(t), routeMock(t)}, 5, 0.2, 0.4)
	f := newSessionFixture(t, cfg, "weekday-1")
	sess, _, _ := pick(f.in.schedule, "weekday-1")
	f.in.local = false
	f.in.now = func() time.Time { return sess.Start.Add(16 * time.Minute) }
	if _, err := runSession(context.Background(), f.in); err == nil || !strings.Contains(err.Error(), "start tolerance") {
		t.Errorf("late start: %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.in.out, "weekday-1")); err == nil {
		t.Error("a refused session left its directory")
	}
	f.in.local = true
	f.in.cfg.Pilot.Targets[1].EndpointURL = "https://inference.example.test/v1/chat/completions"
	if _, err := runSession(context.Background(), f.in); err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Errorf("--local with a remote endpoint: %v", err)
	}
}

// A reserve replaces only a session of its day type that never ran or
// kept a window that is client-invalid or did not start; it takes that
// session's order, and a pilot has one replacement.
func TestReplacementRules(t *testing.T) {
	cfg := testConfig(t, [2]string{"http://127.0.0.1:1/v1/chat/completions", "http://127.0.0.1:2/v1/chat/completions"}, 5, 0.2, 0.4)
	_, s := testSchedule(t, cfg)
	out := t.TempDir()
	reserve, _, err := pick(s, "reserve-"+pilot.Weekday)
	if err != nil {
		t.Fatal(err)
	}
	writeRec := func(label string, windows ...windowRecord) {
		t.Helper()
		if err := writeJSON(filepath.Join(out, label, sessionFile), sessionRecord{Label: label, Windows: windows}); err != nil {
			t.Fatal(err)
		}
	}
	valid := windowRecord{ClientValid: true}
	writeRec("weekday-1", valid, valid)
	writeRec("weekday-2", valid, windowRecord{ClientValid: false, InvalidReasons: []string{"tripwire"}})
	writeRec("weekday-3", valid, windowRecord{Blocked: "probe request returned 401"})

	for _, c := range []struct {
		replaces string
		ok       bool
		msg      string
	}{
		{"", false, "--replaces"},
		{"weekday-1", false, "never re-run"},
		{"weekend-1", false, "replaces only"},
		{"reserve-weekend", false, "replaces only"},
		{"weekday-2", true, ""},
		{"weekday-3", true, ""},
	} {
		old, err := replacement(s, reserve, c.replaces, out)
		switch {
		case c.ok && err != nil:
			t.Errorf("%q: %v", c.replaces, err)
		case c.ok && old.Label != c.replaces:
			t.Errorf("%q: replaced %s", c.replaces, old.Label)
		case !c.ok && (err == nil || !strings.Contains(err.Error(), c.msg)):
			t.Errorf("%q: err %v, want %q", c.replaces, err, c.msg)
		}
	}
	if err := os.MkdirAll(filepath.Join(out, "reserve-"+pilot.Weekend), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := replacement(s, reserve, "weekday-2", out); err == nil || !strings.Contains(err.Error(), "at most one replacement") {
		t.Errorf("second replacement: %v", err)
	}
}

// A replacement session runs in the replaced session's order, admits as
// a replacement, and records what it replaces.
func TestReplacementSession(t *testing.T) {
	cfg := testConfig(t, [2]string{routeMock(t), routeMock(t)}, 5, 0.2, 0.4)
	f := newSessionFixture(t, cfg, "reserve-"+pilot.Weekend)
	f.in.replaces = "weekend-2"
	old, _, _ := pick(f.in.schedule, "weekend-2")
	if _, err := runSession(context.Background(), f.in); err != nil {
		t.Fatal(err)
	}
	rec := readSession(t, filepath.Join(f.in.out, "reserve-"+pilot.Weekend))
	if rec.Replaces != "weekend-2" || len(rec.Windows) != 2 || rec.Windows[0].Endpoint != old.Order[0] || rec.Windows[0].Purpose != admission.PurposeReplacement {
		t.Fatalf("replacement record %+v, replaced order %v", rec, old.Order)
	}
	u := ledgerUsage(t, f.ledger, cfg.Pilot.Targets[0].EndpointURL)
	if u.Replacement != 3 || u.Warmup+u.Measured != 0 {
		t.Errorf("ledger %+v", u)
	}
}

// Warm-up requests carry the permutation's first measured case and the
// measured requests the permutation's cases in order, with their labels.
func TestSessionCases(t *testing.T) {
	cfg := testConfig(t, [2]string{"http://127.0.0.1:1/x", "http://127.0.0.1:2/x"}, 0.1, 60, 1200)
	fx, err := fixture()
	if err != nil {
		t.Fatal(err)
	}
	_, s := testSchedule(t, cfg)
	perm := s.Sessions[0].Permutation
	cases, expected, err := sessionCases(cfg, fx, perm)
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) != 126 {
		t.Fatalf("%d cases", len(cases))
	}
	for i := 0; i < 6; i++ {
		if cases[i] != fx.Measured[perm[0]].Text || expected[i] != "" {
			t.Errorf("warm-up %d: %q %q", i, cases[i], expected[i])
		}
	}
	for j := 0; j < 120; j++ {
		c := fx.Measured[perm[j]]
		if cases[6+j] != c.Text || expected[6+j] != c.Label {
			t.Errorf("measured %d: case %q label %q, want %q %q", j, cases[6+j], expected[6+j], c.Text, c.Label)
		}
	}
	cfg.Run.Phases.BaselineS = 1210
	if _, _, err := sessionCases(cfg, fx, perm); err == nil {
		t.Error("121 measured requests for 120 cases accepted")
	}
}

// The schedule file is checked against its derivation and the config's
// targets.
func TestReadSchedule(t *testing.T) {
	cfg := testConfig(t, [2]string{"http://127.0.0.1:1/x", "http://127.0.0.1:2/x"}, 0.1, 60, 1200)
	path, s := testSchedule(t, cfg)
	got, _, err := readSchedule(path, cfg)
	if err != nil || got.Sessions[0].Label != s.Sessions[0].Label || got.Sessions[0].Date != s.Sessions[0].Date {
		t.Fatalf("schedule %+v, err %v", got, err)
	}
	full, _ := json.Marshal(s)
	if err := os.WriteFile(path, full, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readSchedule(path, cfg); err != nil {
		t.Errorf("full schedule: %v", err)
	}
	s.Sessions[0].Date = "2026-10-08"
	edited, _ := json.Marshal(s)
	if err := os.WriteFile(path, edited, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readSchedule(path, cfg); err == nil || !strings.Contains(err.Error(), "differ") {
		t.Errorf("edited schedule: %v", err)
	}
	cfg.Pilot.Targets[0].Name, cfg.Pilot.Targets[1].Name = cfg.Pilot.Targets[1].Name, cfg.Pilot.Targets[0].Name
	if _, _, err := readSchedule(path, cfg); err == nil || !strings.Contains(err.Error(), "endpoints") {
		t.Errorf("swapped targets: %v", err)
	}
}

// Every target needs an operator file holding the pilot record kinds.
func TestLoadOperators(t *testing.T) {
	cfg := testConfig(t, [2]string{"http://127.0.0.1:1/x", "http://127.0.0.1:2/x"}, 0.1, 60, 1200)
	targets, _ := selectTargets(cfg, nil)
	a, b := targets[0].Name, targets[1].Name
	if _, err := loadOperators([]string{a + "=" + writeOperator(t, true), b + "=" + writeOperator(t, true)}, targets, testBuild); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		flags []string
		msg   string
	}{
		{[]string{a + "=" + writeOperator(t, true)}, "no operator file for target " + b},
		{[]string{a + "=" + writeOperator(t, true), b + "=" + writeOperator(t, false)}, "no cases record"},
		{[]string{a + "=" + writeOperator(t, true), b + "=" + writeOperator(t, true), "other=" + writeOperator(t, true)}, "not a pilot target"},
		{[]string{a + writeOperator(t, true)}, "NAME=FILE"},
	} {
		if _, err := loadOperators(c.flags, targets, testBuild); err == nil || !strings.Contains(err.Error(), c.msg) {
			t.Errorf("%v: %v, want %q", c.flags, err, c.msg)
		}
	}
	if _, err := loadOperators([]string{a + "=" + writeOperator(t, true), b + "=" + writeOperator(t, true)}, targets, nil); err == nil {
		t.Error("a build with no revision accepted")
	}
}
