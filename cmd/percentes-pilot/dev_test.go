package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/pilot"
	"github.com/percentes/percentes/internal/score"
)

// scored returns six records per label: correct answers first, then
// wrong ones, with class c for the correct ones.
func scored(correctPerLabel map[string]int, c score.Class) []evidence.Record {
	var recs []evidence.Record
	for _, l := range pilot.Labels {
		for i := 0; i < pilot.DevelopmentPerLabel; i++ {
			cl := score.WrongLabel
			if i < correctPerLabel[l] {
				cl = c
			}
			recs = append(recs, evidence.Record{Score: &evidence.Score{Expected: l, Class: cl}})
		}
	}
	return recs
}

// The gate passes at 22 of 24 correct, complete answers with at least
// five of six in every label; a late correct answer is correct and
// complete but not by the cutoff.
func TestDevGate(t *testing.T) {
	for _, c := range []struct {
		name     string
		per      map[string]int
		class    score.Class
		pass     bool
		byCutoff int
	}{
		{"22, five or six each", map[string]int{"billing": 6, "technical": 6, "account": 5, "escalation": 5}, score.Success, true, 22},
		{"24", map[string]int{"billing": 6, "technical": 6, "account": 6, "escalation": 6}, score.Success, true, 24},
		{"22, one label at four", map[string]int{"billing": 6, "technical": 6, "account": 6, "escalation": 4}, score.Success, false, 22},
		{"21", map[string]int{"billing": 6, "technical": 5, "account": 5, "escalation": 5}, score.Success, false, 21},
		{"22 late", map[string]int{"billing": 6, "technical": 6, "account": 5, "escalation": 5}, score.LateCorrect, true, 0},
	} {
		e := &devEndpoint{PerLabel: map[string]int{}}
		devGate(e, scored(c.per, c.class))
		total := 0
		for _, n := range c.per {
			total += n
		}
		if e.Pass != c.pass || e.Correct != total || e.ByCutoff != c.byCutoff {
			t.Errorf("%s: pass %t correct %d by cutoff %d", c.name, e.Pass, e.Correct, e.ByCutoff)
		}
		for l, n := range c.per {
			if e.PerLabel[l] != n {
				t.Errorf("%s: %s %d, want %d", c.name, l, e.PerLabel[l], n)
			}
		}
	}
}

// dev-1 runs without a reason; dev-2 needs one and may change only the
// system text and the generation settings; there is no dev-3.
func TestDevAttempts(t *testing.T) {
	raw, err := os.ReadFile("../../configs/pilot.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	parse := func(b []byte) *config.Config {
		t.Helper()
		c, err := config.Parse(b)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	out := t.TempDir()
	cfg := parse(raw)
	if _, err := nextDevAttempt(out, cfg, "early"); err == nil || !strings.Contains(err.Error(), "dev-1 does not exist") {
		t.Errorf("reason on the first attempt: %v", err)
	}
	if n, err := nextDevAttempt(out, cfg, ""); n != 1 || err != nil {
		t.Fatalf("first attempt: %d %v", n, err)
	}
	if err := os.MkdirAll(filepath.Join(out, "dev-1"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "dev-1", devConfigFile), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := nextDevAttempt(out, cfg, ""); err == nil || !strings.Contains(err.Error(), "--revision") {
		t.Errorf("second attempt without a reason: %v", err)
	}
	if _, err := nextDevAttempt(out, cfg, "same config"); err == nil || !strings.Contains(err.Error(), "identical") {
		t.Errorf("unchanged config: %v", err)
	}
	system := parse(bytes.Replace(raw, []byte("System text: frozen at L1."), []byte("Route the ticket."), 1))
	if n, err := nextDevAttempt(out, system, "prompt revised"); n != 2 || err != nil {
		t.Errorf("system text revision: %d %v", n, err)
	}
	effort := parse(bytes.ReplaceAll(raw, []byte("reasoning_effort: low"), []byte("reasoning_effort: medium")))
	if n, err := nextDevAttempt(out, effort, "effort revised"); n != 2 || err != nil {
		t.Errorf("generation settings revision: %d %v", n, err)
	}
	for name, b := range map[string][]byte{
		"model":    bytes.Replace(raw, []byte("model_name: openai/gpt-oss-120b"), []byte("model_name: openai/gpt-oss-20b"), 1),
		"run name": bytes.Replace(raw, []byte("percentes-pilot-reference"), []byte("percentes-pilot-other"), 1),
	} {
		if _, err := nextDevAttempt(out, parse(b), "revised"); err == nil || !strings.Contains(err.Error(), "only the system text") {
			t.Errorf("%s changed: %v", name, err)
		}
	}
	if err := os.MkdirAll(filepath.Join(out, "dev-2"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := nextDevAttempt(out, system, "again"); err == nil || !strings.Contains(err.Error(), "no further") {
		t.Errorf("third attempt: %v", err)
	}
}

// A development run sends the 24 development cases at the session rate
// through the session's request path, scores them and settles its
// admission as development traffic.
func TestDevRun(t *testing.T) {
	cfg := testConfig(t, [2]string{routeMock(t), routeMock(t)}, 20, 60, 1200)
	ledgerPath := testLedger(t, cfg)
	ledger, err := admission.Open(ledgerPath)
	if err != nil {
		t.Fatal(err)
	}
	fx, err := fixture()
	if err != nil {
		t.Fatal(err)
	}
	scorer, err := newScorer()
	if err != nil {
		t.Fatal(err)
	}
	tg := &cfg.Pilot.Targets[0]
	dir := filepath.Join(t.TempDir(), tg.Name)
	e, err := devRun(context.Background(), ledger, cfg, tg, fx.Development, scorer, dir)
	if err != nil {
		t.Fatal(err)
	}
	if e.Sent != 24 || e.Correct != 24 || !e.Pass {
		t.Errorf("result %+v", e)
	}
	if u := ledgerUsage(t, ledgerPath, tg.EndpointURL); u.Development != 24 || u.Reserved != 0 || u.Warmup+u.Measured != 0 {
		t.Errorf("ledger %+v", u)
	}
	fh, err := os.Open(filepath.Join(dir, evidence.AttemptsFile))
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()
	recs, err := evidence.ReadRecords(fh)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 24 {
		t.Fatalf("%d records", len(recs))
	}
	for i, r := range recs {
		if r.Score == nil || r.Score.Expected != fx.Development[i].Label || r.Answer == nil {
			t.Errorf("record %d: %+v", i, r)
		}
		if i > 0 && r.IntendedNs-recs[i-1].IntendedNs != int64(interval(cfg)) {
			t.Errorf("record %d intended %d after %d", i, r.IntendedNs, recs[i-1].IntendedNs)
		}
	}
}
