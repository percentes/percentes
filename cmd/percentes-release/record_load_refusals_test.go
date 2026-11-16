package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/percentes/percentes/internal/probe"
)

// The loaders of ceiling-check records refuse two records of one ledger
// line under --out, a record given to schedule from another ledger, one
// record given twice to run, a record of another route that spends a
// byte cap from another ledger or that Verify refuses, and a run byte
// cap above a cap another route's check spent.
func TestCheckRecordLoadRefusals(t *testing.T) {
	var prompt atomic.Int32
	endpoint, _ := faultMock(t, nil, &prompt)
	groq, snapshots := quickSolo(t, "groq", endpoint)
	gemini, _ := quickSolo(t, "gemini", endpoint)
	ledger := releaseLedger(t, groq, gemini)
	c := newChecker(t, ledger, snapshots)
	ctx := context.Background()
	if code, text, err := c.check(ctx, groq, 2000); code != exitOK || err != nil {
		t.Fatalf("groq: %d, %v\n%s", code, err, text)
	}
	prompt.Store(1300)
	if code, text, err := c.check(ctx, gemini, 2000); code != exitAdverse || err != nil {
		t.Fatalf("gemini: %d, %v\n%s", code, err, text)
	}
	own, spender := c.records("groq")[0], c.records("gemini")[0]
	open := ledgerOpenSHA256(t, ledger)
	cfgs, err := loadReleases([]string{groq})
	if err != nil {
		t.Fatal(err)
	}
	seq := readRecord(t, own).LedgerSeq

	b, err := os.ReadFile(own)
	if err != nil {
		t.Fatal(err)
	}
	dup := filepath.Join(c.out, "ceiling-copy", "groq", probe.File)
	if err := os.MkdirAll(filepath.Dir(dup), 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Dir(dup), probe.File, b)
	if _, err := ledgerChecks(c.out, open); err == nil || !strings.Contains(err.Error(), fmt.Sprintf("both record the check admitted at ledger line %d", seq)) {
		t.Fatalf("two records of one line: %v", err)
	}
	if err := os.RemoveAll(filepath.Dir(filepath.Dir(dup))); err != nil {
		t.Fatal(err)
	}
	if err := scheduleChecks([]string{own}, nil, "0000"); err == nil || !strings.Contains(err.Error(), "admitted in the ledger of open-line sha256 \""+open+"\", not 0000") {
		t.Fatalf("schedule, another ledger: %v", err)
	}

	edited := func(edit func(*probe.ReleaseRecord)) string {
		b, err := os.ReadFile(spender)
		if err != nil {
			t.Fatal(err)
		}
		p := writeFile(t, t.TempDir(), probe.File, b)
		rewriteRecord(t, p, edit)
		return p
	}
	for _, x := range []struct {
		name    string
		files   []string
		byteCap int
		want    string
	}{
		{"one record twice", []string{own, own}, 2000, fmt.Sprintf("a record of ledger line %d given twice", seq)},
		{"another route's spent cap, another ledger", []string{own, edited(func(r *probe.ReleaseRecord) { r.LedgerSHA256 = "0000" })}, 2000,
			"route gemini at 512: ledger open line sha256 \"0000\"; the schedule froze " + open},
		{"another route's spent cap, edited", []string{own, edited(func(r *probe.ReleaseRecord) { r.Requests[0].Pass = false })}, 2000,
			"route gemini at 512: request 0 records pass false"},
		{"byte cap above a spent cap", []string{own, spender}, 2400, "run: --byte-cap 2400 is above byte cap 2000, which route gemini's check at ledger line"},
	} {
		if _, _, _, err := ceilingChoice(cfgs[0], nil, x.files, x.byteCap, open, nil); err == nil || !strings.Contains(err.Error(), x.want) {
			t.Errorf("%s: %v; want %q", x.name, err, x.want)
		}
	}
}
