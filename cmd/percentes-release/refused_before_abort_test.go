package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/probe"
)

// A request the provider closes with no response, then an operator abort
// in the spacing wait, refuses the check. Where its record is not
// written, the check settles at the three sent and --recover --record
// writes the printed record, which is refused. The next check waits the
// quiet interval and sends nothing; the repeat after it, closed again,
// excludes the host group; a third check sends nothing; and schedule
// and pages read both records as the exclusion.
func TestProviderHangUpBeforeAbortRefusesCheck(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	var repeat atomic.Bool
	endpoint, hits := faultMock(t, func(n int32, w http.ResponseWriter) bool {
		switch {
		case repeat.Load():
			drop(w)
			return true
		case n == 3:
			drop(w)
			time.AfterFunc(10*time.Millisecond, func() { cancel(loadgen.ErrOperatorAbort) })
			return true
		}
		return false
	}, new(atomic.Int32))
	groq, snapshots := quickSolo(t, "groq", endpoint)
	ledger := releaseLedger(t, groq)
	c := newChecker(t, ledger, snapshots)
	if err := os.Chmod(c.out, 0o500); err != nil {
		t.Fatal(err)
	}
	var code int
	var text string
	var err error
	printed := stderrOf(t, func() { code, text, err = c.check(ctx, groq, 2000) })
	if err := os.Chmod(c.out, 0o700); err != nil {
		t.Fatal(err)
	}
	if code != exitError || err == nil || !strings.Contains(err.Error(), "is settled at 3 requests sent") || !strings.Contains(text, "groq: REFUSED: a request got no response") {
		t.Fatalf("a hang-up, then an abort: %d, %v\n%s", code, err, text)
	}
	record := writeFile(t, t.TempDir(), "record.json", []byte(printed))
	if code, text, err := c.check(context.Background(), groq, 2000, "--recover", "--record", record); code != exitOK || err != nil || !strings.Contains(text, ": record written ") {
		t.Fatalf("--recover --record: %d, %v\n%s", code, err, text)
	}
	first := c.records("groq")
	rec := readRecord(t, first[0])
	if want := "the check's context ended after 3 of 10 requests: operator abort"; len(first) != 1 || rec.Interrupted != want || rec.Sent() != 3 || rec.Outcome() != probe.CheckRefused || rec.RefusedBy() != "no response" {
		t.Fatalf("records %v: interrupted %q, %d sent, outcome %s, refused by %q", first, rec.Interrupted, rec.Sent(), rec.Outcome(), rec.RefusedBy())
	}
	sent := hits.Load()
	if code, _, err := c.check(context.Background(), groq, 2000); code != exitError || err == nil || !strings.Contains(err.Error(), "may be repeated from") || hits.Load() != sent {
		t.Fatalf("the next check: %d, %v, %d requests", code, err, hits.Load()-sent)
	}
	rewriteRecord(t, first[0], func(r *probe.ReleaseRecord) { r.EndUTC = r.EndUTC.Add(-time.Hour) })
	repeat.Store(true)
	if code, text, err = c.check(context.Background(), groq, 2000); code != exitAdverse || err != nil || !strings.Contains(text, "the host group is excluded, ceiling check inconclusive, no response at 512") {
		t.Fatalf("the repeat: %d, %v\n%s", code, err, text)
	}
	sent = hits.Load()
	if code, _, err := c.check(context.Background(), groq, 2000); code != exitError || err == nil || !strings.Contains(err.Error(), "decides (refused); it is not repeated") || hits.Load() != sent {
		t.Fatalf("a third check: %d, %v, %d requests", code, err, hits.Load()-sent)
	}
	both := c.records("groq")
	args := []string{"--master-seed", "percentes-release-test", "--first", "2026-10-17", "--last", "2026-10-26", "--config", groq, "--ledger", ledger,
		"--release-key", testReleaseKeyPath, "--ceiling-probe", both[0], "--ceiling-probe", both[1], "--excluded", "groq", "--schedule", filepath.Join(t.TempDir(), "schedule.json")}
	text = stdoutOf(t, func() { code, err = scheduleCmd(args) })
	if code != exitOK || err != nil || !strings.Contains(text, "groq: excluded by the ceiling check: groq: ceiling check inconclusive, no response at 512") {
		t.Fatalf("schedule: %d, %v\n%s", code, err, text)
	}
	pages := filepath.Join(t.TempDir(), "pages")
	pargs := []string{"--out", t.TempDir(), "--ledger", ledger, "--frames", t.TempDir(), "--schedule", args[len(args)-1], "--config", groq,
		"--check-record", both[0], "--check-record", both[1], "--pages", pages}
	text = stdoutOf(t, func() { code, err = pagesCmd(pargs) })
	if code != exitOK || err != nil || !strings.Contains(text, "groq: excluded by the ceiling check: groq: ceiling check inconclusive, no response at 512; page ") {
		t.Fatalf("pages: %d, %v\n%s", code, err, text)
	}
	var x routeExclusion
	if err := json.Unmarshal(readPages(t, pages)["routes/groq.json"], &x); err != nil || !x.Excluded || len(x.Records) != 2 || x.Records[0].Outcome != probe.CheckRefused || x.Records[1].Outcome != probe.CheckRefused {
		t.Fatalf("groq.json %+v, %v", x, err)
	}
}
