package main

import (
	"context"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/probe"
)

// A check aborted at its fourth request whose record is not written
// settles at the four sent; --recover --record writes the printed
// interrupted record, which verifies and decides nothing, and the next
// check sends its ten requests and passes.
func TestRecoverRecordWritesInterruptedCheck(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	endpoint, hits := faultMock(t, func(n int32, _ http.ResponseWriter) bool {
		if n != 4 {
			return false
		}
		cancel(loadgen.ErrOperatorAbort)
		time.Sleep(300 * time.Millisecond)
		return true
	}, new(atomic.Int32))
	groq, snapshots := quickSolo(t, "groq", endpoint)
	ledger := releaseLedger(t, groq)
	c := newChecker(t, ledger, snapshots)
	if err := os.Chmod(c.out, 0o500); err != nil {
		t.Fatal(err)
	}
	var code int
	var err error
	printed := stderrOf(t, func() { code, _, err = c.check(ctx, groq, 2000) })
	if err := os.Chmod(c.out, 0o700); err != nil {
		t.Fatal(err)
	}
	if code != exitError || err == nil || !strings.Contains(err.Error(), "is settled at 4 requests sent") {
		t.Fatalf("an aborted check: %d, %v", code, err)
	}
	if code, _, err := c.check(context.Background(), groq, 2000); code != exitError || err == nil || !strings.Contains(err.Error(), "--recover --record <a file of it>") || hits.Load() != 4 {
		t.Fatalf("the next check before --record: %d, %v, %d requests", code, err, hits.Load())
	}
	record := writeFile(t, t.TempDir(), "record.json", []byte(printed))
	if code, text, err := c.check(context.Background(), groq, 2000, "--recover", "--record", record); code != exitOK || err != nil || !strings.Contains(text, ": record written ") {
		t.Fatalf("--recover --record: %d, %v\n%s", code, err, text)
	}
	rec := readRecord(t, c.records("groq")[0])
	if want := "the check's context ended after 4 of 10 requests: operator abort"; rec.Interrupted != want || rec.Sent() != 4 || rec.Outcome() != probe.CheckStopped {
		t.Fatalf("record: interrupted %q, %d sent, outcome %s", rec.Interrupted, rec.Sent(), rec.Outcome())
	}
	if code, text, err := c.check(context.Background(), groq, 2000); code != exitOK || err != nil || hits.Load() != 4+probe.CeilingRequests {
		t.Fatalf("the next check: %d, %v, %d requests\n%s", code, err, hits.Load(), text)
	}
}
