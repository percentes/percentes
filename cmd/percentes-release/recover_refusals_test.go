package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/percentes/percentes/internal/probe"
)

// unsettledCheck runs a groq ceiling check whose ledger refuses its
// settlement, with its record written under the checker's out where
// written, or else printed on standard error, which it returns.
func unsettledCheck(t *testing.T, written bool) (*checker, string, string) {
	t.Helper()
	var ledgerPath atomic.Value
	endpoint, _ := faultMock(t, func(n int32, w http.ResponseWriter) bool {
		if n == 10 {
			os.Chmod(ledgerPath.Load().(string), 0o400) //nolint:errcheck
		}
		return false
	}, new(atomic.Int32))
	groq, snapshots := quickSolo(t, "groq", endpoint)
	ledger := releaseLedger(t, groq)
	ledgerPath.Store(ledger)
	c := newChecker(t, ledger, snapshots)
	if !written {
		if err := os.Chmod(c.out, 0o500); err != nil {
			t.Fatal(err)
		}
	}
	var code int
	var err error
	printed := stderrOf(t, func() { code, _, err = c.check(context.Background(), groq, 2000) })
	if err := os.Chmod(c.out, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(ledger, 0o600); err != nil {
		t.Fatal(err)
	}
	if code != exitError || err == nil || !strings.Contains(err.Error(), "ledger line 4 is not settled: ") || len(c.records("groq")) != map[bool]int{true: 1}[written] {
		t.Fatalf("ceiling-check: %d, %v, %d records", code, err, len(c.records("groq")))
	}
	return c, groq, printed
}

// Plain --recover of a check whose record is written and whose admission
// is unsettled settles the line at the record's requests sent and writes
// no interrupted record.
func TestRecoverSettlesAtWrittenRecord(t *testing.T) {
	c, groq, _ := unsettledCheck(t, true)
	code, text, err := c.check(context.Background(), groq, 2000, "--recover")
	if code != exitOK || err != nil || !strings.Contains(text, "groq: ledger line 4 settled at 10 requests sent") || strings.Contains(text, "interrupted record written") {
		t.Fatalf("--recover: %d, %v\n%s", code, err, text)
	}
	if recs := c.records("groq"); len(recs) != 1 || readRecord(t, recs[0]).Interrupted != "" {
		t.Fatalf("records %v", recs)
	}
}

// --recover --record refuses a record of another route, of another
// ledger, of a line that admits no check of the route, at a ceiling the
// line does not admit, and one Verify refuses, and settles nothing; the
// record as printed is then installed.
func TestRecoverRecordRefusals(t *testing.T) {
	c, groq, printed := unsettledCheck(t, false)
	for _, x := range []struct {
		name, want string
		edit       func(*probe.ReleaseRecord)
	}{
		{"another route", "a record of route gemini, Shape S; --config is route groq, Shape S", func(r *probe.ReleaseRecord) { r.Route = "gemini" }},
		{"another ledger", "admitted in the ledger of open-line sha256 \"0000\"", func(r *probe.ReleaseRecord) { r.LedgerSHA256 = "0000" }},
		{"no admission", "ledger line 99 admits no ceiling check of route groq", func(r *probe.ReleaseRecord) { r.LedgerSeq = 99 }},
		{"another ceiling", "ledger line 4 admits the check at ceiling 512 and byte cap 2000; the record is at 1024 and 2000", func(r *probe.ReleaseRecord) { r.Ceiling.Ceiling = 1024 }},
		{"edited result", "request 0 records pass false", func(r *probe.ReleaseRecord) { r.Requests[0].Pass, r.Requests[0].Fail = false, "status 500" }},
	} {
		var rec probe.ReleaseRecord
		if err := json.Unmarshal([]byte(printed), &rec); err != nil {
			t.Fatal(err)
		}
		x.edit(&rec)
		b, err := probe.Encode(&rec)
		if err != nil {
			t.Fatal(err)
		}
		path := writeFile(t, t.TempDir(), "record.json", b)
		code, text, err := c.check(context.Background(), groq, 2000, "--recover", "--record", path)
		if code != exitError || err == nil || !strings.Contains(err.Error(), x.want) || strings.Contains(text, "settled") || len(c.records("groq")) != 0 {
			t.Fatalf("%s: %d, %v\n%s", x.name, code, err, text)
		}
	}
	path := writeFile(t, t.TempDir(), "record.json", []byte(printed))
	if code, text, err := c.check(context.Background(), groq, 2000, "--recover", "--record", path); code != exitOK || err != nil || !strings.Contains(text, "groq: ledger line 4 settled at 10 requests sent") {
		t.Fatalf("the printed record: %d, %v\n%s", code, err, text)
	}
}
