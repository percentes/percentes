package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/mock"
	"github.com/percentes/percentes/internal/probe"
)

// editConfig replaces old, which must occur once, with new in the
// configuration at path.
func editConfig(t *testing.T, path, old, new string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(b), old) != 1 {
		t.Fatalf("configuration: %q not found once", old)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(b), old, new, 1)), 0o600); err != nil {
		t.Fatal(err)
	}
}

// streamError answers a request 200 with a stream carrying the in-band
// error object e, then its terminator.
func streamError(w http.ResponseWriter, e string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "data: {\"error\":%s}\n\ndata: [DONE]\n\n", e)
}

// An operator abort before admission is refused with its own text, and
// one that lands while request 3 is in flight stops the check with it:
// the STOPPED line and the record's interrupted field name the abort,
// and the record carries no withheld Go error type.
func TestCeilingCheckOperatorAbortText(t *testing.T) {
	endpoint, hits := faultMock(t, nil, new(atomic.Int32))
	groq, snapshots := quickSolo(t, "groq", endpoint)
	c := newChecker(t, releaseLedger(t, groq), snapshots)
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(loadgen.ErrOperatorAbort)
	if code, _, err := c.check(ctx, groq, 2000); code != exitError || err == nil || err.Error() != "ceiling-check: route groq: nothing admitted or sent: operator abort" || hits.Load() != 0 {
		t.Fatalf("abort before admission: %d, %v, %d sent", code, err, hits.Load())
	}

	ctx, cancel = context.WithCancelCause(context.Background())
	defer cancel(nil)
	endpoint, _ = faultMock(t, func(n int32, w http.ResponseWriter) bool {
		if n == 3 {
			time.AfterFunc(5*time.Millisecond, func() { cancel(loadgen.ErrOperatorAbort) })
			time.Sleep(200 * time.Millisecond)
		}
		return false
	}, new(atomic.Int32))
	groq, snapshots = quickSolo(t, "groq", endpoint)
	c = newChecker(t, releaseLedger(t, groq), snapshots)
	code, text, err := c.check(ctx, groq, 2000)
	const want = "the check's context ended after 3 of 10 requests: operator abort"
	if err != nil || code != exitAdverse || !strings.Contains(text, "groq: STOPPED: interrupted: "+want+"; the check may be sent again") {
		t.Fatalf("abort in flight: %d, %v\n%s", code, err, text)
	}
	path := c.records("groq")[0]
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if rec := readRecord(t, path); rec.Interrupted != want || bytes.Contains(b, []byte("text withheld")) {
		t.Fatalf("record: interrupted %q\n%s", rec.Interrupted, b)
	}
}

// Where the record is written and its withheld values are not, the error
// names the record's path, does not say the record is not written, and
// nothing goes to standard error; the record decides.
func TestCeilingCheckFramesWriteFailsNamesRecordPath(t *testing.T) {
	endpoint, _ := withHeaders(t, startMock(t, mock.Shape{Key: testKey, Answer: "The customer was billed twice."}),
		map[string]string{"X-Ratelimit-Remaining-Requests": "qz7919"})
	groq, snapshots := quickSolo(t, "groq", endpoint)
	editConfig(t, groq, rateLimitHeaders, rateLimitHeaders+"  retained_headers: [x-ratelimit-remaining-*]\n")
	c := newChecker(t, releaseLedger(t, groq), snapshots)
	frames := t.TempDir()
	if err := os.Chmod(frames, 0o500); err != nil {
		t.Fatal(err)
	}
	var code int
	var text string
	var err error
	printed := stderrOf(t, func() { code, text, err = c.check(context.Background(), groq, 2000, "--frames", frames) })
	if err := os.Chmod(frames, 0o700); err != nil {
		t.Fatal(err)
	}
	recs := c.records("groq")
	if len(recs) != 1 {
		t.Fatalf("records %v", recs)
	}
	if code != exitError || err == nil || printed != "" || strings.Contains(err.Error(), "the record is not written") ||
		!strings.Contains(err.Error(), "the record is written at "+recs[0]+"; the values it withholds are not written under --frames: ") ||
		!strings.Contains(err.Error(), "ledger line 4 is settled at 10 requests sent") || !strings.Contains(text, "; written "+recs[0]) {
		t.Fatalf("ceiling-check: %d, %v, %d bytes on standard error\n%s", code, err, len(printed), text)
	}
	if rec := readRecord(t, recs[0]); rec.Outcome() != probe.CheckRan || rec.Sent() != probe.CeilingRequests {
		t.Fatalf("record %s, %d sent", rec.Outcome(), rec.Sent())
	}
}

// Where the record's write and the settlement both fail, the record goes
// to standard error, the error says the line is not settled, and
// --recover --record writes the record and settles the line at its
// requests sent. Where standard error cannot take the record either, the
// error says so.
func TestCeilingCheckWriteAndSettleFail(t *testing.T) {
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
	if err := os.Chmod(c.out, 0o500); err != nil {
		t.Fatal(err)
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
	if code != exitError || err == nil || !strings.HasPrefix(printed, "{") || !strings.Contains(err.Error(), "the record is not written") ||
		!strings.Contains(err.Error(), "ledger line 4 is not settled: ") || strings.Contains(err.Error(), "is settled at") ||
		!strings.Contains(err.Error(), "the record is on standard error above, and ceiling-check --recover --record <a file of it> writes it") {
		t.Fatalf("ceiling-check: %d, %v, %d bytes on standard error", code, err, len(printed))
	}
	record := writeFile(t, t.TempDir(), "record.json", []byte(printed))
	if code, text, err := c.check(context.Background(), groq, 2000, "--recover", "--record", record); code != exitOK || err != nil || !strings.Contains(text, "groq: ledger line 4 settled at 10 requests sent") {
		t.Fatalf("--recover --record: %d, %v\n%s", code, err, text)
	}
	if rec := readRecord(t, c.records("groq")[0]); rec.Outcome() != probe.CheckRan || rec.Sent() != probe.CeilingRequests || rec.Interrupted != "" {
		t.Fatalf("record %s, %d sent, interrupted %q", rec.Outcome(), rec.Sent(), rec.Interrupted)
	}

	endpoint, _ = faultMock(t, nil, new(atomic.Int32))
	groq, snapshots = quickSolo(t, "groq", endpoint)
	c = newChecker(t, releaseLedger(t, groq), snapshots)
	if err := os.Chmod(c.out, 0o500); err != nil {
		t.Fatal(err)
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	w.Close()
	old := os.Stderr
	os.Stderr = w
	code, _, err = c.check(context.Background(), groq, 2000)
	os.Stderr = old
	if err := os.Chmod(c.out, 0o700); err != nil {
		t.Fatal(err)
	}
	if code != exitError || err == nil || !strings.Contains(err.Error(), "ceiling-check: the record is not on standard error either: ") ||
		strings.Contains(err.Error(), "the record is on standard error above") {
		t.Fatalf("standard error closed: %d, %v", code, err)
	}
}

// An edit to target.error_docs after a check refused by an in-band error,
// or to target.billing_refusals after a check an in-band billing refusal
// stopped, makes run's ceiling choice and the next ceiling check refuse
// the record, naming the frozen fields; the restored configuration reads
// it again. An in-band error matching a billing refusal stops the check
// as BILLING.
func TestCeilingCheckFrozenErrorAndBillingDocs(t *testing.T) {
	const docs = "  error_docs:\n    - type: overloaded_error\n      status: 529\n      meaning: the API is temporarily overloaded\n      source: https://docs.example.test/errors\n      retrieved: 2026-11-03\n"
	const frozen = "target.error_docs, target.billing_refusals and target.probe.account_limit_statuses are frozen from the release's first ceiling check until the schedule"
	choose := func(cfgPath string, c *checker) (probe.CeilingChoice, error) {
		t.Helper()
		cfgs, err := loadReleases([]string{cfgPath})
		if err != nil {
			t.Fatal(err)
		}
		open := ledgerOpenSHA256(t, c.ledger)
		entries, err := ledgerEntries(c.ledger, open)
		if err != nil {
			t.Fatal(err)
		}
		choice, _, _, err := ceilingChoice(cfgs[0], nil, c.records("groq"), 2000, open, entries)
		return choice, err
	}

	endpoint, hits := faultMock(t, func(n int32, w http.ResponseWriter) bool {
		if n == 2 {
			streamError(w, `{"type":"overloaded_error","message":"Overloaded"}`)
			return true
		}
		return false
	}, new(atomic.Int32))
	groq, snapshots := quickSolo(t, "groq", endpoint)
	editConfig(t, groq, rateLimitHeaders, rateLimitHeaders+docs)
	c := newChecker(t, releaseLedger(t, groq), snapshots)
	if code, text, err := c.check(context.Background(), groq, 2000); code != exitAdverse || err != nil ||
		!strings.Contains(text, "groq: REFUSED: a request answered 200 carried an in-band error its provider documents as HTTP 529") {
		t.Fatalf("in-band 529: %d, %v\n%s", code, err, text)
	}
	if choice, err := choose(groq, c); err != nil || !choice.Excluded || choice.Reason != "groq: ceiling check inconclusive, HTTP 529 in-band at 512" {
		t.Fatalf("choice: %+v, %v", choice, err)
	}
	editConfig(t, groq, "      status: 529\n", "      status: 503\n")
	if _, err := choose(groq, c); err == nil || !strings.Contains(err.Error(), "in-band status 529 recorded; its in-band error and target.error_docs give 503") || !strings.Contains(err.Error(), frozen) {
		t.Fatalf("choice after the edit: %v", err)
	}
	sent := hits.Load()
	if code, _, err := c.check(context.Background(), groq, 2000); code != exitError || err == nil || !strings.Contains(err.Error(), frozen) || hits.Load() != sent {
		t.Fatalf("check after the edit: %d, %v, %d sent", code, err, hits.Load()-sent)
	}
	editConfig(t, groq, "      status: 503\n", "      status: 529\n")
	if choice, err := choose(groq, c); err != nil || !choice.Excluded {
		t.Fatalf("choice restored: %+v, %v", choice, err)
	}

	const billing = "  billing_refusals:\n    - status: 429\n      body_contains: enforced_spend_limit_reached\n      meaning: the spend cap is reached\n      source: https://docs.example.test/limits\n      retrieved: 2026-11-01\n"
	endpoint, _ = faultMock(t, func(n int32, w http.ResponseWriter) bool {
		if n == 2 {
			streamError(w, `{"type":"rate_limit_error","message":"cap","details":{"error_code":"enforced_spend_limit_reached"}}`)
			return true
		}
		return false
	}, new(atomic.Int32))
	groq, snapshots = quickSolo(t, "groq", endpoint)
	editConfig(t, groq, rateLimitHeaders, rateLimitHeaders+strings.NewReplacer("overloaded_error", "rate_limit_error", "529", "429").Replace(docs)+billing)
	c = newChecker(t, releaseLedger(t, groq), snapshots)
	if code, text, err := c.check(context.Background(), groq, 2000); code != exitAdverse || err != nil ||
		!strings.Contains(text, "groq: BILLING: HTTP 429 in-band, the operator's billing setting, a documented billing refusal: the spend cap is reached") {
		t.Fatalf("in-band billing refusal: %d, %v\n%s", code, err, text)
	}
	if rec := readRecord(t, c.records("groq")[0]); rec.Outcome() != probe.CheckStopped || len(rec.Requests) != 2 {
		t.Fatalf("record %s, %d requests", rec.Outcome(), len(rec.Requests))
	}
	editConfig(t, groq, billing, "")
	if _, err := choose(groq, c); err == nil || !strings.Contains(err.Error(), "request 1: in-band status 0 recorded; its in-band error and target.error_docs give 429") || !strings.Contains(err.Error(), frozen) {
		t.Fatalf("choice after the edit: %v", err)
	}
	editConfig(t, groq, rateLimitHeaders, rateLimitHeaders+billing)
	if _, err := choose(groq, c); err == nil || strings.Contains(err.Error(), frozen) {
		t.Fatalf("choice restored: %v", err)
	}
}

// The ceiling-check summary prints a saturated token count as "at least".
func TestCeilingCheckSummarySaturatedCount(t *testing.T) {
	endpoint := startMock(t, mock.Shape{Key: testKey, Answer: "The customer was billed twice."})
	u, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	rp := httputil.NewSingleHostReverseProxy(&url.URL{Scheme: u.Scheme, Host: u.Host})
	rp.FlushInterval = -1
	counts := regexp.MustCompile(`"(prompt|completion)_tokens":\s*\d+`)
	rp.ModifyResponse = func(r *http.Response) error {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			return err
		}
		r.Body.Close()
		b = counts.ReplaceAll(b, []byte(`"${1}_tokens":99999999999999999999999`))
		r.Body, r.ContentLength = io.NopCloser(bytes.NewReader(b)), int64(len(b))
		r.Header.Del("Content-Length")
		return nil
	}
	srv := httptest.NewServer(rp)
	t.Cleanup(srv.Close)
	groq, snapshots := quickSolo(t, "groq", srv.URL+u.Path)
	c := newChecker(t, releaseLedger(t, groq), snapshots)
	_, text, err := c.check(context.Background(), groq, 2000)
	if err != nil || !strings.Contains(text, "largest at least 9223372036854775807, 10 above; output bound 512 tokens: 10 reported, largest at least 9223372036854775807, 10 above") {
		t.Fatalf("ceiling-check: %v\n%s", err, text)
	}
}

// A check at the fallback is refused where another host's deciding
// record at the first ceiling was refused, where the route's own was, and
// where the route's records report another model; a Shape R route's
// refusal names no byte cap.
func TestCeilingCheckFallbackRefusedAfterRefusalOrOtherModel(t *testing.T) {
	cfg, err := config.LoadFile(routeConfigFile(t, "groq", "https://api.example.test/openai/v1/chat/completions", "10"))
	if err != nil {
		t.Fatal(err)
	}
	at1024 := *cfg
	shape := *cfg.Release
	shape.Shape.CeilingTokens = 1024
	at1024.Release = &shape
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	rec := func(route string, seq int, pass, refused bool) *probe.ReleaseRecord {
		r := hostCeiling(cfg, 2000, route, 512, pass, "open")
		r.LedgerSeq, r.EndUTC = seq, now.Add(-time.Hour)
		if refused {
			r.Requests[3].Exchange.Status, r.Requests[3].Pass, r.Requests[3].Fail = http.StatusTooManyRequests, false, "status 429"
		}
		return &r
	}
	all := func(rs ...*probe.ReleaseRecord) map[int]priorCheck {
		m := map[int]priorCheck{}
		for _, r := range rs {
			m[r.LedgerSeq] = priorCheck{fmt.Sprint(r.LedgerSeq), r}
		}
		return m
	}
	wrong := rec("groq", 4, false, false)
	wrong.Requests[0].Shape.Models = []string{"another-model"}
	for _, c := range []struct {
		name string
		all  map[int]priorCheck
		want string
	}{
		{"groq failed, baseten refused twice", all(rec("groq", 4, false, false), rec("baseten", 5, true, true), rec("baseten", 7, true, true)),
			"the check at 1024 follows the one at 512, and the host group's checks there exclude it at byte cap 2000: baseten: ceiling check inconclusive, HTTP 429 at 512"},
		{"groq refused twice, baseten failed", all(rec("groq", 4, true, true), rec("groq", 6, true, true), rec("baseten", 5, false, false)),
			"exclude it at byte cap 2000: groq: ceiling check inconclusive, HTTP 429 at 512"},
		{"groq reports another model", all(wrong, rec("baseten", 5, false, false)), "the route is left out on the reported model id"},
		{"groq and baseten failed", all(rec("groq", 4, false, false), rec("baseten", 5, false, false)), ""},
	} {
		err := repeatCheck(&at1024, c.all, nil, 2000, now)
		if c.want == "" && err != nil || c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)) {
			t.Errorf("%s: %v, want %q", c.name, err, c.want)
		}
	}

	r := *cfg
	rel := *cfg.Release
	yes := true
	rel.Shape = config.Shape{Name: config.ShapeR, InputBoundTokens: 128, CeilingTokens: 256, Fallback: &yes}
	r.Release = &rel
	if err := repeatCheck(&r, nil, nil, 0, now); err == nil || !strings.HasSuffix(err.Error(), "the route has no check at 16 that decides") {
		t.Fatalf("Shape R at 256 with no check at 16: %v", err)
	}
}

// A refused check's line prints its ceiling as the pages do, in the
// repeat clause and in the exclusion its repeat decides.
func TestCeilingCheckRefusedText(t *testing.T) {
	cfg, err := config.LoadFile(routeConfigFile(t, "groq", "https://api.example.test/openai/v1/chat/completions", "10"))
	if err != nil {
		t.Fatal(err)
	}
	refusedAt := func(seq int) *probe.ReleaseRecord {
		r := hostCeiling(cfg, 2000, "groq", 1024, true, "open")
		r.LedgerSeq, r.EndUTC = seq, time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
		r.Requests[3].Exchange.Status = http.StatusTooManyRequests
		return &r
	}
	if got := refusedText(cfg, refusedAt(4), nil, nil, 2000); !strings.Contains(got, "the check may be repeated once at 1,024 from ") {
		t.Fatalf("first refusal: %s", got)
	}
	prior := map[int]priorCheck{4: {"4", refusedAt(4)}}
	if got := refusedText(cfg, refusedAt(6), prior, nil, 2000); !strings.Contains(got, "the host group is excluded, ceiling check inconclusive, HTTP 429 at 1,024 (SPEC-HOSTED HA4, refused checks)") {
		t.Fatalf("repeat: %s", got)
	}
}
