package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/probe"
)

// probeAdmits counts the ceiling-check admissions in the ledger at path.
func probeAdmits(t *testing.T, path string) int {
	t.Helper()
	entries, err := ledgerEntries(path, ledgerOpenSHA256(t, path))
	if err != nil {
		t.Fatal(err)
	}
	return len(probeAdmissions(entries, nil))
}

// A Shape S check is refused before admission at a byte cap other than
// the one another route's record decides at, naming that record's route,
// ledger line and cap, and so is a repeat of a refused check at another
// cap; a stopped record at another cap decides nothing, and schedule
// takes the records that decide.
func TestCeilingCheckByteCapMismatch(t *testing.T) {
	endpoint, hits := faultMock(t, func(n int32, w http.ResponseWriter) bool {
		switch n {
		case 12:
			answer429(w)
			return true
		case 31:
			w.WriteHeader(http.StatusUnauthorized)
			return true
		}
		return false
	}, new(atomic.Int32))
	groq, snapshots := quickSolo(t, "groq", endpoint)
	gemini, _ := quickSolo(t, "gemini", endpoint)
	ledger := releaseLedger(t, groq, gemini)
	c := newChecker(t, ledger, snapshots)
	ctx := context.Background()
	if code, text, err := c.check(ctx, groq, 2000); code != exitOK || err != nil {
		t.Fatalf("groq at 2000: %d, %v\n%s", code, err, text)
	}
	seq := readRecord(t, c.records("groq")[0]).LedgerSeq
	sent, admits := hits.Load(), probeAdmits(t, ledger)
	want := fmt.Sprintf("ceiling-check: route gemini: --byte-cap 1800: the check of route groq at ledger line %d decides at byte cap 2000; send the check with --byte-cap 2000", seq)
	if code, _, err := c.check(ctx, gemini, 1800); code != exitError || err == nil || err.Error() != want || hits.Load() != sent || probeAdmits(t, ledger) != admits {
		t.Fatalf("gemini's first check at 1800: %d, %v; want %q, nothing admitted or sent", code, err, want)
	}

	code, text, err := c.check(ctx, gemini, 2000)
	if code != exitAdverse || err != nil || !strings.Contains(text, "gemini: REFUSED: a request was answered HTTP 429") {
		t.Fatalf("gemini at 2000: %d, %v\n%s", code, err, text)
	}
	refused := c.records("gemini")[0]
	rewriteRecord(t, refused, func(r *probe.ReleaseRecord) { r.EndUTC = r.EndUTC.Add(-time.Hour) })
	sent = hits.Load()
	if code, _, err := c.check(ctx, gemini, 1800); code != exitError || err == nil || !strings.Contains(err.Error(), "decides at byte cap 2000; send the check with --byte-cap 2000") || hits.Load() != sent {
		t.Fatalf("gemini's repeat at 1800: %d, %v", code, err)
	}
	if code, text, err := c.check(ctx, gemini, 2000); code != exitOK || err != nil {
		t.Fatalf("gemini's repeat at 2000: %d, %v\n%s", code, err, text)
	}

	other, _ := quickSolo(t, "baseten", endpoint)
	ledger = releaseLedger(t, other, groq)
	c = newChecker(t, ledger, snapshots)
	if code, text, err := c.check(ctx, other, 1800); code != exitAdverse || err != nil || !strings.Contains(text, "baseten: BLOCKED") {
		t.Fatalf("baseten at 1800: %d, %v\n%s", code, err, text)
	}
	for _, cfg := range []string{other, groq} {
		if code, text, err := c.check(ctx, cfg, 2000); code != exitOK || err != nil {
			t.Fatalf("%s at 2000: %d, %v\n%s", cfg, code, err, text)
		}
	}
	cfgs, err := loadReleases([]string{other, groq})
	if err != nil {
		t.Fatal(err)
	}
	open := ledgerOpenSHA256(t, ledger)
	entries, err := ledgerEntries(ledger, open)
	if err != nil {
		t.Fatal(err)
	}
	if reasons, err := scheduleChoices(cfgs, append(c.records("baseten"), c.records("groq")...), entries, open); err != nil || len(reasons) != 0 {
		t.Fatalf("schedule with a stopped record at 1800: %v, %v", reasons, err)
	}
}

// A check whose request reaches no connection stops at that request,
// prints which and why, settles no request and decides nothing: the
// route may be checked again, and run finds no check that was not
// stopped.
func TestCeilingCheckUnreachedRequestStops(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	groq, snapshots := quickSolo(t, "groq", "http://"+addr+"/openai/v1/chat/completions")
	ledger := releaseLedger(t, groq)
	c := newChecker(t, ledger, snapshots)
	for i := 0; i < 2; i++ {
		code, text, err := c.check(context.Background(), groq, 2000)
		if code != exitAdverse || err != nil || !regexp.MustCompile(`groq: STOPPED: request 0 reached no connection:.*refused.*; the check may be sent again\n`).MatchString(text) {
			t.Fatalf("check %d: %d, %v\n%s", i, code, err, text)
		}
	}
	if u := ledgerState(t, ledger).Routes["groq"]; u == nil || u.Probe != 0 || u.Reserved != 0 {
		t.Fatalf("ledger usage %+v", u)
	}
	cfgs, err := loadReleases([]string{groq})
	if err != nil {
		t.Fatal(err)
	}
	open := ledgerOpenSHA256(t, ledger)
	if _, _, _, err := ceilingChoice(cfgs[0], nil, c.records("groq"), 2000, open, nil); err == nil || err.Error() != "probe: route groq has no check at 512 that was not stopped (2 stopped)" {
		t.Fatalf("run: %v", err)
	}
}

// A check whose request answered 200 has its stream cut is refused with
// its own head and may be repeated once from the printed time, a whole
// second at or after the quiet interval's end; a repeat at that second
// is not refused.
func TestCeilingCheckStreamCutRepeat(t *testing.T) {
	endpoint, _ := faultMock(t, func(n int32, w http.ResponseWriter) bool {
		if n != 3 {
			return false
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"model\":\"openai/gpt-oss-120b\",\"choices\":[{\"delta\":{\"content\":\"The\"}}]}\n\n") //nolint:errcheck
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}, new(atomic.Int32))
	groq, snapshots := quickSolo(t, "groq", endpoint)
	c := newChecker(t, releaseLedger(t, groq), snapshots)
	code, text, err := c.check(context.Background(), groq, 2000)
	head := "groq: REFUSED: a request answered 200 had its stream cut before its terminator, no documented billing refusal; the check may be repeated once at 512 from "
	if code != exitAdverse || err != nil || !strings.Contains(text, head) {
		t.Fatalf("ceiling-check: %d, %v\n%s", code, err, text)
	}
	rec := readRecord(t, c.records("groq")[0])
	printed, err := time.Parse(time.RFC3339, strings.Fields(text[strings.Index(text, head)+len(head):])[0][:20])
	if err != nil {
		t.Fatal(err)
	}
	cfgs, err := loadReleases([]string{groq})
	if err != nil {
		t.Fatal(err)
	}
	from := rec.EndUTC.Add(probe.RepeatQuiet(&cfgs[0].Target))
	if printed.Before(from) || !printed.Before(from.Add(time.Second)) || printed.Nanosecond() != 0 {
		t.Fatalf("printed %s; the quiet interval ends %s", printed, from)
	}
	all := map[int]priorCheck{rec.LedgerSeq: {"r", rec}}
	if err := repeatCheck(cfgs[0], all, nil, 2000, printed); err != nil {
		t.Fatalf("a repeat at the printed time: %v", err)
	}
	if err := repeatCheck(cfgs[0], all, nil, 2000, from.Add(-time.Nanosecond)); err == nil || !strings.Contains(err.Error(), "may be repeated from "+printed.Format(time.RFC3339)) {
		t.Fatalf("a repeat before it: %v", err)
	}
}

// A check whose request answered 200 has its stream end before its
// terminator with no read error is refused with its own head; unrepeated
// it excludes the host group at 512 with no check at 1,024, a repeat
// inside the quiet interval is refused, and the repeat after it that
// passes decides for 512.
func TestCeilingCheckStreamEndedEarlyRepeat(t *testing.T) {
	endpoint, _ := faultMock(t, func(n int32, w http.ResponseWriter) bool {
		if n != 3 {
			return false
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"model\":\"openai/gpt-oss-120b\",\"choices\":[{\"delta\":{\"content\":\"The\"}}]}\n\n") //nolint:errcheck
		return true
	}, new(atomic.Int32))
	groq, snapshots := quickSolo(t, "groq", endpoint)
	ledger := releaseLedger(t, groq)
	c := newChecker(t, ledger, snapshots)
	ctx := context.Background()
	code, text, err := c.check(ctx, groq, 2000)
	head := "groq: REFUSED: a request answered 200 had its stream end before its terminator with no read error, no documented billing refusal; the check may be repeated once at 512 from "
	if code != exitAdverse || err != nil || !strings.Contains(text, head) {
		t.Fatalf("ceiling-check: %d, %v\n%s", code, err, text)
	}
	first := c.records("groq")[0]
	rec := readRecord(t, first)
	if rec.RefusedBy() != probe.RefusedEndedEarly || rec.Requests[2].Shape.ReadError != "" {
		t.Fatalf("refused by %q, request 2 read error %q", rec.RefusedBy(), rec.Requests[2].Shape.ReadError)
	}
	cfgs, err := loadReleases([]string{groq})
	if err != nil {
		t.Fatal(err)
	}
	open := ledgerOpenSHA256(t, ledger)
	choice, _, _, err := ceilingChoiceFor(cfgs[0], nil, []string{first}, 2000, open, nil, true)
	if want := "groq: ceiling check inconclusive, stream ended early at 512"; err != nil || !choice.Excluded || choice.Reason != want {
		t.Fatalf("unrepeated: %+v, %v; want %q", choice, err, want)
	}
	if code, _, err := c.check(ctx, groq, 2000); code != exitError || err == nil || !strings.Contains(err.Error(), "may be repeated from") {
		t.Fatalf("a repeat inside the quiet interval: %d, %v", code, err)
	}
	rewriteRecord(t, first, func(r *probe.ReleaseRecord) { r.EndUTC = r.EndUTC.Add(-time.Hour) })
	if code, text, err := c.check(ctx, groq, 2000); code != exitOK || err != nil {
		t.Fatalf("the repeat: %d, %v\n%s", code, err, text)
	}
	records := c.records("groq")
	choice, _, _, err = ceilingChoice(cfgs[0], nil, records, 2000, open, nil)
	if want := fmt.Sprintf("every host passed at 512; groq: the check refused at 512 (ledger line %d) was repeated", rec.LedgerSeq); err != nil || len(records) != 2 || choice.Excluded || choice.Ceiling != 512 || choice.Reason != want {
		t.Fatalf("both records: %+v, %v; want %q", choice, err, want)
	}
}

// schedule writes nothing unless --excluded names exactly the routes the
// records exclude, and prints an excluded route's line with its reason;
// once the missing re-check is sent, the records exclude nothing. The
// route pages list records by ledger line with their byte caps, mark a
// record at a spent cap above the release's superseded, and cite the
// byte caps rule for an input-bound exclusion.
func TestScheduleExcludedRoutes(t *testing.T) {
	var prompt atomic.Int32
	endpoint, _ := faultMock(t, nil, &prompt)
	groq, snapshots := quickSolo(t, "groq", endpoint)
	gemini, _ := quickSolo(t, "gemini", endpoint)
	ledger := releaseLedger(t, groq, gemini)
	c := newChecker(t, ledger, snapshots)
	ctx := context.Background()
	if code, text, err := c.check(ctx, groq, 2000); code != exitOK || err != nil {
		t.Fatalf("groq at 2000: %d, %v\n%s", code, err, text)
	}
	prompt.Store(1300)
	if code, text, err := c.check(ctx, gemini, 2000); code != exitAdverse || err != nil {
		t.Fatalf("gemini at 2000: %d, %v\n%s", code, err, text)
	}
	prompt.Store(0)
	if code, text, err := c.check(ctx, groq, 1800); code != exitOK || err != nil {
		t.Fatalf("groq at 1800: %d, %v\n%s", code, err, text)
	}
	schedule := func(paths []string, excluded ...string) (int, string, string, error) {
		path := filepath.Join(t.TempDir(), "schedule.json")
		args := []string{"--master-seed", "percentes-release-test", "--first", "2026-10-17", "--last", "2026-10-26", "--config", groq, "--config", gemini,
			"--ledger", ledger, "--release-key", testReleaseKeyPath, "--schedule", path}
		for _, p := range paths {
			args = append(args, "--ceiling-probe", p)
		}
		for _, x := range excluded {
			args = append(args, "--excluded", x)
		}
		var code int
		var err error
		text := stdoutOf(t, func() { code, err = scheduleCmd(args) })
		return code, text, path, err
	}
	paths := append(c.records("groq"), c.records("gemini")...)
	reason := "gemini: the input bound failed at 512 at byte cap 2,000: 10 of 10 requests reported, 10 above 1,200 tokens"
	for _, x := range [][]string{nil, {"groq"}} {
		code, _, path, err := schedule(paths, x...)
		if _, serr := os.Stat(path); code != exitError || err == nil || !errors.Is(serr, os.ErrNotExist) ||
			!strings.Contains(err.Error(), "schedule: route gemini: the ceiling check excludes the host group ("+reason+"); --excluded does not name it") {
			t.Fatalf("--excluded %v: %d, %v", x, code, err)
		}
		if x != nil && !strings.Contains(err.Error(), "schedule: route groq: --excluded names it; the ceiling check runs it at 512") {
			t.Fatalf("--excluded groq: %v", err)
		}
	}
	code, text, _, err := schedule(paths, "gemini")
	if code != exitOK || err != nil || !strings.Contains(text, "gemini: excluded by the ceiling check: "+reason+"; offset ") {
		t.Fatalf("--excluded gemini: %d, %v\n%s", code, err, text)
	}

	cfgs, err := loadReleases([]string{groq, gemini})
	if err != nil {
		t.Fatal(err)
	}
	open := ledgerOpenSHA256(t, ledger)
	x, err := excludedRoute(cfgs[0], cfgs, paths, 1800, open, nil, true)
	if err != nil || len(x.Records) != 2 || x.Records[0].LedgerSeq >= x.Records[1].LedgerSeq || !x.Records[0].Superseded || x.Records[1].Superseded || x.Records[0].ByteCap != 2000 {
		t.Fatalf("groq's page: %+v, %v", x, err)
	}
	line := fmt.Sprintf("- ceiling-probe-groq-512-%d.json: route groq at ceiling 512, byte cap 2,000, ledger line %d, ran, superseded, sha256 ", x.Records[0].LedgerSeq, x.Records[0].LedgerSeq)
	if page := x.text(); !strings.Contains(page, line) || strings.Index(page, line) > strings.Index(page, fmt.Sprintf("ledger line %d,", x.Records[1].LedgerSeq)) {
		t.Fatalf("groq's page:\n%s", page)
	}
	x, err = excludedRoute(cfgs[1], cfgs, paths, 1800, open, nil, false)
	if err != nil || !strings.Contains(x.text(), "is excluded from the release by the ceiling check (SPEC-HOSTED HA3 point 7 and HA4, byte caps): "+reason+".\n") {
		t.Fatalf("gemini's page: %v\n%s", err, x.text())
	}

	if code, text, err := c.check(ctx, gemini, 1800); code != exitOK || err != nil {
		t.Fatalf("gemini at 1800: %d, %v\n%s", code, err, text)
	}
	if code, text, _, err := schedule(append(c.records("groq"), c.records("gemini")...)); code != exitOK || err != nil || strings.Contains(text, "excluded") {
		t.Fatalf("schedule after gemini's re-check: %d, %v\n%s", code, err, text)
	}
}

// A check whose context ended before admission admits and sends nothing,
// and --recover with no unsettled check names the route's newest check
// and what its record reads.
func TestCeilingCheckAbortBeforeAdmissionAndRecover(t *testing.T) {
	endpoint, hits := faultMock(t, func(n int32, w http.ResponseWriter) bool {
		if n == 2 {
			answer429(w)
			return true
		}
		return false
	}, new(atomic.Int32))
	groq, snapshots := quickSolo(t, "groq", endpoint)
	ledger := releaseLedger(t, groq)
	c := newChecker(t, ledger, snapshots)
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(loadgen.ErrOperatorAbort)
	if code, _, err := c.check(ctx, groq, 2000); code != exitError || err == nil || !strings.Contains(err.Error(), "nothing admitted or sent") || hits.Load() != 0 || probeAdmits(t, ledger) != 0 {
		t.Fatalf("a check after an abort: %d, %v, %d requests", code, err, hits.Load())
	}
	if code, text, err := c.check(context.Background(), groq, 2000); code != exitAdverse || err != nil || !strings.Contains(text, "REFUSED") {
		t.Fatalf("ceiling-check: %d, %v\n%s", code, err, text)
	}
	path := c.records("groq")[0]
	seq := readRecord(t, path).LedgerSeq
	code, text, err := c.check(context.Background(), groq, 2000, "--recover")
	if code != exitError || err == nil || !strings.Contains(text, fmt.Sprintf("groq: ledger line %d, the route's newest ceiling check, is settled; its record %s reads refused\n", seq, path)) {
		t.Fatalf("--recover: %d, %v\n%s", code, err, text)
	}
	if code, _, err := c.check(context.Background(), groq, 2000); code != exitError || err == nil || !strings.Contains(err.Error(), "may be repeated from") {
		t.Fatalf("the next check: %d, %v", code, err)
	}
}

// A check at the fallback ceiling is refused unless, at the ceiling
// before it, the route has a deciding record at this byte cap and the
// ceiling rule over its host group's records there moves the group on;
// the refusal names that ceiling.
func TestCeilingCheckFallbackNeedsDecidingRecordAndGroupFailure(t *testing.T) {
	cfg, err := config.LoadFile(routeConfigFile(t, "groq", "https://api.example.test/openai/v1/chat/completions", "10"))
	if err != nil {
		t.Fatal(err)
	}
	at1024 := *cfg
	shape := *cfg.Release
	shape.Shape.CeilingTokens = 1024
	at1024.Release = &shape
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	rec := func(seq, byteCap int, pass, over bool) *probe.ReleaseRecord {
		r := hostCeiling(cfg, byteCap, "groq", 512, pass, "open")
		r.LedgerSeq, r.EndUTC = seq, now.Add(-time.Minute)
		if over {
			for i := range r.Requests {
				n := 1300
				r.Requests[i].PromptTokens = &n
			}
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
	other := rec(6, 2000, true, true)
	other.Route = "gemini"
	baseten := hostCeiling(cfg, 1800, "baseten", 512, true, "open")
	baseten.LedgerSeq = 5
	spent := probe.Spent([]*probe.ReleaseRecord{other})
	for _, c := range []struct {
		name    string
		cfg     *config.Config
		all     map[int]priorCheck
		byteCap int
		want    string
	}{
		{"a first check at 512", cfg, nil, 1800, ""},
		{"1024 with no 512 check", &at1024, nil, 1800, "the check at 1024 follows the one at 512, and the route has no check at 512 that decides at byte cap 1800"},
		{"1024 after a 512 failure the spent cap supersedes", &at1024, all(rec(4, 2000, false, false)), 1800, "no check at 512 that decides at byte cap 1800"},
		{"1024 after a 512 pass", &at1024, all(rec(4, 1800, true, false), &baseten), 1800, "no host of its host group failed the check that decides at 512"},
		{"1024 after a 512 failure, baseten unchecked", &at1024, all(rec(4, 1800, false, false)), 1800, "probe: route baseten has no check at 512"},
		{"1024 after a 512 failure", &at1024, all(rec(4, 1800, false, false), &baseten), 1800, ""},
	} {
		err := repeatCheck(c.cfg, c.all, spent, c.byteCap, now)
		if c.want == "" && err != nil || c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)) {
			t.Errorf("%s: %v, want %q", c.name, err, c.want)
		}
	}
}
