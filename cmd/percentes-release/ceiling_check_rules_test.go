package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/mock"
	"github.com/percentes/percentes/internal/probe"
)

// testSnapshot is the retained copy of the limits page the test
// configurations' file-source limits record (snapshotted).
var testSnapshot = []byte("the provider's rate limits page, retrieved 2026-10-07")

// testSnapshotSHA256 is testSnapshot's SHA-256.
func testSnapshotSHA256() string {
	sum := sha256.Sum256(testSnapshot)
	return hex.EncodeToString(sum[:])
}

// snapshotted rewrites the configuration at cfgPath so each limit sourced
// from groq-rate-limits-page.png records testSnapshot's hash, its
// SHA-256 or its HMAC-SHA256 under the test release key, and returns a
// snapshot directory holding that file.
func snapshotted(t *testing.T, cfgPath string) string {
	t.Helper()
	b, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	const at = "      source: groq-rate-limits-page.png\n      retrieved: 2026-10-07\n      "
	zero := `"` + strings.Repeat("0", 64) + `"`
	s := strings.ReplaceAll(string(b), at+"sha256: "+zero, at+`sha256: "`+testSnapshotSHA256()+`"`)
	s = strings.ReplaceAll(s, at+"hmac_sha256: "+zero, at+`hmac_sha256: "`+testReleaseKey.Sum(testSnapshot)+`"`)
	if err := os.WriteFile(cfgPath, []byte(s), 0o600); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	writeFile(t, dir, "groq-rate-limits-page.png", testSnapshot)
	return dir
}

// passingChecks are --ceiling-probe flags naming, for the configuration
// at each of paths, a record of its route's check that passed at its
// ceiling in the ledger at ledger, at byte cap 2000 for Shape S.
func passingChecks(t *testing.T, ledger string, paths ...string) []string {
	t.Helper()
	cfgs, err := loadReleases(paths)
	if err != nil {
		t.Fatal(err)
	}
	open := ledgerOpenSHA256(t, ledger)
	var args []string
	for _, c := range cfgs {
		byteCap := 0
		if c.Release.Shape.Name == config.ShapeS {
			byteCap = 2000
		}
		args = append(args, "--ceiling-probe", writeCeiling(t, hostCeiling(c, byteCap, c.Target.Name, c.Release.Shape.CeilingTokens, true, open)))
	}
	return args
}

// quickSolo is the release reference as route name at endpoint with no
// other host, whose route cap and limits pace its ceiling check's
// requests about 40 ms apart, its limits' retained copy in the returned
// directory (snapshotted).
func quickSolo(t *testing.T, name, endpoint string) (string, string) {
	t.Helper()
	path := routeConfigFile(t, name, endpoint, "600")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, r := range [][2]string{{"\n      value: 250000\n", "\n      value: 250000000\n"}, {"\n      value: 1000\n", "\n      value: 100000\n"}} {
		if strings.Count(s, r[0]) != 1 {
			t.Fatalf("configuration: %q not found once", r[0])
		}
		s = strings.Replace(s, r[0], r[1], 1)
	}
	if err := os.WriteFile(path, []byte(s), 0o600); err != nil {
		t.Fatal(err)
	}
	path = soloFile(t, path, name+".yaml")
	return path, snapshotted(t, path)
}

// faultMock is a loopback mock route at its chat-completions URL whose
// nth request, 1-based, fault answers where it returns true, and whose
// usage reports prompt prompt tokens while prompt is above 0, with the
// count of requests it received.
func faultMock(t *testing.T, fault func(n int32, w http.ResponseWriter) bool, prompt *atomic.Int32) (string, *atomic.Int32) {
	t.Helper()
	endpoint := startMock(t, mock.Shape{Key: testKey, Answer: "The customer was billed twice."})
	u, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	rp := httputil.NewSingleHostReverseProxy(&url.URL{Scheme: u.Scheme, Host: u.Host})
	rp.FlushInterval = -1
	reported := regexp.MustCompile(`"prompt_tokens":\s*\d+`)
	rp.ModifyResponse = func(r *http.Response) error {
		n := prompt.Load()
		if n <= 0 {
			return nil
		}
		b, err := io.ReadAll(r.Body)
		if err != nil {
			return err
		}
		r.Body.Close()
		b = reported.ReplaceAll(b, []byte(fmt.Sprintf(`"prompt_tokens":%d`, n)))
		r.Body, r.ContentLength = io.NopCloser(bytes.NewReader(b)), int64(len(b))
		r.Header.Del("Content-Length")
		return nil
	}
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fault != nil && fault(hits.Add(1), w) {
			return
		}
		if fault == nil {
			hits.Add(1)
		}
		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL + u.Path, &hits
}

// answer429 and answer529 answer a request as a rate limit and as an
// overload; drop closes its connection with no response.
func answer429(w http.ResponseWriter) {
	w.WriteHeader(http.StatusTooManyRequests)
	io.WriteString(w, `{"error":{"type":"rate_limit_exceeded"}}`) //nolint:errcheck
}

func answer529(w http.ResponseWriter) {
	w.WriteHeader(529)
	io.WriteString(w, `{"type":"error","error":{"type":"overloaded_error"}}`) //nolint:errcheck
}

func drop(w http.ResponseWriter) {
	if c, _, err := w.(http.Hijacker).Hijack(); err == nil {
		c.Close()
	}
}

// checker sends ceiling checks of the configurations it is given on
// ledger, writing under out, and returns the exit code, output and error.
type checker struct {
	t                           *testing.T
	ledger, env, out, snapshots string
}

func newChecker(t *testing.T, ledger, snapshots string) *checker {
	t.Helper()
	return &checker{t: t, ledger: ledger, env: writeFile(t, t.TempDir(), "groq.env", []byte("GROQ_API_KEY="+testKey+"\n")), out: t.TempDir(), snapshots: snapshots}
}

func (c *checker) args(cfg string, byteCap int) []string {
	return []string{"--config", cfg, "--env", c.env, "--ledger", c.ledger, "--out", c.out, "--byte-cap", fmt.Sprint(byteCap), "--snapshots", c.snapshots}
}

func (c *checker) check(ctx context.Context, cfg string, byteCap int, extra ...string) (int, string, error) {
	c.t.Helper()
	var code int
	var err error
	text := stdoutOf(c.t, func() { code, err = ceilingCheckCmd(ctx, append(c.args(cfg, byteCap), extra...)) })
	return code, text, err
}

// records are the records under the checker's out by route, in ledger
// order.
func (c *checker) records(route string) []string {
	c.t.Helper()
	m := checkRecordsOf(c.t, c.out, route)
	var seqs []int
	for s := range m {
		seqs = append(seqs, s)
	}
	slices.Sort(seqs)
	var out []string
	for _, s := range seqs {
		out = append(out, m[s])
	}
	return out
}

// A Shape S check that reports a prompt above the input bound spends its
// byte cap: no check is sent at that cap or above it, every Shape S
// record at it decides nothing, and a route is checked again at its
// ceiling only at a lower cap, which a route that passed there may do
// only once the cap is spent. A check that passed at the lower cap still
// decides. schedule takes the records, run reads them at the lower cap
// and refuses the spent one, and pages publishes each unrun route's page
// with its records, which it refuses once the ledger admits a cell of
// the route.
func TestInputBoundRecheck(t *testing.T) {
	var prompt atomic.Int32
	endpoint, hits := faultMock(t, nil, &prompt)
	groq, snapshots := quickSolo(t, "groq", endpoint)
	gemini, _ := quickSolo(t, "gemini", endpoint)
	ledger := releaseLedger(t, groq, gemini)
	c := newChecker(t, ledger, snapshots)
	ctx := context.Background()
	refused := func(name, cfg string, byteCap int, want string) {
		t.Helper()
		sent := hits.Load()
		if code, _, err := c.check(ctx, cfg, byteCap); code != exitError || err == nil || !strings.Contains(err.Error(), want) || hits.Load() != sent {
			t.Fatalf("%s: %d, %v; want %q, sending nothing", name, code, err, want)
		}
	}
	if code, text, err := c.check(ctx, gemini, 2000); code != exitOK || err != nil {
		t.Fatalf("gemini at 2000: %d, %v\n%s", code, err, text)
	}
	refused("gemini at 1800 before 2000 is spent", gemini, 1800, "decides at byte cap 2000; send the check with --byte-cap 2000")
	prompt.Store(1300)
	if code, text, err := c.check(ctx, groq, 2000); code != exitAdverse || err != nil || !strings.Contains(text, "groq: FAILED the input bound of 1200 tokens") {
		t.Fatalf("groq at 2000: %d, %v\n%s", code, err, text)
	}
	prompt.Store(0)
	for _, byteCap := range []int{2000, 2200} {
		refused(fmt.Sprintf("groq at %d", byteCap), groq, byteCap, "spent byte cap 2000, a prompt above the input bound; a check is sent below 2000")
	}
	refused("gemini at 2000", gemini, 2000, "spent byte cap 2000")
	for _, cfg := range []string{groq, gemini} {
		if code, text, err := c.check(ctx, cfg, 1800); code != exitOK || err != nil {
			t.Fatalf("%s at 1800: %d, %v\n%s", cfg, code, err, text)
		}
	}
	refused("groq at 1600 after a pass at 1800", groq, 1600, "decides at byte cap 1800; send the check with --byte-cap 1800")
	refused("groq at 1800 after a pass at 1800", groq, 1800, "decides (ran); it is not repeated")

	groqRecs, geminiRecs := c.records("groq"), c.records("gemini")
	if len(groqRecs) != 2 || len(geminiRecs) != 2 {
		t.Fatalf("records %v, %v", groqRecs, geminiRecs)
	}
	all := append(slices.Clone(groqRecs), geminiRecs...)
	cfgs, err := loadReleases([]string{groq, gemini})
	if err != nil {
		t.Fatal(err)
	}
	open := ledgerOpenSHA256(t, ledger)
	entries, err := ledgerEntries(ledger, open)
	if err != nil {
		t.Fatal(err)
	}
	if choice, _, _, err := ceilingChoice(cfgs[0], nil, groqRecs, 1800, open, entries); err != nil || choice.Ceiling != 512 {
		t.Fatalf("run of groq at 1800: %+v, %v", choice, err)
	}
	if _, _, _, err := ceilingChoice(cfgs[0], nil, groqRecs, 2000, open, entries); err == nil || !strings.Contains(err.Error(), "byte cap 1800, want --byte-cap 2000") {
		t.Fatalf("run of groq at 2000: %v", err)
	}
	if _, _, _, err := ceilingChoice(cfgs[1], nil, geminiRecs, 1800, open, entries); err == nil || !strings.Contains(err.Error(), "byte cap 2000, want --byte-cap 1800") {
		t.Fatalf("run of gemini without the record that spent 2000: %v", err)
	}
	if choice, _, _, err := ceilingChoice(cfgs[1], nil, append(slices.Clone(geminiRecs), groqRecs[0]), 1800, open, entries); err != nil || choice.Ceiling != 512 {
		t.Fatalf("run of gemini at 1800: %+v, %v", choice, err)
	}

	schedulePath := filepath.Join(t.TempDir(), "schedule.json")
	args := []string{"--master-seed", "percentes-release-test", "--first", "2026-10-17", "--last", "2026-10-26", "--config", groq, "--config", gemini,
		"--ledger", ledger, "--release-key", testReleaseKeyPath, "--schedule", schedulePath}
	for _, p := range all {
		args = append(args, "--ceiling-probe", p)
	}
	var code int
	if text := stdoutOf(t, func() { code, err = scheduleCmd(args) }); err != nil || code != exitOK {
		t.Fatalf("schedule: %d, %v\n%s", code, err, text)
	}

	out, pages := t.TempDir(), t.TempDir()
	pageArgs := []string{"--out", out, "--frames", t.TempDir(), "--config", groq, "--config", gemini, "--pages", pages, "--schedule", schedulePath, "--ledger", ledger}
	for _, p := range all {
		pageArgs = append(pageArgs, "--check-record", p)
	}
	text := stdoutOf(t, func() { code, err = pagesCmd(pageArgs) })
	if err != nil || code != exitOK || !strings.Contains(text, "groq: ran no cell; the ceiling check chose 512 (every host passed at 512); page ") {
		t.Fatalf("pages: %d, %v\n%s", code, err, text)
	}
	files := readPages(t, pages)
	page := string(files["routes/groq.txt"])
	if !strings.HasPrefix(page, "Route groq (Groq, model openai/gpt-oss-120b, Shape S at 512 and 1,024) ran no cell. Its host group's ceiling check chose 512 (every host passed at 512).\n") ||
		strings.Count(page, ", ran, sha256 ") != 1 || strings.Count(page, ", ran, superseded, sha256 ") != 1 || !strings.Contains(string(files[indexPage]), "Scheduled and ran no cell, with no row above:") {
		t.Fatalf("groq's page:\n%s\nindex:\n%s", page, files[indexPage])
	}
	var x routeExclusion
	if err := json.Unmarshal(files["routes/groq.json"], &x); err != nil || x.Excluded || x.Ceiling != 512 || len(x.Records) != 2 {
		t.Fatalf("groq.json %+v, %v", x, err)
	}
	for _, r := range x.Records {
		b, err := os.ReadFile(filepath.Join(out, "groq", r.Name))
		if sum := sha256.Sum256(b); err != nil || hex.EncodeToString(sum[:]) != r.SHA256 {
			t.Fatalf("record %s under --out: %v", r.Name, err)
		}
	}

	l, err := admission.Open(ledger)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Admit("gemini", admission.PurposeCell, 1, admission.ReleaseTokens(cfgs[1])); err != nil {
		t.Fatal(err)
	}
	pageArgs[slices.Index(pageArgs, "--pages")+1] = t.TempDir()
	if code, err := pagesCmd(pageArgs); code != exitError || err == nil || !strings.Contains(err.Error(), "pages: route gemini has no cell, and its ceiling-check records choose 512") {
		t.Fatalf("pages with a cell admitted of gemini: %d, %v", code, err)
	}
}

// repeatCheck refuses a check at a spent byte cap or above it, and lets
// a check at a lower cap pass a record the spent cap supersedes; a check
// that passed its input bound and failed its ceiling, at a cap no check
// spent, still decides at a lower cap. A refused check is repeated once
// after the quiet interval.
func TestRepeatCheckRule(t *testing.T) {
	cfg, err := config.LoadFile(routeConfigFile(t, "groq", "https://api.example.test/openai/v1/chat/completions", "10"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	rec := func(seq, byteCap int, edit func(*probe.ReleaseRecord)) priorCheck {
		r := passedCeiling(cfg, byteCap, "open")
		r.LedgerSeq, r.EndUTC = seq, now.Add(-time.Minute)
		if edit != nil {
			edit(r)
		}
		return priorCheck{fmt.Sprint(seq), r}
	}
	over := func(r *probe.ReleaseRecord) {
		for i := range r.Requests {
			n := 1300
			r.Requests[i].PromptTokens = &n
		}
	}
	failedCeiling := func(r *probe.ReleaseRecord) { r.Ceiling.Pass = false }
	limited := func(r *probe.ReleaseRecord) { r.Requests[3].Exchange.Status = http.StatusTooManyRequests }
	prior := func(ps ...priorCheck) map[int]priorCheck {
		m := map[int]priorCheck{}
		for _, p := range ps {
			m[p.rec.LedgerSeq] = p
		}
		return m
	}
	spentBy := func(ps ...priorCheck) map[int]*probe.ReleaseRecord {
		var rs []*probe.ReleaseRecord
		for _, p := range ps {
			rs = append(rs, p.rec)
		}
		return probe.Spent(rs)
	}
	exceeded, passed := rec(4, 2000, over), rec(5, 2000, nil)
	other := rec(6, 2000, over)
	other.rec.Route = "gemini"
	for _, c := range []struct {
		name    string
		prior   map[int]priorCheck
		spent   map[int]*probe.ReleaseRecord
		byteCap int
		want    string
	}{
		{"first check", nil, nil, 2000, ""},
		{"at the spent cap", prior(exceeded), spentBy(exceeded), 2000, "spent byte cap 2000"},
		{"above the spent cap", prior(exceeded), spentBy(exceeded), 2200, "spent byte cap 2000"},
		{"below the spent cap", prior(exceeded), spentBy(exceeded), 1800, ""},
		{"a pass at a cap another route spent", prior(passed), spentBy(other), 1800, ""},
		{"a pass at a cap no check spent", prior(passed), nil, 1800, "the check at 512 admitted at ledger line 5 decides (ran)"},
		{"a ceiling failure at a lower cap", prior(rec(4, 2000, failedCeiling)), nil, 1800, "decides (ran)"},
		{"a pass below the spent cap decides", prior(exceeded, rec(7, 1800, nil)), spentBy(exceeded), 1600, "the check at 512 admitted at ledger line 7 decides (ran)"},
		{"a refused check inside the quiet interval", prior(rec(4, 2000, limited)), nil, 2000, "may be repeated from"},
		{"a refused check superseded, inside the quiet interval", prior(rec(4, 2000, limited)), spentBy(other), 1800, "may be repeated from"},
	} {
		err := repeatCheck(cfg, c.prior, c.spent, c.byteCap, now)
		if c.want == "" && err != nil || c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)) {
			t.Errorf("%s: %v, want %q", c.name, err, c.want)
		}
	}
	quiet := rec(4, 2000, limited)
	quiet.rec.EndUTC = now.Add(-time.Hour)
	if err := repeatCheck(cfg, prior(quiet), nil, 2000, now); err != nil {
		t.Fatalf("a repeat after the quiet interval: %v", err)
	}
}

// A check answered an ordinary 429 and then interrupted is refused: it
// prints so, with the time its one repeat may start, and the next check
// waits the quiet interval.
func TestCeilingCheckRefusedThenInterrupted(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	endpoint, hits := faultMock(t, func(n int32, w http.ResponseWriter) bool {
		switch n {
		case 2:
			answer429(w)
			return true
		case 3:
			time.AfterFunc(5*time.Millisecond, func() { cancel(loadgen.ErrOperatorAbort) })
		}
		return false
	}, new(atomic.Int32))
	groq, snapshots := quickSolo(t, "groq", endpoint)
	c := newChecker(t, releaseLedger(t, groq), snapshots)
	code, text, err := c.check(ctx, groq, 2000)
	if err != nil || code != exitAdverse || !strings.Contains(text, "groq: REFUSED: a request was answered HTTP 429, no documented billing refusal; the check may be repeated once at 512 from ") ||
		strings.Contains(text, "STOPPED") {
		t.Fatalf("ceiling-check: %d, %v\n%s", code, err, text)
	}
	recs := c.records("groq")
	if len(recs) != 1 {
		t.Fatalf("records %v", recs)
	}
	if rec := readRecord(t, recs[0]); rec.Interrupted == "" || rec.Outcome() != probe.CheckRefused || rec.Sent() == probe.CeilingRequests {
		t.Fatalf("interrupted %q, outcome %s, %d sent", rec.Interrupted, rec.Outcome(), rec.Sent())
	}
	sent := hits.Load()
	if code, _, err := c.check(context.Background(), groq, 2000); code != exitError || err == nil || !strings.Contains(err.Error(), "may be repeated from") || hits.Load() != sent {
		t.Fatalf("the next check: %d, %v", code, err)
	}
}

// A check met by a 5xx status, or by a request with no response, is
// refused as an ordinary 429 is: its repeat after the quiet interval
// decides, and a refused repeat prints that it excludes the host group,
// whose exclusion names the status. A third check is refused.
func TestCeilingCheckServerErrorRefused(t *testing.T) {
	endpoint, hits := faultMock(t, func(n int32, w http.ResponseWriter) bool {
		switch n {
		case 4:
			answer529(w)
			return true
		case 16:
			drop(w)
			return true
		}
		return false
	}, new(atomic.Int32))
	groq, snapshots := quickSolo(t, "groq", endpoint)
	ledger := releaseLedger(t, groq)
	c := newChecker(t, ledger, snapshots)
	code, text, err := c.check(context.Background(), groq, 2000)
	if err != nil || code != exitAdverse || !strings.Contains(text, "groq: REFUSED: a request was answered HTTP 529, no documented billing refusal; the check may be repeated once at 512 from ") ||
		!strings.Contains(text, "the last check at that ceiling decides, and a refused one excludes the host group\n") {
		t.Fatalf("ceiling-check: %d, %v\n%s", code, err, text)
	}
	first := c.records("groq")[0]
	rewriteRecord(t, first, func(r *probe.ReleaseRecord) { r.EndUTC = r.EndUTC.Add(-time.Hour) })
	code, text, err = c.check(context.Background(), groq, 2000)
	seq := readRecord(t, first).LedgerSeq
	want := fmt.Sprintf("groq: REFUSED: a request got no response, no documented billing refusal; this check repeats the one refused at ledger line %d and decides: the host group is excluded, ceiling check inconclusive, no response at 512 (SPEC-HOSTED HA4, refused checks)\n", seq)
	if err != nil || code != exitAdverse || !strings.Contains(text, want) {
		t.Fatalf("the repeat: %d, %v\n%s", code, err, text)
	}
	sent := hits.Load()
	if code, _, err := c.check(context.Background(), groq, 2000); code != exitError || err == nil || !strings.Contains(err.Error(), "decides (refused); it is not repeated") || hits.Load() != sent {
		t.Fatalf("a third check: %d, %v", code, err)
	}
	cfgs, err := loadReleases([]string{groq})
	if err != nil {
		t.Fatal(err)
	}
	open := ledgerOpenSHA256(t, ledger)
	entries, err := ledgerEntries(ledger, open)
	if err != nil {
		t.Fatal(err)
	}
	choice, _, _, err := ceilingChoice(cfgs[0], nil, c.records("groq"), 2000, open, entries)
	if err != nil || !choice.Excluded || choice.Reason != "groq: ceiling check inconclusive, no response at 512" {
		t.Fatalf("run: %+v, %v", choice, err)
	}
}

// A check whose record is not written prints its outcome, settles its
// admission at the requests sent and writes its record to standard
// error, with no file left: the route's next check waits for the record,
// --recover writes no interrupted one, --recover --record writes the
// printed one, and schedule takes it.
func TestCeilingCheckUnwrittenRecordSettlesAndPrints(t *testing.T) {
	endpoint, _ := faultMock(t, nil, new(atomic.Int32))
	groq, snapshots := quickSolo(t, "groq", endpoint)
	ledger := releaseLedger(t, groq)
	c := newChecker(t, ledger, snapshots)
	if err := os.Chmod(c.out, 0o500); err != nil {
		t.Fatal(err)
	}
	var code int
	var text string
	var err error
	printed := stderrOf(t, func() { code, text, err = c.check(context.Background(), groq, 2000) })
	if err := os.Chmod(c.out, 0o700); err != nil {
		t.Fatal(err)
	}
	if code != exitError || err == nil || !strings.Contains(err.Error(), "the record is not written") ||
		!strings.Contains(err.Error(), "is settled at 10 requests sent") ||
		!strings.Contains(err.Error(), "the record is on standard error above, and ceiling-check --recover --record <a file of it> writes it") ||
		!strings.Contains(text, "groq: Shape S ceiling 512: 10 of 10 sent passed;") || !strings.Contains(text, "written nothing: the write failed") {
		t.Fatalf("ceiling-check: %d, %v\n%s", code, err, text)
	}
	st := ledgerState(t, ledger)
	if u := st.Routes["groq"]; u == nil || u.Probe != probe.CeilingRequests || u.Reserved != 0 {
		t.Fatalf("ledger usage %+v", u)
	}
	if es, _ := os.ReadDir(c.out); len(es) != 0 {
		t.Fatalf("--out holds %v", es)
	}
	if code, _, err := c.check(context.Background(), groq, 2000); code != exitError || err == nil || !strings.Contains(err.Error(), "--recover --record <a file of it>") {
		t.Fatalf("the next check: %d, %v", code, err)
	}
	if code, text, err := c.check(context.Background(), groq, 2000, "--recover"); code != exitError || err == nil ||
		!strings.Contains(text, "the route's newest ceiling check, is settled and has no record under --out") {
		t.Fatalf("--recover: %d, %v\n%s", code, err, text)
	}
	record := writeFile(t, t.TempDir(), "record.json", []byte(printed))
	if code, text, err := c.check(context.Background(), groq, 2000, "--recover", "--record", record); code != exitOK || err != nil || !strings.Contains(text, ": record written ") {
		t.Fatalf("--recover --record: %d, %v\n%s", code, err, text)
	}
	if code, _, err := c.check(context.Background(), groq, 2000, "--recover", "--record", record); code != exitError || err == nil || !strings.Contains(err.Error(), "already has its record") {
		t.Fatalf("--recover --record again: %d, %v", code, err)
	}
	if rec := readRecord(t, c.records("groq")[0]); rec.Outcome() != probe.CheckRan || rec.Sent() != probe.CeilingRequests {
		t.Fatalf("record %s, %d sent", rec.Outcome(), rec.Sent())
	}
	open := ledgerOpenSHA256(t, ledger)
	entries, err := ledgerEntries(ledger, open)
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduleChecks(c.records("groq"), entries, open); err != nil {
		t.Fatal(err)
	}
	if left, _ := filepath.Glob(filepath.Join(c.out, "*", "*", ".probe-*")); len(left) != 0 {
		t.Fatalf("temporary files %v", left)
	}
}

// ceiling-check sends nothing and admits nothing without --snapshots on a
// route whose limits name a file, or where a retained copy is missing or
// does not match its recorded hash.
func TestCeilingCheckSnapshotsBeforeAdmission(t *testing.T) {
	endpoint, hits := faultMock(t, nil, new(atomic.Int32))
	groq, _ := quickSolo(t, "groq", endpoint)
	ledger := releaseLedger(t, groq)
	other := t.TempDir()
	writeFile(t, other, "groq-rate-limits-page.png", []byte("another copy"))
	for _, c := range []struct {
		name, snapshots, want string
	}{
		{"no --snapshots", "", `--snapshots is required: the requests_per_minute limit's source "groq-rate-limits-page.png" is a file`},
		{"mismatched", other, `retained copy "groq-rate-limits-page.png" does not match its recorded sha256`},
		{"missing", t.TempDir(), `retained copy "groq-rate-limits-page.png" not read: no such file`},
	} {
		ch := newChecker(t, ledger, c.snapshots)
		args := ch.args(groq, 2000)
		if c.snapshots == "" {
			args = args[:len(args)-2]
		}
		if code, err := ceilingCheckCmd(context.Background(), args); code != exitError || err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %d, %v; want %q", c.name, code, err, c.want)
		}
	}
	if b, err := os.ReadFile(ledger); err != nil || hits.Load() != 0 || bytes.Contains(b, []byte(`"purpose":"probe"`)) {
		t.Fatalf("%d requests sent; ledger %s, %v", hits.Load(), b, err)
	}
}

// --recover refuses a configuration or byte cap other than the ones the
// check's admission line records, settling nothing, and recovers with
// them.
func TestCeilingCheckRecoverRefusesOtherCheck(t *testing.T) {
	endpoint, _ := faultMock(t, nil, new(atomic.Int32))
	groq, snapshots := quickSolo(t, "groq", endpoint)
	ledger := releaseLedger(t, groq)
	cfgs, err := loadReleases([]string{groq})
	if err != nil {
		t.Fatal(err)
	}
	l, err := admission.Open(ledger)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.AdmitCeilingCheck("groq", "groq", probe.CeilingRequests, admission.ReleaseProbeTokens(cfgs[0]), 1024, 2000); err != nil {
		t.Fatal(err)
	}
	c := newChecker(t, ledger, snapshots)
	for _, byteCap := range []int{2000, 1800} {
		if code, _, err := c.check(context.Background(), groq, byteCap, "--recover"); code != exitError || err == nil ||
			!strings.Contains(err.Error(), fmt.Sprintf("admits the check at ceiling 1024 and byte cap 2000; --config runs at 512 and --byte-cap is %d", byteCap)) {
			t.Fatalf("--recover at 512 and %d: %d, %v", byteCap, code, err)
		}
	}
	if u := ledgerState(t, ledger).Routes["groq"]; u.Reserved != probe.CeilingRequests {
		t.Fatalf("ledger usage %+v", u)
	}
	at1024 := fallbackFile(t, groq)
	if code, _, err := c.check(context.Background(), at1024, 2000, "--recover"); code != exitOK || err != nil {
		t.Fatalf("--recover at 1024: %d, %v", code, err)
	}
	if rec := readRecord(t, c.records("groq")[0]); rec.Ceiling.Ceiling != 1024 || rec.ByteCap != 2000 {
		t.Fatalf("recovered record at %d, byte cap %d", rec.Ceiling.Ceiling, rec.ByteCap)
	}
}

// ceiling-check sends nothing once the ledger admits run's traffic.
func TestCeilingCheckAfterFreeze(t *testing.T) {
	endpoint, hits := faultMock(t, nil, new(atomic.Int32))
	groq, snapshots := quickSolo(t, "groq", endpoint)
	ledger := releaseLedger(t, groq)
	cfgs, err := loadReleases([]string{groq})
	if err != nil {
		t.Fatal(err)
	}
	l, err := admission.Open(ledger)
	if err != nil {
		t.Fatal(err)
	}
	a, err := l.Admit("groq", admission.PurposeCell, 1, admission.ReleaseTokens(cfgs[0]))
	if err != nil {
		t.Fatal(err)
	}
	c := newChecker(t, ledger, snapshots)
	if code, _, err := c.check(context.Background(), groq, 2000); code != exitError || err == nil ||
		err.Error() != fmt.Sprintf("ceiling-check: ledger line %d admits run's traffic of route groq: the schedule is frozen, and a ceiling check after it blocks the pages", a.Seq) || hits.Load() != 0 {
		t.Fatalf("ceiling-check: %d, %v, %d requests", code, err, hits.Load())
	}
}

// A route whose own ceiling-check records report a model id run refuses
// it on is excluded as not characterized where pages and schedule read
// the records; run refuses it.
func TestModelFaultRouteExcluded(t *testing.T) {
	groq, _ := quickSolo(t, "groq", "https://api.example.test/openai/v1/chat/completions")
	cfgs, err := loadReleases([]string{groq})
	if err != nil {
		t.Fatal(err)
	}
	rec := hostCeiling(cfgs[0], 2000, "groq", 512, true, "open")
	for i := range rec.Requests {
		rec.Requests[i].Shape.Models = []string{"openai/gpt-oss-120b-v2"}
	}
	path := writeCeiling(t, rec)
	const fault = `the ceiling check reported model "openai/gpt-oss-120b-v2", the requested "openai/gpt-oss-120b", and target.reported_model records no documentation naming both as one build: identifier-not-documented`
	if _, _, _, err := ceilingChoice(cfgs[0], nil, []string{path}, 2000, "open", nil); err == nil || err.Error() != "run: route groq is not characterized (SPEC-HOSTED §2): "+fault {
		t.Fatalf("run: %v", err)
	}
	x, err := excludedRoute(cfgs[0], cfgs, []string{path}, 2000, "open", nil, false)
	if err != nil || !x.Excluded || x.Reason != "not characterized (SPEC-HOSTED §2): "+fault || len(x.Records) != 1 {
		t.Fatalf("pages: %+v, %v", x, err)
	}
	if want := "Route groq (Groq, model openai/gpt-oss-120b, Shape S at 512 and 1,024) is excluded from the release: not characterized (SPEC-HOSTED §2): " + fault + ".\nIt ran no cell.\n"; !strings.HasPrefix(x.text(), want) {
		t.Fatalf("page:\n%s", x.text())
	}
	if reasons, err := scheduleChoices(cfgs, []string{path}, nil, "open"); err != nil || reasons["groq"] != "not characterized (SPEC-HOSTED §2): "+fault {
		t.Fatalf("schedule: %v, %v", reasons, err)
	}
}
