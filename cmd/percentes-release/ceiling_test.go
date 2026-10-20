package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/mock"
	"github.com/percentes/percentes/internal/probe"
)

// ceilingFixture is a route's ceiling-check inputs: its configuration
// file at endpoint, a priced ledger and an env file holding key.
type ceilingFixture struct {
	cfg, ledger, env, out string
}

func newCeilingFixture(t *testing.T, endpoint, key string) *ceilingFixture {
	t.Helper()
	t.Setenv("GROQ_API_KEY", "")
	dir := t.TempDir()
	f := &ceilingFixture{cfg: routeConfigFile(t, "groq", endpoint, "10"), ledger: filepath.Join(dir, "ledger.jsonl"),
		env: writeFile(t, dir, "groq.env", []byte("GROQ_API_KEY="+key+"\n")), out: t.TempDir()}
	if code, err := ledgerCmd([]string{"create", "--ledger", f.ledger, "--config", f.cfg}); err != nil || code != exitOK {
		t.Fatalf("ledger create: %d, %v", code, err)
	}
	if code, err := ledgerCmd([]string{"price", "--ledger", f.ledger, "--config", f.cfg, "--input", "0.15", "--output", "0.60",
		"--source", "https://example.test/pricing", "--retrieved", "2026-10-06"}); err != nil || code != exitOK {
		t.Fatalf("ledger price: %d, %v", code, err)
	}
	return f
}

func (f *ceilingFixture) args() []string {
	return []string{"--config", f.cfg, "--env", f.env, "--ledger", f.ledger, "--out", f.out, "--byte-cap", "2000"}
}

// record reads the check's one record under out.
func (f *ceilingFixture) record(t *testing.T) (string, *probe.ReleaseRecord) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(f.out, "ceiling-*", "groq-512", probe.File))
	if err != nil || len(paths) != 1 {
		t.Fatalf("records %v, %v", paths, err)
	}
	b, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	var rec probe.ReleaseRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		t.Fatal(err)
	}
	return paths[0], &rec
}

// probeCharge is the route's settled probe requests and its open
// reservation in the ledger.
func (f *ceilingFixture) probeCharge(t *testing.T) (int, int) {
	t.Helper()
	l, err := admission.Open(f.ledger)
	if err != nil {
		t.Fatal(err)
	}
	st, err := l.State()
	if err != nil {
		t.Fatal(err)
	}
	if u := st.Routes["groq"]; u != nil {
		return u.Probe, u.Reserved
	}
	return 0, 0
}

// priced512 is a new ledger for the route at cfgPath holding its price at
// the 512-token ceiling only.
func priced512(t *testing.T, cfgPath string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	if code, err := ledgerCmd([]string{"create", "--ledger", path, "--config", cfgPath}); err != nil || code != exitOK {
		t.Fatalf("ledger create: %d, %v", code, err)
	}
	l, err := admission.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.RecordPrice("groq", admission.Price{InputUSDPerMTok: "0.15", OutputUSDPerMTok: "0.60", InputTokens: 1200, OutputTokens: 512,
		Source: "https://example.test/pricing", Retrieved: "2026-10-06"}); err != nil {
		t.Fatal(err)
	}
	return path
}

// fallbackFile is the configuration at cfgPath at the 1,024-token
// ceiling.
func fallbackFile(t *testing.T, cfgPath string) string {
	t.Helper()
	b, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, r := range [][2]string{{"ceiling_tokens: 512 ", "ceiling_tokens: 1024 "}, {`"max_completion_tokens": 512,`, `"max_completion_tokens": 1024,`}} {
		if strings.Count(s, r[0]) != 1 {
			t.Fatalf("configuration: %q not found once", r[0])
		}
		s = strings.Replace(s, r[0], r[1], 1)
	}
	return writeFile(t, t.TempDir(), "groq-1024.yaml", []byte(s))
}

// byRoute are cfgs' targets by route.
func byRoute(cfgs []*config.Config) map[string]*config.Target {
	out := map[string]*config.Target{}
	for _, c := range cfgs {
		out[c.Target.Name] = &c.Target
	}
	return out
}

func startMock(t *testing.T, sh mock.Shape) string {
	t.Helper()
	fixed := config.LatencyDist{Distribution: "fixed"}
	s := mock.NewShaped(config.Mock{ListenAddr: "127.0.0.1:0", Seed: 1, TTFT: fixed, ITL: fixed}, sh)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return "http://" + s.Addr() + "/v1/chat/completions"
}

// ceiling-check sends the ten requests at the pinned spacing, settles
// them in the ledger, and writes a record run accepts as the route's
// check at its ceiling.
func TestCeilingCheckCmd(t *testing.T) {
	f := newCeilingFixture(t, startMock(t, mock.Shape{Key: testKey, Answer: "The customer was billed twice."}), testKey)
	code, err := ceilingCheckCmd(context.Background(), f.args())
	if err != nil || code != exitOK {
		t.Fatalf("ceiling-check: %d, %v", code, err)
	}
	path, rec := f.record(t)
	if !rec.Ceiling.Pass || rec.Ceiling.Sent != probe.CeilingRequests || !rec.InputBound.Pass || rec.Start.Blocked || rec.ByteCap != 2000 {
		t.Fatalf("check %+v, input bound %+v, start %+v, byte cap %d", rec.Ceiling, rec.InputBound, rec.Start, rec.ByteCap)
	}
	open := ledgerOpenSHA256(t, f.ledger)
	if rec.LedgerSHA256 != open {
		t.Fatalf("ledger sha256 %q, want %s", rec.LedgerSHA256, open)
	}
	for i, r := range rec.Requests {
		if !r.Pass || r.Index != i || r.FinishReason != "stop" {
			t.Fatalf("request %d: %+v", i, r)
		}
	}
	if sent, reserved := f.probeCharge(t); sent != probe.CeilingRequests || reserved != 0 {
		t.Fatalf("ledger: %d probe requests, %d reserved", sent, reserved)
	}
	// The check is admitted at the highest pinned ceiling.
	if b, _ := os.ReadFile(f.ledger); !strings.Contains(string(b), `"kind":"admit","route":"groq","input_tokens":1200,"output_tokens":1024,"per_request_usd":"0.0007944","purpose":"probe","reserve":10`) {
		t.Fatalf("ledger %s", b)
	}
	cfgs, err := loadReleases([]string{f.cfg})
	if err != nil {
		t.Fatal(err)
	}
	hosts := byRoute(hostConfigs(t, cfgs[0]))
	baseten := writeCeiling(t, hostCeiling(cfgs[0], 2000, "baseten", 512, true, ledgerOpenSHA256(t, f.ledger)))
	choice, copies, err := readCeilingProbes(cfgs[0], hosts, []string{path, baseten}, 2000, ledgerOpenSHA256(t, f.ledger))
	if err != nil || choice.Ceiling != 512 || len(copies) != 2 {
		t.Fatalf("run reads the record: %+v, %d copies, %v", choice, len(copies), err)
	}
	if _, _, err := readCeilingProbes(cfgs[0], hosts, []string{path, baseten}, 1800, ledgerOpenSHA256(t, f.ledger)); err == nil || !strings.Contains(err.Error(), "byte cap 2000, want --byte-cap 1800") {
		t.Fatalf("another byte cap: %v", err)
	}
	frozen := strings.Repeat("0", 64)
	if _, _, err := readCeilingProbes(cfgs[0], hosts, []string{path, baseten}, 2000, frozen); err == nil ||
		!strings.Contains(err.Error(), fmt.Sprintf("route groq at 512: ledger open line sha256 %q; the schedule froze %s", open, frozen)) {
		t.Fatalf("another ledger: %v", err)
	}
}

// ceiling-check settles in the ledger whose open line it read first: with
// another ledger moved to its path during the check, the settle is
// refused and that ledger is left as it was.
func TestCeilingCheckLedgerSwapped(t *testing.T) {
	var f *ceilingFixture
	var other string
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			if err := os.Rename(other, f.ledger); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(srv.Close)
	f = newCeilingFixture(t, srv.URL+"/v1/chat/completions", testKey)
	open := ledgerOpenSHA256(t, f.ledger)
	other = priced512(t, f.cfg)
	swapped := ledgerOpenSHA256(t, other)
	if swapped == open {
		t.Fatalf("two ledgers open with sha256 %s", open)
	}
	before, err := os.ReadFile(other)
	if err != nil {
		t.Fatal(err)
	}
	code, err := ceilingCheckCmd(context.Background(), f.args())
	if want := "admission: ledger open line sha256 " + swapped + "; bound to " + open; err == nil || code != exitError || !strings.Contains(err.Error(), want) {
		t.Fatalf("ceiling-check: %d, %v; want %q", code, err, want)
	}
	if n := hits.Load(); n != 1 {
		t.Fatalf("%d requests sent", n)
	}
	if after, err := os.ReadFile(f.ledger); err != nil || !bytes.Equal(after, before) {
		t.Fatalf("the other ledger changed: %v", err)
	}
}

// withHeaders serves endpoint's route through a proxy that sets h on
// every response, and returns the proxy's URL for it and its request
// count.
func withHeaders(t *testing.T, endpoint string, h map[string]string) (string, *atomic.Int32) {
	t.Helper()
	u, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	rp := httputil.NewSingleHostReverseProxy(&url.URL{Scheme: u.Scheme, Host: u.Host})
	rp.FlushInterval = -1
	rp.ModifyResponse = func(r *http.Response) error {
		for k, v := range h {
			r.Header.Set(k, v)
		}
		return nil
	}
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL + u.Path, &hits
}

// Under target.retained_headers, ceiling-check refuses to start without
// --frames; with it, the record carries the stand-in for each retained
// value, here on the 401 that blocks the check, and the frame store holds
// the values. Run passes the record on to the ceiling rule, and refuses
// this route's record carrying a retained value.
func TestCeilingCheckWithholds(t *testing.T) {
	endpoint, hits := withHeaders(t, startMock(t, mock.Shape{Key: testKey, Answer: "The customer was billed twice."}),
		map[string]string{"X-Ratelimit-Limit-Requests": "1000", "X-Ratelimit-Remaining-Requests": "qz7919"})
	f := newCeilingFixture(t, endpoint, "wrong-"+testKey)
	b, err := os.ReadFile(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	line := "  response_headers: [Retry-After, x-ratelimit-*]\n"
	if strings.Count(string(b), line) != 1 {
		t.Fatalf("configuration: %q not found once", line)
	}
	f.cfg = writeFile(t, t.TempDir(), "groq.yaml", []byte(strings.Replace(string(b), line, line+"  retained_headers: [x-ratelimit-remaining-*]\n", 1)))

	if code, err := ceilingCheckCmd(context.Background(), f.args()); err == nil || code != exitError || !strings.Contains(err.Error(), "--frames is required") {
		t.Fatalf("without --frames: %d, %v", code, err)
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("%d requests sent without --frames", n)
	}
	frames := t.TempDir()
	if code, err := ceilingCheckCmd(context.Background(), append(f.args(), "--frames", frames)); err != nil || code != exitAdverse {
		t.Fatalf("ceiling-check: %d, %v", code, err)
	}
	path, rec := f.record(t)
	const published = "map[X-Ratelimit-Limit-Requests:[1000] X-Ratelimit-Remaining-Requests:[[retained]]]"
	if len(rec.Requests) != 1 || rec.Requests[0].Exchange.Status != http.StatusUnauthorized || fmt.Sprint(rec.Requests[0].Exchange.Headers) != published {
		t.Fatalf("requests %+v", rec.Requests)
	}
	if fmt.Sprint(rec.RateLimitHeaders) != published || rec.LimitsFrom != probe.LimitsFromHeaders {
		t.Fatalf("rate-limit headers %v, limits from %q", rec.RateLimitHeaders, rec.LimitsFrom)
	}
	raw, err := os.ReadFile(path)
	if err != nil || bytes.Contains(raw, []byte("qz7919")) {
		t.Fatalf("%s carries a retained value (%v)", path, err)
	}
	rel, err := filepath.Rel(f.out, filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	held, err := os.ReadFile(filepath.Join(frames, rel, probe.RetainedFile))
	if err != nil {
		t.Fatal(err)
	}
	if n := bytes.Count(held, []byte(`"qz7919"`)); n != 1 {
		t.Fatalf("retained file holds the value %d times:\n%s", n, held)
	}

	cfgs, err := loadReleases([]string{f.cfg})
	if err != nil {
		t.Fatal(err)
	}
	hosts := byRoute(hostConfigs(t, cfgs[0]))
	baseten := writeCeiling(t, hostCeiling(cfgs[0], 2000, "baseten", 512, true, ledgerOpenSHA256(t, f.ledger)))
	if _, _, err := readCeilingProbes(cfgs[0], hosts, []string{path, baseten}, 2000, ledgerOpenSHA256(t, f.ledger)); err == nil || !strings.Contains(err.Error(), "probe: route groq: the probe was blocked") {
		t.Fatalf("run reads the record: %v", err)
	}
	rec.Requests[0].Exchange.Headers["X-Ratelimit-Remaining-Requests"] = []string{"qz7919"}
	leaked := writeCeiling(t, *rec)
	if _, _, err := readCeilingProbes(cfgs[0], hosts, []string{leaked, baseten}, 2000, ledgerOpenSHA256(t, f.ledger)); err == nil ||
		!strings.Contains(err.Error(), "route groq at 512: carries values the route withholds: X-Ratelimit-Remaining-Requests") {
		t.Fatalf("a record carrying a retained value: %v", err)
	}
}

// markConfidential rewrites the configuration at path with its
// tokens_per_minute limit at 250,013.5, marked confidential, and its
// rate-limit headers retained.
func markConfidential(t *testing.T, path string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	const public = "      value: 250000\n      marking: public\n      counting: input_and_output\n      source: groq-rate-limits-page.png\n      retrieved: 2026-10-07\n      sha256:"
	if !bytes.Contains(b, []byte(public)) || !bytes.Contains(b, []byte(rateLimitHeaders)) {
		t.Fatal("configuration: the lines this test edits not found")
	}
	b = bytes.Replace(b, []byte(rateLimitHeaders), []byte(rateLimitHeaders+"  retained_headers: [x-ratelimit-*]\n"), 1)
	if err := os.WriteFile(path, bytes.Replace(b, []byte(public), []byte("      value: 250013.5\n      marking: confidential\n      counting: input_and_output\n      source: groq-rate-limits-page.png\n      retrieved: 2026-10-07\n      hmac_sha256:"), 1), 0o600); err != nil {
		t.Fatal(err)
	}
}

// A confidential limit's snapshot in the check's record carries no value,
// and run refuses a record of the route whose snapshot carries one, and
// the record with a value or text after it or a key written twice.
func TestCeilingCheckConfidential(t *testing.T) {
	f := newCeilingFixture(t, startMock(t, mock.Shape{Key: testKey, Answer: "The customer was billed twice."}), testKey)
	markConfidential(t, f.cfg)
	if code, err := ceilingCheckCmd(context.Background(), append(f.args(), "--frames", t.TempDir(), "--release-key", testReleaseKeyPath)); err != nil || code != exitOK {
		t.Fatalf("ceiling-check: %d, %v", code, err)
	}
	path, rec := f.record(t)
	raw, err := os.ReadFile(path)
	if err != nil || bytes.Contains(raw, []byte("250013.5")) || bytes.Contains(raw, []byte("2.500135e+05")) {
		t.Fatalf("%s carries the confidential value (%v)", path, err)
	}
	if s := rec.Snapshots[1]; s.Marking != config.MarkingConfidential || s.Value != 0 || s.Dimension != config.LimitTPM {
		t.Fatalf("snapshot %+v", s)
	}
	cfgs, err := loadReleases([]string{f.cfg})
	if err != nil {
		t.Fatal(err)
	}
	hosts := byRoute(hostConfigs(t, cfgs[0]))
	baseten := writeCeiling(t, hostCeiling(cfgs[0], 2000, "baseten", 512, true, ledgerOpenSHA256(t, f.ledger)))
	if _, _, err := readCeilingProbes(cfgs[0], hosts, []string{path, baseten}, 2000, ledgerOpenSHA256(t, f.ledger)); err != nil {
		t.Fatalf("run reads the record: %v", err)
	}
	for name, edit := range map[string]func([]byte) []byte{
		"a second value":  func(b []byte) []byte { return append(b, `{"tokens_per_minute":250013.5}`...) },
		"a tail not JSON": func(b []byte) []byte { return append(b, " tpm=250013.5 }}}{{"...) },
		"a duplicate key": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"limits_from":`), []byte(`"limits_from": "tpm 250013.5",
  "limits_from":`), 1)
		},
		"a case-folded duplicate key": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"limits_from":`), []byte(`"LIMITS_FROM": "tpm 250013.5",
  "limits_from":`), 1)
		},
	} {
		edited := edit(slices.Clone(raw))
		if bytes.Equal(edited, raw) {
			t.Fatalf("%s: the edit did not apply", name)
		}
		file := writeFile(t, t.TempDir(), probe.File, edited)
		if _, _, err := readCeilingProbes(cfgs[0], hosts, []string{file, baseten}, 2000, ledgerOpenSHA256(t, f.ledger)); err == nil ||
			err.Error() != "ceiling probe "+file+": not the record as ceiling-check writes it" {
			t.Errorf("%s: %v", name, err)
		}
	}
	rec.Snapshots[1].Value = 250013.5
	leaked := writeCeiling(t, *rec)
	if _, _, err := readCeilingProbes(cfgs[0], hosts, []string{leaked, baseten}, 2000, ledgerOpenSHA256(t, f.ledger)); err == nil ||
		!strings.Contains(err.Error(), "route groq at 512: carries values the route withholds: snapshot 1 value") {
		t.Fatalf("a record carrying the confidential value: %v", err)
	}
}

// Another host's run judges a ceiling-check record by the configuration
// of the record's route it is given: the record of a check taken while a
// limit was public and no header was retained is refused once that
// configuration retains a header or marks the limit confidential, and
// without that configuration.
func TestCeilingCheckHostConfig(t *testing.T) {
	endpoint, _ := withHeaders(t, startMock(t, mock.Shape{Key: testKey, Answer: "The customer was billed twice."}),
		map[string]string{"X-Ratelimit-Limit-Requests": "1000", "X-Ratelimit-Remaining-Requests": "qz7919"})
	f := newCeilingFixture(t, endpoint, testKey)
	if code, err := ceilingCheckCmd(context.Background(), f.args()); err != nil || code != exitOK {
		t.Fatalf("ceiling-check: %d, %v", code, err)
	}
	path, rec := f.record(t)
	if s := rec.Snapshots[1]; s.Marking != config.MarkingPublic || s.Value != 250000 || fmt.Sprint(rec.RateLimitHeaders) != "map[X-Ratelimit-Limit-Requests:[1000] X-Ratelimit-Remaining-Requests:[qz7919]]" {
		t.Fatalf("snapshot %+v, rate-limit headers %v", s, rec.RateLimitHeaders)
	}
	open := ledgerOpenSHA256(t, f.ledger)
	cfgs, err := loadReleases([]string{f.cfg, routeConfigFile(t, "baseten", endpoint, "10")})
	if err != nil {
		t.Fatal(err)
	}
	baseten := cfgs[1]
	own := writeCeiling(t, hostCeiling(baseten, 2000, "baseten", 512, true, open))
	read := func(groq ...*config.Config) error {
		_, _, err := readCeilingProbes(baseten, byRoute(groq), []string{own, path}, 2000, open)
		return err
	}
	if err := read(cfgs[0]); err != nil {
		t.Fatalf("under the configuration the check ran with: %v", err)
	}
	if err := read(); err == nil || !strings.Contains(err.Error(), "route groq at 512: no configuration of the route") {
		t.Fatalf("no configuration of groq: %v", err)
	}
	b, err := os.ReadFile(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte(rateLimitHeaders)) {
		t.Fatalf("configuration: %q not found", rateLimitHeaders)
	}
	retained := writeFile(t, t.TempDir(), "groq.yaml", bytes.Replace(b, []byte(rateLimitHeaders), []byte(rateLimitHeaders+"  retained_headers: [x-ratelimit-remaining-*]\n"), 1))
	confidential := writeFile(t, t.TempDir(), "groq.yaml", b)
	markConfidential(t, confidential)
	for _, c := range []struct{ name, path, want string }{
		{"a header retained", retained, "route groq at 512: carries values the route withholds: X-Ratelimit-Remaining-Requests"},
		{"the limit confidential", confidential,
			"route groq at 512: carries values the route withholds: X-Ratelimit-Limit-Requests, X-Ratelimit-Remaining-Requests, snapshot 1 value, snapshot 1 sha256"},
	} {
		groq, err := config.LoadFile(c.path)
		if err != nil {
			t.Fatal(err)
		}
		if err := read(groq); err == nil || !strings.HasSuffix(err.Error(), c.want) {
			t.Errorf("%s: %v; want %q", c.name, err, c.want)
		}
	}
}

// A record taken while the route had no tokens_per_minute limit and
// recorded its rate-limit headers in clear is refused, by the route's own
// run and by another host's, once the frozen configuration adds that
// limit marked confidential and records Retry-After alone: the provider's
// echo of the limit in X-Ratelimit-Limit-Tokens is a header that
// configuration does not keep in clear.
func TestCeilingCheckNarrowedHeaders(t *testing.T) {
	endpoint, _ := withHeaders(t, startMock(t, mock.Shape{Key: testKey, Answer: "The customer was billed twice."}),
		map[string]string{"X-Ratelimit-Limit-Tokens": "250000"})
	f := newCeilingFixture(t, endpoint, testKey)
	orig, err := os.ReadFile(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	const tpm = "    - dimension: tokens_per_minute\n      value: 250000\n      marking: public\n      counting: input_and_output\n      source: groq-rate-limits-page.png\n      retrieved: 2026-10-07\n      sha256: \"0000000000000000000000000000000000000000000000000000000000000000\"\n"
	if !bytes.Contains(orig, []byte(tpm)) || !bytes.Contains(orig, []byte(rateLimitHeaders)) {
		t.Fatal("configuration: the lines this test edits not found")
	}
	if err := os.WriteFile(f.cfg, bytes.Replace(orig, []byte(tpm), nil, 1), 0o600); err != nil {
		t.Fatal(err)
	}
	if code, err := ceilingCheckCmd(context.Background(), f.args()); err != nil || code != exitOK {
		t.Fatalf("ceiling-check: %d, %v", code, err)
	}
	path, rec := f.record(t)
	if len(rec.Snapshots) != 1 || fmt.Sprint(rec.RateLimitHeaders) != "map[X-Ratelimit-Limit-Tokens:[250000]]" {
		t.Fatalf("snapshots %+v, rate-limit headers %v", rec.Snapshots, rec.RateLimitHeaders)
	}
	conf := strings.Replace(strings.Replace(tpm, "marking: public", "marking: confidential", 1), "sha256:", "hmac_sha256:", 1)
	frozen := bytes.Replace(bytes.Replace(orig, []byte(tpm), []byte(conf), 1), []byte(rateLimitHeaders), []byte("  response_headers: [Retry-After]\n"), 1)
	groq, err := config.LoadFile(writeFile(t, t.TempDir(), "groq.yaml", frozen))
	if err != nil {
		t.Fatal(err)
	}
	if !groq.Target.Confidential() || len(groq.Target.RetainedHeaders) != 0 {
		t.Fatalf("frozen configuration: confidential %t, retained_headers %v", groq.Target.Confidential(), groq.Target.RetainedHeaders)
	}
	open := ledgerOpenSHA256(t, f.ledger)
	const want = "route groq at 512: carries values the route withholds: X-Ratelimit-Limit-Tokens"

	sibling := writeCeiling(t, hostCeiling(groq, 2000, "baseten", 512, true, open))
	if _, _, err := readCeilingProbes(groq, byRoute(hostConfigs(t, groq)), []string{path, sibling}, 2000, open); err == nil || !strings.HasSuffix(err.Error(), want) {
		t.Errorf("groq's own run: %v; want %q", err, want)
	}
	cfgs, err := loadReleases([]string{routeConfigFile(t, "baseten", endpoint, "10")})
	if err != nil {
		t.Fatal(err)
	}
	own := writeCeiling(t, hostCeiling(cfgs[0], 2000, "baseten", 512, true, open))
	if _, _, err := readCeilingProbes(cfgs[0], byRoute([]*config.Config{groq}), []string{own, path}, 2000, open); err == nil || !strings.HasSuffix(err.Error(), want) {
		t.Errorf("baseten's run: %v; want %q", err, want)
	}
}

// A record taken while the route had no tokens_per_minute limit and kept
// its limits body in clear is refused, by the route's own run and by
// another host's, once the frozen configuration adds that limit marked
// confidential and reads no limits URL or keeps the limits body
// unpublished: the body's echo of the limit is one that configuration
// does not keep in clear.
func TestCeilingCheckLimitsBody(t *testing.T) {
	endpoint := startMock(t, mock.Shape{Key: testKey, Answer: "The customer was billed twice."})
	u, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	rp := httputil.NewSingleHostReverseProxy(&url.URL{Scheme: u.Scheme, Host: u.Host})
	rp.FlushInterval = -1
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/limits" {
			fmt.Fprint(w, `{"tokens_per_minute": 250000}`)
			return
		}
		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	endpoint = srv.URL + u.Path
	f := newCeilingFixture(t, endpoint, testKey)
	orig, err := os.ReadFile(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	const tpm = "    - dimension: tokens_per_minute\n      value: 250000\n      marking: public\n      counting: input_and_output\n      source: groq-rate-limits-page.png\n      retrieved: 2026-10-07\n      sha256: \"0000000000000000000000000000000000000000000000000000000000000000\"\n"
	if !bytes.Contains(orig, []byte(tpm)) || !bytes.Contains(orig, []byte(rateLimitHeaders)) {
		t.Fatal("configuration: the lines this test edits not found")
	}
	const narrowed = "  response_headers: [Retry-After]\n"
	read := "  probe:\n    limits_url: " + srv.URL + "/limits\n    limits_auth_scheme: Bearer\n"
	taken := bytes.Replace(bytes.Replace(orig, []byte(tpm), nil, 1), []byte(rateLimitHeaders), []byte(narrowed+read), 1)
	if err := os.WriteFile(f.cfg, taken, 0o600); err != nil {
		t.Fatal(err)
	}
	if code, err := ceilingCheckCmd(context.Background(), f.args()); err != nil || code != exitOK {
		t.Fatalf("ceiling-check: %d, %v", code, err)
	}
	path, rec := f.record(t)
	if b := rec.Limits; b == nil || b.Body == nil || b.Body.Text == nil || *b.Body.Text != `{"tokens_per_minute": 250000}` {
		t.Fatalf("limits read %+v", rec.Limits)
	}
	conf := strings.Replace(strings.Replace(tpm, "marking: public", "marking: confidential", 1), "sha256:", "hmac_sha256:", 1)
	open := ledgerOpenSHA256(t, f.ledger)
	const want = "route groq at 512: carries values the route withholds: limits body"
	for _, c := range []struct{ name, probe string }{
		{"no probe block", ""},
		{"no limits_url", "  probe:\n    account_limit_statuses: [429]\n"},
		{"limits_retained", read + "    limits_retained: true\n"},
	} {
		frozen := bytes.Replace(bytes.Replace(orig, []byte(tpm), []byte(conf), 1), []byte(rateLimitHeaders), []byte(narrowed+c.probe), 1)
		groq, err := config.LoadFile(writeFile(t, t.TempDir(), "groq.yaml", frozen))
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		sibling := writeCeiling(t, hostCeiling(groq, 2000, "baseten", 512, true, open))
		if _, _, err := readCeilingProbes(groq, byRoute(hostConfigs(t, groq)), []string{path, sibling}, 2000, open); err == nil || !strings.HasSuffix(err.Error(), want) {
			t.Errorf("%s: groq's own run: %v; want %q", c.name, err, want)
		}
		cfgs, err := loadReleases([]string{routeConfigFile(t, "baseten", endpoint, "10")})
		if err != nil {
			t.Fatal(err)
		}
		own := writeCeiling(t, hostCeiling(cfgs[0], 2000, "baseten", 512, true, open))
		if _, _, err := readCeilingProbes(cfgs[0], byRoute([]*config.Config{groq}), []string{own, path}, 2000, open); err == nil || !strings.HasSuffix(err.Error(), want) {
			t.Errorf("%s: baseten's run: %v; want %q", c.name, err, want)
		}
	}
}

// A check whose responses report a model id other than the requested
// one, with no target.reported_model recorded, is written and exits
// adverse with the refusal run gives its record.
func TestCeilingCheckReportedModel(t *testing.T) {
	f := newCeilingFixture(t, startMock(t, mock.Shape{Key: testKey, Answer: "The customer was billed twice.", Model: "openai/gpt-oss-120b-v2"}), testKey)
	var code int
	var err error
	out := stdoutOf(t, func() { code, err = ceilingCheckCmd(context.Background(), f.args()) })
	if err != nil || code != exitAdverse {
		t.Fatalf("ceiling-check: %d, %v\n%s", code, err, out)
	}
	path, rec := f.record(t)
	if !rec.Ceiling.Pass || !rec.InputBound.Pass || rec.ExpectedModel != "openai/gpt-oss-120b" {
		t.Fatalf("check %+v, input bound %+v, expected model %q", rec.Ceiling, rec.InputBound, rec.ExpectedModel)
	}
	const fault = `the ceiling check reported model "openai/gpt-oss-120b-v2", the requested "openai/gpt-oss-120b", and target.reported_model records no documentation naming both as one build: identifier-not-documented`
	if line := "groq: REFUSED: run refuses the route (SPEC-HOSTED §2): " + fault + "\n"; !strings.Contains(out, line) {
		t.Fatalf("output lacks %q:\n%s", line, out)
	}
	cfgs, err := loadReleases([]string{f.cfg})
	if err != nil {
		t.Fatal(err)
	}
	hosts := byRoute(hostConfigs(t, cfgs[0]))
	baseten := writeCeiling(t, hostCeiling(cfgs[0], 2000, "baseten", 512, true, ledgerOpenSHA256(t, f.ledger)))
	if _, _, err := readCeilingProbes(cfgs[0], hosts, []string{path, baseten}, 2000, ledgerOpenSHA256(t, f.ledger)); err == nil || err.Error() != "run: route groq is not characterized (SPEC-HOSTED §2): "+fault {
		t.Fatalf("run reads the record: %v", err)
	}
}

// The refusal ceiling-check prints is run's on a record reporting a model
// id: another id than the expected one, or than a recorded
// target.reported_model. A record reporting none is left to run.
func TestModelRefusal(t *testing.T) {
	cfg := releaseConfig(t, "https://api.example.test/openai/v1/chat/completions")
	tg := &cfg.Target
	rec := func(models ...string) *probe.ReleaseRecord {
		r := hostCeiling(cfg, 2000, "groq", 512, true, "")
		reports(models...)(&r)
		return &r
	}
	documented := &config.ReportedModel{Name: "openai/gpt-oss-120b-2026", Source: "models.html", Retrieved: "2026-10-12", SHA256: strings.Repeat("0", 64)}
	for _, c := range []struct {
		name     string
		recorded *config.ReportedModel
		rec      *probe.ReleaseRecord
		want     string
	}{
		{"requested reported", nil, rec("openai/gpt-oss-120b"), ""},
		{"another reported", nil, rec("openai/gpt-oss-120b", "openai/gpt-oss-120b-v2"),
			`the ceiling check reported model "openai/gpt-oss-120b-v2", the requested "openai/gpt-oss-120b", and target.reported_model records no documentation naming both as one build: identifier-not-documented`},
		{"recorded reported", documented, rec("openai/gpt-oss-120b-2026"), ""},
		{"requested reported under a record", documented, rec("openai/gpt-oss-120b"),
			`the ceiling check reported model "openai/gpt-oss-120b", not target.reported_model "openai/gpt-oss-120b-2026"`},
		{"none reported under a record", documented, rec(), ""},
		{"none reported", nil, rec(), ""},
	} {
		tg.ReportedModel = c.recorded
		if got := modelRefusal(tg, c.rec); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
}

// A check the start gate blocks ends at its first request, is settled at
// one request, is recorded, and exits adverse.
func TestCeilingCheckCmdBlocked(t *testing.T) {
	f := newCeilingFixture(t, startMock(t, mock.Shape{Key: testKey, Answer: "The customer was billed twice."}), "wrong-"+testKey)
	code, err := ceilingCheckCmd(context.Background(), f.args())
	if err != nil || code != exitAdverse {
		t.Fatalf("ceiling-check: %d, %v", code, err)
	}
	_, rec := f.record(t)
	if !rec.Start.Blocked || rec.Sent() != 1 || rec.Requests[0].Exchange.Status != http.StatusUnauthorized || rec.Ceiling.Pass {
		t.Fatalf("start %+v, sent %d, check %+v", rec.Start, rec.Sent(), rec.Ceiling)
	}
	if sent, reserved := f.probeCharge(t); sent != 1 || reserved != 0 {
		t.Fatalf("ledger: %d probe requests, %d reserved", sent, reserved)
	}
}

// ceiling-check refuses each defect of its inputs before any request.
func TestCeilingCheckCmdRefusals(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	endpoint := srv.URL + "/v1/chat/completions"
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, f *ceilingFixture) []string
		want   string
	}{
		{"no price", func(t *testing.T, f *ceilingFixture) []string {
			f.ledger = filepath.Join(t.TempDir(), "ledger.jsonl")
			if code, err := ledgerCmd([]string{"create", "--ledger", f.ledger, "--config", f.cfg}); err != nil || code != exitOK {
				t.Fatalf("ledger create: %d, %v", code, err)
			}
			return f.args()
		}, "no price"},
		{"priced at the ceiling only", func(t *testing.T, f *ceilingFixture) []string {
			f.ledger = priced512(t, f.cfg)
			return f.args()
		}, "no price recorded for groq at 1200 input and 1024 output tokens"},
		{"fallback priced at 512 only", func(t *testing.T, f *ceilingFixture) []string {
			f.cfg = fallbackFile(t, f.cfg)
			f.ledger = priced512(t, f.cfg)
			return f.args()
		}, "no price recorded for groq at 1200 input and 1024 output tokens"},
		{"caps differ", func(t *testing.T, f *ceilingFixture) []string {
			f.cfg = routeConfigFile(t, "groq", endpoint, "9")
			return f.args()
		}, "ledger cap for route groq"},
		{"no key", func(t *testing.T, f *ceilingFixture) []string {
			f.env = writeFile(t, t.TempDir(), "other.env", []byte("OTHER=1\n"))
			return f.args()
		}, "does not set GROQ_API_KEY"},
		{"pilot profile", func(t *testing.T, f *ceilingFixture) []string {
			f.cfg = "../../configs/pilot.reference.yaml"
			return f.args()
		}, "want \"release\""},
		{"byte cap", func(t *testing.T, f *ceilingFixture) []string { return append(f.args(), "--byte-cap", "0") }, "--byte-cap 0"},
		{"no ledger", func(t *testing.T, f *ceilingFixture) []string { return append(f.args(), "--ledger", "") }, "are required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCeilingFixture(t, endpoint, testKey)
			code, err := ceilingCheckCmd(context.Background(), tc.mutate(t, f))
			if err == nil || code != exitError || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("%d, %v; want %q", code, err, tc.want)
			}
			if sent, reserved := f.probeCharge(t); sent != 0 || reserved != 0 {
				t.Fatalf("ledger: %d probe requests, %d reserved", sent, reserved)
			}
		})
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("%d requests reached the endpoint", n)
	}
}

// On a route with a confidential limit, ceiling-check refuses to start
// without the release key. With it, the 401 body that blocks the check
// publishes as a stand-in with its HMAC-SHA256 under the key and neither
// its SHA-256 nor its length, the confidential limit's retained copy is
// checked against the hmac_sha256 key sum prints for it, and nothing the
// command prints or writes carries the key.
func TestCeilingCheckKeyedHashes(t *testing.T) {
	endpoint, hits := withHeaders(t, startMock(t, mock.Shape{Key: testKey, Answer: "The customer was billed twice."}), nil)
	f := newCeilingFixture(t, endpoint, "wrong-"+testKey)
	markConfidential(t, f.cfg)
	snapshots := t.TempDir()
	shot := []byte("console: tokens per minute 250013.5")
	shotPath := writeFile(t, snapshots, "groq-rate-limits-page.png", shot)
	var code int
	var err error
	sum := stdoutOf(t, func() { code, err = keyCmd([]string{"sum", "--key", testReleaseKeyPath, shotPath}) })
	if err != nil || code != exitOK || sum != testReleaseKey.Sum(shot)+"  "+shotPath+"\n" {
		t.Fatalf("key sum: %d, %v, %q", code, err, sum)
	}
	b, err := os.ReadFile(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	recorded := `hmac_sha256: "` + strings.Repeat("0", 64) + `"`
	if strings.Count(string(b), recorded) != 1 {
		t.Fatalf("configuration: %q not found once", recorded)
	}
	f.cfg = writeFile(t, t.TempDir(), "groq.yaml", []byte(strings.Replace(string(b), recorded, `hmac_sha256: "`+testReleaseKey.Sum(shot)+`"`, 1)))
	frames := t.TempDir()
	args := append(f.args(), "--frames", frames, "--snapshots", snapshots)

	if code, err := ceilingCheckCmd(context.Background(), args); err == nil || code != exitError || !strings.Contains(err.Error(), "--release-key is required") {
		t.Fatalf("without --release-key: %d, %v", code, err)
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("%d requests sent without --release-key", n)
	}
	printed := stdoutOf(t, func() {
		code, err = ceilingCheckCmd(context.Background(), append(args, "--release-key", testReleaseKeyPath))
	})
	if err != nil || code != exitAdverse {
		t.Fatalf("ceiling-check: %d, %v\n%s", code, err, printed)
	}
	path, rec := f.record(t)
	rel, err := filepath.Rel(f.out, filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	heldRaw, err := os.ReadFile(filepath.Join(frames, rel, probe.RetainedFile))
	if err != nil {
		t.Fatal(err)
	}
	var held probe.RetainedRecord
	if err := json.Unmarshal(heldRaw, &held); err != nil {
		t.Fatal(err)
	}
	kept := held.Exchanges["requests.0"]
	if kept == nil || kept.Body == nil || kept.Body.Text == nil || *kept.Body.Text == "" {
		t.Fatalf("retained file:\n%s", heldRaw)
	}
	body := []byte(*kept.Body.Text)
	if len(rec.Requests) != 1 || rec.Requests[0].Exchange.Status != http.StatusUnauthorized {
		t.Fatalf("requests %+v", rec.Requests)
	}
	if s := rec.Requests[0].Exchange.Body; s == nil || s.Text != nil || !s.Retained || s.SHA256 != "" || s.HMACSHA256 != testReleaseKey.Sum(body) {
		t.Fatalf("body stand-in %+v", s)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	plain := sha256.Sum256(body)
	if bytes.Contains(raw, body) || bytes.Contains(raw, []byte(hex.EncodeToString(plain[:]))) || bytes.Contains(raw, []byte(`"bytes"`)) {
		t.Fatalf("%s carries the body, its SHA-256 or its length:\n%s", path, raw)
	}
	if s := rec.Snapshots[1]; s.Marking != config.MarkingConfidential || s.HMACSHA256 != testReleaseKey.Sum(shot) || s.SHA256 != "" || s.Retained == nil || !*s.Retained {
		t.Fatalf("confidential snapshot %+v", s)
	}
	key, err := os.ReadFile(testReleaseKeyPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(printed+sum), strings.TrimSpace(string(key))) {
		t.Fatalf("the output carries the release key:\n%s%s", printed, sum)
	}
	noReleaseKey(t, f.out, frames)
}
