package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
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
	baseten := writeCeiling(t, hostCeiling(cfgs[0], 2000, "baseten", 512, true))
	choice, copies, err := readCeilingProbes(cfgs[0], []string{path, baseten}, 2000)
	if err != nil || choice.Ceiling != 512 || len(copies) != 2 {
		t.Fatalf("run reads the record: %+v, %d copies, %v", choice, len(copies), err)
	}
	if _, _, err := readCeilingProbes(cfgs[0], []string{path, baseten}, 1800); err == nil || !strings.Contains(err.Error(), "byte cap 2000, want --byte-cap 1800") {
		t.Fatalf("another byte cap: %v", err)
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
	baseten := writeCeiling(t, hostCeiling(cfgs[0], 2000, "baseten", 512, true))
	if _, _, err := readCeilingProbes(cfgs[0], []string{path, baseten}, 2000); err == nil || !strings.Contains(err.Error(), "probe: route groq: the probe was blocked") {
		t.Fatalf("run reads the record: %v", err)
	}
	rec.Requests[0].Exchange.Headers["X-Ratelimit-Remaining-Requests"] = []string{"qz7919"}
	leaked := writeCeiling(t, *rec)
	if _, _, err := readCeilingProbes(cfgs[0], []string{leaked, baseten}, 2000); err == nil ||
		!strings.Contains(err.Error(), "route groq at 512: carries values the route withholds: X-Ratelimit-Remaining-Requests") {
		t.Fatalf("a record carrying a retained value: %v", err)
	}
}

// markConfidential rewrites the configuration at path with its
// tokens_per_minute limit at 250,013.5, marked confidential.
func markConfidential(t *testing.T, path string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	const public = "      value: 250000\n      marking: public\n"
	if !bytes.Contains(b, []byte(public)) {
		t.Fatal("configuration: the tokens_per_minute lines this test edits not found")
	}
	if err := os.WriteFile(path, bytes.Replace(b, []byte(public), []byte("      value: 250013.5\n      marking: confidential\n"), 1), 0o600); err != nil {
		t.Fatal(err)
	}
}

// A confidential limit's snapshot in the check's record carries no value,
// and run refuses a record of the route whose snapshot carries one.
func TestCeilingCheckConfidential(t *testing.T) {
	f := newCeilingFixture(t, startMock(t, mock.Shape{Key: testKey, Answer: "The customer was billed twice."}), testKey)
	markConfidential(t, f.cfg)
	if code, err := ceilingCheckCmd(context.Background(), f.args()); err != nil || code != exitOK {
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
	baseten := writeCeiling(t, hostCeiling(cfgs[0], 2000, "baseten", 512, true))
	if _, _, err := readCeilingProbes(cfgs[0], []string{path, baseten}, 2000); err != nil {
		t.Fatalf("run reads the record: %v", err)
	}
	rec.Snapshots[1].Value = 250013.5
	leaked := writeCeiling(t, *rec)
	if _, _, err := readCeilingProbes(cfgs[0], []string{leaked, baseten}, 2000); err == nil ||
		!strings.Contains(err.Error(), "route groq at 512: carries values the route withholds: snapshot 1 value") {
		t.Fatalf("a record carrying the confidential value: %v", err)
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
