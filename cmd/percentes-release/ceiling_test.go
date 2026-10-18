package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
