package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
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
