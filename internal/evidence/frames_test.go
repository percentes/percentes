package evidence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
)

const (
	frameKey      = "tok-FRAMES-0123456789abcdef"
	frameUser     = "frames-user-0001"
	framePassword = "frames-pass-0002"
)

var caseIndex = regexp.MustCompile(`cs-\d+-(\d+) `)

// readFrames returns the frame store's records.
func readFrames(t *testing.T, path string) []FrameRecord {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []FrameRecord
	for _, m := range lines(t, b) {
		raw, _ := json.Marshal(m)
		var rec FrameRecord
		if err := json.Unmarshal(raw, &rec); err != nil {
			t.Fatal(err)
		}
		out = append(out, rec)
	}
	return out
}

// The frame store of a hosted run holds no bearer token and no base-URL
// credential, raw or decoded, whole or split across reads, in streamed
// frames or in a non-200 body.
func TestFrameStoreHoldsNoCredentials(t *testing.T) {
	var sawAuth atomic.Bool
	secrets := []string{frameKey, frameUser, framePassword}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		auth := req.Header.Get("Authorization")
		sawAuth.Store(auth == "Bearer "+frameKey)
		b, _ := io.ReadAll(req.Body)
		m := caseIndex.FindSubmatch(b)
		if m == nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		echo := auth + " " + frameUser + ":" + framePassword
		if string(m[1]) == "1" {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"bad key ` + echo + `"}`)) //nolint:errcheck
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		half := len(frameKey) / 2
		for _, part := range []string{
			`data: {"choices":[{"delta":{"content":"` + echo + `"}}]}` + "\n\n",
			`data: {"choices":[{"delta":{"content":"` + frameKey[:half],
			frameKey[half:] + `"}}]}` + "\n\n: " + frameUser[:5],
			frameUser[5:] + "\n\ndata: [DONE]\n\n",
		} {
			w.Write([]byte(part)) //nolint:errcheck
			w.(http.Flusher).Flush()
			time.Sleep(20 * time.Millisecond)
		}
	}))
	defer srv.Close()
	t.Setenv("PERCENTES_FRAMES_TEST_KEY", frameKey)
	cfg := hostedCfg(strings.Replace(srv.URL, "http://", "http://"+frameUser+":"+framePassword+"@", 1))
	cfg.Target.APIKeyEnv = "PERCENTES_FRAMES_TEST_KEY"

	res, err := loadgen.Run(context.Background(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !sawAuth.Load() {
		t.Fatal("server did not receive the bearer token; the test proves nothing")
	}
	path, err := WriteRunFrames(t.TempDir(), &cfg.Target, res)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	half := len(frameKey) / 2
	for _, s := range append(secrets, frameKey[:half], frameKey[half:], frameUser[5:]) {
		if bytes.Contains(file, []byte(s)) {
			t.Errorf("frame store carries %q", s)
		}
	}

	recs := readFrames(t, path)
	if len(recs) != len(res.Requests) || len(recs) != 3 {
		t.Fatalf("%d records for %d requests, want 3", len(recs), len(res.Requests))
	}
	for i, rec := range recs {
		var decoded []byte
		prev := res.Requests[i].DispatchNs
		for _, f := range rec.Frames {
			if f.Ns < prev {
				t.Errorf("request %d: frame at %d before %d", i, f.Ns, prev)
			}
			prev = f.Ns
			decoded = append(decoded, f.Raw()...)
		}
		if rec.ErrorBody != nil {
			decoded = append(decoded, rec.ErrorBody.Raw()...)
		}
		for _, s := range secrets {
			if bytes.Contains(decoded, []byte(s)) {
				t.Errorf("request %d: decoded bytes carry %q", i, s)
			}
		}
		masked := strings.Repeat("*", len(frameKey))
		switch i {
		case 1:
			if rec.Status != http.StatusUnauthorized || rec.Frames != nil || !strings.Contains(string(decoded), "bad key Bearer "+masked) {
				t.Errorf("request 1: status %d, %d frames, body %q", rec.Status, len(rec.Frames), decoded)
			}
		default:
			if rec.Status != http.StatusOK || len(rec.Frames) < 2 || rec.ErrorBody != nil ||
				strings.Count(string(decoded), masked) != 2 || !strings.HasSuffix(string(decoded), "data: [DONE]\n\n") {
				t.Errorf("request %d: status %d, %d frames, bytes %q", i, rec.Status, len(rec.Frames), decoded)
			}
		}
	}
}

// With no credential to mask, the frames are the body's bytes in order,
// bytes that are not UTF-8 included.
func TestFramesAreTheBodyBytes(t *testing.T) {
	parts := []string{
		`data: {"choices":[{"delta":{"content":"a"}}]}` + "\n\n",
		": \xff\xfe\n\n",
		`data: {"choices":[{"delta":{"content":"b"},"finish_reason":"stop"}]}` + "\n\ndata: [DONE]\n\n",
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for _, p := range parts {
			w.Write([]byte(p)) //nolint:errcheck
			w.(http.Flusher).Flush()
			time.Sleep(20 * time.Millisecond)
		}
	}))
	defer srv.Close()

	cfg := hostedCfg(srv.URL)
	res, err := loadgen.Run(context.Background(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	path, err := WriteRunFrames(t.TempDir(), &cfg.Target, res)
	if err != nil {
		t.Fatal(err)
	}
	for i, rec := range readFrames(t, path) {
		var got []byte
		b64 := false
		for _, f := range rec.Frames {
			got = append(got, f.Raw()...)
			b64 = b64 || f.Base64 != nil
		}
		if string(got) != strings.Join(parts, "") || !b64 || rec.DroppedBytes != 0 {
			t.Errorf("request %d: %q (base64 frame %v, dropped %d)", i, got, b64, rec.DroppedBytes)
		}
	}
}

// A target whose retention keeps no model output has each 200 body's
// bytes counted and none kept, beside the withheld header values and a
// non-200 body; one with no retention, or protocol_period, keeps them.
func TestFrameStoreRetention(t *testing.T) {
	headers := map[string][]string{"X-Ratelimit-Limit-Tokens": {"limit-tokens-250000"}}
	res := &loadgen.Result{EpochWall: time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC), Requests: []loadgen.Request{
		{Index: 0, Status: http.StatusOK, RetainedHeaders: headers, FrameBytesDropped: 7,
			Frames: []loadgen.Frame{{Ns: 10, Data: []byte(`data: {"choices":[{"delta":{"content":"answer-qz31"}}]}` + "\n\n")}, {Ns: 20, Data: []byte("data: [DONE]\n\n")}}},
		{Index: 1, Status: http.StatusTooManyRequests, RetainedHeaders: headers, ErrorBody: []byte(`{"error":"slow down"}`)},
	}}
	body := int64(len(res.Requests[0].Frames[0].Data) + len(res.Requests[0].Frames[1].Data))
	for _, tc := range []struct {
		name      string
		retention *config.Retention
		keep      bool
	}{
		{"no retention", nil, true},
		{"protocol_period", &config.Retention{ModelOutput: config.RetainProtocolPeriod}, true},
		{"none", &config.Retention{ModelOutput: config.RetainNone}, false},
	} {
		path, err := WriteRunFrames(t.TempDir(), &config.Target{Retention: tc.retention}, res)
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		recs := readFrames(t, path)
		if len(recs) != 2 {
			t.Fatalf("%s: %d records", tc.name, len(recs))
		}
		ok, refused := recs[0], recs[1]
		if tc.keep {
			if len(ok.Frames) != 2 || ok.DroppedBytes != 7 || !bytes.Contains(b, []byte("answer-qz31")) {
				t.Errorf("%s: %d frames, dropped %d", tc.name, len(ok.Frames), ok.DroppedBytes)
			}
		} else if ok.Frames != nil || ok.DroppedBytes != 7+body || bytes.Contains(b, []byte("answer-qz31")) {
			t.Errorf("%s: %d frames, dropped %d, want none and %d", tc.name, len(ok.Frames), ok.DroppedBytes, 7+body)
		}
		if ok.Status != http.StatusOK || !reflect.DeepEqual(ok.Headers, headers) || !reflect.DeepEqual(refused.Headers, headers) ||
			refused.ErrorBody == nil || string(refused.ErrorBody.Raw()) != `{"error":"slow down"}` || refused.DroppedBytes != 0 {
			t.Errorf("%s: records %+v %+v", tc.name, ok, refused)
		}
	}
}

// A retained header's values are in the frame store and not in the
// attempts file, which carries the stand-in for each value; a published
// header keeps its values in the attempts file.
func TestRetainedHeadersInTheFrameStoreOnly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		h := w.Header()
		h.Set("X-Ratelimit-Limit-Tokens", "limit-tokens-250000")
		h.Set("X-Ratelimit-Remaining-Tokens", "remaining-249000")
		h.Set("Content-Type", "text/event-stream")
		w.Write([]byte(`data: {"choices":[{"delta":{"content":"a"}}]}` + "\n\ndata: [DONE]\n\n")) //nolint:errcheck
	}))
	defer srv.Close()
	cfg := hostedCfg(srv.URL)
	cfg.Target.ResponseHeaders = []string{"x-ratelimit-*"}
	cfg.Target.RetainedHeaders = []string{"x-ratelimit-limit-*"}

	res, err := loadgen.Run(context.Background(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	attemptsPath, err := WriteRun(dir, cfg, res, Options{})
	if err != nil {
		t.Fatal(err)
	}
	attempts, err := os.ReadFile(attemptsPath)
	if err != nil {
		t.Fatal(err)
	}
	framesPath, err := WriteRunFrames(t.TempDir(), &cfg.Target, res)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(attempts, []byte("limit-tokens")) {
		t.Errorf("attempts file carries a retained value:\n%s", attempts)
	}
	recs, err := ReadRecords(bytes.NewReader(attempts))
	if err != nil {
		t.Fatal(err)
	}
	frames := readFrames(t, framesPath)
	if len(recs) == 0 || len(frames) != len(recs) {
		t.Fatalf("%d attempts, %d frame records", len(recs), len(frames))
	}
	for i := range recs {
		published := map[string][]string{
			"X-Ratelimit-Limit-Tokens":     {config.RetainedHeaderValue},
			"X-Ratelimit-Remaining-Tokens": {"remaining-249000"},
		}
		kept := map[string][]string{"X-Ratelimit-Limit-Tokens": {"limit-tokens-250000"}}
		if !reflect.DeepEqual(recs[i].Headers, published) || !reflect.DeepEqual(frames[i].Headers, kept) {
			t.Errorf("request %d: attempts headers %v, frame store headers %v", i, recs[i].Headers, frames[i].Headers)
		}
	}
}

// On a target with a confidential limit, an in-band error's text is in
// the frame store and not in the attempts file, which carries its
// stand-in with the text's SHA-256 and length; without one the attempts
// file carries the text and the frame store does not.
func TestStreamErrorInTheFrameStoreOnly(t *testing.T) {
	const inBand = `{"type":"rate_limit_error","message":"300001 tokens per minute"}`
	res := &loadgen.Result{EpochWall: time.Date(2026, 10, 17, 8, 20, 0, 0, time.UTC), Requests: []loadgen.Request{
		{Index: 0, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrMalformedStream, Status: 200, StreamError: inBand},
		{Index: 1, Outcome: loadgen.OutcomeCompleted, Status: 200},
	}}
	sum := sha256.Sum256([]byte(inBand))
	for _, tc := range []struct {
		name            string
		cfg             *config.Config
		published, kept string
		sha256          string
		bytes           int
	}{
		{"confidential", confidentialCfg(t), `{"type":"rate_limit_error"}`, inBand, hex.EncodeToString(sum[:]), len(inBand)},
		{"public", releaseCfg(t), inBand, "", "", 0},
	} {
		attemptsPath, err := WriteRun(t.TempDir(), tc.cfg, res, Options{})
		if err != nil {
			t.Fatal(err)
		}
		attempts, err := os.ReadFile(attemptsPath)
		if err != nil {
			t.Fatal(err)
		}
		recs, err := ReadRecords(bytes.NewReader(attempts))
		if err != nil {
			t.Fatal(err)
		}
		framesPath, err := WriteRunFrames(t.TempDir(), &tc.cfg.Target, res)
		if err != nil {
			t.Fatal(err)
		}
		frames := readFrames(t, framesPath)
		if len(recs) != 2 || len(frames) != 2 {
			t.Fatalf("%s: %d attempts, %d frame records", tc.name, len(recs), len(frames))
		}
		if r := recs[0]; r.StreamError != tc.published || r.StreamErrorSHA256 != tc.sha256 || r.StreamErrorBytes != tc.bytes || frames[0].StreamError != tc.kept {
			t.Errorf("%s: attempts %q %q %d, frame store %q", tc.name, r.StreamError, r.StreamErrorSHA256, r.StreamErrorBytes, frames[0].StreamError)
		}
		if r := recs[1]; r.StreamError != "" || r.StreamErrorSHA256 != "" || r.StreamErrorBytes != 0 || frames[1].StreamError != "" {
			t.Errorf("%s: no in-band error: attempts %+v, frame store %+v", tc.name, r, frames[1])
		}
		if tc.kept != "" && bytes.Contains(attempts, []byte("300001")) {
			t.Errorf("%s: attempts file carries the withheld text:\n%s", tc.name, attempts)
		}
	}
}

// The store is never written inside the instrument's source tree, and
// never over an existing file; the file is readable by its owner only.
func TestFrameStorePlacement(t *testing.T) {
	res := &loadgen.Result{EpochWall: time.Date(2026, 10, 6, 20, 0, 0, 0, time.UTC), Requests: []loadgen.Request{{Index: 0}}}

	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	inRepo := filepath.Join(wd, "frames-must-not-exist")
	t.Cleanup(func() { os.RemoveAll(inRepo) })
	if _, err := WriteRunFrames(inRepo, &config.Target{}, res); err == nil {
		t.Errorf("frame store written inside the repository at %s", inRepo)
	}

	tree := t.TempDir()
	if err := os.WriteFile(filepath.Join(tree, "go.mod"), []byte("// clone\nmodule github.com/percentes/percentes\n\ngo 1.21\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := WriteRunFrames(filepath.Join(tree, "a", "b"), &config.Target{}, res); err == nil {
		t.Error("frame store written inside a source tree")
	}
	if _, err := os.Stat(filepath.Join(tree, "a")); !os.IsNotExist(err) {
		t.Errorf("refused store left %s/a behind: %v", tree, err)
	}
	if err := CheckFramesRoot(""); err == nil {
		t.Error("empty frame store directory accepted")
	}

	other := t.TempDir()
	if err := os.WriteFile(filepath.Join(other, "go.mod"), []byte("module example.com/estate\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	path, err := WriteRunFrames(filepath.Join(other, "frames"), &config.Target{}, res)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(other, "frames", "20261006T200000.000000000Z", FramesFile); path != want {
		t.Errorf("path %s, want %s", path, want)
	}
	if fi, err := os.Stat(path); err != nil {
		t.Fatal(err)
	} else if fi.Mode().Perm() != 0o600 {
		t.Errorf("file mode %v, want 0600", fi.Mode().Perm())
	}
	if b, _ := os.ReadFile(path); string(b) != `{"index":0}`+"\n" {
		t.Errorf("file %q", b)
	}
	if _, err := WriteRunFrames(filepath.Join(other, "frames"), &config.Target{}, res); err == nil {
		t.Error("existing frame store overwritten")
	}
}
