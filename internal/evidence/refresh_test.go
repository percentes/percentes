package evidence

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/releasekey"
)

// refreshRun is a refresh command's run that exited 0 and printed out.
func refreshRun(out string) *StatusRecord {
	code := 0
	return &StatusRecord{Command: []string{"refresh"}, ExitCode: &code, Stdout: Bytes{Text: out}, Stderr: Bytes{Text: "fetched"}}
}

// The refresh command's output gives each recorded limit's value, or why
// it could not be read, in target.limits order; anything else is refused.
func TestReadRefreshedLimits(t *testing.T) {
	cfg := releaseCfg(t)
	got, err := ReadRefreshedLimits(refreshRun(`{"limits": [{"dimension": "requests_per_minute", "value": 500},
		{"dimension": "tokens_per_minute", "unavailable": "console only"}]}`+"\n"), &cfg.Target)
	want := []RefreshedLimit{{Dimension: config.LimitRPM, Value: 500}, {Dimension: config.LimitTPM, Unavailable: "console only"}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("read %+v, %v; want %+v", got, err, want)
	}
	failed, signalled := refreshRun(""), refreshRun("")
	failed.Error = "exit status 4"
	signalled.ExitCode = nil
	for _, tc := range []struct {
		rec  *StatusRecord
		want string
	}{
		{failed, "the command failed: exit status 4"},
		{signalled, "the command did not exit 0"},
		{refreshRun(""), "the output is not one JSON object with limits"},
		{refreshRun(`{"limits": []} {}`), "the output holds more than the JSON object"},
		{refreshRun(`{"limits": [], "tier": "x"}`), "unknown field"},
		{refreshRun(`{"limits": [{"dimension": "requests_per_minute", "value": 500}]}`), "1 entries, target.limits holds 2"},
		{refreshRun(`{"limits": [{"dimension": "tokens_per_minute", "value": 500}, {"dimension": "tokens_per_minute", "value": 9}]}`),
			`limits[0] is "tokens_per_minute", target.limits[0] "requests_per_minute"`},
		{refreshRun(`{"limits": [{"dimension": "requests_per_minute"}, {"dimension": "tokens_per_minute", "value": 9}]}`), "limits[0] holds neither or both"},
		{refreshRun(`{"limits": [{"dimension": "requests_per_minute", "value": 5, "unavailable": "x"}, {"dimension": "tokens_per_minute", "value": 9}]}`), "limits[0] holds neither or both"},
		{refreshRun(`{"limits": [{"dimension": "requests_per_minute", "unavailable": " "}, {"dimension": "tokens_per_minute", "value": 9}]}`), "limits[0]: unavailable gives no reason"},
		{refreshRun(`{"limits": [{"dimension": "requests_per_minute", "value": 0}, {"dimension": "tokens_per_minute", "value": 9}]}`), "limits[0]: value 0 is not a positive number"},
		{refreshRun(`{"limits": [{"dimension": "requests_per_minute", "value": 5}, {"dimension": "tokens_per_minute", "value": -9}]}`), "limits[1]: value -9 is not a positive number"},
	} {
		if got, err := ReadRefreshedLimits(tc.rec, &cfg.Target); err == nil || got != nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%q: %+v, %v; want %q", tc.rec.Stdout.Text, got, err, tc.want)
		}
	}
}

// A route's published refresh record carries the limits read, without a
// confidential limit's value; a route with a confidential limit
// publishes neither output stream and keeps the record as run, readable
// by the owner only.
func TestPublishedRefresh(t *testing.T) {
	const out = `{"limits": [{"dimension": "requests_per_minute", "value": 1000}, {"dimension": "tokens_per_minute", "value": 300001}]}`
	public := releaseCfg(t)
	rec := refreshRun(out)
	read, err := ReadRefreshedLimits(rec, &public.Target)
	if err != nil {
		t.Fatal(err)
	}
	pub := PublishedRefresh(rec, &public.Target, read)
	if pub.Stdout.Text != out || pub.Stderr.Text != "fetched" || !reflect.DeepEqual(pub.Limits, read) || rec.Limits != nil {
		t.Fatalf("public route: %+v", pub)
	}

	cfg := confidentialCfg(t)
	if read, err = ReadRefreshedLimits(rec, &cfg.Target); err != nil {
		t.Fatal(err)
	}
	if !read[1].Confidential || read[1].Value != 300001 {
		t.Fatalf("read %+v", read)
	}
	pub = PublishedRefresh(rec, &cfg.Target, read)
	want := []RefreshedLimit{{Dimension: config.LimitRPM, Value: 1000}, {Dimension: config.LimitTPM, Confidential: true}}
	if pub.Stdout.Text != config.RetainedHeaderValue || pub.Stderr.Text != config.RetainedHeaderValue || !reflect.DeepEqual(pub.Limits, want) {
		t.Fatalf("confidential route: %+v", pub)
	}
	b, err := json.Marshal(pub)
	if err != nil || bytes.Contains(b, []byte("300001")) {
		t.Fatalf("published record %s: %v", b, err)
	}
	dir := t.TempDir()
	if err := WriteRetainedRefresh(dir, rec); err != nil {
		t.Fatal(err)
	}
	held, err := os.ReadFile(filepath.Join(dir, RefreshFile))
	if err != nil || !bytes.Contains(held, []byte("300001")) {
		t.Fatalf("retained record %s: %v", held, err)
	}
	if fi, err := os.Stat(filepath.Join(dir, RefreshFile)); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("retained record mode %v, %v", fi, err)
	}
	if err := WriteRetainedRefresh(dir, rec); err == nil {
		t.Fatal("a second retained record was written")
	}
}

// A window-close record reads back as written; a field it does not
// define or data after it is refused. On a target with a confidential
// limit its records are keyed under the release key, which it requires.
func TestReadWindowClose(t *testing.T) {
	op := &Operator{AccountTier: "Developer", File: FileRef{Name: "operator.json", SHA256: strings.Repeat("a", 64), Bytes: "9"},
		Records: []OperatorRecord{{Kind: RecordLimits, FileRef: FileRef{Name: "limits.txt", SHA256: strings.Repeat("b", 64), Bytes: "4"}, URL: "https://example.test/limits", Retrieved: "2026-10-27T01:00:00Z"}}}
	at := time.Date(2026, 10, 27, 1, 0, 0, 0, time.UTC)
	w, err := NewWindowClose(&config.Target{Name: "groq"}, at, refreshRun(`{"limits": []}`), op, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.Copies = filepath.Join("window-close", "groq", "x")
	dir := t.TempDir()
	path, err := writeJSON(dir, WindowCloseFile, w)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ReadWindowClose(path)
	if err != nil || !reflect.DeepEqual(got, w) {
		t.Fatalf("read %+v, %v; want %+v", got, err, w)
	}
	if w.Records[0].FileRef != op.Records[0].FileRef {
		t.Fatalf("public target's record %+v", w.Records[0].FileRef)
	}
	confidential := &config.Target{Name: "groq", Limits: []config.Limit{{Marking: config.MarkingConfidential}}}
	if _, err := NewWindowClose(confidential, at, refreshRun(`{"limits": []}`), op, nil); err == nil || !strings.Contains(err.Error(), "needs the release key") {
		t.Fatalf("confidential target with no key: %v", err)
	}
	key, err := releasekey.New()
	if err != nil {
		t.Fatal(err)
	}
	op.Records[0].data = []byte("limits")
	held, err := NewWindowClose(confidential, at, refreshRun(`{"limits": []}`), op, key)
	if err != nil {
		t.Fatal(err)
	}
	if want := (FileRef{Name: "limits.txt", HMACSHA256: key.Sum([]byte("limits"))}); held.Records[0].FileRef != want || held.Operator != op.File {
		t.Fatalf("confidential target's record %+v, want %+v; operator %+v", held.Records[0].FileRef, want, held.Operator)
	}
	if got := WindowCloseDir("/frames", "groq", at.Add(time.Hour)); got != filepath.Join("/frames", "window-close", "groq", "20261027T020000.000000000Z") {
		t.Fatalf("dir %s", got)
	}
	b, _ := os.ReadFile(path)
	for name, bad := range map[string][]byte{
		"unknown field": bytes.Replace(b, []byte(`"route"`), []byte(`"extra": 1, "route"`), 1),
		"data after":    append(b, '{', '}'),
	} {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, bad, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadWindowClose(p); err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
