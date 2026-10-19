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
// confidential limit's value; a route with a confidential limit publishes
// no limit's unavailable reason and neither output stream, and keeps the
// record as run, readable by the owner only.
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
	back, err := ReadRetainedRefresh(dir)
	if err != nil {
		t.Fatal(err)
	}
	if again, err := ReadRefreshedLimits(back, &cfg.Target); err != nil || !reflect.DeepEqual(again, read) {
		t.Fatalf("retained record read back as %+v, %v; want %+v", again, err, read)
	}
	if err := os.WriteFile(filepath.Join(dir, RefreshFile), bytes.Replace(held, []byte(`"command"`), []byte(`"extra": 1, "command"`), 1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadRetainedRefresh(dir); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("retained record with an unknown field: %v", err)
	}

	const reason, rpmReason = "console API 403; last seen Limit 250013.5", "console 403: rpm hidden, tpm 300001"
	unread := refreshRun(`{"limits": [{"dimension": "requests_per_minute", "unavailable": "` + rpmReason + `"},
		{"dimension": "tokens_per_minute", "unavailable": "` + reason + `"}]}`)
	if read, err = ReadRefreshedLimits(unread, &public.Target); err != nil {
		t.Fatal(err)
	}
	if pub = PublishedRefresh(unread, &public.Target, read); !reflect.DeepEqual(pub.Limits, read) || pub.Limits[0].Unavailable != rpmReason || pub.Limits[1].Unavailable != reason {
		t.Fatalf("public route's reasons: %+v", pub.Limits)
	}
	if read, err = ReadRefreshedLimits(unread, &cfg.Target); err != nil {
		t.Fatal(err)
	}
	pub = PublishedRefresh(unread, &cfg.Target, read)
	want = []RefreshedLimit{{Dimension: config.LimitRPM, Unavailable: config.RetainedHeaderValue}, {Dimension: config.LimitTPM, Confidential: true, Unavailable: config.RetainedHeaderValue}}
	if !reflect.DeepEqual(pub.Limits, want) || read[0].Unavailable != rpmReason || read[1].Unavailable != reason {
		t.Fatalf("confidential route's reasons: published %+v, read %+v", pub.Limits, read)
	}
	if b, err = json.Marshal(pub); err != nil || bytes.Contains(b, []byte("250013.5")) || bytes.Contains(b, []byte("300001")) {
		t.Fatalf("published record %s: %v", b, err)
	}
	dir = t.TempDir()
	if err := WriteRetainedRefresh(dir, unread); err != nil {
		t.Fatal(err)
	}
	if held, err = os.ReadFile(filepath.Join(dir, RefreshFile)); err != nil || !bytes.Contains(held, []byte(reason)) || !bytes.Contains(held, []byte(rpmReason)) {
		t.Fatalf("retained record %s: %v", held, err)
	}
}

// A window-close record reads back as written; a field it does not
// define or data after it is refused. On a target with a confidential
// limit its operator file and records are keyed under the release key,
// with no size, and it requires the key.
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
	op.Records[0].data, op.raw = []byte("limits"), []byte(`{"provider":"p"}`)
	held, err := NewWindowClose(confidential, at, refreshRun(`{"limits": []}`), op, key)
	if err != nil {
		t.Fatal(err)
	}
	if want := (FileRef{Name: "limits.txt", HMACSHA256: key.Sum([]byte("limits"))}); held.Records[0].FileRef != want {
		t.Fatalf("confidential target's record %+v, want %+v", held.Records[0].FileRef, want)
	}
	if want := (FileRef{Name: "operator.json", HMACSHA256: key.Sum(op.raw)}); held.Operator != want {
		t.Fatalf("confidential target's operator file %+v, want %+v", held.Operator, want)
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

// A window-close record verifies when it and its refresh began after the
// route's last cell ended, the refresh exited 0 and publishes as read,
// and its copies match its references; any other is refused, naming each
// problem. On a target with a confidential limit the copies are keyed,
// the key is required, and the refresh record as run kept beside them
// must publish as the record's.
func TestWindowCloseVerify(t *testing.T) {
	const out = `{"limits": [{"dimension": "requests_per_minute", "value": 1000}, {"dimension": "tokens_per_minute", "value": 300001}]}`
	lastEnd := time.Date(2026, 10, 26, 23, 0, 0, 0, time.UTC)
	at := lastEnd.Add(2 * time.Hour)
	key, err := releasekey.New()
	if err != nil {
		t.Fatal(err)
	}
	// record writes target's record at at, of a refresh begun an hour
	// after lastEnd, with its copies under a new frame store root.
	record := func(target *config.Target) (*WindowClose, string) {
		t.Helper()
		root := t.TempDir()
		rec := refreshRun(out)
		rec.StartUTC, rec.EndUTC = lastEnd.Add(time.Hour), lastEnd.Add(time.Hour+time.Second)
		read, err := ReadRefreshedLimits(rec, target)
		if err != nil {
			t.Fatal(err)
		}
		var k *releasekey.Key
		if target.Confidential() {
			k = key
		}
		op := loadOperator(t)
		w, err := NewWindowClose(target, at, PublishedRefresh(rec, target, read), op, k)
		if err != nil {
			t.Fatal(err)
		}
		dir := WindowCloseDir(root, target.Name, at)
		if w.Copies, err = filepath.Rel(root, dir); err != nil {
			t.Fatal(err)
		}
		if err := op.WriteCopies(dir); err != nil {
			t.Fatal(err)
		}
		if target.Confidential() {
			if err := WriteRetainedRefresh(dir, rec); err != nil {
				t.Fatal(err)
			}
		}
		return w, root
	}
	replace := func(path, content string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	public, confidential := releaseCfg(t), confidentialCfg(t)
	for _, tc := range []struct {
		name string
		cfg  *config.Config
		key  *releasekey.Key
		// end is the route's last cell's end, lastEnd where zero.
		end  time.Time
		edit func(w *WindowClose, held string)
		want []string
	}{
		{"complete", public, nil, time.Time{}, nil, nil},
		{"complete, confidential", confidential, key, time.Time{}, nil, nil},
		{"before the last cell's end", public, nil, at.Add(time.Minute), nil, []string{
			"recorded 2026-10-27T01:00:00Z, not after the route's last cell ended at 2026-10-27T01:01:00Z",
			"refresh: started 2026-10-27T00:00:00Z, not after the route's last cell ended at 2026-10-27T01:01:00Z"}},
		{"refresh between the end and the record", public, nil, lastEnd.Add(90 * time.Minute), nil, []string{
			"refresh: started 2026-10-27T00:00:00Z, not after the route's last cell ended at 2026-10-27T00:30:00Z"}},
		{"another route", public, nil, time.Time{}, func(w *WindowClose, _ string) { w.Route = "baseten" }, []string{`route "baseten", the configuration's "groq"`}},
		{"no refresh", public, nil, time.Time{}, func(w *WindowClose, _ string) { w.Refresh = nil }, []string{"refresh: not recorded"}},
		{"refresh failed", public, nil, time.Time{}, func(w *WindowClose, _ string) {
			code := 4
			w.Refresh.Error, w.Refresh.ExitCode = "exit status 4", &code
		}, []string{"refresh: refreshed limits: the command failed: exit status 4"}},
		{"refresh not exited", public, nil, time.Time{}, func(w *WindowClose, _ string) { w.Refresh.ExitCode = nil }, []string{"refresh: refreshed limits: the command did not exit 0"}},
		{"limit edited", public, nil, time.Time{}, func(w *WindowClose, _ string) { w.Refresh.Limits[1].Value = 1 }, []string{"refresh: not the published form of the refresh record as run"}},
		{"copies elsewhere", public, nil, time.Time{}, func(w *WindowClose, _ string) { w.Copies = "window-close/groq/x" }, []string{
			`copies "window-close/groq/x", want "` + filepath.Join("window-close", "groq", "20261027T010000.000000000Z") + `"`}},
		{"operator copy edited", public, nil, time.Time{}, func(_ *WindowClose, held string) {
			replace(filepath.Join(held, OperatorCopyFile), "{}")
		}, []string{"operator copy: " + OperatorCopyFile + " has SHA-256 " + sha("{}")}},
		{"record copy missing", public, nil, time.Time{}, func(_ *WindowClose, held string) {
			if err := os.Remove(filepath.Join(held, RecordsCopyDir, "limits.png")); err != nil {
				t.Fatal(err)
			}
		}, []string{"limits record copy: limits.png is missing"}},
		{"account tier edited", public, nil, time.Time{}, func(w *WindowClose, _ string) { w.AccountTier = "Enterprise" }, []string{`account_tier "Enterprise", the operator copy's "Basic"`}},
		{"keyed on a public target", public, key, time.Time{}, func(w *WindowClose, _ string) {
			w.Records[0].FileRef = FileRef{Name: "limits.png", HMACSHA256: key.Sum([]byte(recordFiles["limits.png"]))}
		}, []string{"limits record copy: limits.png is keyed on a target with public limits only"}},
		{"no key", confidential, nil, time.Time{}, nil, []string{"release key: the target has a confidential limit, whose window-close copies are keyed, and no release key was given"}},
		{"keyed copy edited", confidential, key, time.Time{}, func(_ *WindowClose, held string) {
			replace(filepath.Join(held, RecordsCopyDir, "quota.txt"), "quota at start: 30 RPM")
		}, []string{"quota record copy: quota.txt has another HMAC-SHA256 under the release key than the window-close record holds"}},
		{"plain on a confidential target", confidential, key, time.Time{}, func(w *WindowClose, held string) {
			b, err := os.ReadFile(filepath.Join(held, OperatorCopyFile))
			if err != nil {
				t.Fatal(err)
			}
			w.Operator = refOf("operator.json", b)
		}, []string{"operator copy: operator.json is named by its SHA-256 on a target with a confidential limit"}},
		{"no record as run", confidential, key, time.Time{}, func(_ *WindowClose, held string) {
			if err := os.Remove(filepath.Join(held, RefreshFile)); err != nil {
				t.Fatal(err)
			}
		}, []string{"refresh: the record as run: evidence: open ", RefreshFile + ": no such file"}},
		{"another record as run", confidential, key, time.Time{}, func(_ *WindowClose, held string) {
			b, err := os.ReadFile(filepath.Join(held, RefreshFile))
			if err != nil {
				t.Fatal(err)
			}
			replace(filepath.Join(held, RefreshFile), strings.Replace(string(b), `\"value\": 1000`, `\"value\": 999`, 1))
		}, []string{"refresh: not the published form of the refresh record as run"}},
	} {
		w, root := record(&tc.cfg.Target)
		if tc.edit != nil {
			tc.edit(w, filepath.Join(root, w.Copies))
		}
		end := lastEnd
		if !tc.end.IsZero() {
			end = tc.end
		}
		err := w.Verify(&tc.cfg.Target, root, tc.key, end)
		if tc.want == nil {
			if err != nil {
				t.Errorf("%s: %v", tc.name, err)
			}
			continue
		}
		if err == nil || !strings.HasPrefix(err.Error(), "evidence: window-close record refused: ") {
			t.Errorf("%s: %v, want a refusal", tc.name, err)
			continue
		}
		for _, want := range tc.want {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%s: %v, want %q", tc.name, err, want)
			}
		}
	}
}
