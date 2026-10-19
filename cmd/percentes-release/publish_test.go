package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
)

// stdoutOf runs f with standard output captured and returns what it
// printed.
func stdoutOf(t *testing.T, f func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		r.Close()
		done <- string(b)
	}()
	func() {
		defer func() {
			os.Stdout = old
			w.Close()
		}()
		f()
	}()
	return <-done
}

// published is path's content under dir as a published file.
func published(t *testing.T, dir, path string) *evidence.PublishedFile {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(path)))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	return &evidence.PublishedFile{Path: path, SHA256: hex.EncodeToString(sum[:])}
}

// publication writes the record of a release whose one route, groq,
// needs its naming permission: the manifests and window-close records
// under out and the pages under pages by hash, a notice of 1 September 2026 whose window closed
// on 7 September, no response, and one disclosure before it.
func publication(t *testing.T, out, pages string, edit func(*evidence.Publication)) string {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, dir, "groq-pack.tar", []byte("tables and evidence"))
	writeFile(t, dir, "permission.eml", []byte("written permission"))
	p := evidence.Publication{
		Routes: []evidence.RoutePublication{{Route: "groq",
			Notice:     &evidence.Notice{Sent: "2026-09-01", Recipient: "groq support", Account: "org-1", Pack: published(t, dir, "groq-pack.tar"), WindowCloses: "2026-09-07"},
			Responses:  []evidence.Response{},
			Permission: published(t, dir, "permission.eml")}},
		Disclosures:    []evidence.Disclosure{{Provider: "Groq", How: evidence.DisclosureEmail, Date: "2026-08-20", Recipient: "groq support"}},
		Prerelease:     []evidence.Prerelease{},
		NonWorkingDays: []string{},
	}
	manifests, err := releaseManifests(out)
	if err != nil {
		t.Fatal(err)
	}
	for name := range manifests {
		p.Manifests = append(p.Manifests, *published(t, out, name))
	}
	closes, err := releaseFiles(out, evidence.WindowCloseFile)
	if err != nil {
		t.Fatal(err)
	}
	for name := range closes {
		p.WindowCloses = append(p.WindowCloses, *published(t, out, name))
	}
	for name := range readPages(t, pages) {
		p.Pages = append(p.Pages, *published(t, pages, name))
	}
	if edit != nil {
		edit(&p)
	}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return writeFile(t, dir, "publication.json", b)
}

// publish-check passes a complete record and prints the providers told
// before the notices; pages runs with no record and no window-close
// record; and the check refuses, exiting 2, a paged route with no
// window-close record, a record that lacks the naming permission the
// configuration requires, misses responses, has an open window, misses a
// later cell's manifest or window-close record, or a page or window-close
// record edited after the record named it. It exits 1 on a window-close
// record of a refresh that failed, with an edited retained copy, or taken
// before the route's last cell ended, and with no record, an absent one,
// no schedule, or a schedule other than the one the cells ran under.
func TestPublishCheck(t *testing.T) {
	e := newEndpoint(t, func(int64) (int, string) { return http.StatusNotFound, "" })
	path := routeConfigFile(t, "groq", e.url(), "10")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	const naming = "\n  naming:\n    provider: Groq\n"
	if strings.Count(string(b), naming) != 1 {
		t.Fatalf("reference config: %q not found once", naming)
	}
	b = []byte(strings.Replace(string(b), naming, naming+"    permission_required: true\n    permission_record: estate/naming/permission.eml\n", 1))
	cfgPath := writeFile(t, t.TempDir(), "groq.yaml", b)
	cfg, err := config.LoadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(cfg.Target.APIKeyEnv, testKey)
	f := newFixture(t, cfg, "+0.005032")
	groq, err := newRoute(f.in)
	if err != nil {
		t.Fatal(err)
	}
	shortLadder(groq)
	for _, run := range groq.runs[:2] {
		if rec := runCellOK(t, groq, run); rec.Outcome != outcomeBlocked {
			t.Fatalf("outcome %s %q", rec.Outcome, rec.Reasons)
		}
	}
	pages := t.TempDir()
	base := []string{"--out", groq.out, "--frames", groq.framesRoot, "--config", cfgPath, "--schedule", f.in.schedulePath, "--pages", pages}
	if code, err := pagesCmd(base); code != exitOK || err != nil {
		t.Fatalf("pages with no record: exit %d: %v", code, err)
	}
	check := func(record string) (int, string) {
		t.Helper()
		var code int
		var err error
		text := stdoutOf(t, func() { code, err = publishCheckCmd(append(base[:len(base):len(base)], "--record", record)) })
		if err != nil {
			t.Fatalf("exit %d: %v", code, err)
		}
		return code, text
	}

	if code, text := check(publication(t, groq.out, pages, nil)); code != exitAdverse ||
		!strings.Contains(text, "NOT READY to publish: evidence: publication refused: route groq: window close: no window-close.json; a route with a page needs its window-close record\n") {
		t.Fatalf("no window-close record: exit %d:\n%s", code, text)
	}
	closeRoute := func() {
		t.Helper()
		st, err := groq.state()
		if err != nil {
			t.Fatal(err)
		}
		stdoutOf(t, func() {
			if err := groq.windowClose(context.Background(), st); err != nil {
				t.Error(err)
			}
		})
		if code, err := pagesCmd(base); code != exitOK || err != nil {
			t.Fatalf("pages after the window close: exit %d: %v", code, err)
		}
	}
	closeRoute()

	var code int
	complete := publication(t, groq.out, pages, nil)
	text := stdoutOf(t, func() { code = dispatch(append([]string{"publish-check", "--record", complete}, base...)) })
	if code != exitOK || !strings.Contains(text, "publication record complete: 2 manifests, 4 pages, 1 routes\n") ||
		!strings.HasSuffix(text, "told of the measurement before the notices:\n  Groq, by email, 2026-08-20\n") {
		t.Fatalf("complete record: exit %d:\n%s", code, text)
	}
	for _, c := range []struct {
		name string
		edit func(*evidence.Publication)
		want string
	}{
		{"no permission", func(p *evidence.Publication) { p.Routes[0].Permission = nil }, "route groq: permission: required"},
		{"no responses", func(p *evidence.Publication) { p.Routes[0].Responses = nil }, "route groq: responses: required"},
		{"window open", func(p *evidence.Publication) {
			p.Routes[0].Notice.Sent, p.Routes[0].Notice.WindowCloses = "2099-01-05", "2099-01-09"
		}, "route groq: notice: the window is open until the end of 2099-01-09 UTC"},
	} {
		if code, text := check(publication(t, groq.out, pages, c.edit)); code != exitAdverse || !strings.Contains(text, "NOT READY to publish: ") || !strings.Contains(text, c.want) {
			t.Errorf("%s: exit %d, want %q:\n%s", c.name, code, c.want, text)
		}
	}

	closePath := filepath.Join(groq.dir, evidence.WindowCloseFile)
	closeBytes, err := os.ReadFile(closePath)
	if err != nil {
		t.Fatal(err)
	}
	w, err := evidence.ReadWindowClose(closePath)
	if err != nil {
		t.Fatal(err)
	}
	opCopy := filepath.Join(groq.framesRoot, w.Copies, evidence.OperatorCopyFile)
	opBytes, err := os.ReadFile(opCopy)
	if err != nil {
		t.Fatal(err)
	}
	failed := *w
	refresh := *w.Refresh
	refresh.Error = "exit status 1"
	failed.Refresh = &refresh
	failedBytes, err := json.Marshal(&failed)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name, path string
		b, was     []byte
		want       string
	}{
		{"refresh failed", closePath, failedBytes, closeBytes, "pages: route groq: evidence: window-close record refused: refresh: refreshed limits: the command failed: exit status 1"},
		{"operator copy edited", opCopy, []byte("{}"), opBytes, "pages: route groq: evidence: window-close record refused: operator copy: operator.json has SHA-256 " + sha("{}")},
	} {
		if err := os.WriteFile(c.path, c.b, 0o600); err != nil {
			t.Fatal(err)
		}
		if code, err := publishCheckCmd(append(base[:len(base):len(base)], "--record", complete)); code != exitError || err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: exit %d: %v, want %q", c.name, code, err, c.want)
		}
		if err := os.WriteFile(c.path, c.was, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(closePath, append(closeBytes, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	if code, text := check(complete); code != exitAdverse || !strings.Contains(text, "window_closes: groq/window-close.json has SHA-256 "+sha(string(closeBytes)+"\n")) {
		t.Errorf("window-close record edited: exit %d:\n%s", code, text)
	}
	if err := os.WriteFile(closePath, closeBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	record := publication(t, groq.out, pages, nil)
	if err := os.WriteFile(filepath.Join(pages, "routes", "groq.txt"), []byte("edited\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if code, text := check(record); code != exitAdverse || !strings.Contains(text, "pages: routes/groq.txt under "+pages+" is not the page the cells generate") {
		t.Errorf("edited page: exit %d:\n%s", code, text)
	}
	if code, err := pagesCmd(base); code != exitOK || err != nil {
		t.Fatalf("pages again: exit %d: %v", code, err)
	}
	if code, _ := check(record); code != exitOK {
		t.Errorf("pages rewritten: exit %d", code)
	}
	later := runCellOK(t, groq, groq.runs[2])
	if later.Outcome != outcomeBlocked {
		t.Fatalf("outcome %s %q", later.Outcome, later.Reasons)
	}
	want := "pages: route groq: evidence: window-close record refused: recorded " + w.RecordedUTC.UTC().Format(time.RFC3339Nano) +
		", not after the route's last cell ended at " + later.EndUTC.UTC().Format(time.RFC3339Nano)
	if code, err := publishCheckCmd(append(base[:len(base):len(base)], "--record", record)); code != exitError || err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("window close before a later cell: exit %d: %v, want %q", code, err, want)
	}
	if err := os.Remove(closePath); err != nil {
		t.Fatal(err)
	}
	closeRoute()
	if code, text := check(record); code != exitAdverse || !strings.Contains(text, "manifests: groq/"+cellLabel(groq.runs[2])+"/manifest.json is not in the record") ||
		!strings.Contains(text, "window_closes: groq/window-close.json has SHA-256 ") {
		t.Errorf("later cell: exit %d:\n%s", code, text)
	}

	raw, err := os.ReadFile(f.in.schedulePath)
	if err != nil {
		t.Fatal(err)
	}
	other, err := testSchedule(t, nil, "groq", "other").JSON()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		args []string
		want string
	}{
		{base, "publish-check: --record is required"},
		{append(base[:len(base):len(base)], "--record", filepath.Join(t.TempDir(), "absent.json")), "absent.json: no such file"},
		{[]string{"--out", groq.out, "--frames", groq.framesRoot, "--config", cfgPath, "--pages", pages, "--record", record}, "publish-check: --schedule is required"},
		{[]string{"--out", groq.out, "--frames", groq.framesRoot, "--config", cfgPath, "--schedule", writeFile(t, t.TempDir(), "schedule.json", other), "--pages", pages, "--record", record},
			"pages: route groq: the cells ran under schedule sha256 \"" + sha(string(raw)) + "\"; --schedule has sha256 " + sha(string(other))},
	} {
		if code, err := publishCheckCmd(c.args); code != exitError || err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%q: exit %d: %v, want %q", c.args, code, err, c.want)
		}
	}
}

// An AWS endpoint's route needs the replication pack.
func TestReplicationRoute(t *testing.T) {
	for url, want := range map[string]bool{
		"https://bedrock-runtime.us-east-1.amazonaws.com/model/x/invoke-with-response-stream": true,
		"https://BEDROCK-RUNTIME.eu-west-2.AMAZONAWS.COM/model/x":                             true,
		"https://api.groq.com/openai/v1/chat/completions":                                     false,
		"https://amazonaws.com.example.test/v1":                                               false,
	} {
		cfg := &config.Config{Target: config.Target{EndpointURL: url}}
		if got := replicationRoute(cfg); got != want {
			t.Errorf("%s: %t, want %t", url, got, want)
		}
	}
}
