package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/mock"
)

// A release whose every route the ceiling check excluded, here for a
// wrong model, has no cell manifest and publishes once its notices'
// windows close; publish-check then refuses it once a ceiling-check
// record pages copied under --out is edited.
func TestPublishCheckAllRoutesExcluded(t *testing.T) {
	endpoint := startMock(t, mock.Shape{Key: testKey, Answer: "The customer was billed twice.", Model: "wrong-model"})
	groq, snapshots := quickSolo(t, "groq", endpoint)
	gemini, _ := quickSolo(t, "gemini", endpoint)
	configs := []string{groq, gemini}
	ledger := releaseLedger(t, configs...)
	c := newChecker(t, ledger, snapshots)
	for _, cfg := range configs {
		code, output, err := c.check(context.Background(), cfg, 2000)
		if err != nil || code != exitAdverse {
			t.Fatalf("check: %d %v %s", code, err, output)
		}
	}
	records := append(c.records("groq"), c.records("gemini")...)
	sched := filepath.Join(t.TempDir(), "schedule.json")
	args := []string{"--master-seed", "percentes-release-test", "--first", "2026-10-17", "--last", "2026-10-26", "--config", groq, "--config", gemini, "--ledger", ledger, "--release-key", testReleaseKeyPath, "--schedule", sched, "--excluded", "groq", "--excluded", "gemini"}
	for _, p := range records {
		args = append(args, "--ceiling-probe", p)
	}
	var code int
	var err error
	output := stdoutOf(t, func() { code, err = scheduleCmd(args) })
	if code != exitOK || err != nil {
		t.Fatalf("schedule: %d %v %s", code, err, output)
	}
	out, frames, pages := t.TempDir(), t.TempDir(), t.TempDir()
	base := []string{"--out", out, "--frames", frames, "--config", groq, "--config", gemini, "--ledger", ledger, "--schedule", sched}
	for _, p := range records {
		base = append(base, "--check-record", p)
	}
	output = stdoutOf(t, func() { code, err = pagesCmd(append(append([]string{}, base...), "--pages", pages)) })
	if code != exitOK || err != nil || strings.Count(output, "not characterized (SPEC-HOSTED §2)") != 2 {
		t.Fatalf("pages: %d %v %s", code, err, output)
	}
	dir := t.TempDir()
	pub := evidence.Publication{Manifests: []evidence.PublishedFile{}, WindowCloses: []evidence.PublishedFile{}, Disclosures: []evidence.Disclosure{}, Prerelease: []evidence.Prerelease{}, NonWorkingDays: []string{}}
	cfgs, err := loadReleases(configs)
	if err != nil {
		t.Fatal(err)
	}
	for _, cfg := range cfgs {
		route := cfg.Target.Name
		writeFile(t, dir, route+".tar", packOf(t, base, route))
		r := evidence.RoutePublication{Route: route, Notice: &evidence.Notice{Sent: "2099-01-05", Recipient: route + " support", Account: "synthetic-account", Pack: published(t, dir, route+".tar"), WindowCloses: "2099-01-09"}, Responses: []evidence.Response{}}
		if cfg.Target.Naming.PermissionRequired {
			writeFile(t, dir, route+"-permission.txt", []byte("synthetic permission"))
			r.Permission = published(t, dir, route+"-permission.txt")
		}
		pub.Routes = append(pub.Routes, r)
	}
	for p := range readPages(t, pages) {
		pub.Pages = append(pub.Pages, *published(t, pages, p))
	}
	b, err := json.Marshal(pub)
	if err != nil {
		t.Fatal(err)
	}
	record := writeFile(t, dir, "publication.json", b)
	publishAt(t, noticeClosed)
	output = stdoutOf(t, func() {
		code, err = publishCheckCmd(append(append([]string{}, base...), "--pages", pages, "--record", record, "--write-page"))
	})
	if code != exitOK || err != nil || !strings.Contains(output, "publication record complete: 0 manifests, 6 pages, 2 routes\n") {
		t.Fatalf("publish-check: %d %v %s", code, err, output)
	}

	copies, err := filepath.Glob(filepath.Join(out, "groq", "*"))
	if err != nil || len(copies) == 0 {
		t.Fatalf("groq's copies under --out: %v, %v", copies, err)
	}
	if err := os.WriteFile(copies[0], []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	output = stdoutOf(t, func() {
		code, err = publishCheckCmd(append(append([]string{}, base...), "--pages", pages, "--record", record))
	})
	if want := "NOT READY to publish: groq/" + filepath.Base(copies[0]) + " under " + out + " is not the copy pages writes under route groq of a ceiling-check record\n"; code != exitAdverse || err != nil || output != want {
		t.Fatalf("an edited copy: %d %v %q; want %q", code, err, output, want)
	}
}
