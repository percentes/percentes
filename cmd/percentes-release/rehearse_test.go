package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/probe"
)

// shortRehearsal scales the rehearsal to shortLadder's ladder and CPU
// window, the end-to-end test's offset minute and short mock streams.
func shortRehearsal(t *testing.T) {
	t.Helper()
	orig := settingsAt
	settingsAt = func(int) rehearsalSettings {
		return rehearsalSettings{
			ladder:     config.Ladder{StepsRPS: []float64{2, 4, 8, 16, 32}, WarmupS: 1, StepS: 2, DrainS: 1},
			cpuWindowS: 1,
			minute:     e2eMinute,
			ttft:       config.LatencyDist{Distribution: "fixed", FixedMs: 5},
			itl:        config.LatencyDist{Distribution: "fixed", FixedMs: 1},
			reasoning:  3,
			answer:     e2eAnswer,
		}
	}
	t.Cleanup(func() { settingsAt = orig })
}

// The rehearsal sends each route's ceiling check to its mock, writing the
// record run reads, then runs one cell of each release form against its
// own mock, concurrently at the schedule's offsets, and its summary names
// every cell with its G2 verdicts, its completeness check and its
// manifest's hash, and the host's dispatch series summing every cell's
// sends. The rehearsal is green, and exits 0, when every cell counts;
// under the race detector the client-validity gate can fail, and when
// that is the only reason a cell does not count the test is skipped after
// the rest is checked. A second rehearsal into the same directory is
// refused.
func TestRehearse(t *testing.T) {
	enoughOpenFiles(t)
	fastDispatch(t)
	shortRehearsal(t)
	in := rehearsalInput{out: filepath.Join(t.TempDir(), "rehearsal"), frames: t.TempDir(), clockSync: fakeSNTP(t, "+0.004211"),
		masterSeed: "percentes-rehearsal-test", scale: 1, byteCap: 2000, build: testBuild}
	code, err := rehearse(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(in.out, rehearsalFile))
	if err != nil {
		t.Fatal(err)
	}
	var sum rehearsal
	if err := json.Unmarshal(b, &sum); err != nil {
		t.Fatal(err)
	}
	if sum.FullLength || sum.Source.Revision == "" || sum.Cell == "" || len(sum.Routes) != len(releaseForms) || sum.Dispatch == nil {
		t.Fatalf("summary full length %t, source %+v, cell %q, %d routes, dispatch %v", sum.FullLength, sum.Source, sum.Cell, len(sum.Routes), sum.Dispatch)
	}

	gateOnly := false
	var offsets []int
	sent := 0
	for i, rr := range sum.Routes {
		f := releaseForms[i]
		t.Logf("%s at %d min: %s %q", rr.Route, rr.OffsetMinutes, rr.Outcome, rr.Problems)
		if rr.Route != f.name || rr.Adapter != f.adapter || rr.AuthHeader != f.auth || rr.Run.Label != sum.Cell || rr.Run.Route != f.name {
			t.Errorf("route %d: %+v", i, rr)
		}
		offsets = append(offsets, rr.OffsetMinutes)
		switch {
		case rr.Outcome == outcomeCounted:
			if !rr.Green || len(rr.Problems) > 0 {
				t.Errorf("%s: counted, green %t, problems %q", rr.Route, rr.Green, rr.Problems)
			}
		case rr.Outcome == outcomeInvalid && gateReasons(rr.Reasons):
			gateOnly = true
			if rr.Green {
				t.Errorf("%s: client-invalid and green", rr.Route)
			}
		default:
			t.Errorf("%s: outcome %s %q", rr.Route, rr.Outcome, rr.Reasons)
		}
		if len(rr.Steps) != 5 || !rr.Steps[0].Attempted || rr.Steps[0].G2 == nil {
			t.Errorf("%s: steps %+v", rr.Route, rr.Steps)
		}
		n := rr.Sent.Probe + rr.Sent.Warmup + rr.Sent.Measured
		if rr.Sent.Probe != 1 || len(rr.MockFinished) != 1 || rr.MockFinished["200"] != n+probe.CeilingRequests {
			t.Errorf("%s: sent %+v, the mock finished %v", rr.Route, rr.Sent, rr.MockFinished)
		}
		if rr.CeilingCheck != (probe.CeilingCheck{Ceiling: 512, LengthValue: probe.LengthValue(f.adapter), Sent: 10, Passed: 10, Pass: true}) {
			t.Errorf("%s: ceiling check %+v", rr.Route, rr.CeilingCheck)
		}
		var check probe.ReleaseRecord
		if b, err := os.ReadFile(filepath.Join(in.out, "inputs", "ceiling-"+f.name+".json")); err != nil || json.Unmarshal(b, &check) != nil ||
			len(check.Requests) != probe.CeilingRequests || check.Ceiling != rr.CeilingCheck || !check.InputBound.Pass {
			t.Errorf("%s: ceiling check record %+v with %d requests, %v", rr.Route, check.Ceiling, len(check.Requests), err)
		}
		for _, q := range check.Requests {
			if !q.Pass || q.Exchange.Status != 200 || q.PromptTokens == nil {
				t.Errorf("%s: ceiling check request %+v", rr.Route, q)
			}
		}
		sent += rr.Sent.Warmup + rr.Sent.Measured
		if rr.Completeness != "pass" || rr.Manifest == nil {
			t.Errorf("%s: completeness %q, manifest %v", rr.Route, rr.Completeness, rr.Manifest)
			continue
		}
		if ref, err := evidence.HashFile(filepath.Join(in.out, filepath.FromSlash(rr.Dir), evidence.ManifestFile), evidence.ManifestFile); err != nil || ref != *rr.Manifest {
			t.Errorf("%s: manifest %+v, summary %+v, %v", rr.Route, ref, *rr.Manifest, err)
		}
	}
	slices.Sort(offsets)
	if !slices.Equal(offsets, []int{0, 10, 20, 30, 40, 50}) {
		t.Errorf("offsets %v, want a permutation of 0 to 50 in tens", offsets)
	}
	total := 0
	for _, c := range sum.Dispatch.Counts {
		total += c
	}
	if len(sum.Dispatch.Files) != len(releaseForms) || total != sent {
		t.Errorf("dispatch series of %d files counts %d sends, the cells sent %d", len(sum.Dispatch.Files), total, sent)
	}
	if green := len(sum.Reasons) == 0; sum.Green != green || sum.Green != (code == exitOK) || !sum.Green && code != exitAdverse {
		t.Errorf("green %t with reasons %q, exit %d", sum.Green, sum.Reasons, code)
	}
	if !gateOnly && !sum.Green {
		t.Errorf("not green: %q", sum.Reasons)
	}

	if _, err := rehearse(context.Background(), in); err == nil || !strings.Contains(err.Error(), "is not empty") {
		t.Errorf("second rehearsal into %s: %v", in.out, err)
	}
	if t.Failed() {
		t.FailNow()
	}
	if gateOnly {
		t.Skip("client-validity gate failed under the race detector; outcomes logged above")
	}
}

// The rehearse command refuses a missing flag, a scale whose step is
// shorter than the CPU window, a missing byte cap, a frame store in the
// source tree, an empty master seed, a clock-sync command it cannot
// parse, an --out path holding white space, too few open files and a
// build from a modified tree without its diff, and writes nothing.
func TestRehearseRefusals(t *testing.T) {
	enoughOpenFiles(t)
	out := filepath.Join(t.TempDir(), "rehearsal")
	frames := t.TempDir()
	set := []string{"--out", out, "--frames", frames, "--clock-sync", "chronyc tracking"}
	with := func(args ...string) []string { return append(slices.Clone(set), args...) }
	for _, tc := range []struct {
		args  []string
		files uint64
		want  string
	}{
		{[]string{"--frames", frames, "--clock-sync", "chronyc tracking"}, minOpenFiles, "--out is required"},
		{[]string{"--out", out, "--clock-sync", "chronyc tracking"}, minOpenFiles, "--frames is required"},
		{[]string{"--out", out, "--frames", frames}, minOpenFiles, "--clock-sync is required"},
		{with("--scale", "0"), minOpenFiles, "--scale 0"},
		{with("--scale", "97", "--byte-cap", "2000"), minOpenFiles, "shorter than the 5 s CPU window"},
		{with("--scale", "96"), minOpenFiles, "--byte-cap 0"},
		{[]string{"--out", out, "--frames", ".", "--clock-sync", "chronyc tracking", "--byte-cap", "2000"}, minOpenFiles, "inside the instrument source tree"},
		{[]string{"--out", out, "--frames", frames, "--clock-sync", "ntpdate -q pool", "--byte-cap", "2000"}, minOpenFiles, "clock-sync command"},
		{with("--byte-cap", "2000", "--master-seed", ""), minOpenFiles, "--master-seed is empty"},
		{[]string{"--out", out + " two", "--frames", frames, "--clock-sync", "chronyc tracking", "--byte-cap", "2000"}, minOpenFiles, "holds white space"},
		{with("--byte-cap", "2000"), 10240, "open-file limit 10240"},
	} {
		openFilesLimit = func() (uint64, error) { return tc.files, nil }
		code, err := rehearseCmd(context.Background(), tc.args)
		if code != exitError || err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%q: exit %d, %v; want %q", tc.args, code, err, tc.want)
		}
	}
	modified := &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "0123456789abcdef0123456789abcdef01234567"}, {Key: "vcs.modified", Value: "true"}}}
	openFilesLimit = func() (uint64, error) { return minOpenFiles, nil }
	in := rehearsalInput{out: out, frames: frames, clockSync: "chronyc tracking", masterSeed: "percentes-rehearsal-test", scale: 1, byteCap: 2000, build: modified}
	if _, err := rehearse(context.Background(), in); err == nil || !strings.Contains(err.Error(), "--source-diff is required") {
		t.Errorf("modified build: %v", err)
	}
	for _, p := range []string{out, out + " two"} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s: %v, want nothing written", p, err)
		}
	}
}
