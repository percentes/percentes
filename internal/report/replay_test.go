package report

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/mock"
	"github.com/percentes/percentes/internal/run"
	"github.com/percentes/percentes/internal/validity"
)

const replayKeyEnv = "PERCENTES_REPLAY_TEST_KEY"

// hostedConfig is the AC reference config made hosted against baseURL,
// with a baseline one second longer than the pinned 30 s guard.
func hostedConfig(t *testing.T, baseURL string) []byte {
	t.Helper()
	raw, err := os.ReadFile("../../configs/ac.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	for old, repl := range map[string]string{
		"warmup_s: 10\n":                             "warmup_s: 0.5\n",
		"baseline_s: 60\n":                           "baseline_s: 31\n",
		"fault_window_timeout_s: 120\n":              "fault_window_timeout_s: 1.5\n",
		"cooldown_s: 5\n":                            "cooldown_s: 0.5\n",
		"t_inject_offset_s: 60 ":                     "t_inject_offset_s: 31 ",
		"ignore_eos: true ":                          "ignore_eos: false ",
		"base_url: http://127.0.0.1:18000\n":         "base_url: " + baseURL + "\n  hosted: true\n  model_name: openai/gpt-oss-120b\n  api_key_env: " + replayKeyEnv + "\n  response_shape:\n    reasoning: [reasoning]\n",
		"  replicas: 2\n":                            "  replicas: 1\n",
		"name: percentes-ac-reference\n":             "name: percentes-replay-test\n",
		"    distribution: fixed\n    fixed_ms: 200": "    distribution: fixed\n    fixed_ms: 20",
	} {
		if strings.Count(s, old) != 1 {
			t.Fatalf("config text %q not found once", old)
		}
		s = strings.Replace(s, old, repl, 1)
	}
	return []byte(s)
}

// hostedRun runs a hosted config against the shaped mock and writes what
// the command writes before the report pair: the attempts file, the clock
// and status records, and the manifest input. The clock record carries a
// flag, so the run is invalid on two counts.
func hostedRun(t *testing.T) (dir string, cfgRaw []byte, m *evidence.Manifest, art *run.Artifacts, gates validity.Report) {
	t.Helper()
	fixed := config.LatencyDist{Distribution: "fixed", FixedMs: 2}
	srv := mock.NewShaped(config.Mock{ListenAddr: "127.0.0.1:0", Seed: 1, TTFT: fixed, ITL: fixed},
		mock.Shape{ReasoningKey: "reasoning", ReasoningTokens: 2, Answer: `{"route":"billing"}`, Usage: mock.UsageAlways})
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	t.Setenv(replayKeyEnv, "replay-test-key-0123456789")
	cfgRaw = hostedConfig(t, "http://"+srv.Addr())
	cfg, err := config.Parse(cfgRaw)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	var canary *loadgen.Canary
	var canaryErr error
	art, err = run.Execute(ctx, cfg, run.Options{OnEpoch: func(e time.Time) { canary, canaryErr = loadgen.StartCanary(ctx, e) }})
	if err != nil {
		t.Fatal(err)
	}
	if canaryErr != nil {
		t.Fatal(canaryErr)
	}
	art.AttachObservations(run.Observed{TTFTFamily: cfg.Target.TTFTHistogram, Canary: canary.Stop()})
	gates = validity.Evaluate(art, validity.Observations{})
	if reasons := gates.FailReasons("G1", "G2"); len(reasons) > 0 {
		art.RunValid = false
		art.InvalidReasons = append(art.InvalidReasons, reasons...)
	}
	clock := &evidence.ClockRecord{Tool: evidence.ClockSNTP, StartUTC: art.Loadgen.EpochWall.Add(-2 * time.Second), EndUTC: art.Loadgen.EpochWall.Add(-time.Second),
		Flag: "clock offset: 75 s from sntp exceeds 60 s"}
	art.RunValid = false
	art.InvalidReasons = append(art.InvalidReasons, clock.Flag)

	dir = t.TempDir()
	attempts, err := evidence.WriteRun(dir, cfg, art.Loadgen, evidence.Options{RunFailures: art.InvalidReasons})
	if err != nil {
		t.Fatal(err)
	}
	clockPath, err := evidence.WriteClock(dir, clock)
	if err != nil {
		t.Fatal(err)
	}
	statusPath, err := evidence.WriteStatus(dir, evidence.StatusStartFile, &evidence.StatusRecord{Command: []string{"status"}})
	if err != nil {
		t.Fatal(err)
	}
	build := &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc123"}, {Key: "vcs.modified", Value: "true"}}}
	m, err = evidence.NewManifest(evidence.ManifestInput{
		Config: cfg, ConfigName: "hosted.yaml", Target: &cfg.Target, Result: art.Loadgen, Build: build,
		Operator: &evidence.Operator{Provider: "Example Cloud", AccountTier: "Basic", ClientHost: "test host"},
		Clock:    clock, ClockPath: clockPath, StatusStartPath: statusPath, ArtifactPaths: []string{attempts},
		Gates: &gates, RunValid: art.RunValid, InvalidReasons: art.InvalidReasons, Replay: art.Record(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return dir, cfgRaw, m, art, gates
}

func sum(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func TestReplay(t *testing.T) {
	dir, cfgRaw, m, art, gates := hostedRun(t)
	t.Run("reproduces the run", func(t *testing.T) { replayReproduces(t, dir, cfgRaw, m, art, gates) })
	t.Run("refuses altered evidence", func(t *testing.T) { replayRefuses(t, dir, cfgRaw) })
}

// The pair the command writes is the pair a replay of the published
// files renders, byte for byte, and its counts are the run's own.
func replayReproduces(t *testing.T, dir string, cfgRaw []byte, m *evidence.Manifest, art *run.Artifacts, gates validity.Report) {
	jsonPath, txtPath, text, err := WriteHostedPair(dir, cfgRaw, m)
	if err != nil {
		t.Fatal(err)
	}
	written, _ := os.ReadFile(jsonPath)
	writtenText, _ := os.ReadFile(txtPath)
	if string(writtenText) != text {
		t.Fatal("returned text differs from report.txt")
	}
	final, err := evidence.ReadManifest(filepath.Join(dir, evidence.ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	for name, b := range map[string][]byte{JSONFile: written, TextFile: writtenText} {
		if ref := final.Artifact(name); ref == nil || ref.SHA256 != sum(b) {
			t.Errorf("manifest reference for %s: %+v", name, ref)
		}
	}

	raw, replayText, err := FromRun(dir, cfgRaw)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, written) || replayText != text {
		t.Fatal("the replay differs from the written pair")
	}

	var rep map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rep); err != nil {
		t.Fatal(err)
	}
	if string(rep["instrument_commit"]) != `"abc123-dirty"` {
		t.Errorf("instrument commit %s, want the manifest's", rep["instrument_commit"])
	}
	compact := func(v any) string {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		var c bytes.Buffer
		if err := json.Compact(&c, b); err != nil {
			t.Fatal(err)
		}
		return c.String()
	}
	if string(rep["caveat"]) != compact(CaveatHosted) {
		t.Errorf("caveat %s, want the hosted caveat", rep["caveat"])
	}
	if bytes.Contains(raw, []byte("goodput")) || strings.Contains(text, "goodput") {
		t.Error("the hosted pair carries a goodput field")
	}
	for key, v := range map[string]any{
		"windows":           newHostedReport(&Report{Artifacts: art}).Windows,
		"in_flight_at_fire": art.InFlight,
		"receive_path":      art.ReceivePath,
		"share_gate":        art.ShareGate,
		"run_valid":         art.RunValid,
		"invalid_reasons":   art.InvalidReasons,
		"validity_gates":    gates,
	} {
		var got bytes.Buffer
		if err := json.Compact(&got, rep[key]); err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if want := compact(v); got.String() != want {
			t.Errorf("%s from the replay\n%s\nfrom the run\n%s", key, got.String(), want)
		}
	}
	base := art.Windows["baseline"]
	if base == nil || base.Completed == 0 || base.ITLPooled.Count == 0 || art.ReceivePath["baseline"].Canary == nil {
		t.Fatalf("the run left nothing to compare: %+v", base)
	}
	if !strings.Contains(text, "clock offset: 75 s from sntp exceeds 60 s") || !strings.Contains(text, "mock fault schedule unattested") {
		t.Errorf("invalid reasons missing from the text:\n%s", text)
	}
}

// Evidence that does not match itself, or does not match the run's
// recorded counts, renders nothing.
func replayRefuses(t *testing.T, dir string, cfgRaw []byte) {
	attempts, _ := os.ReadFile(filepath.Join(dir, evidence.AttemptsFile))
	manifest, _ := os.ReadFile(filepath.Join(dir, evidence.ManifestFile))

	editManifest := func(edit func(*evidence.Manifest)) []byte {
		var mm evidence.Manifest
		if err := json.Unmarshal(manifest, &mm); err != nil {
			t.Fatal(err)
		}
		edit(&mm)
		b, err := json.Marshal(&mm)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	// rehash points the manifest's attempts reference at b.
	rehash := func(b []byte) []byte {
		return editManifest(func(mm *evidence.Manifest) { mm.Artifact(evidence.AttemptsFile).SHA256 = sum(b) })
	}
	editLine := func(b []byte, n int, old, repl string) []byte {
		lines := bytes.SplitAfter(b, []byte("\n"))
		if !bytes.Contains(lines[n], []byte(old)) {
			t.Fatalf("line %d holds no %q", n, old)
		}
		lines[n] = bytes.Replace(lines[n], []byte(old), []byte(repl), 1)
		return bytes.Join(lines, nil)
	}
	// The first completed record after warm-up, which every window
	// count covers.
	firstCompleted := -1
	var events int
	for i, l := range bytes.Split(attempts, []byte("\n")) {
		var r evidence.Record
		if json.Unmarshal(l, &r) == nil && r.Outcome == loadgen.OutcomeCompleted && r.IntendedNs >= 500_000_000 {
			firstCompleted, events = i, r.ContentEvents
			break
		}
	}
	if firstCompleted < 0 {
		t.Fatal("no completed record after warm-up")
	}
	moreEvents := editLine(attempts, firstCompleted, `"content_events":`+itoa(events), `"content_events":`+itoa(events+1))
	errored := editLine(attempts, firstCompleted, `"outcome":"completed"`, `"outcome":"errored","err_class":"reset"`)
	fewerFailures := editLine(attempts, 0, `"run_failures":[`, `"run_failures":["extra",`)

	for _, c := range []struct {
		name          string
		cfg, att, man []byte
		want          string
	}{
		{"config edited", append(append([]byte{}, cfgRaw...), "# edited\n"...), attempts, manifest, "not hosted.yaml"},
		{"attempts edited, hash kept", cfgRaw, moreEvents, manifest, "not the file the manifest names"},
		{"content events edited", cfgRaw, moreEvents, rehash(moreEvents), "pooled gaps recorded"},
		{"outcome edited", cfgRaw, errored, rehash(errored), "pooled gaps recorded"},
		{"run failures edited", cfgRaw, fewerFailures, rehash(fewerFailures), "record 0 run failures"},
		{"invalid reasons edited", cfgRaw, attempts, editManifest(func(mm *evidence.Manifest) { mm.InvalidReasons[len(mm.InvalidReasons)-1] += "!" }), "the manifest valid=false"},
		{"undispatched edited", cfgRaw, attempts, editManifest(func(mm *evidence.Manifest) { mm.Replay.ClientGates.Undispatched++ }), "undispatched records"},
		{"protocols edited", cfgRaw, attempts, editManifest(func(mm *evidence.Manifest) { mm.Endpoint.Protocols["HTTP/1.1"]-- }), "protocols"},
		{"model edited", cfgRaw, attempts, editManifest(func(mm *evidence.Manifest) { mm.Endpoint.ReportedModels = nil }), "models in the records"},
		{"window bounds edited", cfgRaw, attempts, editManifest(func(mm *evidence.Manifest) { mm.Replay.Windows[0].EndNs++ }), "is not one the requests give"},
		{"window dropped", cfgRaw, attempts, editManifest(func(mm *evidence.Manifest) { mm.Replay.Windows = mm.Replay.Windows[1:] }), "windows, the requests give"},
		{"gates edited", cfgRaw, attempts, editManifest(func(mm *evidence.Manifest) { mm.Gates[1].Detail += "!" }), "gates G2 and G6"},
		{"replay record removed", cfgRaw, attempts, editManifest(func(mm *evidence.Manifest) { mm.Replay = nil }), "no replay record"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if bytes.Equal(c.att, attempts) && bytes.Equal(c.man, manifest) && bytes.Equal(c.cfg, cfgRaw) {
				t.Fatal("nothing was altered")
			}
			d := t.TempDir()
			for name, b := range map[string][]byte{evidence.AttemptsFile: c.att, evidence.ManifestFile: c.man} {
				if err := os.WriteFile(filepath.Join(d, name), b, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := FromRun(d, c.cfg); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("got %v, want an error containing %q", err, c.want)
			}
		})
	}
}

func itoa(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}
