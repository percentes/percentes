package evidence

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/histo"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/mock"
	"github.com/percentes/percentes/internal/run"
)

// The records of a shaped hosted run read back as written, and each gives
// its request without the fields the file does not carry.
func TestReadRecordsRoundTrip(t *testing.T) {
	fixed := config.LatencyDist{Distribution: "fixed", FixedMs: 1}
	srv := mock.NewShaped(config.Mock{ListenAddr: "127.0.0.1:0", Seed: 1, TTFT: fixed, ITL: fixed},
		mock.Shape{ReasoningKey: "reasoning", ReasoningTokens: 2, Answer: `{"route":"billing"}`, Usage: mock.UsageAlways})
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	cfg := hostedCfg("http://" + srv.Addr())
	cfg.Target.ResponseShape = &config.ResponseShape{Reasoning: []string{"reasoning"}}
	res, err := loadgen.Run(context.Background(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	res.Requests = append(res.Requests,
		loadgen.Request{Index: 3, IntendedNs: 400_000_000, DispatchNs: 400_100_000, DoneNs: 401_000_000, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrStatus429,
			Proto: "HTTP/1.1", Status: 429, Headers: map[string][]string{"Retry-After": {"7"}}, ErrText: "status 429", ConnReused: true},
		loadgen.Request{Index: 4, IntendedNs: 500_000_000})
	recs := Records(cfg, res, Options{RunFailures: []string{"client-validity gate failed (§2)"}})
	var b bytes.Buffer
	if err := Write(&b, recs); err != nil {
		t.Fatal(err)
	}
	back, err := ReadRecords(&b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back, recs) {
		t.Fatalf("read back\n%+v\nwritten\n%+v", back, recs)
	}
	for i := range back {
		want := res.Requests[i]
		if want.Tokens < 2 && want.ITLsUs != nil {
			t.Fatalf("request %d: gaps without tokens", i)
		}
		want.ITLsUs, want.Reasoning, want.Frames, want.ErrorBody, want.FrameBytesDropped = nil, "", nil, nil, 0
		if got := back[i].Request(); !reflect.DeepEqual(got, want) {
			t.Errorf("request %d:\n got %+v\nwant %+v", i, got, want)
		}
	}
	if back[0].Request().Content != `{"route":"billing"}` || !back[0].Request().UsageSeen || back[0].Request().Tokens != 5 {
		t.Errorf("request 0: %+v", back[0].Request())
	}
}

func TestReadRecordsRefusals(t *testing.T) {
	line0 := `{"index":0,"intended_ns":1,"dispatch_ns":0,"first_token_ns":0,"done_ns":0,"outcome":"","content_events":0,"reasoning":false}`
	line1 := strings.Replace(line0, `"index":0`, `"index":1`, 1)
	if recs, err := ReadRecords(strings.NewReader(line0 + "\n" + line1 + "\n")); err != nil || len(recs) != 2 {
		t.Fatalf("two lines: %d records, %v", len(recs), err)
	}
	for name, body := range map[string]string{
		"unknown field":   strings.Replace(line0, `"reasoning":false`, `"reasoning":false,"extra":1`, 1),
		"index skipped":   line1,
		"index repeated":  line0 + "\n" + line0,
		"trailing data":   line0 + ` {}`,
		"two per line":    line0 + line1,
		"blank line":      line0 + "\n\n" + line1,
		"not an object":   `[1]`,
		"truncated":       line0[:20],
		"wrong type":      strings.Replace(line0, `"intended_ns":1`, `"intended_ns":"1"`, 1),
		"answer not text": strings.Replace(line0, `"reasoning":false`, `"reasoning":false,"answer":3`, 1),
	} {
		if _, err := ReadRecords(strings.NewReader(body)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// A manifest with its replay record reads back field for field; fields
// it does not define and data after it are refused.
func TestReadManifest(t *testing.T) {
	in, out := manifestRun(t)
	in.Replay = &run.Record{
		ClientGates: loadgen.GateReport{SendSkewP99Us: 120, Undispatched: 1, CPUMeasured: true, CPUWorstWindowPct: 12.5, GCPauseP99Ms: 0.25, Pass: false},
		Tripwires:   &loadgen.Tripwires{Scheduled: 126, Dispatches: 126, InFlightLimit: 4, MaxInFlight: 2},
		Windows: []run.WindowRecord{
			{Window: collect.Window{Name: "baseline", StartNs: 60e9, EndNs: 1260e9}, ITLPooled: histo.Summary{Count: 3, P50Us: 900, MaxUs: 2000},
				Canary: &loadgen.CanarySummary{TTFTMs: 20, ITLMs: 10, Tokens: 32, Streams: 4, Completed: 4, EventLagP99Us: 300}},
		},
		CanaryError: "canary: listen: refused",
	}
	m, err := NewManifest(in)
	if err != nil {
		t.Fatal(err)
	}
	path, err := WriteManifest(out, m)
	if err != nil {
		t.Fatal(err)
	}
	back, err := ReadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back.Replay, in.Replay) {
		t.Errorf("replay record:\n got %+v\nwant %+v", back.Replay, in.Replay)
	}
	if !back.StartUTC.Equal(m.StartUTC) || !reflect.DeepEqual(back.Gates, m.Gates) || !reflect.DeepEqual(back.InvalidReasons, m.InvalidReasons) ||
		!reflect.DeepEqual(back.Endpoint, m.Endpoint) || !reflect.DeepEqual(back.Source, m.Source) || back.Config != m.Config || back.Clock.Flag != m.Clock.Flag {
		t.Errorf("read back\n%+v\nwritten\n%+v", back, m)
	}
	if ref := back.Artifact(AttemptsFile); ref == nil || *ref != m.Artifacts[0] {
		t.Errorf("attempts reference %+v", ref)
	}
	if back.Artifact("frames.jsonl") != nil {
		t.Error("a file the artifacts do not name was found")
	}

	b, _ := os.ReadFile(path)
	for name, body := range map[string]string{
		"unknown field":  strings.Replace(string(b), `"schema_version": 1,`, `"schema_version": 1, "extra": true,`, 1),
		"unknown replay": strings.Replace(string(b), `"client_gates": {`, `"client_gates": {"extra": 1,`, 1),
		"trailing data":  string(b) + "{}",
	} {
		if body == string(b) {
			t.Fatalf("%s: mutation did not apply", name)
		}
		p := filepath.Join(t.TempDir(), ManifestFile)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadManifest(p); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
