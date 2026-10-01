package vllmlog

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/detect"
)

const fixtureSHA256 = "ed288d1cab130695b96c30b40cb52ce27a8235f30ab009748dc3a528a62513c2"

var fire = time.Date(2026, 9, 16, 19, 22, 27, 0, time.UTC)

var logRowNames = []string{"log_bringup", "engine_init", "weight_download", "weight_load", "torch_compile", "profile_kv_capture", "engine_ready", "server_ready"}

func fixture(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/vllm-startup-16Sep2026.log")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	if got := hex.EncodeToString(sum[:]); got != fixtureSHA256 {
		t.Fatalf("fixture sha256 %s, want %s", got, fixtureSHA256)
	}
	return b
}

// dockerBoots prefixes every fixture line with a Docker stamp, once per
// boot start, one millisecond apart.
func dockerBoots(t *testing.T, starts ...time.Time) []byte {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(string(fixture(t)), "\n"), "\n")
	var sb strings.Builder
	for _, s := range starts {
		for i, l := range lines {
			sb.WriteString(s.Add(time.Duration(i) * time.Millisecond).Format("2006-01-02T15:04:05.000000000Z07:00"))
			sb.WriteString(" ")
			sb.WriteString(l)
			sb.WriteString("\n")
		}
	}
	return []byte(sb.String())
}

func segment(t *testing.T, d *detect.Decomposition, name string) detect.Segment {
	t.Helper()
	for _, s := range d.Segments {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("no segment %q", name)
	return detect.Segment{}
}

func TestParseColdStartFixture(t *testing.T) {
	b := Parse(fixture(t), VLLM0290, fire, 2026)
	if b.Lines != 121 {
		t.Fatalf("Lines = %d, want 121", b.Lines)
	}
	want := []struct {
		key     Key
		line    int
		hms     string
		figures map[string]float64
	}{
		{KeyBanner, 3, "19:22:27", nil},
		{KeyEngineInit, 12, "19:22:47", nil},
		{KeyModelLoadStart, 18, "19:22:54", nil},
		{KeyWeightDownload, 21, "19:23:16", map[string]float64{"weight_download_s": 21.733314}},
		{KeyWeightsLoaded, 31, "19:23:19", map[string]float64{"weights_loaded_s": 2.71}},
		{KeyModelLoaded, 32, "19:23:20", map[string]float64{"model_loaded_gib": 14.29, "model_loaded_s": 26.309395}},
		{KeyCompileCacheDir, 35, "19:23:26", nil},
		{KeyDynamo, 36, "19:23:26", map[string]float64{"dynamo_s": 5.82}},
		{KeyGraphCompile, 37, "19:23:34", map[string]float64{"graph_compile_s": 7.50}},
		{KeyTorchCompile, 40, "19:23:37", map[string]float64{"torch_compile_s": 17.12}},
		{KeyGraphCapture, 48, "19:23:44", map[string]float64{"graph_capture_s": 5, "graph_capture_gib": 0.52}},
		{KeyEngineReady, 51, "19:23:49", map[string]float64{"init_engine_s": 29.24, "init_engine_compilation_s": 17.12}},
		{KeyServerStart, 57, "19:23:50", nil},
	}
	if len(b.Matches) != len(want)+1 {
		t.Fatalf("%d matches, want %d", len(b.Matches), len(want)+1)
	}
	for _, w := range want {
		m, ok := b.Matches[w.key]
		if !ok {
			t.Errorf("%s: no match", w.key)
			continue
		}
		at, err := time.Parse("2006-01-02 15:04:05", "2026-09-16 "+w.hms)
		if err != nil {
			t.Fatal(err)
		}
		if m.Line != w.line || !m.At.Equal(at) || m.Stamp != StampVLLM || m.Resolution != time.Second {
			t.Errorf("%s: line %d at %v stamp %s resolution %v; want line %d at %v vllm 1s", w.key, m.Line, m.At, m.Stamp, m.Resolution, w.line, at)
		}
		if !reflect.DeepEqual(m.Figures, w.figures) {
			t.Errorf("%s: figures %v, want %v", w.key, m.Figures, w.figures)
		}
	}
	sc := b.Matches[KeyStartupComplete]
	if sc.Line != 112 || sc.Stamp != StampNone || !sc.At.IsZero() || sc.Resolution != 0 {
		t.Errorf("startup_complete: line %d stamp %s at %v resolution %v; want 112, none, zero, 0", sc.Line, sc.Stamp, sc.At, sc.Resolution)
	}

	late := Parse(fixture(t), VLLM0290, time.Date(2026, 9, 16, 19, 23, 49, 500e6, time.UTC), 2026)
	var keys []string
	for k := range late.Matches {
		keys = append(keys, string(k))
	}
	if len(late.Matches) != 3 || late.Matches[KeyEngineReady].Line != 51 || late.Matches[KeyStartupComplete].Line != 112 {
		t.Errorf("since 19:23:49.5 kept %v, want engine_ready (same second), server_start, startup_complete", keys)
	}
}

func TestApplyColdStart(t *testing.T) {
	d := detect.NewDecomposition(config.VariantProcessKill)
	Apply(d, Parse(fixture(t), VLLM0290, fire, 2026), fire, 0)
	want := map[string]float64{
		"log_bringup": 83, "engine_init": 20, "weight_download": 22, "weight_load": 25,
		"torch_compile": 17, "profile_kv_capture": 7, "engine_ready": 82,
	}
	for name, w := range want {
		got := segment(t, d, name).DurationS()
		if got == nil || *got != w {
			t.Errorf("%s = %v, want %v s", name, got, w)
		}
		if n := segment(t, d, name).Note; n != "" {
			t.Errorf("%s measured with note %q", name, n)
		}
	}
	sr := segment(t, d, "server_ready")
	if sr.Measured || sr.Note != "pattern startup_complete found on line 112 with no Docker stamp" {
		t.Errorf("server_ready measured=%v note %q", sr.Measured, sr.Note)
	}
	if rs := segment(t, d, "reschedule"); rs.Measured || rs.Note != "no scheduler: the container runtime restarts in place" {
		t.Errorf("reschedule changed: %+v", rs)
	}
	figures := map[string]float64{
		"weight_download_s": 21.733314, "weights_loaded_s": 2.71, "model_loaded_s": 26.309395,
		"model_loaded_gib": 14.29, "dynamo_s": 5.82, "graph_compile_s": 7.50, "torch_compile_s": 17.12,
		"graph_capture_s": 5, "graph_capture_gib": 0.52, "init_engine_s": 29.24, "init_engine_compilation_s": 17.12,
	}
	if !reflect.DeepEqual(d.LogFigures, figures) {
		t.Errorf("LogFigures = %v, want %v", d.LogFigures, figures)
	}
}

func TestParseDockerStampsTakeTheLastBoot(t *testing.T) {
	boot1 := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC)
	boot2 := time.Date(2026, 10, 1, 1, 10, 0, 0, time.UTC)
	b := Parse(dockerBoots(t, boot1, boot2), VLLM0290, boot1.Add(5*time.Minute), 2026)
	if b.Lines != 242 {
		t.Fatalf("Lines = %d, want 242", b.Lines)
	}
	if len(b.Matches) != len(VLLM0290) {
		t.Fatalf("%d matches, want %d", len(b.Matches), len(VLLM0290))
	}
	for k, m := range b.Matches {
		at := boot2.Add(time.Duration(m.Line-122) * time.Millisecond)
		if m.Line <= 121 || m.Stamp != StampDocker || !m.At.Equal(at) || m.Resolution != time.Nanosecond {
			t.Errorf("%s: line %d stamp %s at %v resolution %v; want second boot, docker, %v", k, m.Line, m.Stamp, m.At, m.Resolution, at)
		}
	}

	d := detect.NewDecomposition(config.VariantProcessKill)
	Apply(d, b, boot2, 0)
	if got := segment(t, d, "server_ready").DurationS(); got == nil || *got != 0.111 {
		t.Errorf("server_ready = %v, want 0.111 s", got)
	}
}

func TestOffsetShiftsEveryBoundary(t *testing.T) {
	boot := time.Date(2026, 10, 1, 1, 0, 0, 0, time.UTC)
	b := Parse(dockerBoots(t, boot), VLLM0290, boot, 2026)
	const offset = 2500 * time.Millisecond
	base := detect.NewDecomposition(config.VariantProcessKill)
	Apply(base, b, boot, 0)
	shifted := detect.NewDecomposition(config.VariantProcessKill)
	Apply(shifted, b, boot, offset)
	for _, name := range logRowNames {
		s0, s1 := segment(t, base, name), segment(t, shifted, name)
		if !s0.Measured || !s1.Measured {
			t.Errorf("%s: measured %v and %v, want both", name, s0.Measured, s1.Measured)
			continue
		}
		if !s1.EndAt.Equal(s0.EndAt.Add(-offset)) {
			t.Errorf("%s: end %v, want %v", name, s1.EndAt, s0.EndAt.Add(-offset))
		}
		wantStart := s0.StartAt.Add(-offset)
		if s0.StartAt.Equal(boot) {
			wantStart = boot
		}
		if !s1.StartAt.Equal(wantStart) {
			t.Errorf("%s: start %v, want %v", name, s1.StartAt, wantStart)
		}
	}
}

func TestAbsentPatternStaysNA(t *testing.T) {
	b := Parse(fixture(t), Mock, fire, 2026)
	if len(b.Matches) != 0 || b.Lines != 121 {
		t.Fatalf("mock patterns on the vLLM log: %d matches over %d lines, want 0 over 121", len(b.Matches), b.Lines)
	}
	d := detect.NewDecomposition(config.VariantProcessKill)
	Apply(d, b, fire, 0)
	first := map[string]Key{
		"log_bringup": KeyBanner, "engine_init": KeyEngineInit, "weight_download": KeyModelLoadStart,
		"weight_load": KeyModelLoadStart, "torch_compile": KeyModelLoaded, "profile_kv_capture": KeyTorchCompile,
		"engine_ready": KeyEngineReady, "server_ready": KeyStartupComplete,
	}
	for _, name := range logRowNames {
		s := segment(t, d, name)
		want := "pattern " + string(first[name]) + " not found in the log after the fire"
		if s.Measured || s.Note != want {
			t.Errorf("%s: measured=%v note %q, want %q", name, s.Measured, s.Note, want)
		}
	}
	if d.LogFigures != nil {
		t.Errorf("LogFigures = %v, want nil", d.LogFigures)
	}

	var noDownload []Pattern
	for _, p := range VLLM0290 {
		if p.Key != KeyWeightDownload {
			noDownload = append(noDownload, p)
		}
	}
	d = detect.NewDecomposition(config.VariantProcessKill)
	Apply(d, Parse(fixture(t), noDownload, fire, 2026), fire, 0)
	if s := segment(t, d, "weight_download"); s.Measured || s.Note != "no download line after the fire: weights served from the mounted cache" {
		t.Errorf("weight_download: measured=%v note %q", s.Measured, s.Note)
	}
	if s := segment(t, d, "weight_load"); !s.Measured {
		t.Errorf("weight_load unmeasured: %q", s.Note)
	}
}
