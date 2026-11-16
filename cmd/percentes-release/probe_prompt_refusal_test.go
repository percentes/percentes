package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/report"
)

// The refusal line of a counted status_429 written 1 s after a cell probe
// that reported 40 prompt tokens counts the probe at 40, so only the
// refused request is at the input bound. The cell that wrote probe.json
// may end counted or client-invalid: the probe runs before the ladder.
func TestRefusalLineCountsReportedProbePrompt(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body) //nolint:errcheck
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, probeUsageStream) //nolint:errcheck
	}))
	defer srv.Close()
	url := srv.URL + "/openai/v1/chat/completions"
	release, _ := oneRelease(t, url, "groq", "baseten")
	groq, groqCfg := pagesRoute(t, "groq", url, "", "", release)
	shortLadder(groq)
	groq.cfg.Release.Ladder = config.Ladder{StepFractions: []float64{2, 4, 8, 16, 32}, WarmupS: 0.2, StepS: 0.2, DrainS: 0.05}
	rec := runCellOK(t, groq, groq.runs[0])
	if rec.Outcome != outcomeCounted && !clientGateOnly(rec) {
		t.Fatalf("outcome %s %q", rec.Outcome, rec.Reasons)
	}

	h, err := readHistory(groq.out, "groq", []string{"groq"}, []*config.Config{groq.cfg}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var probes []report.Send
	for _, s := range h.Sends {
		if s.Kind == report.SendProbe {
			probes = append(probes, s)
		}
	}
	if len(probes) != 1 || probes[0].WroteUTC == nil {
		t.Fatalf("probe sends %+v", probes)
	}
	ps := probes[0]

	cfg, err := config.LoadFile(groqCfg)
	if err != nil {
		t.Fatal(err)
	}
	m, err := evidence.ReadManifest(filepath.Join(groq.cellDir(groq.runs[0]), evidence.ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	v := m.Release.Admission
	atS := cfg.Release.Ladder.WarmupS + 1
	epoch := ps.WroteUTC.Add(time.Second - time.Duration(atS*float64(time.Second)))
	ns := int64(atS * 1e9)
	refusal := evidence.Record{Index: 0, Step: 1, Shape: "S", CeilingTokens: cfg.Release.Shape.CeilingTokens, IntendedNs: ns, DispatchNs: ns,
		WroteRequestNs: ns + 2_000_000, DoneNs: ns + 10_000_000, Outcome: loadgen.OutcomeErrored, ErrClass: loadgen.ErrStatus429, Status: 429}
	cell := report.CellRecords{Cell: "fixture", Outcome: evidence.CellCounted, EpochUTC: epoch, Records: []evidence.Record{refusal}}
	st, err := report.NewStepTables(cfg, v, []report.CellRecords{cell})
	if err != nil {
		t.Fatal(err)
	}
	g := &report.History{Group: h.Group, Routes: h.Routes, Sends: append([]report.Send{ps},
		report.RecordSends("groq", "fixture", epoch, cell.Records, cfg.Release.Shape.InputBoundTokens, cfg.Client.HTTPTimeoutS)...)}
	if err := st.Attribute(cfg, v, g); err != nil {
		t.Fatal(err)
	}
	page := st.Text()
	var line string
	for _, l := range strings.Split(page, "\n") {
		if strings.Contains(l, "tokens_per_minute over 60 s") {
			line = l
			break
		}
	}
	if !strings.Contains(line, "tokens_per_minute over 60 s: 2 requests") || !strings.Contains(line, "(1 at the input bound, no usable prompt count reported)") {
		t.Errorf("refusal line counts the reported probe at the bound:\n%s\n%s", line, page)
	}
}
