package report

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/run"
	"github.com/percentes/percentes/internal/validity"
)

// The report pair's file names in a run's output directory.
const (
	JSONFile = "report.json"
	TextFile = "report.txt"
)

// FromRun renders the report pair of the hosted run whose manifest and
// attempts file are in dir, from those two files and cfgRaw, the
// configuration file the manifest names. Every count the manifest and the
// attempts file both state is checked against the requests: undispatched
// requests, reported models and mismatches, protocols, run validity, the
// invalid reasons on the manifest and on every record, and the G2 and G6
// gates. A release cell's pair is its CellReport, checked for protocols,
// the invalid reasons on every record, the hosted-G6 results and the
// steps its G2 results cover.
func FromRun(dir string, cfgRaw []byte) ([]byte, string, error) {
	m, err := evidence.ReadManifest(filepath.Join(dir, evidence.ManifestFile))
	if err != nil {
		return nil, "", err
	}
	if sum := sha256.Sum256(cfgRaw); hex.EncodeToString(sum[:]) != m.Config.SHA256 {
		return nil, "", fmt.Errorf("report: the config is not %s, the file the manifest names", m.Config.Name)
	}
	release := m.Profile == config.ProfileRelease
	if m.Replay == nil && !release {
		return nil, "", errors.New("report: the manifest has no replay record (the run ended in an error)")
	}
	cfg, err := config.Parse(cfgRaw)
	if err != nil {
		return nil, "", fmt.Errorf("report: %w", err)
	}
	ref := m.Artifact(evidence.AttemptsFile)
	if ref == nil {
		return nil, "", fmt.Errorf("report: the manifest names no %s", evidence.AttemptsFile)
	}
	b, err := os.ReadFile(filepath.Join(dir, evidence.AttemptsFile))
	if err != nil {
		return nil, "", fmt.Errorf("report: %w", err)
	}
	if sum := sha256.Sum256(b); hex.EncodeToString(sum[:]) != ref.SHA256 {
		return nil, "", fmt.Errorf("report: %s is not the file the manifest names", evidence.AttemptsFile)
	}
	recs, err := evidence.ReadRecords(bytes.NewReader(b))
	if err != nil {
		return nil, "", err
	}
	requests := make([]loadgen.Request, len(recs))
	protocols := map[string]int{}
	undispatched := 0
	for i := range recs {
		requests[i] = recs[i].Request()
		if p := requests[i].Proto; p != "" {
			protocols[p]++
		}
		if requests[i].DispatchNs == 0 {
			undispatched++
		}
	}
	if !maps.Equal(protocols, m.Endpoint.Protocols) {
		return nil, "", fmt.Errorf("report: protocols %v in the records, %v in the manifest", protocols, m.Endpoint.Protocols)
	}
	if release {
		return cellReport(m, cfg, recs)
	}
	if undispatched != m.Replay.ClientGates.Undispatched {
		return nil, "", fmt.Errorf("report: %d undispatched records, the client gate counted %d", undispatched, m.Replay.ClientGates.Undispatched)
	}

	art, err := run.Replay(cfg, m.StartUTC, requests, m.Replay)
	if err != nil {
		return nil, "", fmt.Errorf("report: %w", err)
	}
	res, e := art.Loadgen, m.Endpoint
	if res.RequestedModel != e.RequestedModel || !slices.Equal(res.ReportedModels, e.ReportedModels) || res.ModelMismatches != e.ModelMismatches || res.SplitSeries != e.SplitSeries {
		return nil, "", fmt.Errorf("report: models in the records (requested %q, reported %q, %d mismatches) differ from the manifest (requested %q, reported %q, %d mismatches)",
			res.RequestedModel, res.ReportedModels, res.ModelMismatches, e.RequestedModel, e.ReportedModels, e.ModelMismatches)
	}
	gates := validity.Evaluate(art, validity.Observations{})
	if reasons := gates.FailReasons("G1", "G2"); len(reasons) > 0 {
		art.RunValid = false
		art.InvalidReasons = append(art.InvalidReasons, reasons...)
	}
	if m.Clock.Flag != "" {
		art.RunValid = false
		art.InvalidReasons = append(art.InvalidReasons, m.Clock.Flag)
	}
	if art.RunValid != m.RunValid || !slices.Equal(art.InvalidReasons, m.InvalidReasons) {
		return nil, "", fmt.Errorf("report: the records give valid=%v %q, the manifest valid=%v %q", art.RunValid, art.InvalidReasons, m.RunValid, m.InvalidReasons)
	}
	for i := range recs {
		if !slices.Equal(recs[i].RunFailures, art.InvalidReasons) {
			return nil, "", fmt.Errorf("report: record %d run failures %q, the run's %q", i, recs[i].RunFailures, art.InvalidReasons)
		}
	}
	var g2g6 []validity.Gate
	for _, g := range gates.Gates {
		if g.ID == "G2" || g.ID == "G6" {
			g2g6 = append(g2g6, g)
		}
	}
	if !reflect.DeepEqual(g2g6, m.Gates) {
		return nil, "", fmt.Errorf("report: gates G2 and G6 from the records differ from the manifest's")
	}
	return generate(art, &gates, commitText(m.Source.Revision, m.Source.Modified))
}

// WriteHostedPair writes m to dir, renders the report pair from dir with
// FromRun, writes it there as JSONFile and TextFile, and rewrites m naming
// both. m must already name the attempts file in dir.
func WriteHostedPair(dir string, cfgRaw []byte, m *evidence.Manifest) (jsonPath, txtPath, text string, err error) {
	if _, err := evidence.WriteManifest(dir, m); err != nil {
		return "", "", "", err
	}
	raw, text, err := FromRun(dir, cfgRaw)
	if err != nil {
		return "", "", "", err
	}
	jsonPath, txtPath = filepath.Join(dir, JSONFile), filepath.Join(dir, TextFile)
	for path, b := range map[string][]byte{jsonPath: raw, txtPath: []byte(text)} {
		if err := os.WriteFile(path, b, 0o644); err != nil {
			return "", "", "", fmt.Errorf("report: %w", err)
		}
	}
	for _, path := range []string{jsonPath, txtPath} {
		ref, err := evidence.HashFile(path, filepath.Base(path))
		if err != nil {
			return "", "", "", err
		}
		m.Artifacts = append(m.Artifacts, ref)
	}
	if _, err := evidence.WriteManifest(dir, m); err != nil {
		return "", "", "", err
	}
	return jsonPath, txtPath, text, nil
}
