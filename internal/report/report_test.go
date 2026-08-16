package report

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/detect"
	"github.com/percentes/percentes/internal/histo"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/run"
)

// minimalArtifacts is the smallest well-formed run product: no windows,
// no fault, gates zero-valued. Everything the renderer touches must
// tolerate it — a report generator that panics on a sparse run would
// also panic on a degenerate real one.
func minimalArtifacts() *run.Artifacts {
	return &run.Artifacts{
		Config:        &config.Config{},
		Loadgen:       &loadgen.Result{},
		Windows:       map[string]*collect.Stats{},
		Detector:      &detect.Result{},
		Decomposition: &detect.Decomposition{},
	}
}

// The JSON artifact carries the instrument commit so a published number
// traces to the build that produced it. Test binaries lack VCS stamping,
// so the field must degrade to "unknown", never to empty.
func TestReportCarriesInstrumentCommit(t *testing.T) {
	raw, _, err := Generate(minimalArtifacts())
	if err != nil {
		t.Fatal(err)
	}
	var rep struct {
		InstrumentCommit string `json:"instrument_commit"`
	}
	if err := json.Unmarshal(raw, &rep); err != nil {
		t.Fatal(err)
	}
	if rep.InstrumentCommit == "" {
		t.Error("instrument_commit must never serialize empty")
	}
}

// The caveat is the report's honesty banner. It must appear in the JSON
// artifact and twice in the human report (header and footer).
func TestGenerateCarriesCaveatAndValidJSON(t *testing.T) {
	raw, humanText, err := Generate(minimalArtifacts())
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(raw) {
		t.Fatal("JSON artifact is not valid JSON")
	}
	var rep struct {
		SchemaVersion int    `json:"schema_version"`
		ConfigSHA256  string `json:"config_sha256"`
		Caveat        string `json:"caveat"`
	}
	if err := json.Unmarshal(raw, &rep); err != nil {
		t.Fatal(err)
	}
	if rep.SchemaVersion != 2 {
		t.Errorf("schema_version = %d, want 2", rep.SchemaVersion)
	}
	if len(rep.ConfigSHA256) != 64 {
		t.Errorf("config_sha256 must be 64 hex chars, got %q", rep.ConfigSHA256)
	}
	if rep.Caveat != Caveat {
		t.Error("JSON caveat field lost or altered")
	}
	if got := strings.Count(humanText, Caveat); got != 2 {
		t.Errorf("human report must carry the caveat at header AND footer, found %d", got)
	}
}

// A missing fault or baseline window must yield the explicit
// not-applicable headline, never a partially-filled template.
func TestHeadlineRefusesWithoutWindows(t *testing.T) {
	got := headline(minimalArtifacts())
	if !strings.Contains(got, "headline not applicable") {
		t.Fatalf("want explicit not-applicable headline, got %q", got)
	}
}

// The unmeasured-CPU wording is the documented macOS behavior (an
// uncertified gate does not pass); the report must say it in exactly
// those honest terms rather than printing a zero that looks measured.
func TestHumanReportNamesUnmeasuredCPU(t *testing.T) {
	art := minimalArtifacts()
	art.Loadgen.Gates.CPUMeasured = false
	_, humanText, err := Generate(art)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(humanText, "UNMEASURED on this platform build") {
		t.Error("unmeasured CPU must be named, not rendered as a measured 0%")
	}
}

// incidenceText's two branches are the §3 never-extrapolate rule made
// visible: a crossed quantile renders a time; an uncrossed one renders the
// "curve does not cross" refusal. Oracle: completions at 1s and 2s plus
// two censored-at-horizon observations — incidence reaches 0.5 exactly,
// so p50 crosses (ties-events-first) and p90 cannot.
func TestIncidenceTextRefusesUncrossedQuantiles(t *testing.T) {
	cif := collect.EstimateIncidence([]collect.Obs{
		{TimeUs: 1_000_000, Kind: collect.ObsCompletion},
		{TimeUs: 2_000_000, Kind: collect.ObsCompletion},
		{TimeUs: 30_000_000, Kind: collect.ObsCensored},
		{TimeUs: 30_000_000, Kind: collect.ObsCensored},
	}, 30_000_000)
	text := incidenceText(&cif)
	if !strings.Contains(text, "p50   completion at 2.000s") {
		t.Errorf("p50 crosses at the second completion (2/4 = 0.5):\n%s", text)
	}
	if !strings.Contains(text, "p90   > 30s") {
		t.Errorf("p90 is never reached and must be refused, not extrapolated:\n%s", text)
	}
}

// The curve rendering samples at most 12 points, but the last point is the
// window's final incidence: in a window with errors that is where the curve
// settles below 1.0. Oracle: 14 completions plus one error, so the sampling
// stride (14/12+1 = 2) does not land on the last index.
func TestIncidenceTextAlwaysShowsFinalPoint(t *testing.T) {
	obs := []collect.Obs{{TimeUs: 500_000, Kind: collect.ObsError}}
	for i := 1; i <= 14; i++ {
		obs = append(obs, collect.Obs{TimeUs: int64(i) * 1_000_000, Kind: collect.ObsCompletion})
	}
	cif := collect.EstimateIncidence(obs, 30_000_000)
	final := cif.Points[len(cif.Points)-1]
	text := incidenceText(&cif)
	want := fmt.Sprintf("incidence=%.4f", final.Incidence)
	if !strings.Contains(text, want) {
		t.Errorf("final incidence %q missing from the rendered curve:\n%s", want, text)
	}
	if final.Incidence >= 1.0 {
		t.Fatalf("oracle broken: with one error the curve must plateau below 1.0, got %v", final.Incidence)
	}
}

// ciText must render a refused interval as an explicit omission (§7),
// and a permitted one with its bounds.
func TestCITextRefusalAndBounds(t *testing.T) {
	refused := ciText(collect.TailCIs{P99: &collect.OrderStatCI{Permitted: false}})
	if !strings.Contains(refused, "p99-CI omitted (sample budget insufficient") {
		t.Errorf("refused CI must be an explicit omission, got %q", refused)
	}
	granted := ciText(collect.TailCIs{P95: &collect.OrderStatCI{LoUs: 1500, HiUs: 2500, Permitted: true}})
	if !strings.Contains(granted, "p95-CI [1.5, 2.5]ms") {
		t.Errorf("permitted CI must render its bounds, got %q", granted)
	}
}

// Zero completed samples is a legitimate window state (total outage) and
// must render as words, not as a row of zero percentiles.
func TestSummaryZeroSamples(t *testing.T) {
	if got := summary(histo.Summary{}); got != "no completed samples" {
		t.Errorf("empty summary must say so in words, got %q", got)
	}
}
