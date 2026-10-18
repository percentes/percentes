package admission

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

// routeConfig is the release reference as route name with its caps.
func routeConfig(t *testing.T, name, routeUSD, globalUSD string) *config.Config {
	t.Helper()
	cfg, err := config.LoadFile(releaseRef)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Target.Name, cfg.Release.Caps = name, config.LedgerCaps{RouteUSD: routeUSD, GlobalUSD: globalUSD}
	return cfg
}

func releaseLedger(t *testing.T, cfgs ...*config.Config) (*Ledger, string) {
	t.Helper()
	caps, err := ReleaseCaps(cfgs...)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	l, err := Create(path, caps)
	if err != nil {
		t.Fatal(err)
	}
	return l, path
}

// shapePrice is p taken at the release shape's input bound and ceiling.
func shapePrice(p Price, ceiling int) Price {
	p.InputTokens, p.OutputTokens = config.PinnedReleaseInputBoundTokens, ceiling
	return p
}

// A release ledger's caps come from the routes' configurations: each
// route's cap under its name, the one global cap, no request caps.
func TestReleaseCaps(t *testing.T) {
	groqCfg, googleCfg := routeConfig(t, "groq", "10", "1300"), routeConfig(t, "google", "80", "1300")
	_, path := releaseLedger(t, groqCfg, googleCfg)
	b, _ := os.ReadFile(path)
	if !strings.Contains(string(b), `"caps":{"route_requests":0,"total_requests":0,"liability_usd":"1300","route_liability_usd":{"google":"80","groq":"10"},"window_requests":0,"max_input_tokens":0,"max_output_tokens":0}`) {
		t.Errorf("open line %s", b)
	}
	pilot, err := config.LoadFile("../../configs/pilot.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for name, cfgs := range map[string][]*config.Config{
		"none":                  nil,
		"global caps differ":    {groqCfg, routeConfig(t, "google", "80", "1700")},
		"route listed twice":    {groqCfg, routeConfig(t, "groq", "10", "1300")},
		"route cap over global": {routeConfig(t, "groq", "1400", "1300")},
		"cap not a decimal":     {routeConfig(t, "groq", "ten", "1300")},
		"pilot config":          {pilot},
	} {
		if _, err := ReleaseCaps(cfgs...); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	for name, c := range map[string]Caps{
		"zero liability":          {LiabilityUSD: "0"},
		"route over global":       {LiabilityUSD: "10", RouteLiabilityUSD: map[string]string{"groq": "11"}},
		"empty route name":        {LiabilityUSD: "10", RouteLiabilityUSD: map[string]string{"": "1"}},
		"negative request cap":    {LiabilityUSD: "10", TotalRequests: -1},
		"one token bound":         {LiabilityUSD: "10", MaxInputTokens: 1200},
		"liability not a decimal": {LiabilityUSD: "1e3"},
	} {
		if _, err := Create(filepath.Join(t.TempDir(), "ledger.jsonl"), c); err == nil {
			t.Errorf("Create with %s: accepted", name)
		}
	}
}

// A route process checks the ledger carries its configuration's caps.
func TestCheckRelease(t *testing.T) {
	l, _ := releaseLedger(t, routeConfig(t, "groq", "10", "1300"), routeConfig(t, "google", "80", "1300"))
	st := state(t, l)
	if err := st.CheckRelease(routeConfig(t, "google", "80.0", "1300")); err != nil {
		t.Fatal(err)
	}
	for name, cfg := range map[string]*config.Config{
		"route cap changed":  routeConfig(t, "groq", "11", "1300"),
		"global cap changed": routeConfig(t, "groq", "10", "1700"),
		"route not listed":   routeConfig(t, "azure", "10", "1300"),
	} {
		if err := st.CheckRelease(cfg); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// Each route's projected liability may reach its cap and no further, the
// sum across routes the global cap, and a route the ledger does not cap
// admits nothing.
func TestReleaseLedgerCaps(t *testing.T) {
	l, _ := releaseLedger(t, routeConfig(t, "groq", "10", "85"), routeConfig(t, "google", "80", "85"))
	refused(t, l, "groq", PurposeCell, 1, "no price recorded for groq")
	must(t, l.RecordPrice("groq", shapePrice(groqPrice, 512)))
	must(t, l.RecordPrice("google", shapePrice(price("0.75", "3.75"), 512)))
	must(t, l.RecordPrice("azure", shapePrice(price("2", "10"), 512)))

	// US$0.0004872 a request: 20,525 requests are US$9.99978.
	admit(t, l, "groq", PurposeCell, 20525)
	refused(t, l, "groq", PurposeProbe, 1, "projected liability for the route US$10.0002672 exceeds US$10")
	// US$0.00282 a request.
	admit(t, l, "google", PurposeCell, 26000)
	refused(t, l, "google", PurposeCell, 1000, "projected liability US$86.13978 exceeds US$85")
	refused(t, l, "azure", PurposeProbe, 1, "no liability cap for route azure")

	if _, err := l.Admit("groq", PurposeWindow, WindowRequests); err == nil || errors.As(err, new(*Refusal)) || !strings.Contains(err.Error(), "admits no window") {
		t.Errorf("window on a release ledger: %v", err)
	}
	if _, err := l.Admit("groq", PurposeCell, 0); err == nil {
		t.Error("empty cell admitted")
	}
	st := state(t, l)
	if liab, err := st.Liability(); err != nil || decimalString(liab) != "83.31978" {
		t.Fatalf("liability %v %v", liab, err)
	}
}

// A cell settles its probe, warm-up and measured requests; never
// development requests.
func TestReleaseCellSettles(t *testing.T) {
	l, _ := releaseLedger(t, routeConfig(t, "groq", "10", "1300"))
	must(t, l.RecordPrice("groq", shapePrice(groqPrice, 512)))
	a := admit(t, l, "groq", PurposeCell, 3735+2)
	if err := l.Settle(a, Counts{Development: 1}); err == nil {
		t.Fatal("development settled on a cell")
	}
	must(t, l.Settle(a, Counts{Probe: 2, Warmup: 14, Measured: 3700}))
	u := state(t, l).Routes["groq"]
	if u.Probe != 2 || u.Warmup != 14 || u.Measured != 3700 || u.Replacement != 0 || u.Reserved != 0 {
		t.Fatalf("usage %+v", *u)
	}
}

// A price is taken at the token bounds it names, written on its line;
// a release ledger has no default bounds.
func TestPricePerShape(t *testing.T) {
	l, path := releaseLedger(t, routeConfig(t, "groq", "10", "1300"))
	if err := l.RecordPrice("groq", groqPrice); err == nil || !strings.Contains(err.Error(), "token bounds 0 and 0") {
		t.Fatalf("price without bounds: %v", err)
	}
	must(t, l.RecordPrice("groq", shapePrice(groqPrice, 1024)))
	must(t, l.RecordPrice("groq", shapePrice(groqPrice, 512)))
	if got := state(t, l).Routes["groq"].MaxPerRequestUSD; decimalString(got) != "0.0007944" {
		t.Fatalf("max price %s, want the 1,024-token price", decimalString(got))
	}
	b, _ := os.ReadFile(path)
	if !strings.Contains(string(b), `"input_tokens":1200,"output_tokens":512,"per_request_usd":"0.0004872"`) {
		t.Fatalf("price line %s", b)
	}
	altered := filepath.Join(t.TempDir(), "ledger.jsonl")
	must(t, os.WriteFile(altered, []byte(strings.Replace(string(b), `"output_tokens":512`, `"output_tokens":256`, 1)), 0o600))
	if _, err := Open(altered); err == nil || !strings.Contains(err.Error(), "prices give") {
		t.Fatalf("altered bounds: %v", err)
	}

	pl, ppath := newLedger(t)
	must(t, pl.RecordPrice(groq, groqPrice))
	if b, _ := os.ReadFile(ppath); !strings.Contains(string(b), `"input_tokens":2000,"output_tokens":4096,"per_request_usd":"0.0027576"`) {
		t.Fatalf("pilot price line %s", b)
	}
}
