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

// at512 and at1024 are a release request's token bounds at each pinned
// ceiling.
var (
	at512  = Tokens{config.PinnedReleaseInputBoundTokens, 512}
	at1024 = Tokens{config.PinnedReleaseInputBoundTokens, 1024}
)

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
	refusedAt(t, l, "groq", PurposeCell, 1, at512, "no price recorded for groq")
	must(t, l.RecordPrice("groq", shapePrice(groqPrice, 512)))
	must(t, l.RecordPrice("google", shapePrice(price("0.75", "3.75"), 512)))
	must(t, l.RecordPrice("azure", shapePrice(price("2", "10"), 512)))

	// US$0.0004872 a request: 20,525 requests are US$9.99978.
	admitAt(t, l, "groq", PurposeCell, 20525, at512)
	refusedAt(t, l, "groq", PurposeProbe, 1, at512, "projected liability for the route US$10.0002672 exceeds US$10")
	// US$0.00282 a request.
	admitAt(t, l, "google", PurposeCell, 26000, at512)
	refusedAt(t, l, "google", PurposeCell, 1000, at512, "projected liability US$86.13978 exceeds US$85")
	refusedAt(t, l, "azure", PurposeProbe, 1, at512, "no liability cap for route azure")

	if _, err := l.Admit("groq", PurposeWindow, WindowRequests, at512); err == nil || errors.As(err, new(*Refusal)) || !strings.Contains(err.Error(), "admits no window") {
		t.Errorf("window on a release ledger: %v", err)
	}
	if _, err := l.Admit("groq", PurposeCell, 0, at512); err == nil {
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
	a := admitAt(t, l, "groq", PurposeCell, 3735+2, at512)
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
// a release ledger has no default bounds. Each bound keeps its own
// highest price.
func TestPricePerShape(t *testing.T) {
	l, path := releaseLedger(t, routeConfig(t, "groq", "10", "1300"))
	if err := l.RecordPrice("groq", groqPrice); err == nil || !strings.Contains(err.Error(), "token bounds 0 and 0") {
		t.Fatalf("price without bounds: %v", err)
	}
	must(t, l.RecordPrice("groq", shapePrice(groqPrice, 1024)))
	must(t, l.RecordPrice("groq", shapePrice(groqPrice, 512)))
	if p := state(t, l).Routes["groq"].Prices; len(p) != 2 || decimalString(p[at1024]) != "0.0007944" || decimalString(p[at512]) != "0.0004872" {
		t.Fatalf("prices %v, want US$0.0007944 at 1,024 and US$0.0004872 at 512", p)
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

// An admission names its requests' token bounds: it is refused where the
// route has no price at them, its line carries the price it counts at,
// and each request counts at the highest price recorded at its bounds.
func TestAdmitAtShapeBound(t *testing.T) {
	l, path := releaseLedger(t, routeConfig(t, "groq", "10", "1300"))
	must(t, l.RecordPrice("groq", shapePrice(groqPrice, 512)))
	refusedAt(t, l, "groq", PurposeProbe, 10, at1024, "no price recorded for groq at 1200 input and 1024 output tokens")
	b, _ := os.ReadFile(path)
	if !strings.Contains(string(b), `"kind":"refused","route":"groq","input_tokens":1200,"output_tokens":1024,"purpose":"probe","reserve":10`) {
		t.Fatalf("refused line %s", b)
	}

	a := admitAt(t, l, "groq", PurposeCell, 100, at512)
	if a.Tokens != at512 {
		t.Fatalf("admission %+v", a)
	}
	b, _ = os.ReadFile(path)
	if !strings.Contains(string(b), `"kind":"admit","route":"groq","input_tokens":1200,"output_tokens":512,"per_request_usd":"0.0004872","purpose":"cell","reserve":100`) {
		t.Fatalf("admit line %s", b)
	}
	altered := filepath.Join(t.TempDir(), "ledger.jsonl")
	must(t, os.WriteFile(altered, []byte(strings.Replace(string(b), `"per_request_usd":"0.0004872","purpose"`, `"per_request_usd":"0.0001","purpose"`, 1)), 0o600))
	if _, err := Open(altered); err == nil || !strings.Contains(err.Error(), "not the route's highest price") {
		t.Fatalf("altered admit price: %v", err)
	}

	// 100 at US$0.0004872 and 10 at US$0.0007944.
	must(t, l.RecordPrice("groq", shapePrice(groqPrice, 1024)))
	p := admitAt(t, l, "groq", PurposeProbe, 10, at1024)
	liability := func(want string) {
		t.Helper()
		if liab, err := state(t, l).Liability(); err != nil || decimalString(liab) != want {
			t.Fatalf("liability %v %v, want US$%s", liab, err, want)
		}
	}
	liability("0.056664")
	must(t, l.Settle(p, Counts{Probe: 4}))
	liability("0.0518976")
	// A higher price at 512 counts every request there at it, sent or
	// reserved; a lower one later changes nothing.
	must(t, l.RecordPrice("groq", shapePrice(price("0.20", "0.80"), 512)))
	liability("0.0681376")
	must(t, l.RecordPrice("groq", shapePrice(price("0.10", "0.40"), 512)))
	liability("0.0681376")
	must(t, l.Settle(a, Counts{Probe: 1, Warmup: 9, Measured: 60}))
	liability("0.0486496")
}

// A reservation is its requests at the highest price recorded at their
// bounds, and has no value where the route has no price there.
func TestReservationUSD(t *testing.T) {
	l, _ := releaseLedger(t, routeConfig(t, "groq", "10", "1300"))
	must(t, l.RecordPrice("groq", shapePrice(groqPrice, 512)))
	must(t, l.RecordPrice("groq", shapePrice(price("0.20", "0.80"), 512)))
	must(t, l.RecordPrice("groq", shapePrice(price("0.10", "0.40"), 512)))
	if got, err := state(t, l).ReservationUSD("groq", 3721, at512); err != nil || got != "2.4171616" {
		t.Fatalf("3,721 requests at 512: %q %v, want US$2.4171616 (US$0.0006496 each)", got, err)
	}
	for _, tc := range []struct {
		route string
		tok   Tokens
	}{{"groq", at1024}, {"google", at512}} {
		if got, err := state(t, l).ReservationUSD(tc.route, 1, tc.tok); err == nil || got != "" || !strings.Contains(err.Error(), "no price recorded for "+tc.route) {
			t.Errorf("%s at %v: %q %v", tc.route, tc.tok, got, err)
		}
	}
}

// A route's price revision is the line of its last price line. AdmitAt
// admits at the revision its caller checked, which another route's price
// line or admission leaves unchanged, and refuses a revision that differs
// with a *PriceChanged and no ledger line.
func TestAdmitAtPriceRevision(t *testing.T) {
	l, path := releaseLedger(t, routeConfig(t, "groq", "10", "1300"), routeConfig(t, "google", "80", "1300"))
	if rev := state(t, l).PriceRevision("groq"); rev != 0 {
		t.Fatalf("revision with no price: %d", rev)
	}
	must(t, l.RecordPrice("groq", shapePrice(groqPrice, 512)))
	rev := state(t, l).PriceRevision("groq")
	if rev != 2 {
		t.Fatalf("revision %d, want line 2", rev)
	}
	must(t, l.RecordPrice("google", shapePrice(price("0.75", "3.75"), 512)))
	admitAt(t, l, "google", PurposeCell, 10, at512)
	if got := state(t, l).PriceRevision("groq"); got != rev {
		t.Fatalf("revision %d after google's lines, want %d", got, rev)
	}
	a, err := l.AdmitAt("groq", PurposeCell, 10, at512, rev)
	if err != nil || a.Seq != 5 {
		t.Fatalf("admit at the checked revision: %+v %v", a, err)
	}
	// A lower price is a new revision too.
	must(t, l.RecordPrice("groq", shapePrice(price("0.10", "0.40"), 512)))
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = l.AdmitAt("groq", PurposeCell, 10, at512, rev)
	var changed *PriceChanged
	if !errors.As(err, &changed) || *changed != (PriceChanged{Route: "groq", Checked: 2, Ledger: 6}) || errors.As(err, new(*Refusal)) {
		t.Fatalf("admit at a stale revision: %v", err)
	}
	if want := "admission: groq's last price line is line 6; the admission checked line 2"; err.Error() != want {
		t.Errorf("error %q, want %q", err, want)
	}
	if after, err := os.ReadFile(path); err != nil || string(after) != string(before) {
		t.Fatalf("ledger changed: %v", err)
	}
	if _, err := l.AdmitAt("groq", PurposeCell, 10, at512, 6); err != nil {
		t.Fatalf("admit at the new revision: %v", err)
	}
}

// The ledger covers a ceiling rule verdict only with prices at both the
// cells' and the ceiling check's token bounds, each at least the
// verdict's.
func TestCovers(t *testing.T) {
	cfg := routeConfig(t, "groq", "10", "1300")
	l, _ := releaseLedger(t, cfg)
	verdict := func(p Price) *Verdict {
		t.Helper()
		v, err := CeilingRule(cfg, p, ExpectedPlan(cfg))
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	must(t, l.RecordPrice("groq", shapePrice(groqPrice, 512)))
	if err := state(t, l).Covers(cfg, verdict(groqPrice)); err == nil || !strings.Contains(err.Error(), "no price recorded for groq at 1200 input and 1024 output tokens") {
		t.Fatalf("priced at 512 only: %v", err)
	}
	must(t, l.RecordPrice("groq", shapePrice(groqPrice, 1024)))
	must(t, state(t, l).Covers(cfg, verdict(groqPrice)))
	newer := price("0.20", "0.80")
	must(t, l.RecordPrice("groq", shapePrice(newer, 512)))
	if err := state(t, l).Covers(cfg, verdict(newer)); err == nil || !strings.Contains(err.Error(), "US$0.0007944 a request at 1200 input and 1024 output tokens, under the ceiling rule's US$0.0010592") {
		t.Fatalf("1,024 price older than the 512 one: %v", err)
	}
	must(t, l.RecordPrice("groq", shapePrice(newer, 1024)))
	must(t, state(t, l).Covers(cfg, verdict(newer)))
	must(t, state(t, l).Covers(cfg, verdict(groqPrice)))
}
