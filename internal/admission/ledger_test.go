package admission

import (
	"bytes"
	"errors"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
)

const (
	groq    = "https://api.groq.com/openai/v1/chat/completions"
	baseten = "https://inference.baseten.co/v1/chat/completions"
	third   = "https://example.test/v1/chat/completions"
)

var groqPrice = Price{InputUSDPerMTok: "0.15", OutputUSDPerMTok: "0.60", Source: "https://console.groq.com/docs/model/openai/gpt-oss-120b", Retrieved: "2026-10-06"}

// fortyPerRequest makes one request cost exactly US$40.
var fortyPerRequest = Price{InputUSDPerMTok: "0", OutputUSDPerMTok: "9765.625", Source: "https://example.test/pricing", Retrieved: "2026-10-06"}

func newLedger(t *testing.T) (*Ledger, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	l, err := Create(path, PilotCaps())
	if err != nil {
		t.Fatal(err)
	}
	return l, path
}

func state(t *testing.T, l *Ledger) *State {
	t.Helper()
	st, err := l.State()
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// admit and refused admit at the pilot's token bounds, admitAt and
// refusedAt at tok.
func admit(t *testing.T, l *Ledger, route, purpose string, n int) Admission {
	t.Helper()
	return admitAt(t, l, route, purpose, n, PilotTokens)
}

func admitAt(t *testing.T, l *Ledger, route, purpose string, n int, tok Tokens) Admission {
	t.Helper()
	a, err := l.Admit(route, purpose, n, tok)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func refused(t *testing.T, l *Ledger, route, purpose string, n int, reason string) {
	t.Helper()
	refusedAt(t, l, route, purpose, n, PilotTokens, reason)
}

func refusedAt(t *testing.T, l *Ledger, route, purpose string, n int, tok Tokens, reason string) {
	t.Helper()
	before := state(t, l)
	_, err := l.Admit(route, purpose, n, tok)
	var r *Refusal
	if !errors.As(err, &r) || !strings.Contains(r.Reason, reason) {
		t.Fatalf("Admit(%s, %s, %d, %v) = %v, want a refusal containing %q", route, purpose, n, tok, err, reason)
	}
	after := state(t, l)
	if after.Lines != before.Lines+1 || after.Total() != before.Total() {
		t.Fatalf("refusal: lines %d -> %d, total %d -> %d; want one refused line and no charge", before.Lines, after.Lines, before.Total(), after.Total())
	}
}

// A window reserves the pilot schedule's requests plus its probe request.
func TestWindowRequestsIsTheScheduleAndProbe(t *testing.T) {
	cfg, err := config.LoadFile("../../configs/pilot.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if n := len(loadgen.BuildSchedule(cfg)) + 1; n != WindowRequests {
		t.Fatalf("WindowRequests %d, schedule plus probe %d", WindowRequests, n)
	}
}

// A ledger is created once and must exist to be opened; it starts with
// the caps in force.
func TestCreateAndOpen(t *testing.T) {
	_, path := newLedger(t)
	if _, err := Create(path, PilotCaps()); err == nil {
		t.Error("Create over an existing ledger succeeded")
	}
	if _, err := Open(filepath.Join(t.TempDir(), "none.jsonl")); err == nil {
		t.Error("Open of a missing ledger succeeded")
	}
	missing := &Ledger{path: filepath.Join(t.TempDir(), "none.jsonl"), now: time.Now}
	if _, err := missing.Admit(groq, PurposeProbe, 1, PilotTokens); err == nil {
		t.Error("Admit on a missing ledger succeeded")
	}
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(path)
	if !bytes.Contains(b, []byte(`"caps":{"route_requests":1024,"total_requests":2048,"liability_usd":"40","window_requests":127,"max_input_tokens":2000,"max_output_tokens":4096}`)) {
		t.Errorf("open line %s", b)
	}
	if fi, _ := os.Stat(path); fi.Mode().Perm() != 0o600 {
		t.Errorf("mode %v", fi.Mode())
	}
	if st := state(t, l); st.Lines != 1 || st.Total() != 0 {
		t.Errorf("new ledger: %d lines, total %d", st.Lines, st.Total())
	}
}

// The ledger plus the reservation may reach 1,024 for a route and no
// further.
func TestRouteRequestCap(t *testing.T) {
	l, _ := newLedger(t)
	must(t, l.RecordPrice(groq, groqPrice))
	must(t, l.RecordPrice(baseten, groqPrice))
	admit(t, l, groq, PurposeDevelopment, PilotRouteRequestCap-WindowRequests)
	admit(t, l, groq, PurposeWindow, WindowRequests)
	refused(t, l, groq, PurposeProbe, 1, "1025 requests for the route exceed the cap of 1024")
	refused(t, l, groq, PurposeReplacement, WindowRequests, "1151 requests for the route exceed the cap of 1024")

	admit(t, l, baseten, PurposeDevelopment, PilotRouteRequestCap-WindowRequests+1)
	refused(t, l, baseten, PurposeWindow, WindowRequests, "1025 requests for the route exceed the cap of 1024")
	refused(t, l, baseten, PurposeReplacement, WindowRequests, "1025 requests for the route exceed the cap of 1024")
	admit(t, l, baseten, PurposeProbe, WindowRequests-1)
	st := state(t, l)
	if st.Routes[groq].Charged() != PilotRouteRequestCap || st.Routes[baseten].Charged() != PilotRouteRequestCap {
		t.Errorf("charged %d and %d", st.Routes[groq].Charged(), st.Routes[baseten].Charged())
	}
}

// The total across routes may reach 2,048 and no further.
func TestTotalCap(t *testing.T) {
	l, _ := newLedger(t)
	for _, e := range []string{groq, baseten, third} {
		must(t, l.RecordPrice(e, groqPrice))
	}
	admit(t, l, groq, PurposeDevelopment, 1024)
	admit(t, l, baseten, PurposeDevelopment, 1000)
	refused(t, l, third, PurposeWindow, WindowRequests, "2151 requests in total exceed the cap of 2048")
	admit(t, l, third, PurposeProbe, 24)
	refused(t, l, third, PurposeProbe, 1, "2049 requests in total exceed the cap of 2048")
	if st := state(t, l); st.Total() != PilotTotalRequestCap {
		t.Errorf("total %d", st.Total())
	}
}

// An unsettled admission counts at its reservation, across a reopen;
// settling replaces the reservation with the requests sent.
func TestReservationHoldsUntilSettled(t *testing.T) {
	l, path := newLedger(t)
	must(t, l.RecordPrice(groq, groqPrice))
	a := admit(t, l, groq, PurposeWindow, WindowRequests)
	l2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	st := state(t, l2)
	if u := st.Routes[groq]; u.Reserved != WindowRequests || u.Charged() != WindowRequests || len(st.Open) != 1 {
		t.Fatalf("after admit: %+v open %v", u, st.Open)
	}
	must(t, l2.Settle(a, Counts{Probe: 1, Warmup: 6, Measured: 50}))
	st = state(t, l)
	u := st.Routes[groq]
	if u.Reserved != 0 || u.Probe != 1 || u.Warmup != 6 || u.Measured != 50 || u.Charged() != 57 || len(st.Open) != 0 {
		t.Fatalf("after settle: %+v open %v", u, st.Open)
	}
}

// Projected liability (requests at the highest per-request price recorded
// at their token bounds, plus hosting) may reach US$40 and no further.
func TestLiabilityCap(t *testing.T) {
	l, _ := newLedger(t)
	refused(t, l, groq, PurposeProbe, 1, "no price recorded for "+groq)
	must(t, l.RecordPrice(groq, groqPrice))
	must(t, l.RecordPrice(groq, fortyPerRequest))
	must(t, l.RecordPrice(groq, groqPrice))
	if st := state(t, l); st.Routes[groq].Prices[PilotTokens].Cmp(big.NewRat(40, 1)) != 0 {
		t.Fatalf("max price %v, want the highest recorded", st.Routes[groq].Prices[PilotTokens])
	}
	a := admit(t, l, groq, PurposeProbe, 1)
	refused(t, l, groq, PurposeProbe, 1, "projected liability US$80 exceeds US$40")
	must(t, l.Settle(a, Counts{}))
	must(t, l.RecordHosting("0.01", "client host"))
	refused(t, l, groq, PurposeProbe, 1, "projected liability US$40.01 exceeds US$40")

	l2, _ := newLedger(t)
	must(t, l2.RecordPrice(groq, groqPrice))
	must(t, l2.RecordHosting("37.17", "client host, October"))
	// 1,024 requests at US$0.0027576 is US$2.8237824.
	admit(t, l2, groq, PurposeDevelopment, 1024-WindowRequests)
	admit(t, l2, groq, PurposeWindow, WindowRequests)
	st := state(t, l2)
	if liab, err := st.Liability(); err != nil || decimalString(liab) != "39.9937824" {
		t.Fatalf("liability %v %v", liab, err)
	}
	must(t, l2.RecordHosting("0.0062176", "storage"))
	must(t, l2.RecordPrice(baseten, groqPrice))
	refused(t, l2, baseten, PurposeProbe, 1, "projected liability US$40.0027576 exceeds US$40")
}

// Settlement rules: only an open admission, once, with counts its purpose
// allows; requests above the reservation are recorded and reported.
func TestSettle(t *testing.T) {
	l, _ := newLedger(t)
	must(t, l.RecordPrice(groq, groqPrice))
	w := admit(t, l, groq, PurposeWindow, WindowRequests)
	p := admit(t, l, groq, PurposeProbe, 2)
	d := admit(t, l, groq, PurposeDevelopment, 24)
	r := admit(t, l, groq, PurposeReplacement, WindowRequests)
	lines := state(t, l).Lines
	for name, err := range map[string]error{
		"development on a window": l.Settle(w, Counts{Development: 1}),
		"warm-up on a probe":      l.Settle(p, Counts{Warmup: 1}),
		"probe on development":    l.Settle(d, Counts{Probe: 1}),
		"negative":                l.Settle(w, Counts{Measured: -1}),
		"not an admit line":       l.Settle(Admission{Seq: 2, Route: groq, Purpose: PurposeProbe, Reserve: 1}, Counts{}),
		"altered admission":       l.Settle(Admission{Seq: w.Seq, Route: groq, Purpose: PurposeWindow, Reserve: 200}, Counts{}),
	} {
		if err == nil {
			t.Errorf("%s: settled", name)
		}
	}
	if got := state(t, l).Lines; got != lines {
		t.Fatalf("refused settlements wrote %d lines", got-lines)
	}
	must(t, l.Settle(p, Counts{Probe: 2}))
	if err := l.Settle(p, Counts{Probe: 2}); err == nil {
		t.Error("second settlement accepted")
	}
	must(t, l.Settle(d, Counts{Development: 24}))
	must(t, l.Settle(r, Counts{Probe: 1, Warmup: 6, Measured: 120}))
	err := l.Settle(w, Counts{Probe: 2, Warmup: 6, Measured: 120})
	if err == nil || !strings.Contains(err.Error(), "128 requests sent against a reservation of 127") {
		t.Fatalf("overrun: %v", err)
	}
	u := state(t, l).Routes[groq]
	if u.Probe != 5 || u.Development != 24 || u.Warmup != 6 || u.Measured != 120 || u.Replacement != 126 || u.Reserved != 0 || u.Charged() != 281 {
		t.Fatalf("usage %+v", *u)
	}
}

// Purpose and size rules on admission write nothing.
func TestAdmitArguments(t *testing.T) {
	l, _ := newLedger(t)
	must(t, l.RecordPrice(groq, groqPrice))
	for _, c := range []struct {
		route, purpose string
		n              int
	}{
		{groq, PurposeWindow, 126}, {groq, PurposeWindow, 128}, {groq, PurposeReplacement, 1},
		{groq, PurposeProbe, 0}, {groq, PurposeDevelopment, -1}, {groq, "measured", 1}, {"", PurposeProbe, 1},
	} {
		if _, err := l.Admit(c.route, c.purpose, c.n, PilotTokens); err == nil {
			t.Errorf("Admit(%q, %q, %d) accepted", c.route, c.purpose, c.n)
		}
	}
	for _, tok := range []Tokens{{}, {PilotMaxInputTokens, 0}, {0, PilotMaxOutputTokens}, {-1, PilotMaxOutputTokens}} {
		if _, err := l.Admit(groq, PurposeProbe, 1, tok); err == nil || errors.As(err, new(*Refusal)) {
			t.Errorf("Admit at %v: %v", tok, err)
		}
	}
	if st := state(t, l); st.Lines != 2 {
		t.Errorf("%d lines", st.Lines)
	}
	for _, p := range []Price{
		{"0.15", "", 0, 0, groqPrice.Source, groqPrice.Retrieved}, {"-1", "0.6", 0, 0, groqPrice.Source, groqPrice.Retrieved},
		{"1e3", "0.6", 0, 0, groqPrice.Source, groqPrice.Retrieved}, {"0.15", "0.6", 0, 0, "console.groq.com", groqPrice.Retrieved},
		{"0.15", "0.6", 0, 0, groqPrice.Source, "6 Oct 2026"}, {"0.15", "0.6", 1200, 0, groqPrice.Source, groqPrice.Retrieved},
	} {
		if err := l.RecordPrice(groq, p); err == nil {
			t.Errorf("price %+v accepted", p)
		}
	}
	for _, h := range [][2]string{{"0", "x"}, {"-2", "x"}, {"2", ""}, {"1/3", "x"}} {
		if err := l.RecordHosting(h[0], h[1]); err == nil {
			t.Errorf("hosting %v accepted", h)
		}
	}
	if st := state(t, l); st.Lines != 2 {
		t.Errorf("%d lines", st.Lines)
	}
}

// Every operation appends: the file before is a prefix of the file after.
func TestAppendOnly(t *testing.T) {
	l, path := newLedger(t)
	prev, _ := os.ReadFile(path)
	step := func(name string, err error) {
		t.Helper()
		b, _ := os.ReadFile(path)
		if !bytes.HasPrefix(b, prev) || len(b) <= len(prev) || bytes.Count(b, []byte("\n")) != bytes.Count(prev, []byte("\n"))+1 {
			t.Fatalf("%s (%v) did not append one line:\n%s\nafter:\n%s", name, err, prev, b)
		}
		prev = b
	}
	step("price", l.RecordPrice(groq, groqPrice))
	step("hosting", l.RecordHosting("3", "client host"))
	a, err := l.Admit(groq, PurposeWindow, WindowRequests, PilotTokens)
	step("admit", err)
	_, err = l.Admit(baseten, PurposeWindow, WindowRequests, PilotTokens)
	step("refused", err)
	step("settle", l.Settle(a, Counts{Probe: 1, Warmup: 6, Measured: 120}))
}

// A ledger whose bytes do not check admits nothing and is left as it is.
func TestLedgerRefusesAnAlteredFile(t *testing.T) {
	l, path := newLedger(t)
	must(t, l.RecordPrice(groq, groqPrice))
	a := admit(t, l, groq, PurposeWindow, WindowRequests)
	must(t, l.Settle(a, Counts{Probe: 1, Warmup: 6, Measured: 120}))
	admit(t, l, groq, PurposeProbe, 3)
	good, _ := os.ReadFile(path)
	lines := strings.SplitAfter(string(good), "\n")
	lines = lines[:len(lines)-1]
	pl, ppath := newLedger(t)
	must(t, pl.RecordPrice(groq, groqPrice))
	priced, _ := os.ReadFile(ppath)

	for name, b := range map[string]string{
		"settled count lowered": strings.Replace(string(good), `"measured":120`, `"measured":12`, 1),
		"line removed":          strings.Join(append(append([]string{}, lines[:2]...), lines[3:]...), ""),
		"lines swapped":         lines[0] + lines[2] + lines[1] + strings.Join(lines[3:], ""),
		"last line torn":        string(good[:len(good)-5]),
		"unknown field":         strings.Replace(string(good), `"kind":"admit"`, `"kind":"admit","x":1`, 1),
		"empty":                 "",
		"last price misstated":  strings.Replace(string(priced), `"per_request_usd":"0.0027576"`, `"per_request_usd":"0.0000001"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "ledger.jsonl")
			if err := os.WriteFile(p, []byte(b), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(p); err == nil {
				t.Fatal("Open accepted the file")
			}
			bad := &Ledger{path: p, now: time.Now}
			if _, err := bad.Admit(groq, PurposeProbe, 1, PilotTokens); err == nil {
				t.Fatal("Admit accepted the file")
			}
			if got, _ := os.ReadFile(p); string(got) != b {
				t.Fatal("file changed")
			}
		})
	}
}

// Admissions from many handles at once never pass the cap.
func TestConcurrentAdmissions(t *testing.T) {
	l, path := newLedger(t)
	must(t, l.RecordPrice(groq, groqPrice))
	var wg sync.WaitGroup
	var mu sync.Mutex
	admitted, refusals := 0, 0
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h, err := Open(path)
			if err != nil {
				t.Error(err)
				return
			}
			_, err = h.Admit(groq, PurposeWindow, WindowRequests, PilotTokens)
			var r *Refusal
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				admitted++
			case errors.As(err, &r):
				refusals++
			default:
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	st := state(t, l)
	if admitted != PilotRouteRequestCap/WindowRequests || refusals != 20-admitted || st.Total() != admitted*WindowRequests || st.Lines != 2+20 {
		t.Fatalf("admitted %d, refused %d, total %d, lines %d", admitted, refusals, st.Total(), st.Lines)
	}
}

// Under any sequence of admissions and settlements within their
// reservations, no state the ledger records passes a cap.
func TestCapsHoldUnderRandomTraffic(t *testing.T) {
	l, _ := newLedger(t)
	rng := rand.New(rand.NewSource(20261006))
	routes := []string{groq, baseten, third}
	// At US$0.0199608 a request the liability cap binds at 2,003
	// requests, under the total cap.
	for _, e := range routes {
		must(t, l.RecordPrice(e, Price{InputUSDPerMTok: "0.15", OutputUSDPerMTok: "4.8", Source: groqPrice.Source, Retrieved: groqPrice.Retrieved}))
	}
	purposes := []string{PurposeProbe, PurposeDevelopment, PurposeWindow, PurposeReplacement}
	var open []Admission
	cap40 := big.NewRat(40, 1)
	admits, refusals := 0, map[string]int{}
	for i := 0; i < 200; i++ {
		if len(open) > 0 && rng.Intn(3) == 0 {
			k := rng.Intn(len(open))
			a := open[k]
			open = append(open[:k], open[k+1:]...)
			sent := rng.Intn(a.Reserve + 1)
			var c Counts
			switch a.Purpose {
			case PurposeProbe:
				c.Probe = sent
			case PurposeDevelopment:
				c.Development = sent
			default:
				c.Probe = min(sent, 1)
				c.Warmup = min(sent-c.Probe, 6)
				c.Measured = sent - c.Probe - c.Warmup
			}
			must(t, l.Settle(a, c))
		} else {
			purpose := purposes[rng.Intn(len(purposes))]
			n := WindowRequests
			if purpose == PurposeProbe || purpose == PurposeDevelopment {
				n = 1 + rng.Intn(48)
			}
			a, err := l.Admit(routes[rng.Intn(len(routes))], purpose, n, PilotTokens)
			var r *Refusal
			switch {
			case err == nil:
				open = append(open, a)
				admits++
			case errors.As(err, &r):
				for _, k := range []string{"for the route", "in total", "liability"} {
					if strings.Contains(r.Reason, k) {
						refusals[k]++
					}
				}
			default:
				t.Fatal(err)
			}
		}
		st := state(t, l)
		for e, u := range st.Routes {
			if u.Charged() > PilotRouteRequestCap {
				t.Fatalf("step %d: %s charged %d", i, e, u.Charged())
			}
		}
		liab, err := st.Liability()
		if err != nil || st.Total() > PilotTotalRequestCap || liab.Cmp(cap40) > 0 {
			t.Fatalf("step %d: total %d liability %v %v", i, st.Total(), liab, err)
		}
	}
	if admits == 0 || len(refusals) != 3 {
		t.Fatalf("admitted %d, refused %v: the sequence did not reach every cap", admits, refusals)
	}
}
