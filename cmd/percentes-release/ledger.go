package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
)

// ledgerEntries reads and checks the ledger at path and returns its lines.
func ledgerEntries(path string) ([]admission.Entry, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("ledger: %w", err)
	}
	if _, err := admission.Parse(b); err != nil {
		return nil, err
	}
	var out []admission.Entry
	sc := bufio.NewScanner(bytes.NewReader(b))
	sc.Buffer(nil, len(b)+1)
	for sc.Scan() {
		var e admission.Entry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			return nil, fmt.Errorf("ledger line %d: %w", len(out)+1, err)
		}
		out = append(out, e)
	}
	return out, sc.Err()
}

// routePrice is route's newest recorded price: the price line with the
// latest retrieval date, the last of equals.
func routePrice(entries []admission.Entry, route string) (admission.Price, error) {
	var best *admission.Entry
	for i := range entries {
		e := &entries[i]
		if e.Kind == admission.KindPrice && e.Route == route && (best == nil || e.Retrieved >= best.Retrieved) {
			best = e
		}
	}
	if best == nil {
		return admission.Price{}, fmt.Errorf("ledger: no price recorded for route %s", route)
	}
	return admission.Price{InputUSDPerMTok: best.InputUSDPerMTok, OutputUSDPerMTok: best.OutputUSDPerMTok,
		InputTokens: best.InputTokens, OutputTokens: best.OutputTokens, Source: best.Source, Retrieved: best.Retrieved}, nil
}

// recordPrice records cfg's route's list prices at each token bound its
// requests are admitted at, cells' first, and returns the bounds.
func recordPrice(l *admission.Ledger, cfg *config.Config, in, out, source, retrieved string) ([]admission.Tokens, error) {
	bounds := []admission.Tokens{admission.ReleaseTokens(cfg)}
	if p := admission.ReleaseProbeTokens(cfg); p != bounds[0] {
		bounds = append(bounds, p)
	}
	for _, tok := range bounds {
		if err := l.RecordPrice(cfg.Target.Name, admission.Price{InputUSDPerMTok: in, OutputUSDPerMTok: out,
			InputTokens: tok.Input, OutputTokens: tok.Output, Source: source, Retrieved: retrieved}); err != nil {
			return nil, err
		}
	}
	return bounds, nil
}

// sentBefore counts the requests the ledger settled for route at times
// before t.
func sentBefore(entries []admission.Entry, route string, t time.Time) int {
	n := 0
	for _, e := range entries {
		if e.Kind == admission.KindSettle && e.Route == route && e.Sent != nil && e.Time.Before(t) {
			n += e.Sent.Probe + e.Sent.Development + e.Sent.Warmup + e.Sent.Measured
		}
	}
	return n
}

// ledgerCmd creates the release ledger with the caps of every route's
// configuration (admission.ReleaseCaps), or records a route's prices at
// the token bounds its requests are admitted at: its shape's input bound
// and ceiling, and the input bound and the ceiling check's ceiling.
func ledgerCmd(args []string) (int, error) {
	if len(args) == 0 {
		return exitError, errors.New("ledger: create or price")
	}
	fs := flag.NewFlagSet("ledger "+args[0], flag.ContinueOnError)
	path := fs.String("ledger", "", "the release admission ledger file (required)")
	var cfgPaths listFlag
	fs.Var(&cfgPaths, "config", "create: every route's release configuration (repeatable); price: the route's (required)")
	in := fs.String("input", "", "price: input list price, US$ per million tokens")
	out := fs.String("output", "", "price: output list price, US$ per million tokens")
	source := fs.String("source", "", "price: the URL the prices were read from")
	retrieved := fs.String("retrieved", "", "price: the date they were read, 2006-01-02")
	if err := fs.Parse(args[1:]); err != nil {
		return exitError, err
	}
	if *path == "" {
		return exitError, errors.New("ledger: --ledger is required")
	}
	if args[0] != "create" && args[0] != "price" {
		return exitError, fmt.Errorf("ledger: unknown action %q", args[0])
	}
	cfgs, err := loadReleases(cfgPaths)
	if err != nil {
		return exitError, err
	}
	if args[0] == "create" {
		caps, err := admission.ReleaseCaps(cfgs...)
		if err != nil {
			return exitError, err
		}
		if _, err := admission.Create(*path, caps); err != nil {
			return exitError, err
		}
		fmt.Printf("ledger created: %s; global cap US$%s; %d routes\n", *path, caps.LiabilityUSD, len(caps.RouteLiabilityUSD))
		return exitOK, nil
	}
	if len(cfgs) != 1 {
		return exitError, fmt.Errorf("ledger price: %d configurations, want the route's one", len(cfgs))
	}
	cfg := cfgs[0]
	l, err := admission.Open(*path)
	if err != nil {
		return exitError, err
	}
	st, err := l.State()
	if err != nil {
		return exitError, err
	}
	if err := st.CheckRelease(cfg); err != nil {
		return exitError, err
	}
	bounds, err := recordPrice(l, cfg, *in, *out, *source, *retrieved)
	if err != nil {
		return exitError, err
	}
	if st, err = l.State(); err != nil {
		return exitError, err
	}
	for _, tok := range bounds {
		fmt.Printf("%s: price recorded at %d input and %d output tokens; the ledger admits these at US$%s a request\n",
			cfg.Target.Name, tok.Input, tok.Output, st.Routes[cfg.Target.Name].Prices[tok].FloatString(7))
	}
	return exitOK, nil
}
