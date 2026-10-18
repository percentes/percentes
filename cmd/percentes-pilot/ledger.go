package main

import (
	"errors"
	"flag"
	"fmt"
	"sort"

	"github.com/percentes/percentes/internal/admission"
)

// ledgerCmd creates the admission ledger, records an endpoint's prices or
// a hosting cost, or prints the ledger's position.
func ledgerCmd(args []string) (int, error) {
	if len(args) == 0 {
		return exitError, errors.New("ledger: create, price, hosting or show")
	}
	fs := flag.NewFlagSet("ledger "+args[0], flag.ContinueOnError)
	path := fs.String("ledger", "", "admission ledger file (required)")
	endpoint := fs.String("endpoint", "", "price: the target's endpoint_url")
	in := fs.String("input", "", "price: input list price, US$ per million tokens")
	out := fs.String("output", "", "price: output list price, US$ per million tokens")
	source := fs.String("source", "", "price: the URL the prices were read from")
	retrieved := fs.String("retrieved", "", "price: the date they were read, 2006-01-02")
	usd := fs.String("usd", "", "hosting: the cost in US$")
	note := fs.String("note", "", "hosting: what the cost is for")
	if err := fs.Parse(args[1:]); err != nil {
		return exitError, err
	}
	if *path == "" {
		return exitError, errors.New("ledger: --ledger is required")
	}
	switch args[0] {
	case "create":
		if _, err := admission.Create(*path, admission.PilotCaps()); err != nil {
			return exitError, err
		}
		fmt.Printf("ledger created: %s\n", *path)
		return exitOK, nil
	case "price":
		l, err := admission.Open(*path)
		if err != nil {
			return exitError, err
		}
		if err := l.RecordPrice(*endpoint, admission.Price{InputUSDPerMTok: *in, OutputUSDPerMTok: *out, Source: *source, Retrieved: *retrieved}); err != nil {
			return exitError, err
		}
	case "hosting":
		l, err := admission.Open(*path)
		if err != nil {
			return exitError, err
		}
		if err := l.RecordHosting(*usd, *note); err != nil {
			return exitError, err
		}
	case "show":
	default:
		return exitError, fmt.Errorf("ledger: unknown action %q", args[0])
	}
	return showLedger(*path)
}

// showLedger prints each endpoint's charge and the projected liability.
func showLedger(path string) (int, error) {
	l, err := admission.Open(path)
	if err != nil {
		return exitError, err
	}
	st, err := l.State()
	if err != nil {
		return exitError, err
	}
	names := make([]string, 0, len(st.Routes))
	for e := range st.Routes {
		names = append(names, e)
	}
	sort.Strings(names)
	for _, e := range names {
		u := st.Routes[e]
		price := "none"
		if u.MaxPerRequestUSD != nil {
			price = u.MaxPerRequestUSD.FloatString(6)
		}
		fmt.Printf("%s: %d of %d (probe %d, development %d, warm-up %d, measured %d, replacement %d, reserved %d); per-request price US$%s\n",
			e, u.Charged(), st.Caps.RouteRequests, u.Probe, u.Development, u.Warmup, u.Measured, u.Replacement, u.Reserved, price)
	}
	liab := ""
	if r, err := st.Liability(); err != nil {
		liab = "not projectable: " + err.Error()
	} else {
		liab = "US$" + r.FloatString(6)
	}
	fmt.Printf("total: %d of %d; liability %s of US$%s; %d lines\n", st.Total(), st.Caps.TotalRequests, liab, st.Caps.LiabilityUSD, st.Lines)
	return exitOK, nil
}
