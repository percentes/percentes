package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/percentes/percentes/internal/admission"
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

// routePrice is the price the ledger admits route at: the recorded price
// line with the highest per-request price, the first of equals.
func routePrice(entries []admission.Entry, route string) (admission.Price, error) {
	var (
		best *admission.Entry
		high *big.Rat
	)
	for i := range entries {
		e := &entries[i]
		if e.Kind != admission.KindPrice || e.Route != route {
			continue
		}
		r, ok := new(big.Rat).SetString(e.PerRequestUSD)
		if !ok {
			return admission.Price{}, fmt.Errorf("ledger line %d: per-request price %q", e.Seq, e.PerRequestUSD)
		}
		if high == nil || r.Cmp(high) > 0 {
			best, high = e, r
		}
	}
	if best == nil {
		return admission.Price{}, fmt.Errorf("ledger: no price recorded for route %s", route)
	}
	return admission.Price{InputUSDPerMTok: best.InputUSDPerMTok, OutputUSDPerMTok: best.OutputUSDPerMTok,
		InputTokens: best.InputTokens, OutputTokens: best.OutputTokens, Source: best.Source, Retrieved: best.Retrieved}, nil
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
