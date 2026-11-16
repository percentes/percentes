package main

import (
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
)

// billingStopFixture is billingStopRoute with its fixture.
func billingStopFixture(t *testing.T) (*route, *endpoint, *fixture) {
	t.Helper()
	e := newEndpoint(t, func(int64) (int, string) { return 402, "no credit" })
	cfg := releaseConfig(t, e.url())
	cfg.Target.BillingRefusals = []config.BillingRefusal{{Status: 402, Meaning: "balance exhausted", Source: "errors.png", Retrieved: "2026-10-07"}}
	r, f := newTestRoute(t, cfg, "+0.005")
	run := r.runs[0]
	r.now = func() time.Time { return run.Start }
	return r, e, f
}

// stopsPages builds r's pages over its fixture f.
func stopsPages(t *testing.T, r *route, f *fixture) *releasePages {
	t.Helper()
	cfgs := append([]*config.Config{r.cfg}, f.in.hostCfgs...)
	rp, err := buildPages(r.out, r.framesRoot, r.sched, r.scheduleSHA256, r.ledgerPath, f.in.probeFiles, cfgs, r.key, "")
	if err != nil {
		t.Fatal(err)
	}
	return rp
}
