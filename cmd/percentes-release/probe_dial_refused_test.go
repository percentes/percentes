package main

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/report"
)

// A probe whose dial is refused reaches no connection: its record, the
// ledger and the account group's history count no probe, and the
// history shows no gap.
func TestDialRefusedProbeCountsNoSend(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	fastDispatch(t)
	r, _ := newTestRoute(t, releaseConfig(t, "http://"+addr+"/openai/v1/chat/completions"), "+0.005")
	shortLadder(r)
	run := r.runs[0]
	runCellOK(t, r, run)
	rec := readCell(t, r, run)
	b, err := os.ReadFile(filepath.Join(r.cellDir(run), probe.File))
	if err != nil {
		t.Fatal(err)
	}
	var pr probe.Record
	if err := json.Unmarshal(b, &pr); err != nil {
		t.Fatal(err)
	}
	if pr.Request.DispatchUTC.IsZero() || probe.Reached(&pr.Request) || pr.Request.Error == "" {
		t.Fatalf("probe exchange %+v: want a dispatched request that reached no connection", pr.Request)
	}
	u := ledgerState(t, r.ledgerPath).Routes[r.cfg.Target.Name]
	if rec.Sent.Probe != 0 || u.Probe != 0 {
		t.Errorf("outcome %s %q: record sent %+v, ledger probe %d", rec.Outcome, rec.Reasons, rec.Sent, u.Probe)
	}
	entries, _, err := readLedger(r.ledgerPath, "")
	if err != nil {
		t.Fatal(err)
	}
	h, err := readHistory(r.out, r.cfg.Release.AccountGroup, []string{r.cfg.Target.Name}, []*config.Config{r.cfg}, entries, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range h.Sends {
		if s.Source == cellLabel(run) && s.Kind == report.SendProbe {
			t.Errorf("the history holds the probe as a send: %+v", s)
		}
	}
	if len(h.Gaps) != 0 {
		t.Errorf("history gaps %+v", h.Gaps)
	}
}
