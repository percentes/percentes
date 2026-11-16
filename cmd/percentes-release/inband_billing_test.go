package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
)

// spendCapInBand is a rate_limit_error in-band error object carrying the
// spend cap's marker.
const spendCapInBand = `{"type":"rate_limit_error","message":"cap","details":{"error_code":"enforced_spend_limit_reached"}}`

// spendCapDocs gives cfg the rate_limit_error type at HTTP 429 and the
// spend cap's billing refusal at 429.
func spendCapDocs(cfg *config.Config) {
	cfg.Target.ErrorDocs = []config.ErrorDoc{{Type: "rate_limit_error", Status: 429, Meaning: "a rate limit", Source: "https://docs.example.test/errors", Retrieved: "2026-11-03"}}
	cfg.Target.BillingRefusals = []config.BillingRefusal{{Status: 429, BodyContains: "enforced_spend_limit_reached", Meaning: "the spend cap is reached", Source: "https://docs.example.test/limits", Retrieved: "2026-11-01"}}
}

// A 200 stream whose in-band error the route documents as its billing
// refusal stops the route as an HTTP-level billing refusal does, at the
// probe or at a ladder request: the cell is stopped at the first
// refusal, counts it, holds the route, and its evidence replays with
// each refused attempt marked. Other in-band errors stop nothing.
func TestInBandBillingRefusalStopsRoute(t *testing.T) {
	const reason = "route stop: HTTP 429 in-band, the operator's billing setting, a documented billing refusal: the spend cap is reached"
	for _, from := range []int64{1, 3} {
		var hits atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			io.Copy(io.Discard, req.Body) //nolint:errcheck
			w.Header().Set("Content-Type", "text/event-stream")
			if hits.Add(1) >= from {
				io.WriteString(w, "data: {\"error\":"+spendCapInBand+"}\n\ndata: [DONE]\n\n") //nolint:errcheck
				return
			}
			io.WriteString(w, chatStream) //nolint:errcheck
		}))
		t.Cleanup(srv.Close)
		cfg := releaseConfig(t, srv.URL+"/openai/v1/chat/completions")
		spendCapDocs(cfg)
		r, _ := newTestRoute(t, cfg, "+0.005032")
		shortLadder(r)
		run := r.runs[0]
		r.now, r.wait = func() time.Time { return run.Start }, noWait
		code, err := r.loop(context.Background())
		if err != nil || code != exitStopped {
			t.Fatalf("from request %d: loop: code %d, %v", from, code, err)
		}
		rec := readCell(t, r, run)
		if rec.Outcome != outcomeStopped || len(rec.Reasons) == 0 || !strings.HasPrefix(rec.Reasons[0], reason) || rec.BillingRefusals < 1 {
			t.Fatalf("from request %d: outcome %s, reasons %q, %d billing refusals", from, rec.Outcome, rec.Reasons, rec.BillingRefusals)
		}
		st, err := r.state()
		if err != nil {
			t.Fatal(err)
		}
		if h, err := r.holds(st); err != nil || len(h) == 0 {
			t.Fatalf("from request %d: holds %q, %v", from, h, err)
		}
		m, _ := checkRetained(t, r, run)
		if c := m.Release; m.Counted() || c.Outcome != outcomeStopped || c.BillingRefusals != rec.BillingRefusals {
			t.Fatalf("from request %d: manifest counted %t, cell label %+v", from, m.Counted(), c)
		}
		if from == 1 {
			if n := hits.Load(); n != 1 || rec.BillingRefusals != 1 {
				t.Fatalf("a refusal at the probe: server saw %d requests, %d billing refusals", n, rec.BillingRefusals)
			}
			continue
		}
		if rec.Interrupted == nil || rec.Interrupted.Unsent == 0 {
			t.Fatalf("a refusal in the ladder: interruption %+v", rec.Interrupted)
		}
		b, err := os.ReadFile(filepath.Join(r.cellDir(run), evidence.AttemptsFile))
		if err != nil {
			t.Fatal(err)
		}
		recs, err := evidence.ReadRecords(bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		marked := 0
		for _, q := range recs {
			if q.BillingRefusal {
				marked++
			}
		}
		if marked != rec.BillingRefusals {
			t.Fatalf("a refusal in the ladder: %d attempts marked, %d billing refusals counted", marked, rec.BillingRefusals)
		}
	}

	cfg := releaseConfig(t, "http://127.0.0.1:1/openai/v1/chat/completions")
	spendCapDocs(cfg)
	for _, text := range []string{`{"type":"rate_limit_error","message":"slow down"}`, `{"type":"overloaded_error","message":"Overloaded"}`} {
		if n, s := billingRefusals(&cfg.Target, []loadgen.Request{{Status: http.StatusOK, StreamError: text}}); n != 0 || s != nil {
			t.Errorf("%s: %d billing refusals, %v", text, n, s)
		}
	}
	if n, s := billingRefusals(&cfg.Target, []loadgen.Request{{Status: http.StatusOK, StreamError: spendCapInBand}}); n != 1 || s == nil || !strings.HasPrefix(s.Error(), reason) {
		t.Errorf("spend cap in-band: %d billing refusals, %v", n, s)
	}
}
