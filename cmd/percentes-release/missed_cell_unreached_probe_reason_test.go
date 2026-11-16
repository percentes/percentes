package main

import (
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/schedule"
)

// A cell missed after a probe that reached no connection says so in its
// reason, at the start-tolerance check and at the late epoch alike, and
// counts no probe; the manifest carries the same reason.
func TestMissedCellReasonNamesProbeThatReachedNoConnection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		prefix string
		late   time.Duration
		pinNow bool
	}{
		{"start tolerance", "ladder not started: its epoch would fall 15m", schedule.StartTolerance + time.Second, false},
		{"late epoch", "ladder not started: its epoch fell beyond the 15m0s start tolerance", schedule.StartTolerance + time.Hour, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			due := time.Now().Add(-tc.late)
			r.due = func(schedule.Run) time.Time { return due }
			if tc.pinNow {
				r.now = func() time.Time { return due }
			}
			run := r.runs[0]
			rec := runCellOK(t, r, run)
			if rec.Outcome != outcomeMissed || rec.Sent.Probe != 0 || len(rec.Reasons) != 1 ||
				!strings.HasPrefix(rec.Reasons[0], tc.prefix) || !strings.HasSuffix(rec.Reasons[0], "(SPEC-HOSTED §4); the probe reached no connection") {
				t.Fatalf("outcome %s, reasons %q, sent %+v", rec.Outcome, rec.Reasons, rec.Sent)
			}
			if m, _ := checkRetained(t, r, run); !slices.Equal(m.Release.Reasons, rec.Reasons) {
				t.Fatalf("manifest reasons %q, record %q", m.Release.Reasons, rec.Reasons)
			}
		})
	}
}
