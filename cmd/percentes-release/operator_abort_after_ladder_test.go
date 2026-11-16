package main

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/schedule"
)

// SIGINT or SIGHUP after the ladder's end, its last request still
// answered past the drain, leaves the cell its own label: counted with
// its evidence retained, and the route held with one abort marker, exit 3
// and no re-run. A start with no acknowledgement stays held; once
// acknowledged, the next start goes on to its wait with no cell run. A
// cell the client-validity gate failed keeps client_invalid and its
// evidence. SIGINT inside the last drain stops the cell.
func TestOperatorAbortAfterLadderEndKeepsLabel(t *testing.T) {
	afterDrain := func(d []time.Duration) time.Duration { return d[0] + 300*time.Millisecond }
	for _, sig := range []syscall.Signal{syscall.SIGINT, syscall.SIGHUP} {
		t.Run(sig.String(), func(t *testing.T) {
			r, run, code, out := ladderSignalRun(t, sig, 1, 0, http.StatusOK, 700*time.Millisecond, afterDrain, hostGatesWide)
			rec := readCell(t, r, run)
			if code != exitStopped || rec.Interrupted != nil || rec.Outcome != outcomeCounted && !hostGateFailed(rec) {
				t.Fatalf("exit %d, %s %q, interrupted %+v\n%s", code, rec.Outcome, rec.Reasons, rec.Interrupted, out)
			}
			skipOnHostGate(t, rec)
			m, _ := checkRetained(t, r, run)
			if m.Release.Outcome != evidence.CellCounted {
				t.Fatalf("label %+v", m.Release)
			}
			if ms := markers(t, r); len(ms) != 1 || ms[0].Run != run || !ms[0].During {
				t.Fatalf("markers %+v", ms)
			}
			st, err := r.state()
			if err != nil {
				t.Fatal(err)
			}
			if holds, err := r.holds(st); err != nil || len(holds) != 1 || !strings.Contains(holds[0], "during "+run.Label+" attempt 1") {
				t.Fatalf("holds %q: %v", holds, err)
			}
			for i := range st.runs {
				if st.runs[i].Label == run.Label && st.runs[i].Attempt == 2 {
					t.Fatalf("%s re-run scheduled", run.Label)
				}
			}

			ran := 0
			cell := r.cell
			r.cell = func(ctx context.Context, next schedule.Run) (*cellRecord, error) {
				ran++
				return cell(ctx, next)
			}
			waits := countWaits(r)
			stdoutOf(t, func() { code, err = r.loop(context.Background()) })
			if err != nil || code != exitStopped || *waits != 0 || ran != 0 {
				t.Fatalf("start with no acknowledgement: exit %d, %v; %d waits, %d cells", code, err, *waits, ran)
			}
			ids, err := filepath.Glob(filepath.Join(r.dir, abortPrefix+"*.json"))
			if err != nil || len(ids) != 1 {
				t.Fatalf("abort markers %v: %v", ids, err)
			}
			acknowledge(t, r, strings.TrimSuffix(filepath.Base(ids[0]), ".json"))
			stdoutOf(t, func() { code, err = r.loop(context.Background()) })
			if err != nil || code != exitStopped || *waits != 1 || ran != 0 {
				t.Fatalf("acknowledged start: exit %d, %v; %d waits, %d cells", code, err, *waits, ran)
			}
			if rec := readCell(t, r, run); rec.Outcome != outcomeCounted {
				t.Fatalf("after the next start: %s %q", rec.Outcome, rec.Reasons)
			}
		})
	}
	t.Run("failed gate", func(t *testing.T) {
		r, run, code, out := ladderSignalRun(t, syscall.SIGINT, 1, 0, http.StatusOK, 700*time.Millisecond, afterDrain, gateShut)
		rec := readCell(t, r, run)
		if code != exitStopped || rec.Interrupted != nil || !clientGateOnly(rec) {
			t.Fatalf("exit %d, %s %q, interrupted %+v\n%s", code, rec.Outcome, rec.Reasons, rec.Interrupted, out)
		}
		if m, _ := checkRetained(t, r, run); m.Release.Outcome != evidence.CellInvalid {
			t.Fatalf("label %+v", m.Release)
		}
		if ms := markers(t, r); len(ms) != 1 || ms[0].Run != run || !ms[0].During {
			t.Fatalf("markers %+v", ms)
		}
	})
	t.Run("SIGINT inside the last drain", func(t *testing.T) {
		r, run, code, out := ladderSignalRun(t, syscall.SIGINT, 1, 0, http.StatusOK, 0, func(d []time.Duration) time.Duration { return d[0] - 500*time.Millisecond }, nil)
		rec := readCell(t, r, run)
		if code != exitStopped || rec.Outcome != outcomeStopped || rec.Interrupted == nil || len(markers(t, r)) != 1 {
			t.Fatalf("exit %d, %s %q, interrupted %+v, markers %+v\n%s", code, rec.Outcome, rec.Reasons, rec.Interrupted, markers(t, r), out)
		}
	})
}
