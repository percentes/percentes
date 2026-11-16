//go:build unix

package loadgen

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/config"
)

// A whole-process stall in the middle of a release step sends none of
// the requests overdue by more than the send-skew maximum when the
// process resumes: arrivals at the server in any one second stay within
// the window's capacity, and the step fails its client-validity gate.
func TestProcessStallArrivals(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("no sh")
	}
	var mu sync.Mutex
	var arrivals []time.Time
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		arrivals = append(arrivals, time.Now())
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte(completeStream)) //nolint:errcheck
	}))
	defer srv.Close()
	cfg := ladderRunCfg(srv.URL, 1)
	cfg.Release.BindingRPS = 150.909
	cfg.Target.Limits = []config.Limit{{Dimension: config.LimitRPM, Value: 10000, EvaluationWindowsS: []float64{1, 60}}}
	cfg.Release.Ladder.WarmupS = 0
	cfg.Release.Ladder.StepS = 2.5
	cfg.Release.Ladder.DrainS = 0.5
	var stall *exec.Cmd
	res, err := Run(context.Background(), cfg, &Hooks{OnEpoch: func(time.Time) {
		stall = exec.Command("sh", "-c", fmt.Sprintf("sleep 0.8; kill -STOP %d; sleep 0.4; kill -CONT %d", os.Getpid(), os.Getpid()))
		if err := stall.Start(); err != nil {
			t.Error(err)
		}
	}})
	if stall != nil {
		if werr := stall.Wait(); werr != nil {
			t.Fatalf("stall: %v", werr)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	peak := peakIn(arrivals, time.Second)
	mu.Unlock()
	if peak > 166 {
		t.Fatalf("%d arrivals in one second after the stall; the window admits 166", peak)
	}
	if g := res.Ladder.Steps[0].Gates; g.Undispatched == 0 || g.Pass {
		t.Fatalf("overdue requests sent after the stall: gate %+v", *g)
	}
}
