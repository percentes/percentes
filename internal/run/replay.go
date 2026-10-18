package run

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/percentes/percentes/internal/collect"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/histo"
	"github.com/percentes/percentes/internal/loadgen"
)

// Record is what a hosted run's report holds beyond its config and its
// per-attempt records: the client-validity gate, which reads host CPU and
// garbage-collection pauses, the tripwires, and per window the pooled
// inter-token gaps and the canary summary, which reduce data the
// per-attempt records do not carry (SPEC-HOSTED §9).
type Record struct {
	ClientGates loadgen.GateReport    `json:"client_gates"`
	Tripwires   *loadgen.Tripwires    `json:"tripwires,omitempty"`
	Interrupted *loadgen.Interruption `json:"interrupted,omitempty"`
	Windows     []WindowRecord        `json:"windows"`
	CanaryError string                `json:"canary_error,omitempty"`
}

// WindowRecord is one window's bounds, pooled inter-token gaps and canary
// summary.
type WindowRecord struct {
	collect.Window
	ITLPooled histo.Summary          `json:"itl_pooled"`
	Canary    *loadgen.CanarySummary `json:"canary,omitempty"`
}

// Record returns a's record, windows in name order.
func (a *Artifacts) Record() *Record {
	rec := &Record{ClientGates: a.Loadgen.Gates, Tripwires: a.Loadgen.Tripwires, Interrupted: a.Loadgen.Interrupted, Windows: []WindowRecord{}}
	names := make([]string, 0, len(a.Windows))
	for name := range a.Windows {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		st := a.Windows[name]
		w := WindowRecord{Window: st.Window, ITLPooled: st.ITLPooled}
		if rp := a.ReceivePath[name]; rp != nil {
			w.Canary = rp.Canary
			if rp.CanaryError != "" {
				rec.CanaryError = rp.CanaryError
			}
		}
		rec.Windows = append(rec.Windows, w)
	}
	return rec
}

// Replay analyses a hosted run from cfg, its run epoch, its requests and
// rec as Execute and AttachObservations do, with no orchestration, §5
// probe or victim replica. The requests carry no inter-token gaps: each
// window takes its pooled gaps from rec once its bounds, and its gap
// count of one fewer than the content events of each completed request,
// match the requests.
func Replay(cfg *config.Config, epoch time.Time, requests []loadgen.Request, rec *Record) (*Artifacts, error) {
	if !cfg.Target.Hosted {
		return nil, errors.New("run: replay: the config's target is not hosted")
	}
	res := loadgen.NewResult(cfg)
	res.EpochWall, res.Requests = epoch, requests
	res.Gates, res.Tripwires, res.Interrupted = rec.ClientGates, rec.Tripwires, rec.Interrupted
	loadgen.CompareModels(cfg, res)
	art := &Artifacts{Config: cfg, Loadgen: res, Windows: map[string]*collect.Stats{}}
	if _, err := analyze(art, runInputs{}); err != nil {
		return nil, fmt.Errorf("run: replay: %w", err)
	}
	if len(rec.Windows) != len(art.Windows) {
		return nil, fmt.Errorf("run: replay: the record has %d windows, the requests give %d", len(rec.Windows), len(art.Windows))
	}
	byName := map[string]*WindowRecord{}
	for i := range rec.Windows {
		w := &rec.Windows[i]
		st := art.Windows[w.Name]
		if st == nil || st.Window != w.Window || byName[w.Name] != nil {
			return nil, fmt.Errorf("run: replay: window %q [%d, %d) is not one the requests give", w.Name, w.StartNs, w.EndNs)
		}
		if n := gapCount(requests, w.Window); w.ITLPooled.Count != n {
			return nil, fmt.Errorf("run: replay: window %q: %d pooled gaps recorded, the requests give %d", w.Name, w.ITLPooled.Count, n)
		}
		st.ITLPooled = w.ITLPooled
		byName[w.Name] = w
	}
	art.attach(Observed{TTFTFamily: cfg.Target.TTFTHistogram}, rec.CanaryError, func(w collect.Window) *loadgen.CanarySummary {
		return byName[w.Name].Canary
	})
	return art, nil
}

// gapCount is the number of inter-token gaps collect pools over w.
func gapCount(requests []loadgen.Request, w collect.Window) int64 {
	var n int64
	for i := range requests {
		r := &requests[i]
		if r.IntendedNs < w.StartNs || r.IntendedNs >= w.EndNs || w.Replica != "" && r.Replica != w.Replica {
			continue
		}
		if r.Outcome == loadgen.OutcomeCompleted && r.Tokens > 1 {
			n += int64(r.Tokens - 1)
		}
	}
	return n
}
