package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/schedule"
)

// abortPrefix names an operator abort's marker in the route's directory,
// and stopPrefix a stopped cell's marker whose record failed to write.
const (
	abortPrefix = "abort-"
	stopPrefix  = "stop-"
)

// abortMarker is an operator abort before or during Run's cell, or a stop
// of that cell whose record failed to write, each of which holds the
// route as a stopped cell does.
type abortMarker struct {
	Route  string       `json:"route"`
	Run    schedule.Run `json:"run"`
	During bool         `json:"during,omitempty"`
	Stop   bool         `json:"stop,omitempty"`
	AtUTC  time.Time    `json:"at_utc"`
	Reason string       `json:"reason"`
	// Reasons and BillingRefusals are a stop marker's cell label, and
	// ProbeStatus and Sent its cell's probe status and requests sent.
	Reasons         []string          `json:"reasons,omitempty"`
	BillingRefusals int               `json:"billing_refusals,omitempty"`
	ProbeStatus     int               `json:"probe_status,omitempty"`
	Sent            *admission.Counts `json:"sent,omitempty"`
}

// ackFile is the acknowledgement file that releases the hold named id.
func (r *route) ackFile(id string) string {
	return filepath.Join(r.ackDir, "resume-"+r.cfg.Target.Name+"-"+id)
}

// writeAbort records an operator abort during run's cell, or before it
// where during is false.
func (r *route) writeAbort(run schedule.Run, during bool, cause error) (string, error) {
	at := r.now().UTC()
	id := abortPrefix + at.Format("20060102T150405.000000000Z")
	m := &abortMarker{Route: r.cfg.Target.Name, Run: run, During: during, AtUTC: at, Reason: cause.Error()}
	return id, replaceJSON(filepath.Join(r.dir, id+".json"), m)
}

// writeStopHold records rec's stop, for a stopped cell whose record
// failed to write.
func (r *route) writeStopHold(rec *cellRecord) (string, error) {
	id := stopPrefix + cellLabel(rec.Run)
	m := &abortMarker{Route: r.cfg.Target.Name, Run: rec.Run, Stop: true, AtUTC: r.now().UTC(), Reason: strings.Join(rec.Reasons, "; "),
		Reasons: slices.Clone(rec.Reasons), BillingRefusals: rec.BillingRefusals, ProbeStatus: rec.ProbeStatus, Sent: &rec.Sent}
	return id, replaceJSON(filepath.Join(r.dir, id+".json"), m)
}

// stopMarkerPath is the stop marker file of run's cell.
func (r *route) stopMarkerPath(run schedule.Run) string {
	return filepath.Join(r.dir, stopPrefix+cellLabel(run)+".json")
}

// stopMarker is the stop marker of run's cell, or nil where none is
// written.
func (r *route) stopMarker(run schedule.Run) (*abortMarker, error) {
	p := r.stopMarkerPath(run)
	b, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	var m abortMarker
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("%s: %w", p, err)
	}
	return &m, nil
}

// abortHold writes at most one operator-abort marker for a route
// process: at the signal, for the run whose cell is in progress or the
// next run while the loop waits, or when the cell or the wait for the
// next run ends. A write that fails is tried once more.
type abortHold struct {
	r      *route
	mu     sync.Mutex
	run    *schedule.Run
	during bool
	wmu    sync.Mutex
	tries  int
	id     string
	err    error
	fired  chan struct{}
}

// at sets the run an abort names; nil names none.
func (h *abortHold) at(run *schedule.Run, during bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.run, h.during = run, during
}

// write writes the marker for run, unless one is written or two writes
// failed.
func (h *abortHold) write(run schedule.Run, during bool, cause error) (string, error) {
	h.wmu.Lock()
	defer h.wmu.Unlock()
	if h.tries == 0 || h.err != nil && h.tries < 2 {
		h.tries++
		h.id, h.err = h.r.writeAbort(run, during, cause)
	}
	return h.id, h.err
}

// settle waits, once ctx has ended, for the write watch starts then.
func (h *abortHold) settle(ctx context.Context) {
	if ctx.Err() != nil && h.fired != nil {
		<-h.fired
	}
}

// watch writes the marker when ctx ends with loadgen.ErrOperatorAbort
// and a run is named. The returned stop waits for a marker being
// written.
func (h *abortHold) watch(ctx context.Context) (stop func()) {
	fired := make(chan struct{})
	h.fired = fired
	cancel := context.AfterFunc(ctx, func() {
		defer close(fired)
		cause := context.Cause(ctx)
		if !errors.Is(cause, loadgen.ErrOperatorAbort) {
			return
		}
		h.mu.Lock()
		run, during := h.run, h.during
		h.mu.Unlock()
		if run != nil {
			h.write(*run, during, cause) //nolint:errcheck
		}
	})
	return func() {
		if !cancel() {
			<-fired
		}
	}
}

// holds lists each stopped cell of st with no stop marker and each
// operator abort or stop marker of the route with no acknowledgement
// file in the acknowledgement directory, one line each naming the file
// that releases it.
func (r *route) holds(st *routeState) ([]string, error) {
	var out []string
	held := func(id, what string) error {
		switch _, err := os.Stat(r.ackFile(id)); {
		case err == nil:
			return nil
		case !errors.Is(err, os.ErrNotExist):
			return err
		}
		out = append(out, fmt.Sprintf("%s: %s; held until %s exists", r.cfg.Target.Name, what, r.ackFile(id)))
		return nil
	}
	var stopped []*cellRecord
	for _, rec := range st.records {
		if rec.Outcome == outcomeStopped && !fileExists(r.stopMarkerPath(rec.Run)) {
			stopped = append(stopped, rec)
		}
	}
	slices.SortFunc(stopped, func(a, b *cellRecord) int { return a.Run.Start.Compare(b.Run.Start) })
	for _, rec := range stopped {
		if err := held(cellLabel(rec.Run), fmt.Sprintf("%s attempt %d %s", rec.Run.Label, rec.Run.Attempt, cellSummary(rec))); err != nil {
			return nil, err
		}
	}
	var paths []string
	for _, prefix := range []string{abortPrefix, stopPrefix} {
		p, err := filepath.Glob(filepath.Join(r.dir, prefix+"*.json"))
		if err != nil {
			return nil, err
		}
		paths = append(paths, p...)
	}
	slices.Sort(paths)
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		var m abortMarker
		if err := json.Unmarshal(b, &m); err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		what := fmt.Sprintf("aborted at %s before %s attempt %d: %s", m.AtUTC.Format(time.RFC3339), m.Run.Label, m.Run.Attempt, m.Reason)
		switch {
		case m.Stop:
			what = fmt.Sprintf("%s attempt %d stopped at %s, its record not written: %s", m.Run.Label, m.Run.Attempt, m.AtUTC.Format(time.RFC3339), m.Reason)
		case m.During:
			what = fmt.Sprintf("aborted at %s during %s attempt %d: %s", m.AtUTC.Format(time.RFC3339), m.Run.Label, m.Run.Attempt, m.Reason)
		}
		if err := held(strings.TrimSuffix(filepath.Base(p), ".json"), what); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// checkAckDir refuses an acknowledgement directory under out, one this
// process can write, and one it cannot read and search; dir must exist.
// Both paths are compared with their symbolic links resolved.
func checkAckDir(dir, out string) error {
	fi, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("run: --ack-dir: %w", err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("run: --ack-dir %s is not a directory", dir)
	}
	a, err := resolvedPath(dir)
	if err != nil {
		return fmt.Errorf("run: --ack-dir: %w", err)
	}
	o, err := resolvedPath(out)
	if err != nil {
		return fmt.Errorf("run: --out: %w", err)
	}
	if rel, err := filepath.Rel(o, a); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("run: --ack-dir %s is under --out %s", dir, out)
	}
	if syscall.Access(dir, accessWrite) == nil {
		return fmt.Errorf("run: --ack-dir %s is writable by this process; the route's user may read and search it and may not write it", dir)
	}
	if err := syscall.Access(dir, accessRead|accessSearch); err != nil {
		return fmt.Errorf("run: --ack-dir %s is not readable and searchable by this process: %w", dir, err)
	}
	return nil
}

// resolvedPath is p made absolute with its symbolic links resolved; a
// part that does not exist yet is joined to its resolved parent.
func resolvedPath(p string) (string, error) {
	p, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	r, err := filepath.EvalSymlinks(p)
	switch {
	case err == nil:
		return r, nil
	case errors.Is(err, os.ErrNotExist) && filepath.Dir(p) != p:
		parent, err := resolvedPath(filepath.Dir(p))
		if err != nil {
			return "", err
		}
		return filepath.Join(parent, filepath.Base(p)), nil
	}
	return "", err
}

// access(2)'s R_OK, W_OK and X_OK.
const (
	accessRead   = 0x4
	accessWrite  = 0x2
	accessSearch = 0x1
)
