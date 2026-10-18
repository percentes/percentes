package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/schedule"
)

// The dispatch series' file names: one per cell in its directory, and the
// sum over every cell at the --out root.
const (
	dispatchFile  = "dispatch.json"
	aggregateFile = "dispatch-aggregate.json"
)

// dispatchSeries counts a cell's dispatches per UTC second, from
// StartUTC. A finished cell's series is final, from the second of the
// epoch; a running cell's is rewritten as it runs, from the second of
// its start, and counts every dispatch before ThroughUTC.
type dispatchSeries struct {
	Run        schedule.Run `json:"run"`
	StartUTC   time.Time    `json:"start_utc"`
	Counts     []int        `json:"counts"`
	Final      bool         `json:"final"`
	ThroughUTC *time.Time   `json:"through_utc,omitempty"`
}

// add counts one dispatch at.
func (s *dispatchSeries) add(at time.Time) {
	at = at.UTC().Truncate(time.Second)
	if at.Before(s.StartUTC) {
		s.Counts = append(make([]int, int(s.StartUTC.Sub(at)/time.Second)), s.Counts...)
		s.StartUTC = at
	}
	k := int(at.Sub(s.StartUTC) / time.Second)
	for len(s.Counts) <= k {
		s.Counts = append(s.Counts, 0)
	}
	s.Counts[k]++
}

// newDispatchSeries is a finished cell's final series: each dispatched
// request's send time on the wall clock from the run epoch.
func newDispatchSeries(run schedule.Run, res *loadgen.Result) *dispatchSeries {
	s := &dispatchSeries{Run: run, StartUTC: res.EpochWall.UTC().Truncate(time.Second), Counts: []int{}, Final: true}
	for i := range res.Requests {
		if r := &res.Requests[i]; r.DispatchNs != 0 {
			s.add(res.EpochWall.Add(time.Duration(r.DispatchNs)))
		}
	}
	return s
}

// The running series' timing: it is rewritten every dispatchFlush and
// counts the dispatches before dispatchLag ago, the bound on the time
// from a request's dispatch time to its count. A cell that ends waits up
// to dispatchWaitMax, reading every dispatchPoll, for each concurrent
// run's series to cover its end.
var (
	dispatchFlush   = time.Second
	dispatchLag     = time.Second
	dispatchWaitMax = 10 * time.Second
	dispatchPoll    = 100 * time.Millisecond
)

// liveSeries is a running cell's series, written to path from start
// until stop. Its times, as the run epoch's, are the host's wall clock.
type liveSeries struct {
	path  string
	start time.Time

	mu    sync.Mutex
	s     dispatchSeries
	epoch time.Time

	stopOnce sync.Once
	stopped  chan struct{}
	done     chan struct{}
}

// startLiveSeries writes run's empty series to path and rewrites it every
// dispatchFlush until stop. A failed rewrite leaves the last one, whose
// ThroughUTC shows how far it counts.
func startLiveSeries(path string, run schedule.Run) (*liveSeries, error) {
	start := time.Now()
	l := &liveSeries{path: path, start: start, s: dispatchSeries{Run: run, StartUTC: start.UTC().Truncate(time.Second), Counts: []int{}},
		stopped: make(chan struct{}), done: make(chan struct{})}
	if err := l.flush(); err != nil {
		return nil, err
	}
	go func() {
		defer close(l.done)
		tick := time.NewTicker(dispatchFlush)
		defer tick.Stop()
		for {
			select {
			case <-l.stopped:
				return
			case <-tick.C:
				_ = l.flush()
			}
		}
	}()
	return l, nil
}

// setEpoch records the run epoch the requests' dispatch times count from.
func (l *liveSeries) setEpoch(e time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.epoch = e
}

// dispatched counts q; it runs on q's goroutine.
func (l *liveSeries) dispatched(q *loadgen.Request) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.epoch.IsZero() {
		l.s.add(l.epoch.Add(time.Duration(q.DispatchNs)))
	}
}

func (l *liveSeries) flush() error {
	l.mu.Lock()
	s := l.s
	s.Counts = append([]int{}, l.s.Counts...)
	through := time.Now().Add(-dispatchLag).UTC()
	s.ThroughUTC = &through
	l.mu.Unlock()
	return replaceJSON(l.path, &s)
}

// stop ends the rewrites; once it returns the file is not written again.
func (l *liveSeries) stop() {
	l.stopOnce.Do(func() { close(l.stopped) })
	<-l.done
}

// hostDispatch is the host's aggregate dispatch series over a cell, from
// the second of from through the second of to on the host's wall clock:
// the cell's own series, final unless the cell was interrupted, and each
// concurrent run's, read from its cell directory once it is final or
// counts every dispatch through to, or as it stands after
// dispatchWaitMax.
func (r *route) hostDispatch(from, to time.Time, own *dispatchSeries, runs []schedule.Run) (*evidence.Dispatch, error) {
	from, to = from.UTC(), to.UTC()
	lo := from.Truncate(time.Second)
	d := &evidence.Dispatch{StartUTC: lo, EndUTC: to, Counts: make([]int, int(to.Sub(lo)/time.Second)+1)}
	sum := func(s *dispatchSeries) int {
		n := 0
		for k, c := range s.Counts {
			if i := int(s.StartUTC.UTC().Add(time.Duration(k)*time.Second).Sub(lo) / time.Second); i >= 0 && i < len(d.Counts) {
				d.Counts[i] += c
				n += c
			}
		}
		return n
	}
	src := evidence.DispatchSource{Run: own.Run, Dispatches: sum(own), Final: own.Final}
	if !own.Final {
		src.ThroughUTC = own.ThroughUTC
	}
	d.Sources = append(d.Sources, src)
	deadline := time.Now().Add(dispatchWaitMax)
	for _, run := range runs {
		s, err := waitSeries(filepath.Join(runDir(filepath.Join(r.out, run.Route), run), dispatchFile), run, to, deadline)
		if err != nil {
			return nil, err
		}
		src := evidence.DispatchSource{Run: run}
		if s != nil {
			src.Dispatches, src.Final = sum(s), s.Final
			if !s.Final {
				src.ThroughUTC = s.ThroughUTC
			}
		}
		d.Sources = append(d.Sources, src)
	}
	for k, n := range d.Counts {
		if n > d.MaxPerSecond {
			at := lo.Add(time.Duration(k) * time.Second)
			d.MaxPerSecond, d.MaxAtUTC = n, &at
		}
	}
	return d, nil
}

// waitSeries reads run's series at path until it is final or counts
// through to, or until deadline; nil when there is none.
func waitSeries(path string, run schedule.Run, to, deadline time.Time) (*dispatchSeries, error) {
	for {
		var s *dispatchSeries
		b, err := os.ReadFile(path)
		switch {
		case err == nil:
			s = &dispatchSeries{}
			if err := json.Unmarshal(b, s); err != nil {
				return nil, fmt.Errorf("dispatch: %s: %w", path, err)
			}
			if s.Run != run {
				return nil, fmt.Errorf("dispatch: %s is the series of %s %s attempt %d", path, s.Run.Route, s.Run.Label, s.Run.Attempt)
			}
			if s.Final || s.ThroughUTC != nil && !s.ThroughUTC.Before(to) {
				return s, nil
			}
		case !errors.Is(err, os.ErrNotExist):
			return nil, err
		}
		if !time.Now().Before(deadline) {
			return s, nil
		}
		time.Sleep(dispatchPoll)
	}
}

// aggregateSeries is the sum of the cells' series over the host, per UTC
// second from StartUTC, with the files it read.
type aggregateSeries struct {
	Files        []aggregateFileRef `json:"files"`
	StartUTC     time.Time          `json:"start_utc"`
	Counts       []int              `json:"counts"`
	MaxPerSecond int                `json:"max_per_second"`
	MaxAtUTC     time.Time          `json:"max_at_utc"`
}

type aggregateFileRef struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
}

// aggregate sums every <route>/<cell>/dispatch.json under out.
func aggregate(out string) (*aggregateSeries, error) {
	paths, err := filepath.Glob(filepath.Join(out, "*", "*", dispatchFile))
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("dispatch: no %s under %s", dispatchFile, out)
	}
	sort.Strings(paths)
	per := map[time.Time]int{}
	a := &aggregateSeries{Files: []aggregateFileRef{}}
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		var s dispatchSeries
		if err := json.Unmarshal(b, &s); err != nil {
			return nil, fmt.Errorf("dispatch: %s: %w", p, err)
		}
		rel, _ := filepath.Rel(out, p)
		sum := sha256.Sum256(b)
		a.Files = append(a.Files, aggregateFileRef{Name: filepath.ToSlash(rel), SHA256: hex.EncodeToString(sum[:])})
		for k, n := range s.Counts {
			per[s.StartUTC.UTC().Add(time.Duration(k)*time.Second)] += n
		}
	}
	var first, last time.Time
	for t := range per {
		if first.IsZero() || t.Before(first) {
			first = t
		}
		if t.After(last) {
			last = t
		}
	}
	a.StartUTC = first
	a.Counts = make([]int, int(last.Sub(first)/time.Second)+1)
	for t, n := range per {
		k := int(t.Sub(first) / time.Second)
		a.Counts[k] += n
	}
	for k, n := range a.Counts {
		if n > a.MaxPerSecond {
			a.MaxPerSecond, a.MaxAtUTC = n, first.Add(time.Duration(k)*time.Second)
		}
	}
	return a, nil
}

// dispatchCmd writes the host's aggregate dispatch series to the --out
// root.
func dispatchCmd(args []string) (int, error) {
	fs := flag.NewFlagSet("dispatch", flag.ContinueOnError)
	out := fs.String("out", "", "the release output directory the route processes write under (required)")
	if err := fs.Parse(args); err != nil {
		return exitError, err
	}
	if *out == "" {
		return exitError, errors.New("dispatch: --out is required")
	}
	a, err := aggregate(*out)
	if err != nil {
		return exitError, err
	}
	path := filepath.Join(*out, aggregateFile)
	if err := replaceJSON(path, a); err != nil {
		return exitError, err
	}
	fmt.Printf("%d cells; at most %d dispatches in one second, at %s; written %s\n", len(a.Files), a.MaxPerSecond, a.MaxAtUTC.Format(time.RFC3339), path)
	return exitOK, nil
}
