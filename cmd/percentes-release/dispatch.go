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
	"time"

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
// StartUTC, the second of the epoch.
type dispatchSeries struct {
	Run      schedule.Run `json:"run"`
	StartUTC time.Time    `json:"start_utc"`
	Counts   []int        `json:"counts"`
}

// newDispatchSeries reads each dispatched request's send time on the wall
// clock from the run epoch.
func newDispatchSeries(run schedule.Run, res *loadgen.Result) *dispatchSeries {
	s := &dispatchSeries{Run: run, StartUTC: res.EpochWall.UTC().Truncate(time.Second), Counts: []int{}}
	for i := range res.Requests {
		r := &res.Requests[i]
		if r.DispatchNs == 0 {
			continue
		}
		at := res.EpochWall.UTC().Add(time.Duration(r.DispatchNs)).Truncate(time.Second)
		k := int(at.Sub(s.StartUTC) / time.Second)
		for len(s.Counts) <= k {
			s.Counts = append(s.Counts, 0)
		}
		s.Counts[k]++
	}
	return s
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
