package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
)

// routeStop is a cause that stops the route's process (SPEC-HOSTED HA3
// point 16): the cell is retained, labelled with it, and re-run within
// the route's bound.
type routeStop struct{ reason string }

func (s *routeStop) Error() string { return "route stop: " + s.reason }

// errEMFILE ends a ladder whose process ran out of file descriptors; the
// cell is client-invalid.
var errEMFILE = errors.New("a request failed with EMFILE (too many open files)")

// billingRefusal returns the stop for a response the target documents as
// its spend-limit or exhausted-balance refusal, or nil.
func billingRefusal(t *config.Target, status int, body []byte) *routeStop {
	for i := range t.BillingRefusals {
		if b := &t.BillingRefusals[i]; b.Matches(status, body) {
			return &routeStop{fmt.Sprintf("HTTP %d, the documented billing refusal: %s (%s, retrieved %s)", status, b.Meaning, b.Source, b.Retrieved)}
		}
	}
	return nil
}

// authStatuses are the statuses that, returned again by the route's next
// probe, stop the route as a credential or account fault.
var authStatuses = []int{http.StatusUnauthorized, http.StatusForbidden}

// countAuth counts the 401 and 403 responses among reqs by status; nil
// when there are none.
func countAuth(reqs []loadgen.Request) map[int]int {
	var n map[int]int
	for i := range reqs {
		if s := reqs[i].Status; slices.Contains(authStatuses, s) {
			if n == nil {
				n = map[int]int{}
			}
			n[s]++
		}
	}
	return n
}

// authFault returns the stop for a probe that returned a 401 or 403 when
// the route's previous cell to send its probe saw the same status, on its
// probe or inside its ladder: the next probe failing the same way is the
// independent evidence of a credential or account fault (SPEC-HOSTED HA3
// point 16). Otherwise it returns nil and the probe blocks the start. An
// interrupted cell that records no probe ends the search, since what its
// probe returned is unknown.
func (r *route) authFault(rec *cellRecord, status int) (*routeStop, error) {
	if !slices.Contains(authStatuses, status) {
		return nil, nil
	}
	paths, err := filepath.Glob(filepath.Join(r.dir, "*", cellFile))
	if err != nil {
		return nil, err
	}
	var earlier []*cellRecord
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		var c cellRecord
		if err := json.Unmarshal(b, &c); err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		if c.StartUTC.Before(rec.StartUTC) {
			earlier = append(earlier, &c)
		}
	}
	slices.SortFunc(earlier, func(a, b *cellRecord) int { return b.StartUTC.Compare(a.StartUTC) })
	for _, c := range earlier {
		if c.Sent.Probe == 0 {
			if c.Outcome == outcomeInterrupted || c.Outcome == outcomeRunning {
				return nil, nil
			}
			continue
		}
		var seen string
		switch {
		case c.ProbeStatus == status:
			seen = "its probe"
		case c.AuthStatuses[status] > 0:
			seen = fmt.Sprintf("%d requests inside its ladder", c.AuthStatuses[status])
		default:
			return nil, nil
		}
		return &routeStop{fmt.Sprintf("probe request returned %d, as did %s at %s attempt %d: independent evidence of a credential or account fault (SPEC-HOSTED HA3 point 16)",
			status, seen, c.Run.Label, c.Run.Attempt)}, nil
	}
	return nil, nil
}

// isEMFILE reports whether a request failed for want of a file
// descriptor.
func isEMFILE(r *loadgen.Request) bool {
	return r.ErrText != "" && strings.Contains(r.ErrText, syscall.EMFILE.Error())
}

// minOpenFiles is the open-file limit a route process requires.
const minOpenFiles = 65536

// openFilesLimit returns the process's soft limit on open files.
var openFilesLimit = func() (uint64, error) {
	var l syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &l); err != nil {
		return 0, fmt.Errorf("open-file limit: %w", err)
	}
	return l.Cur, nil
}

// checkOpenFiles returns the open-file limit, refusing one below
// minOpenFiles.
func checkOpenFiles() (uint64, error) {
	n, err := openFilesLimit()
	if err != nil {
		return 0, err
	}
	if n < minOpenFiles {
		return n, fmt.Errorf("open-file limit %d is below %d; raise it (LimitNOFILE in the service unit, or ulimit -n)", n, minOpenFiles)
	}
	return n, nil
}
