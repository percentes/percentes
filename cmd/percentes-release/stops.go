package main

import (
	"errors"
	"fmt"
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
