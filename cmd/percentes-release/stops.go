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
	"github.com/percentes/percentes/internal/probe"
)

// routeStop is a cause that stops the route's process (SPEC-HOSTED HA3
// point 16): the cell is retained, labelled with it, and re-run within
// the route's bound.
type routeStop struct{ reason string }

func (s *routeStop) Error() string { return "route stop: " + s.reason }

// errEMFILE and errEADDRNOTAVAIL end a ladder whose process ran out of
// file descriptors or local ports; the cell is client-invalid.
var (
	errEMFILE        = errors.New("a request failed with EMFILE (too many open files)")
	errEADDRNOTAVAIL = errors.New("a request failed with EADDRNOTAVAIL (no local port left)")
)

// billingSetting names a billing refusal's stop as the operator's own.
const billingSetting = probe.BillingSetting

// billingRefusal returns the stop for a response the target documents as
// its spend-limit or exhausted-balance refusal, the operator's billing
// setting, by its status and body (probe.BillingReason) or by the in-band
// error streamErr of a 200 stream (probe.InBandBilling), or nil.
func billingRefusal(t *config.Target, status int, body []byte, streamErr string) *routeStop {
	if r := probe.BillingReason(t, status, body); r != "" {
		return &routeStop{r}
	}
	if r := probe.InBandBilling(t, status, streamErr); r != "" {
		return &routeStop{r}
	}
	return nil
}

// billingRefusals counts the responses among reqs that billingRefusal
// stops on, and returns the first one's stop.
func billingRefusals(t *config.Target, reqs []loadgen.Request) (int, *routeStop) {
	n := 0
	var first *routeStop
	for i := range reqs {
		if s := billingRefusal(t, reqs[i].Status, reqs[i].ErrorBody, reqs[i].StreamError); s != nil {
			if n++; first == nil {
				first = s
			}
		}
	}
	return n, first
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
// the route's previous probed cell, the newest earlier cell whose probe
// request was dispatched, whether or not it reached a connection, saw the
// same status, on its probe or inside its ladder: the next probe failing
// the same way is the independent evidence of a credential or account
// fault (SPEC-HOSTED HA3 point 16). Otherwise it returns nil and the probe
// blocks the start. A cell with no dispatched probe and no ladder is
// passed over, except that a running one, or an interrupted one other than
// a cell SIGTERM ended before admission (endedBeforeAdmission), ends the
// search, since what its probe returned is unknown.
func (r *route) authFault(rec *cellRecord, status int) (*routeStop, error) {
	if !slices.Contains(authStatuses, status) {
		return nil, nil
	}
	paths, err := filepath.Glob(filepath.Join(r.dir, "*", cellFile))
	if err != nil {
		return nil, err
	}
	type cellAt struct {
		*cellRecord
		dir string
	}
	var earlier []cellAt
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
			earlier = append(earlier, cellAt{&c, filepath.Dir(p)})
		}
	}
	slices.SortFunc(earlier, func(a, b cellAt) int { return b.StartUTC.Compare(a.StartUTC) })
	for _, c := range earlier {
		if c.Sent.Probe == 0 && c.EpochUTC == nil && !probeDispatched(c.dir) {
			if c.Outcome == outcomeRunning || c.Outcome == outcomeInterrupted && !endedBeforeAdmission(c.cellRecord) {
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

// endedBeforeAdmission reports whether c, a cell with no dispatched
// probe and no ladder, ended on SIGTERM with no execution error; a
// restart's label appends to its reasons.
func endedBeforeAdmission(c *cellRecord) bool {
	return c.Error == "" && len(c.Reasons) > 0 && c.Reasons[0] == terminatedReason
}

// probeDispatched reports whether dir holds a probe record whose request
// was dispatched, whether or not it reached a connection.
func probeDispatched(dir string) bool {
	b, err := os.ReadFile(filepath.Join(dir, probe.File))
	if err != nil {
		return false
	}
	var pr probe.Record
	return json.Unmarshal(b, &pr) == nil && !pr.Request.DispatchUTC.IsZero()
}

// isEMFILE reports whether a request failed for want of a file
// descriptor.
func isEMFILE(r *loadgen.Request) bool {
	return r.ErrText != "" && strings.Contains(r.ErrText, syscall.EMFILE.Error())
}

// isEADDRNOTAVAIL reports whether a request failed for want of a local
// port.
func isEADDRNOTAVAIL(r *loadgen.Request) bool {
	return r.ErrText != "" && strings.Contains(r.ErrText, syscall.EADDRNOTAVAIL.Error())
}

// clientExhausted is the error that ends a ladder for a request that
// failed for want of a file descriptor or a local port, or nil.
func clientExhausted(r *loadgen.Request) error {
	switch {
	case isEMFILE(r):
		return errEMFILE
	case isEADDRNOTAVAIL(r):
		return errEADDRNOTAVAIL
	}
	return nil
}

// pricingBreach returns the stop for a cell in which a request, its
// probe's included, reported more prompt tokens than the shape's input
// bound or more output tokens, reasoning included, than the ceiling, the
// bounds the ledger prices each request at, or nil. probePrompt and
// probeOutput are the probe's reported counts, nil where unreported.
func pricingBreach(sh *config.Shape, reqs []loadgen.Request, probePrompt, probeOutput *int) *routeStop {
	n, prompt, output := 0, 0, 0
	note := func(p, o *int) {
		breach := false
		if p != nil {
			prompt, breach = max(prompt, *p), breach || *p > sh.InputBoundTokens
		}
		if o != nil {
			output, breach = max(output, *o), breach || *o > sh.CeilingTokens
		}
		if breach {
			n++
		}
	}
	note(probePrompt, probeOutput)
	for i := range reqs {
		var p, o *int
		if v, ok := reqs[i].PromptTokens(); ok {
			p = &v
		}
		if v, ok := reqs[i].OutputTokens(); ok {
			o = &v
		}
		note(p, o)
	}
	if n == 0 {
		return nil
	}
	return &routeStop{fmt.Sprintf("pricing-bound breach: %d of the cell's requests reported more than the %d-token input bound or the %d-token ceiling, reasoning included, that the ledger prices each request at; the largest reported were %s prompt and %s output tokens",
		n, sh.InputBoundTokens, sh.CeilingTokens, loadgen.TokenCountText(prompt), loadgen.TokenCountText(output))}
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
