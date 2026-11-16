package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/schedule"
)

// The previous probed cell is the newest earlier cell whose probe request
// was dispatched, whether or not it reached a connection: its probe and
// ladder are read, and a cell with no probe record and no ladder is
// passed over unless it is running, or interrupted other than by SIGTERM
// before admission with no execution error.
func TestAuthFaultReadsCellWhoseProbeReachedNoConnection(t *testing.T) {
	base := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	type earlierCell struct {
		rec        *cellRecord
		dispatched bool
	}
	cell := func(i int, outcome string, probeStatus, probeSent int, ladder bool, auth map[int]int, dispatched bool) earlierCell {
		c := &cellRecord{Run: schedule.Run{Route: "groq", Label: fmt.Sprintf("weekday-%02d", i), Attempt: 1}, Outcome: outcome, StartUTC: base.Add(time.Duration(i) * time.Hour),
			ProbeStatus: probeStatus, AuthStatuses: auth}
		c.Sent.Probe = probeSent
		if ladder {
			e := c.StartUTC.Add(time.Minute)
			c.EpochUTC = &e
			c.Sent.Warmup, c.Sent.Measured = 2, 124
		}
		return earlierCell{c, dispatched}
	}
	interrupted := func(errText string, reasons ...string) earlierCell {
		c := cell(2, outcomeInterrupted, 0, 0, false, nil, false)
		c.rec.Error, c.rec.Reasons = errText, reasons
		return c
	}
	probe401 := cell(1, outcomeBlocked, 401, 1, false, nil, true)
	clean := cell(1, outcomeCounted, 200, 1, true, nil, true)
	cur := cell(10, outcomeRunning, 0, 0, false, nil, false)
	opNote := "operator records not read when the cell was labelled: open operator.json: no such file or directory; the manifest carries the records read when the route started"
	for _, tc := range []struct {
		name    string
		earlier []earlierCell
		want    string
	}{
		{"unreached probe, ladder 401s", []earlierCell{clean, cell(2, outcomeCounted, 0, 0, true, map[int]int{401: 50}, true)}, "probe request returned 401, as did 50 requests inside its ladder at weekday-02 attempt 1"},
		{"unreached probe, clean ladder", []earlierCell{probe401, cell(2, outcomeCounted, 0, 0, true, nil, true)}, ""},
		{"unreached probe, ladder missed", []earlierCell{probe401, cell(2, outcomeMissed, 0, 0, false, nil, true)}, ""},
		{"unreached probe, operator abort", []earlierCell{probe401, cell(2, outcomeStopped, 0, 0, false, nil, true)}, ""},
		{"interrupted, unreached probe, ladder 401s", []earlierCell{clean, cell(2, outcomeInterrupted, 0, 0, true, map[int]int{401: 7}, true)}, "probe request returned 401, as did 7 requests inside its ladder at weekday-02 attempt 1"},
		{"blocked before admission", []earlierCell{probe401, cell(2, outcomeBlocked, 0, 0, false, nil, false)}, "probe request returned 401, as did its probe at weekday-01 attempt 1"},
		{"stopped before admission", []earlierCell{probe401, cell(2, outcomeStopped, 0, 0, false, nil, false)}, "probe request returned 401, as did its probe at weekday-01 attempt 1"},
		{"interrupted, no probe record", []earlierCell{probe401, cell(2, outcomeInterrupted, 0, 0, false, nil, false)}, ""},
		{"running, no probe record", []earlierCell{probe401, cell(2, outcomeRunning, 0, 0, false, nil, false)}, ""},
		{"SIGTERM before admission", []earlierCell{probe401, interrupted("", terminatedReason)}, "probe request returned 401, as did its probe at weekday-01 attempt 1"},
		{"SIGTERM before admission, labelled", []earlierCell{probe401, interrupted("", terminatedReason, opNote)}, "probe request returned 401, as did its probe at weekday-01 attempt 1"},
		{"SIGTERM before admission, clean probe before", []earlierCell{clean, interrupted("", terminatedReason)}, ""},
		{"hard kill, no probe record", []earlierCell{probe401, interrupted("", "the process ended during the cell")}, ""},
		{"execution error, SIGTERM reason", []earlierCell{probe401, interrupted("read ledger: unexpected EOF", terminatedReason, "execution error: read ledger: unexpected EOF")}, ""},
		{"SIGTERM reason not first", []earlierCell{probe401, interrupted("", "the process ended during the cell", terminatedReason)}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &route{dir: t.TempDir()}
			for _, c := range append(tc.earlier, cur) {
				dir := runDir(r.dir, c.rec.Run)
				if err := replaceJSON(filepath.Join(dir, cellFile), c.rec); err != nil {
					t.Fatal(err)
				}
				if c.dispatched {
					pr := probe.Record{Request: probe.Exchange{DispatchUTC: c.rec.StartUTC, Status: c.rec.ProbeStatus}}
					if err := replaceJSON(filepath.Join(dir, probe.File), &pr); err != nil {
						t.Fatal(err)
					}
				}
			}
			stop, err := r.authFault(cur.rec, 401)
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case tc.want == "" && stop != nil:
				t.Fatalf("stop %q", stop.Error())
			case tc.want != "" && (stop == nil || stop.Error() != "route stop: "+tc.want+": independent evidence of a credential or account fault (SPEC-HOSTED HA3 point 16)"):
				t.Fatalf("stop %v, want %q", stop, tc.want)
			}
		})
	}
}

// switchable serves one loopback address only while up, answering each
// request with status and an error body, or 200 and chatStream.
type switchable struct {
	t      *testing.T
	addr   string
	status atomic.Int64
	mu     sync.Mutex
	srv    *httptest.Server
}

func newSwitchable(t *testing.T, status int) *switchable {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &switchable{t: t, addr: ln.Addr().String()}
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	s.status.Store(int64(status))
	t.Cleanup(s.down)
	return s
}

func (s *switchable) url() string { return "http://" + s.addr + "/openai/v1/chat/completions" }

// up starts serving where s is down.
func (s *switchable) up() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.srv != nil {
		return
	}
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.t.Error(err)
		return
	}
	s.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body) //nolint:errcheck
		if st := int(s.status.Load()); st != http.StatusOK {
			w.WriteHeader(st)
			io.WriteString(w, `{"error":{"message":"Invalid API Key","code":"invalid_api_key"}}`) //nolint:errcheck
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, chatStream) //nolint:errcheck
	}))
	s.srv.Listener = l
	s.srv.Start()
}

// down stops serving where s is up.
func (s *switchable) down() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.srv != nil {
		s.srv.CloseClientConnections()
		s.srv.Close()
		s.srv = nil
	}
}

// A cell whose probe reached no connection and whose ladder then received
// 401s is the previous probed cell: the next probe's 401 stops the route,
// citing that ladder, and the route is held.
func TestAuthFaultStopsOnLadderOfCellWhoseProbeReachedNoConnection(t *testing.T) {
	fastDispatch(t)
	s := newSwitchable(t, http.StatusUnauthorized)
	r, _ := newTestRoute(t, releaseConfig(t, s.url()), "+0.005")
	shortLadder(r)
	first, second := r.runs[0], r.runs[1]
	now := first.Start
	r.now = func() time.Time { return now }
	r.wait = func(_ context.Context, at time.Time) error {
		now = at
		return nil
	}
	r.onEpoch = func(time.Time) { s.up() }
	var (
		code int
		err  error
	)
	stdout := stdoutOf(t, func() { code, err = r.loop(context.Background()) })
	if err != nil || code != exitStopped {
		t.Fatalf("exit %d, %v:\n%s", code, err, stdout)
	}
	c0 := readCell(t, r, first)
	if c0.Sent.Probe != 0 || c0.EpochUTC == nil || c0.AuthStatuses[http.StatusUnauthorized] == 0 || !probeDispatched(r.cellDir(first)) {
		t.Fatalf("%s: %s %q, sent %+v, epoch %v, auth %v", first.Label, c0.Outcome, c0.Reasons, c0.Sent, c0.EpochUTC != nil, c0.AuthStatuses)
	}
	want := fmt.Sprintf("probe request returned 401, as did %d requests inside its ladder at %s attempt 1: independent evidence", c0.AuthStatuses[http.StatusUnauthorized], first.Label)
	c1 := readCell(t, r, second)
	if c1.Outcome != outcomeStopped || len(c1.Reasons) != 1 || !strings.Contains(c1.Reasons[0], want) {
		t.Fatalf("%s: %s %q", second.Label, c1.Outcome, c1.Reasons)
	}
	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	if holds, err := r.holds(st); err != nil || len(holds) != 1 || !strings.Contains(holds[0], r.ackFile(cellLabel(second))) {
		t.Fatalf("holds %q: %v", holds, err)
	}
}

// A cell whose probe reached no connection and whose ladder ran clean is
// the previous probed cell: a later probe's 401 blocks its cell, though
// the cell before it received a 401 on its probe.
func TestAuthFaultBlocksAfterCleanLadderOfCellWhoseProbeReachedNoConnection(t *testing.T) {
	fastDispatch(t)
	s := newSwitchable(t, http.StatusUnauthorized)
	r, _ := newTestRoute(t, releaseConfig(t, s.url()), "+0.005")
	shortLadder(r)
	first, second, third := r.runs[0], r.runs[1], r.runs[2]
	now := first.Start
	r.now = func() time.Time { return now }
	waits := 0
	r.wait = func(_ context.Context, at time.Time) error {
		now = at
		switch waits++; waits {
		case 1:
			s.up()
		case 2:
			s.down()
			s.status.Store(http.StatusOK)
		case 3:
			s.status.Store(http.StatusUnauthorized)
		default:
			return errEndLoop
		}
		return nil
	}
	r.onEpoch = func(time.Time) { s.up() }
	var (
		code int
		err  error
	)
	stdout := stdoutOf(t, func() { code, err = r.loop(context.Background()) })
	if err == nil || !strings.Contains(err.Error(), errEndLoop.Error()) || code != exitError || waits != 4 {
		t.Fatalf("exit %d, %v, %d waits:\n%s", code, err, waits, stdout)
	}
	if c0 := readCell(t, r, first); c0.Outcome != outcomeBlocked || c0.ProbeStatus != http.StatusUnauthorized {
		t.Fatalf("%s: %s %q, probe %d", first.Label, c0.Outcome, c0.Reasons, c0.ProbeStatus)
	}
	c1 := readCell(t, r, second)
	if c1.Sent.Probe != 0 || c1.EpochUTC == nil || len(c1.AuthStatuses) != 0 || !probeDispatched(r.cellDir(second)) {
		t.Fatalf("%s: %s %q, sent %+v, epoch %v, auth %v", second.Label, c1.Outcome, c1.Reasons, c1.Sent, c1.EpochUTC != nil, c1.AuthStatuses)
	}
	c2 := readCell(t, r, third)
	if c2.Outcome != outcomeBlocked || len(c2.Reasons) != 1 || c2.Reasons[0] != "probe request returned 401, a setup defect (SPEC-HOSTED §5); the window does not start" {
		t.Fatalf("%s: %s %q", third.Label, c2.Outcome, c2.Reasons)
	}
	st, err := r.state()
	if err != nil {
		t.Fatal(err)
	}
	if holds, err := r.holds(st); err != nil || len(holds) != 0 {
		t.Fatalf("holds %q: %v", holds, err)
	}
}

// errEndLoop ends a test's route loop at its wait.
var errEndLoop = fmt.Errorf("end of test")
