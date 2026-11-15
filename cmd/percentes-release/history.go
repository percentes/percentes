package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/report"
	"github.com/percentes/percentes/internal/schedule"
)

// groupMembers are the routes of each account group: those of cfgs and
// those the schedule's input records, by group, each sorted.
func groupMembers(cfgs []*config.Config, sched *schedule.Schedule) map[string][]string {
	out := map[string][]string{}
	add := func(group, route string) {
		if group != "" && !slices.Contains(out[group], route) {
			out[group] = append(out[group], route)
		}
	}
	for _, c := range cfgs {
		add(c.Release.AccountGroup, c.Target.Name)
	}
	if sched != nil {
		for _, c := range sched.Input.Configs {
			add(c.AccountGroup, c.Route)
		}
	}
	for g := range out {
		slices.Sort(out[g])
	}
	return out
}

// readHistory is account group group's send history (SPEC-HOSTED HA4,
// §5 status_429): every request of every cell directory under out of the
// group's routes among cfgs, whatever the cell's outcome, its ceiling-check
// records and its probe, and the ceiling-check records at checkPaths
// (checkRecords), each record read once, all on UTC. A cell's attempts
// are read where its manifest names them; a cell whose records are not
// complete is a gap over its dispatch series, to the hard timeout after
// its last counted second. Each ledger admission of a route of members,
// the group's routes, is reconciled with the sends it brackets in time
// (reconcile). A send no admission of its route brackets is counted as
// unadmitted.
func readHistory(out, group string, members []string, cfgs []*config.Config, entries []admission.Entry, checkPaths []string) (*report.History, error) {
	h := &report.History{Group: group, Routes: slices.Clone(members)}
	checks := map[string]bool{}
	var spans []attemptSpan
	for _, cfg := range cfgs {
		if cfg.Release.AccountGroup != group {
			continue
		}
		name := cfg.Target.Name
		paths, err := filepath.Glob(filepath.Join(out, name, "*", cellFile))
		if err != nil {
			return nil, err
		}
		for _, p := range paths {
			span, err := cellHistory(h, filepath.Dir(p), cfg, members, checks)
			if err != nil {
				return nil, fmt.Errorf("pages: history: %w", err)
			}
			spans = append(spans, span)
		}
	}
	for _, p := range checkPaths {
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("pages: history: %w", err)
		}
		sum := sha256.Sum256(b)
		if k := hex.EncodeToString(sum[:]); !checks[k] {
			checks[k] = true
			if err := checkHistory(h, p, filepath.Base(p), members, config.PinnedReleaseHardTimeoutS); err != nil {
				return nil, fmt.Errorf("pages: history: %w", err)
			}
		}
	}
	reconcile(h, entries, members, spans)
	h.Prices = groupPrices(entries, members)
	return h, nil
}

// attemptSpan is a cell attempt of route: when its process began it, and
// the latest time a request of it can have been written, the hard
// timeout after its ladder's latest end under the start tolerance.
type attemptSpan struct {
	route        string
	began, until time.Time
}

// cellHistory adds the sends and gaps of the cell in dir, a cell of
// cfg's route, to h, and returns its span; checks holds the SHA-256 of
// every ceiling-check record read, each read once.
func cellHistory(h *report.History, dir string, cfg *config.Config, members []string, checks map[string]bool) (attemptSpan, error) {
	b, err := os.ReadFile(filepath.Join(dir, cellFile))
	if err != nil {
		return attemptSpan{}, err
	}
	var rec cellRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		return attemptSpan{}, fmt.Errorf("%s: %w", dir, err)
	}
	l := cfg.Release.Ladder
	ladder := time.Duration((l.WarmupS + float64(len(l.StepFractions))*(l.StepS+l.DrainS)) * float64(time.Second))
	span := attemptSpan{route: cfg.Target.Name, began: rec.StartUTC.UTC(),
		until: rec.Run.Start.Add(admission.ReleaseStartTolerance + ladder + time.Duration(cfg.Client.HTTPTimeoutS)*time.Second).UTC()}
	return span, cellSends(h, dir, cfg, members, checks, &rec)
}

// cellSends adds the sends and gaps of the cell in dir, whose run record
// is rec, to h (cellHistory).
func cellSends(h *report.History, dir string, cfg *config.Config, members []string, checks map[string]bool, rec *cellRecord) error {
	name, label := cfg.Target.Name, cellLabel(rec.Run)
	in, timeout := cfg.Release.Shape.InputBoundTokens, cfg.Client.HTTPTimeoutS
	complete := false
	if mp := filepath.Join(dir, evidence.ManifestFile); fileExists(mp) {
		m, err := evidence.ReadManifest(mp)
		if err != nil {
			return err
		}
		if m.Release == nil {
			return fmt.Errorf("%s: the manifest has no cell record", dir)
		}
		recs, err := readAttempts(dir, m)
		if err != nil {
			return err
		}
		epoch := m.StartUTC
		if rec.EpochUTC != nil {
			epoch = *rec.EpochUTC
		}
		h.Sends = append(h.Sends, report.RecordSends(name, label, epoch, recs, in, timeout)...)
		complete = m.Release.RecordsComplete
		for _, ref := range m.Release.Probes {
			if checks[ref.SHA256] {
				continue
			}
			checks[ref.SHA256] = true
			if err := checkHistory(h, filepath.Join(dir, ref.Name), ref.Name, members, timeout); err != nil {
				return err
			}
		}
	}
	if p := filepath.Join(dir, probe.File); fileExists(p) {
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		var pr probe.Record
		if err := json.Unmarshal(b, &pr); err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
		ceiling := cfg.Release.Shape.CeilingTokens
		if s, ok := report.ExchangeSend(name, label, report.SendProbe, 0, &pr.Request, pr.Shape, pr.PromptTokens, in, ceiling, timeout); ok && probe.LeftClient(&pr.Request) {
			h.Sends = append(h.Sends, s)
		}
		if s, ok := report.ExchangeSend(name, label, report.SendProbe, 1, pr.WrongModel, nil, nil, in, ceiling, timeout); ok && probe.LeftClient(pr.WrongModel) {
			h.Sends = append(h.Sends, s)
		}
	}
	if complete {
		return nil
	}
	return dispatchGap(h, dir, name, label, rec, timeout)
}

// checkRecords refuses a ceiling-check record at paths that does not
// decode, or that was not sent on the ledger sched froze for one of its
// routes.
func checkRecords(paths []string, sched *schedule.Schedule) error {
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			return fmt.Errorf("pages: %w", err)
		}
		var rec probe.ReleaseRecord
		switch err := json.Unmarshal(b, &rec); {
		case err != nil:
			return fmt.Errorf("pages: --check-record %s: %w", p, err)
		case rec.LedgerSHA256 != sched.Input.LedgerSHA256:
			return fmt.Errorf("pages: --check-record %s: admitted in the ledger of open-line sha256 %q; the schedule froze %s", p, rec.LedgerSHA256, sched.Input.LedgerSHA256)
		case !slices.Contains(sched.Input.Routes, rec.Route):
			return fmt.Errorf("pages: --check-record %s: route %q is not a route of the schedule", p, rec.Route)
		}
	}
	return nil
}

// checkHistory adds the requests of the ceiling-check record at path to
// h where its route is one of members.
func checkHistory(h *report.History, path, name string, members []string, timeoutS int) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var rec probe.ReleaseRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if !slices.Contains(members, rec.Route) {
		return nil
	}
	pin, ok := config.LookupShape(rec.Shape)
	if !ok {
		return fmt.Errorf("%s: no request shape %q", path, rec.Shape)
	}
	for i := range rec.Requests {
		q := &rec.Requests[i]
		if !probe.Reached(&q.Exchange) {
			continue
		}
		if s, ok := report.ExchangeSend(rec.Route, "ceiling check "+name, report.SendCheck, int64(q.Index), &q.Exchange, q.Shape, q.PromptTokens,
			pin.InputBoundTokens, rec.Ceiling.Ceiling, timeoutS); ok {
			h.Sends = append(h.Sends, s)
		}
	}
	return nil
}

// dispatchGap adds the gap of a cell whose per-request records are not
// complete: from its dispatch series' first counted second to the hard
// timeout after its last, or, with no series, from its epoch to the hard
// timeout after its end, open where it has none. A cell with no series
// and no epoch sent no ladder request.
func dispatchGap(h *report.History, dir, route, label string, rec *cellRecord, timeoutS int) error {
	timeout := time.Duration(timeoutS) * time.Second
	reason := fmt.Sprintf("cell %s has no per-request records for its ladder", label)
	b, err := os.ReadFile(filepath.Join(dir, dispatchFile))
	switch {
	case errors.Is(err, os.ErrNotExist):
		if rec.EpochUTC == nil {
			return nil
		}
		g := report.HistoryGap{Route: route, FromUTC: rec.EpochUTC.UTC(), Reason: reason + " and no dispatch series"}
		if rec.EndUTC != nil {
			to := rec.EndUTC.UTC().Add(timeout)
			g.ToUTC = &to
		}
		h.Gaps = append(h.Gaps, g)
		return nil
	case err != nil:
		return err
	}
	var s dispatchSeries
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("%s: %w", dispatchFile, err)
	}
	first, last, n := -1, -1, 0
	for k, c := range s.Counts {
		if c > 0 {
			if first < 0 {
				first = k
			}
			last, n = k, n+c
		}
	}
	if first < 0 {
		return nil
	}
	to := s.StartUTC.Add(time.Duration(last+1)*time.Second + timeout).UTC()
	h.Gaps = append(h.Gaps, report.HistoryGap{Route: route, FromUTC: s.StartUTC.Add(time.Duration(first) * time.Second).UTC(), ToUTC: &to,
		Reason: fmt.Sprintf("%s; its dispatch series counts %d dispatches", reason, n)})
	return nil
}

// sendPurpose is the ledger purpose that admits a send of kind.
func sendPurpose(kind string) string {
	if kind == report.SendCheck {
		return admission.PurposeProbe
	}
	return admission.PurposeCell
}

// reconcile checks each admission of a route of members against the
// sends of h it brackets in time, from the admission to its settlement.
// One whose settled count differs from the sends the history holds is a
// gap over the admission. One with no settlement is a gap from the
// admission: for a cell admission, to the until of the route's attempt
// in spans begun latest among those whose span holds it; otherwise, or
// with no such attempt, to the route's next admission; with neither,
// open. Both bounds take a route's processes to run one at a time.
func reconcile(h *report.History, entries []admission.Entry, members []string, spans []attemptSpan) {
	settles := map[int]*admission.Entry{}
	for i := range entries {
		if e := &entries[i]; e.Kind == admission.KindSettle {
			settles[e.Admit] = e
		}
	}
	admitted := make([]bool, len(h.Sends))
	for i := range entries {
		a := &entries[i]
		if a.Kind != admission.KindAdmit || !slices.Contains(members, a.Route) {
			continue
		}
		s := settles[a.Seq]
		recorded := 0
		for j := range h.Sends {
			x := &h.Sends[j]
			if x.Route != a.Route || sendPurpose(x.Kind) != a.Purpose || x.DispatchUTC.Before(a.Time) || s != nil && x.DispatchUTC.After(s.Time) {
				continue
			}
			admitted[j] = true
			recorded++
		}
		switch {
		case s == nil:
			g := report.HistoryGap{Route: a.Route, FromUTC: a.Time.UTC(),
				Reason: fmt.Sprintf("ledger line %d admits %d %s requests with no settlement", a.Seq, a.Reserve, a.Purpose)}
			if a.Purpose == admission.PurposeCell {
				var at *attemptSpan
				for k := range spans {
					if sp := &spans[k]; sp.route == a.Route && !sp.began.After(a.Time) && !sp.until.Before(a.Time) && (at == nil || sp.began.After(at.began)) {
						at = sp
					}
				}
				if at != nil {
					to := at.until
					g.ToUTC = &to
				}
			}
			if n := nextAdmit(entries, a); n != nil && g.ToUTC == nil {
				to := n.Time.UTC()
				g.ToUTC = &to
			}
			h.Gaps = append(h.Gaps, g)
		case s.Sent == nil || s.Sent.Probe+s.Sent.Development+s.Sent.Warmup+s.Sent.Measured != recorded:
			sent := 0
			if s.Sent != nil {
				sent = s.Sent.Probe + s.Sent.Development + s.Sent.Warmup + s.Sent.Measured
			}
			to := s.Time.UTC()
			h.Gaps = append(h.Gaps, report.HistoryGap{Route: a.Route, FromUTC: a.Time.UTC(), ToUTC: &to,
				Reason: fmt.Sprintf("ledger line %d settles %d %s requests, the records hold %d", s.Seq, sent, a.Purpose, recorded)})
		}
	}
	for _, ok := range admitted {
		if !ok {
			h.Unadmitted++
		}
	}
}

// nextAdmit is the first admission of a's route on a later ledger line
// timed no earlier than a, or nil.
func nextAdmit(entries []admission.Entry, a *admission.Entry) *admission.Entry {
	for i := range entries {
		if e := &entries[i]; e.Kind == admission.KindAdmit && e.Route == a.Route && e.Seq > a.Seq && !e.Time.Before(a.Time) {
			return e
		}
	}
	return nil
}

// groupPrices are the price revisions of each route of members at each
// pair of token bounds, in ledger order, each in effect from its line's
// time: every price line whose retrieval date is no earlier than that of
// any earlier line at the same bounds.
func groupPrices(entries []admission.Entry, members []string) []report.RoutePrice {
	type key struct {
		route   string
		in, out int
	}
	latest := map[key]string{}
	var out []report.RoutePrice
	for i := range entries {
		e := &entries[i]
		if e.Kind != admission.KindPrice || !slices.Contains(members, e.Route) {
			continue
		}
		k := key{e.Route, e.InputTokens, e.OutputTokens}
		if e.Retrieved < latest[k] {
			continue
		}
		latest[k] = e.Retrieved
		out = append(out, report.RoutePrice{Route: k.route, InputTokens: k.in, OutputTokens: k.out, PerRequestUSD: e.PerRequestUSD, RecordedUTC: e.Time.UTC()})
	}
	return out
}
