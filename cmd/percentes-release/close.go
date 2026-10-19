package main

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/report"
)

// limitsStop applies the ceiling rule again with the limits the refresh
// command read at a cell's start in place of the recorded ones, an
// unavailable limit keeping its recorded value, and at the route's
// newest price in the ledger as the cell starts (routePrice), and
// returns the route stop when it no longer admits a step the route runs:
// the route keeps its steps (SPEC-HOSTED §4) and stops instead. The stop
// names the lowest such step, the price's retrieval date and the binding
// limit.
func (r *route) limitsStop(read []evidence.RefreshedLimit) (*routeStop, error) {
	c := *r.rule.cfg
	c.Target.Limits = slices.Clone(c.Target.Limits)
	for i, l := range read {
		if l.Unavailable == "" {
			c.Target.Limits[i].Value = l.Value
		}
	}
	entries, err := ledgerEntries(r.ledgerPath, r.ledger.Bound())
	if err != nil {
		return nil, err
	}
	price, err := routePrice(entries, c.Target.Name)
	if err != nil {
		return nil, err
	}
	v, err := admission.CeilingRule(&c, price, r.rule.plan)
	if err != nil {
		return nil, err
	}
	for k, s := range r.verdict.Steps {
		if !s.Admitted || v.Steps[k].Admitted {
			continue
		}
		return &routeStop{fmt.Sprintf("the limits read at the cell's start, at the price retrieved %s, no longer admit step %d (%g rps) of the %d the route runs: %s; the route keeps its steps (SPEC-HOSTED §4)",
			price.Retrieved, k+1, s.RPS, r.steps, bindingText(r.rule.cfg.Target.Limits, v.Steps[k].Binding, read))}, nil
	}
	return nil, nil
}

// bindingText names b, a binding demand against target.limits, with the
// recorded and the read value of a public limit, and the source and hash
// of a confidential one.
func bindingText(limits []config.Limit, b *admission.Demand, read []evidence.RefreshedLimit) string {
	bound := "half of it"
	if !b.Half {
		bound = "it"
	}
	demand := strconv.FormatFloat(b.Demand, 'f', -1, 64)
	if b.Limit < 0 || b.Limit >= len(limits) {
		return fmt.Sprintf("%s: nominal demand %s above %s", b.Dimension, demand, bound)
	}
	l := &limits[b.Limit]
	if l.Marking == config.MarkingConfidential {
		return fmt.Sprintf("%s confidential (%s, sha256 %s): nominal demand %s above %s", l.Dimension, l.Source, l.SHA256, demand, bound)
	}
	return fmt.Sprintf("%s recorded %s, read %s: nominal demand %s above %s", l.Dimension,
		strconv.FormatFloat(l.Value, 'f', -1, 64), strconv.FormatFloat(read[b.Limit].Value, 'f', -1, 64), demand, bound)
}

// windowClose writes the route's window-close record once its last cell
// is done (SPEC-HOSTED §4 drift inside the window), unless one exists:
// the refresh command runs once more and the operator file and every
// record are read again as at a cell's start (cellOperator), the copies
// are kept under the frame store, and the record is compared with the
// last counted cell's manifest. A refresh fault writes no record and is
// returned, so the process's next run tries again.
func (r *route) windowClose(ctx context.Context, st *routeState) error {
	path := filepath.Join(r.dir, evidence.WindowCloseFile)
	if fileExists(path) {
		return nil
	}
	t := &r.cfg.Target
	refresh := evidence.ReadStatus(ctx, r.refresh)
	op, oerr := r.cellOperator(refresh)
	read, lerr := evidence.ReadRefreshedLimits(refresh, t)
	if oerr == nil {
		oerr = lerr
	}
	if oerr != nil {
		return fmt.Errorf("%s: window close: the records were not refreshed (SPEC-HOSTED §4): %w", t.Name, oerr)
	}
	w := evidence.NewWindowClose(t.Name, r.now(), evidence.PublishedRefresh(refresh, t, read), op)
	cells, err := r.countedDocs(st)
	if err != nil {
		return err
	}
	c, err := report.CompareWindowClose(t.Name, cells, w)
	if err != nil {
		return err
	}
	held := evidence.WindowCloseDir(r.framesRoot, t.Name, w.RecordedUTC)
	if w.Copies, err = filepath.Rel(r.framesRoot, held); err != nil {
		return err
	}
	if err := op.WriteCopies(held); err != nil {
		return err
	}
	if t.Confidential() {
		if err := evidence.WriteRetainedRefresh(held, refresh); err != nil {
			return err
		}
	}
	if err := replaceJSON(path, w); err != nil {
		return err
	}
	at := c.RecordedUTC.Format(time.RFC3339)
	switch {
	case c.Cell == "":
		fmt.Printf("%s: window close recorded %s; no counted cell to compare\n", t.Name, at)
	case len(c.Removed) == 0 && len(c.Added) == 0:
		fmt.Printf("%s: window close recorded %s: limits and documentation as at %s\n", t.Name, at, c.Cell)
	default:
		fmt.Printf("%s: window close recorded %s: limits or documentation changed since %s: removed %v; added %v\n", t.Name, at, c.Cell, c.Removed, c.Added)
	}
	return nil
}

// countedDocs are the documentation states the manifests of the route's
// counted cells record, with each cell's scheduled start.
func (r *route) countedDocs(st *routeState) ([]report.CellRecords, error) {
	var cells []report.CellRecords
	for _, rec := range st.records {
		if rec.Outcome != outcomeCounted {
			continue
		}
		p := filepath.Join(r.cellDir(rec.Run), evidence.ManifestFile)
		if !fileExists(p) {
			continue
		}
		m, err := evidence.ReadManifest(p)
		if err != nil {
			return nil, err
		}
		cells = append(cells, report.CellRecords{Cell: cellLabel(rec.Run), Start: rec.Run.Start, Docs: report.DocsOf(m)})
	}
	return cells, nil
}
