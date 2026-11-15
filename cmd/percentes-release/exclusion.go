package main

import (
	"fmt"
	"slices"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/probe"
)

// probeCopyName is the name a cell carries rec's copy under.
func probeCopyName(rec *probe.ReleaseRecord) string {
	return fmt.Sprintf("ceiling-probe-%s-%d-%d.json", rec.Route, rec.Ceiling.Ceiling, rec.LedgerSeq)
}

// choiceFiles are the ceiling-check records among paths that cfg's
// host group's choice reads: its hosts', and for Shape S other routes'
// that spend a byte cap (probe.Spent); own counts its hosts'.
func choiceFiles(cfg *config.Config, paths []string) ([]string, int, error) {
	hosts := hostGroup(cfg)
	var files []string
	own := 0
	for _, p := range paths {
		rec, err := readCheckRecord(p)
		switch {
		case err != nil:
			return nil, 0, err
		case slices.Contains(hosts, rec.Route):
			files = append(files, p)
			own++
		case cfg.Release.Shape.Name == config.ShapeS && len(probe.Spent([]*probe.ReleaseRecord{rec})) > 0:
			files = append(files, p)
		}
	}
	return files, own, nil
}

// runAdmission reports whether e admits run's traffic: a cell, window or
// replacement admission.
func runAdmission(e admission.Entry) bool {
	return e.Kind == admission.KindAdmit && (e.Purpose == admission.PurposeCell || e.Purpose == admission.PurposeWindow || e.Purpose == admission.PurposeReplacement)
}

// releaseByteCap is the Shape S ticket byte cap the records at paths
// decide at: the least byte cap of a Shape S record that was not
// stopped, or where every one was, of any Shape S record; 0 where there
// is none.
func releaseByteCap(paths []string) (int, error) {
	decided, all := 0, 0
	least := func(c, byteCap int) int {
		if c == 0 || byteCap < c {
			return byteCap
		}
		return c
	}
	for _, p := range paths {
		rec, err := readCheckRecord(p)
		if err != nil {
			return 0, err
		}
		if rec.Shape != config.ShapeS {
			continue
		}
		if all = least(all, rec.ByteCap); rec.Outcome() != probe.CheckStopped {
			decided = least(decided, rec.ByteCap)
		}
	}
	if decided == 0 {
		return all, nil
	}
	return decided, nil
}
