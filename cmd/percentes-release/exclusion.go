package main

import (
	"fmt"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/probe"
)

// probeCopyName is the name a cell carries rec's copy under.
func probeCopyName(rec *probe.ReleaseRecord) string {
	return fmt.Sprintf("ceiling-probe-%s-%d-%d.json", rec.Route, rec.Ceiling.Ceiling, rec.LedgerSeq)
}

// runAdmission reports whether e admits run's traffic: a cell, window or
// replacement admission.
func runAdmission(e admission.Entry) bool {
	return e.Kind == admission.KindAdmit && (e.Purpose == admission.PurposeCell || e.Purpose == admission.PurposeWindow || e.Purpose == admission.PurposeReplacement)
}
