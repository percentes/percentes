package main

import (
	"github.com/percentes/percentes/internal/admission"
)

// runAdmission reports whether e admits run's traffic: a cell, window or
// replacement admission.
func runAdmission(e admission.Entry) bool {
	return e.Kind == admission.KindAdmit && (e.Purpose == admission.PurposeCell || e.Purpose == admission.PurposeWindow || e.Purpose == admission.PurposeReplacement)
}
