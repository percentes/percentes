package main

import (
	"slices"
	"testing"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/probe"
)

// A rehearsal summary with no cell of a route projects no disk for it and
// names the route missing, for every route of the rehearsal.
func TestRehearsalWindowDiskNamesUnsampledRoutes(t *testing.T) {
	h := &rehearsalRun{specs: rehearsalRoutes, streams: stepOneStreams(settingsAt(1)), in: rehearsalInput{out: t.TempDir(), frames: t.TempDir()}}
	var names []string
	for _, sp := range rehearsalRoutes {
		h.checks = append(h.checks, &routeCheck{choice: probe.CeilingChoice{Ceiling: sp.ceiling}})
		h.cfgs = append(h.cfgs, &config.Config{Release: &config.Release{Shape: config.Shape{Name: sp.shape, Fallback: sp.fallback}}})
		names = append(names, sp.name)
	}
	w, err := h.windowDisk(&rehearsal{})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(w.Missing, names) || len(w.Routes) != len(names) || w.Routes[0].OutputBytes != 0 {
		t.Fatalf("missing %q, %d routes, %+v", w.Missing, len(w.Routes), w.Routes)
	}
}
