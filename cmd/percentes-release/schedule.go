package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/percentes/percentes/internal/schedule"
)

// scheduleCmd writes the committed schedule (SPEC-HOSTED HA3 point 9) of
// the routes whose configurations are given, named by their targets. It
// never replaces an existing file.
func scheduleCmd(args []string) (int, error) {
	fs := flag.NewFlagSet("schedule", flag.ContinueOnError)
	seed := fs.String("master-seed", "", "the release's master seed (required)")
	first := fs.String("first", "", "the window's first UTC date, 2006-01-02 (required)")
	last := fs.String("last", "", "the window's last UTC date, 2006-01-02 (required)")
	var cfgPaths listFlag
	fs.Var(&cfgPaths, "config", "a route's release configuration (repeatable, required)")
	path := fs.String("schedule", "", "the schedule file to write; it must not exist (required)")
	if err := fs.Parse(args); err != nil {
		return exitError, err
	}
	if *path == "" {
		return exitError, errors.New("schedule: --schedule is required")
	}
	cfgs, err := loadReleases(cfgPaths)
	if err != nil {
		return exitError, err
	}
	in := schedule.Input{MasterSeed: *seed, First: *first, Last: *last}
	for _, cfg := range cfgs {
		in.Routes = append(in.Routes, cfg.Target.Name)
	}
	s, err := schedule.New(in)
	if err != nil {
		return exitError, err
	}
	b, err := s.JSON()
	if err != nil {
		return exitError, err
	}
	f, err := os.OpenFile(*path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return exitError, fmt.Errorf("schedule: %w", err)
	}
	_, err = f.Write(b)
	if err = errors.Join(err, f.Close()); err != nil {
		os.Remove(*path)
		return exitError, fmt.Errorf("schedule: %w", err)
	}
	fmt.Printf("schedule written: %s; window %s to %s; counter %d\n", *path, s.Input.First, s.Input.Last, s.Counter)
	for _, o := range s.Offsets {
		runs, err := s.Runs(o.Route)
		if err != nil {
			return exitError, err
		}
		fmt.Printf("%s: offset %d min; first cell %s at %s\n", o.Route, o.Minutes, runs[0].Label, runs[0].Start.Format("2006-01-02 15:04 UTC"))
	}
	return exitOK, nil
}
