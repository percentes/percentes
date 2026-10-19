package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"slices"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/schedule"
)

// scheduleCmd writes the committed schedule (SPEC-HOSTED HA3 point 9) of
// the routes whose configurations are given, named by their targets,
// whose host groups agree, with each configuration's SHA-256 and host
// group and the open-line SHA-256 of the release ledger, which must cap
// each route as its configuration does; run requires them. It never
// replaces an existing file.
func scheduleCmd(args []string) (int, error) {
	fs := flag.NewFlagSet("schedule", flag.ContinueOnError)
	seed := fs.String("master-seed", "", "the release's master seed (required)")
	first := fs.String("first", "", "the window's first UTC date, 2006-01-02 (required)")
	last := fs.String("last", "", "the window's last UTC date, 2006-01-02 (required)")
	var cfgPaths listFlag
	fs.Var(&cfgPaths, "config", "a route's release configuration (repeatable, required)")
	ledgerPath := fs.String("ledger", "", "the release admission ledger the routes' runs admit in (required)")
	path := fs.String("schedule", "", "the schedule file to write; it must not exist (required)")
	if err := fs.Parse(args); err != nil {
		return exitError, err
	}
	if *path == "" || *ledgerPath == "" {
		return exitError, errors.New("schedule: --ledger and --schedule are required")
	}
	cfgs, err := loadReleases(cfgPaths)
	if err != nil {
		return exitError, err
	}
	l, err := admission.Open(*ledgerPath)
	if err != nil {
		return exitError, err
	}
	st, err := l.State()
	if err != nil {
		return exitError, err
	}
	for _, cfg := range cfgs {
		if err := st.CheckRelease(cfg); err != nil {
			return exitError, err
		}
	}
	in := schedule.Input{MasterSeed: *seed, First: *first, Last: *last, LedgerSHA256: st.OpenSHA256}
	for _, cfg := range cfgs {
		in.Routes = append(in.Routes, cfg.Target.Name)
		in.Configs = append(in.Configs, frozenConfig(cfg))
	}
	s, err := schedule.New(in)
	if err != nil {
		return exitError, err
	}
	if err := checkHostGroups(cfgs); err != nil {
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
	fmt.Printf("schedule written: %s; window %s to %s; counter %d; ledger %s\n", *path, s.Input.First, s.Input.Last, s.Counter, s.Input.LedgerSHA256)
	for _, o := range s.Offsets {
		runs, err := s.Runs(o.Route)
		if err != nil {
			return exitError, err
		}
		fmt.Printf("%s: offset %d min; first cell %s at %s\n", o.Route, o.Minutes, runs[0].Label, runs[0].Start.Format("2006-01-02 15:04 UTC"))
	}
	return exitOK, nil
}

// hostGroup is the routes of cfg's model: its target and its
// release.other_hosts, sorted.
func hostGroup(cfg *config.Config) []string {
	g := []string{cfg.Target.Name}
	for _, h := range cfg.Release.OtherHosts {
		g = append(g, h.Route)
	}
	slices.Sort(g)
	return g
}

// frozenConfig is cfg's record in the schedule: the SHA-256 of its file
// and its host group.
func frozenConfig(cfg *config.Config) schedule.RouteConfig {
	sum := sha256.Sum256(cfg.Raw)
	return schedule.RouteConfig{Route: cfg.Target.Name, ConfigSHA256: hex.EncodeToString(sum[:]), HostGroup: hostGroup(cfg)}
}

// checkFrozen refuses cfg unless the schedule froze its route's
// configuration as cfg's file and host group.
func checkFrozen(cfg *config.Config, sched *schedule.Schedule) error {
	name := cfg.Target.Name
	want, ok := sched.Config(name)
	if !ok {
		return fmt.Errorf("run: the schedule freezes no configuration of route %s", name)
	}
	got := frozenConfig(cfg)
	if !slices.Equal(got.HostGroup, want.HostGroup) {
		return fmt.Errorf("run: route %s: host group %v; the schedule froze %v", name, got.HostGroup, want.HostGroup)
	}
	if got.ConfigSHA256 != want.ConfigSHA256 {
		return fmt.Errorf("run: route %s: configuration sha256 %s; the schedule froze %s", name, got.ConfigSHA256, want.ConfigSHA256)
	}
	return nil
}

// checkLedger refuses the ledger st unless the schedule froze it.
func checkLedger(st *admission.State, sched *schedule.Schedule) error {
	want := sched.Input.LedgerSHA256
	if want == "" {
		return errors.New("run: the schedule freezes no ledger")
	}
	if st.OpenSHA256 != want {
		return fmt.Errorf("run: ledger open line sha256 %s; the schedule froze %s", st.OpenSHA256, want)
	}
	return nil
}

// checkHostGroups requires each route's other hosts to be routes given,
// at the model their configurations send, and to name the same hosts.
func checkHostGroups(cfgs []*config.Config) error {
	byName := map[string]*config.Config{}
	for _, c := range cfgs {
		byName[c.Target.Name] = c
	}
	for _, c := range cfgs {
		for _, h := range c.Release.OtherHosts {
			o := byName[h.Route]
			switch {
			case o == nil:
				return fmt.Errorf("schedule: route %s: other host %s has no configuration given", c.Target.Name, h.Route)
			case o.Target.ModelName != h.Model:
				return fmt.Errorf("schedule: route %s: other host %s at model %q; its configuration sends %q", c.Target.Name, h.Route, h.Model, o.Target.ModelName)
			case !slices.Equal(hostGroup(c), hostGroup(o)):
				return fmt.Errorf("schedule: routes %s and %s name different hosts of their model: %v and %v", c.Target.Name, h.Route, hostGroup(c), hostGroup(o))
			}
		}
	}
	return nil
}
