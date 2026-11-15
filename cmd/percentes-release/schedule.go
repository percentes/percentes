package main

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/releasekey"
	"github.com/percentes/percentes/internal/schedule"
)

// scheduleCmd writes the committed schedule (SPEC-HOSTED HA3 point 9) of
// the routes whose configurations are given, named by their targets,
// whose host groups agree and whose account groups' route caps sum to at
// most each group's recorded spend limit, with the SHA-256 of each
// configuration as published, its HMAC-SHA256 as loaded under the release
// key, its host group, account group and frozen binding rate
// (freezeBinding), and the open-line SHA-256 of the release ledger, which
// must cap each route as its configuration does, with its line count and
// the SHA-256 of its bytes; run requires them. Every ceiling-check
// admission on the ledger must have its record among --ceiling-probe,
// each written by ceiling-check on that ledger (scheduleChecks), and each
// route's host group's records must exclude the group or run it at the
// route's ceiling (scheduleChoices), and the routes they exclude must be
// the ones --excluded names (checkExcluded). It never replaces an
// existing file.
func scheduleCmd(args []string) (int, error) {
	fs := flag.NewFlagSet("schedule", flag.ContinueOnError)
	seed := fs.String("master-seed", "", "the release's master seed (required)")
	first := fs.String("first", "", "the window's first UTC date, 2006-01-02 (required)")
	last := fs.String("last", "", "the window's last UTC date, 2006-01-02 (required)")
	var cfgPaths listFlag
	fs.Var(&cfgPaths, "config", "a route's release configuration (repeatable, required)")
	ledgerPath := fs.String("ledger", "", "the release admission ledger the routes' runs admit in (required)")
	keyPath := fs.String("release-key", "", "the release key file key create wrote (required)")
	path := fs.String("schedule", "", "the schedule file to write; it must not exist (required)")
	var checkPaths listFlag
	fs.Var(&checkPaths, "ceiling-probe", "the record of a ceiling check admitted on the ledger (repeatable; one for every check)")
	var excluded listFlag
	fs.Var(&excluded, "excluded", "a route the ceiling-check records exclude (repeatable; one for each)")
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
	key, err := readReleaseKey(*keyPath)
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
	in := schedule.Input{MasterSeed: *seed, First: *first, Last: *last, LedgerSHA256: st.OpenSHA256, LedgerLines: st.Lines, LedgerHeadSHA256: st.HeadSHA256}
	for _, cfg := range cfgs {
		frozen, err := frozenConfig(cfg, key)
		if err != nil {
			return exitError, err
		}
		in.Routes = append(in.Routes, cfg.Target.Name)
		in.Configs = append(in.Configs, frozen)
	}
	s, err := schedule.New(in)
	if err != nil {
		return exitError, err
	}
	entries, read, err := readLedger(*ledgerPath, st.OpenSHA256)
	if err != nil {
		return exitError, err
	}
	frozen, err := frozenEntries(entries, read.HeadSHA256, in)
	if err != nil {
		return exitError, fmt.Errorf("schedule: %w", err)
	}
	if err := scheduleChecks(checkPaths, frozen, st.OpenSHA256); err != nil {
		return exitError, err
	}
	if s, err = freezeBinding(s, cfgs, frozen); err != nil {
		return exitError, err
	}
	if err := checkHostGroups(cfgs); err != nil {
		return exitError, err
	}
	if err := checkAccountGroups(cfgs); err != nil {
		return exitError, err
	}
	reasons, err := scheduleChoices(cfgs, checkPaths, frozen, st.OpenSHA256)
	if err != nil {
		return exitError, err
	}
	if err := checkExcluded(cfgs, reasons, excluded); err != nil {
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
	fmt.Printf("schedule written: %s; window %s to %s; counter %d; ledger %s at line %d\n", *path, s.Input.First, s.Input.Last, s.Counter, s.Input.LedgerSHA256, s.Input.LedgerLines)
	for _, o := range s.Offsets {
		runs, err := s.Runs(o.Route)
		if err != nil {
			return exitError, err
		}
		if reason, ok := reasons[o.Route]; ok {
			fmt.Printf("%s: excluded by the ceiling check: %s; offset %d min; first cell %s at %s\n", o.Route, reason, o.Minutes,
				runs[0].Label, runs[0].Start.Format("2006-01-02 15:04 UTC"))
			continue
		}
		fc, _ := s.Config(o.Route)
		fmt.Printf("%s: offset %d min; binding rate %g rps, binding limit %s; first cell %s at %s\n", o.Route, o.Minutes, fc.BindingRPS, bindingName(fc, cfgs),
			runs[0].Label, runs[0].Start.Format("2006-01-02 15:04 UTC"))
	}
	return exitOK, nil
}

// checkExcluded refuses unless the routes of cfgs the ceiling-check
// records exclude, the keys of reasons, are the routes given, naming
// each one that differs.
func checkExcluded(cfgs []*config.Config, reasons map[string]string, given []string) error {
	var errs []error
	for _, cfg := range cfgs {
		name := cfg.Target.Name
		switch reason, ok := reasons[name]; {
		case ok && !slices.Contains(given, name):
			errs = append(errs, fmt.Errorf("schedule: route %s: the ceiling check excludes the host group (%s); --excluded does not name it", name, reason))
		case !ok && slices.Contains(given, name):
			errs = append(errs, fmt.Errorf("schedule: route %s: --excluded names it; the ceiling check runs it at %d", name, cfg.Release.Shape.CeilingTokens))
		}
	}
	for _, g := range given {
		if !slices.ContainsFunc(cfgs, func(c *config.Config) bool { return c.Target.Name == g }) {
			errs = append(errs, fmt.Errorf("schedule: --excluded %s names no route given", g))
		}
	}
	return errors.Join(errs...)
}

// bindingName is c's binding limit with its evaluation window, if any,
// and the pinned hard timeout where a public limit of route c's config
// in cfgs, of that dimension and window, counts output at generation.
func bindingName(c schedule.RouteConfig, cfgs []*config.Config) string {
	if c.BindingWindowS <= 0 {
		return c.BindingLimit
	}
	name := fmt.Sprintf("%s over %g s", c.BindingLimit, c.BindingWindowS)
	for _, cfg := range cfgs {
		if cfg.Target.Name != c.Route {
			continue
		}
		for _, l := range cfg.Target.Limits {
			if l.Dimension == c.BindingLimit && l.Marking == config.MarkingPublic && l.OutputAtGeneration && slices.Contains(l.WindowsS(), c.BindingWindowS) {
				return name + fmt.Sprintf(" plus the %d s hard timeout", config.PinnedReleaseHardTimeoutS)
			}
		}
	}
	return name
}

// freezeBinding records in s's input each route's binding rate, binding
// limit and its window (SPEC-HOSTED HA4) as the binding-rate rule gives
// them (bindingSolution) under s at the newest price in entries, the
// ledger's lines up to the position s freezes, and returns the schedule
// that input derives. It refuses a rate whose lowest step rounds down to
// 0 requests per second.
func freezeBinding(s *schedule.Schedule, cfgs []*config.Config, entries []admission.Entry) (*schedule.Schedule, error) {
	in := s.Input
	in.Configs = slices.Clone(in.Configs)
	for _, cfg := range cfgs {
		name := cfg.Target.Name
		i := slices.IndexFunc(in.Configs, func(c schedule.RouteConfig) bool { return c.Route == name })
		if i < 0 {
			return nil, fmt.Errorf("schedule: no configuration of route %s frozen", name)
		}
		sol, err := bindingSolution(cfg, s, entries)
		if err != nil {
			return nil, err
		}
		if rates := cfg.Release.Ladder.StepRates(sol.BindingRPS); slices.Min(rates) <= 0 {
			return nil, fmt.Errorf("schedule: route %s: binding rate %g rps, binding limit %s, puts a step at %v rps", name, sol.BindingRPS, sol.Binding.Dimension, rates)
		}
		c := &in.Configs[i]
		c.BindingRPS, c.BindingLimit, c.BindingWindowS = sol.BindingRPS, sol.Binding.Dimension, sol.Binding.WindowS
	}
	return schedule.New(in)
}

// hostGroup is the routes of cfg's host group, every route of cfg's
// model with its request shape: its target and its release.other_hosts,
// sorted.
func hostGroup(cfg *config.Config) []string {
	g := []string{cfg.Target.Name}
	for _, h := range cfg.Release.OtherHosts {
		g = append(g, h.Route)
	}
	slices.Sort(g)
	return g
}

// frozenConfig is cfg's record in the schedule: the SHA-256 of its file
// as published (config.Config.Published), the HMAC-SHA256 of its file as
// loaded under key, its host group and its account group.
func frozenConfig(cfg *config.Config, key *releasekey.Key) (schedule.RouteConfig, error) {
	published, err := cfg.Published()
	if err != nil {
		return schedule.RouteConfig{}, err
	}
	sum := sha256.Sum256(published)
	return schedule.RouteConfig{Route: cfg.Target.Name, ConfigSHA256: hex.EncodeToString(sum[:]), ConfigHMACSHA256: key.Sum(cfg.Raw), HostGroup: hostGroup(cfg), AccountGroup: cfg.Release.AccountGroup}, nil
}

// checkFrozen refuses cfg unless the schedule froze its route's
// configuration as cfg's file as published, cfg's file as loaded under
// key and cfg's host group.
func checkFrozen(cfg *config.Config, sched *schedule.Schedule, key *releasekey.Key) error {
	name := cfg.Target.Name
	want, ok := sched.Config(name)
	if !ok {
		return fmt.Errorf("run: the schedule freezes no configuration of route %s", name)
	}
	got, err := frozenConfig(cfg, key)
	if err != nil {
		return err
	}
	if !slices.Equal(got.HostGroup, want.HostGroup) {
		return fmt.Errorf("run: route %s: host group %v; the schedule froze %v", name, got.HostGroup, want.HostGroup)
	}
	if got.ConfigSHA256 != want.ConfigSHA256 {
		return fmt.Errorf("run: route %s: configuration sha256 %s; the schedule froze %s", name, got.ConfigSHA256, want.ConfigSHA256)
	}
	switch {
	case want.ConfigHMACSHA256 == "":
		return fmt.Errorf("run: route %s: the schedule freezes no hmac_sha256 of the configuration as loaded", name)
	case got.ConfigHMACSHA256 != want.ConfigHMACSHA256:
		return fmt.Errorf("run: route %s: the configuration as loaded, or the release key, is not the one the schedule froze: hmac_sha256 %s; the schedule froze %s", name, got.ConfigHMACSHA256, want.ConfigHMACSHA256)
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
// at the model their configurations send, of its request shape and
// ceilings (sameShape), and to name the same hosts.
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
			case !sameShape(c, o):
				return fmt.Errorf("schedule: route %s: other host %s sends Shape %s at ceilings %v, the route Shape %s at %v; a host group has one request shape",
					c.Target.Name, h.Route, o.Release.Shape.Name, o.Release.Shape.Ceilings(), c.Release.Shape.Name, c.Release.Shape.Ceilings())
			case !slices.Equal(hostGroup(c), hostGroup(o)):
				return fmt.Errorf("schedule: routes %s and %s name different hosts of their host group: %v and %v", c.Target.Name, h.Route, hostGroup(c), hostGroup(o))
			}
		}
	}
	return nil
}

// sameShape reports whether a and b send one request shape at the same
// permitted ceilings.
func sameShape(a, b *config.Config) bool {
	return a.Release.Shape.Name == b.Release.Shape.Name && slices.Equal(a.Release.Shape.Ceilings(), b.Release.Shape.Ceilings())
}

// checkAccountGroups requires the routes of each account group to record
// the same spend_per_month_usd limits and, where they record one, their
// release.caps.route_usd to sum to at most the least of them
// (SPEC-HOSTED HA4). A confidential limit's value is not printed.
func checkAccountGroups(cfgs []*config.Config) error {
	groups := map[string][]*config.Config{}
	var names []string
	for _, c := range cfgs {
		g := c.Release.AccountGroup
		if groups[g] == nil {
			names = append(names, g)
		}
		groups[g] = append(groups[g], c)
	}
	slices.Sort(names)
	for _, g := range names {
		routes := groups[g]
		limits := spendLimits(routes[0])
		sum := new(big.Rat)
		var caps []string
		for _, c := range routes {
			if got := spendLimits(c); !slices.EqualFunc(got, limits, func(a, b config.Limit) bool { return a.Value == b.Value }) {
				return fmt.Errorf("schedule: account group %s: routes %s and %s record different spend_per_month_usd limits", g, routes[0].Target.Name, c.Target.Name)
			}
			r, ok := new(big.Rat).SetString(c.Release.Caps.RouteUSD)
			if !ok {
				return fmt.Errorf("schedule: route %s: release.caps.route_usd %q is not a decimal number", c.Target.Name, c.Release.Caps.RouteUSD)
			}
			sum.Add(sum, r)
			caps = append(caps, fmt.Sprintf("%s US$%s", c.Target.Name, c.Release.Caps.RouteUSD))
		}
		if len(limits) == 0 {
			continue
		}
		l := limits[0]
		if v, ok := new(big.Rat).SetString(strconv.FormatFloat(l.Value, 'f', -1, 64)); !ok || sum.Cmp(v) > 0 {
			value := "its value withheld"
			if l.Marking == config.MarkingPublic {
				value = "US$" + strconv.FormatFloat(l.Value, 'f', -1, 64)
			}
			return fmt.Errorf("schedule: account group %s: route caps %s sum to US$%s, above the group's spend_per_month_usd limit (%s, source %s)",
				g, strings.Join(caps, ", "), strings.TrimSuffix(strings.TrimRight(sum.FloatString(6), "0"), "."), value, l.Source)
		}
	}
	return nil
}

// spendLimits are cfg's spend_per_month_usd limits, least value first.
func spendLimits(cfg *config.Config) []config.Limit {
	var out []config.Limit
	for _, l := range cfg.Target.Limits {
		if l.Dimension == config.LimitSpendMonth {
			out = append(out, l)
		}
	}
	slices.SortFunc(out, func(a, b config.Limit) int { return cmp.Compare(a.Value, b.Value) })
	return out
}

// scheduleChecks refuses the ceiling-check records at paths unless each
// is the record ceiling-check writes, sent on the ledger of open-line
// SHA-256 open, and they hold one record of every ceiling-check
// admission among entries (checkAdmitted).
func scheduleChecks(paths []string, entries []admission.Entry, open string) error {
	var recs []*probe.ReleaseRecord
	for _, p := range paths {
		rec, err := readCheckRecord(p)
		if err != nil {
			return fmt.Errorf("schedule: %w", err)
		}
		if rec.LedgerSHA256 != open {
			return fmt.Errorf("schedule: --ceiling-probe %s: admitted in the ledger of open-line sha256 %q, not %s", p, rec.LedgerSHA256, open)
		}
		recs = append(recs, rec)
	}
	if err := checkAdmitted(entries, nil, recs); err != nil {
		return fmt.Errorf("schedule: %w", err)
	}
	return nil
}

// scheduleChoices refuses a route of cfgs unless the ceiling-check
// records at paths that its host group's choice reads (choiceFiles), at
// the byte cap the records decide at (releaseByteCap), against the
// targets of cfgs and entries, the ledger lines the schedule freezes, in
// the ledger of open-line SHA-256 open, exclude the group or run it at
// the route's ceiling (ceilingChoiceFor, which excludes a route on a
// model fault in its own records). A route with no record of its host
// group is refused. It returns the reason of each route excluded, by
// route.
func scheduleChoices(cfgs []*config.Config, paths []string, entries []admission.Entry, open string) (map[string]string, error) {
	byteCap, err := releaseByteCap(paths)
	if err != nil {
		return nil, fmt.Errorf("schedule: %w", err)
	}
	reasons := map[string]string{}
	for _, cfg := range cfgs {
		name, hosts := cfg.Target.Name, hostGroup(cfg)
		files, own, err := choiceFiles(cfg, paths)
		if err != nil {
			return nil, fmt.Errorf("schedule: %w", err)
		}
		if own == 0 {
			return nil, fmt.Errorf("schedule: route %s: no --ceiling-probe record of its host group (%s)", name, strings.Join(hosts, ", "))
		}
		targets := map[string]*config.Target{}
		for _, c := range cfgs {
			if c.Target.Name != name && slices.Contains(hosts, c.Target.Name) {
				targets[c.Target.Name] = &c.Target
			}
		}
		at := byteCap
		if cfg.Release.Shape.Name != config.ShapeS {
			at = 0
		}
		choice, _, _, err := ceilingChoiceFor(cfg, targets, files, at, open, entries, true)
		if err != nil {
			return nil, fmt.Errorf("schedule: route %s: %w", name, err)
		}
		if c := cfg.Release.Shape.CeilingTokens; !choice.Excluded && choice.Ceiling != c {
			return nil, fmt.Errorf("schedule: route %s: the ceiling check chose %d (%s); the configuration runs at %d", name, choice.Ceiling, choice.Reason, c)
		}
		if choice.Excluded {
			reasons[name] = choice.Reason
		}
	}
	return reasons, nil
}
