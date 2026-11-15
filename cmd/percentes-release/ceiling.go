package main

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/evidence"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/pilot"
	"github.com/percentes/percentes/internal/probe"
	"github.com/percentes/percentes/internal/releasekey"
)

// ceilingCheckCmd sends the route's ceiling check (SPEC-HOSTED HA3 point
// 7 and HA4) at its configured ceiling: probe.CeilingRequests requests of
// its request shape's text, at least the interval of the lowest ladder
// step apart at the rate its limits admit (checkStepRPS), admitted in the
// release ledger at the shape's input bound and the highest ceiling it
// permits the route before the first is sent and settled with the
// requests sent, both in the file whose open line the command read
// first. The check fails on its pass rule, which accepts the length
// finish reason for Shape R only, on the input bound or on the output
// bound. It writes the record run reads with --ceiling-probe, also when
// the check fails or is blocked, and what the record withholds under
// --frames. On a route with a confidential limit the record's
// fingerprints of what it withholds, and the limits' retained copies'
// check, are keyed by the release key. The record names the ledger by
// its open-line SHA-256; run refuses it unless the schedule froze that
// ledger. A record reporting a model id on which run refuses the route
// (probe.ModelFault) is refused here too.
func ceilingCheckCmd(ctx context.Context, args []string) (int, error) {
	fs := flag.NewFlagSet("ceiling-check", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "the route's release configuration at the ceiling checked (required)")
	envFile := fs.String("env", "", "env file holding the target's api_key_env (required)")
	ledgerPath := fs.String("ledger", "", "the release admission ledger the schedule freezes, holding the route's price (required)")
	out := fs.String("out", "", "output directory; the check writes ceiling-<UTC time>/<route>-<ceiling>/"+probe.File+" under it (required)")
	byteCap := fs.Int("byte-cap", 0, "Shape S ticket byte cap (required)")
	snapshots := fs.String("snapshots", "", "directory of the retained limit copies the configuration names by file name")
	keyPath := fs.String("release-key", "", "the release key file key create wrote (required when the configuration records a confidential limit)")
	framesRoot := fs.String("frames", "", "raw-frame store directory outside the source tree; the check writes ceiling-<UTC time>/<route>-<ceiling>/"+probe.RetainedFile+
		" under it (required when the configuration sets target.retained_headers or target.probe.limits_retained, or records a confidential limit)")
	if err := fs.Parse(args); err != nil {
		return exitError, err
	}
	if *cfgPath == "" || *ledgerPath == "" || *out == "" {
		return exitError, errors.New("ceiling-check: --config, --ledger and --out are required")
	}
	if *byteCap <= 0 {
		return exitError, fmt.Errorf("ceiling-check: --byte-cap %d: the Shape S ticket byte cap is required", *byteCap)
	}
	cfgs, err := loadReleases([]string{*cfgPath})
	if err != nil {
		return exitError, err
	}
	cfg := cfgs[0]
	name := cfg.Target.Name
	for _, l := range cfg.Target.Limits {
		if *snapshots == "" && !strings.Contains(l.Source, "://") {
			return exitError, fmt.Errorf("ceiling-check: --snapshots is required: the %s limit's source %q is a file", l.Dimension, l.Source)
		}
	}
	if *framesRoot == "" && probe.Retains(&cfg.Target) {
		return exitError, errors.New("ceiling-check: --frames is required: the configuration withholds probe values")
	}
	if *framesRoot != "" {
		if err := evidence.CheckFramesRoot(*framesRoot); err != nil {
			return exitError, err
		}
	}
	var key *releasekey.Key
	if cfg.Target.Confidential() {
		if key, err = readReleaseKey(*keyPath); err != nil {
			return exitError, fmt.Errorf("ceiling-check: %w", err)
		}
	}
	shape, err := routeShape("ceiling-check", cfg, *byteCap)
	if err != nil {
		return exitError, err
	}
	ledger, err := admission.Open(*ledgerPath)
	if err != nil {
		return exitError, err
	}
	st, err := ledger.State()
	if err != nil {
		return exitError, err
	}
	ledger.Bind(st.OpenSHA256)
	if err := st.CheckRelease(cfg); err != nil {
		return exitError, err
	}
	entries, err := ledgerEntries(*ledgerPath, st.OpenSHA256)
	if err != nil {
		return exitError, err
	}
	all, err := ledgerChecks(*out, st.OpenSHA256)
	if err != nil {
		return exitError, err
	}
	prior := routeChecks(all, name)
	if e, ok := runAdmitted(entries); ok {
		return exitError, fmt.Errorf("ceiling-check: ledger line %d admits run's traffic of route %s: the schedule is frozen, and a ceiling check after it blocks the pages", e.Seq, e.Route)
	}
	if err := checkPriorRecords(name, entries, prior); err != nil {
		return exitError, err
	}
	for _, p := range prior {
		if err := p.rec.Verify(&cfg.Target, probe.LengthValue(p.rec.Adapter), cfg.Release.Shape.InputBoundTokens); err != nil {
			return exitError, fmt.Errorf("ceiling-check: %s: %w", p.path, frozenVerify(err))
		}
	}
	stepRPS, err := checkStepRPS(cfg, entries)
	if err != nil {
		return exitError, err
	}
	spent := probe.Spent(recordsOf(all))
	if err := releaseCapCheck(cfg, all, spent, *byteCap); err != nil {
		return exitError, err
	}
	if err := repeatCheck(cfg, all, spent, *byteCap, time.Now()); err != nil {
		return exitError, err
	}
	if err := loadKey(*envFile, &cfg.Target); err != nil {
		return exitError, err
	}

	path := ""
	rec, err := sendCeiling(ctx, ledger, cfg, shape, *snapshots, *byteCap, key, stepRPS, nil, func(r *probe.ReleaseRecord) error {
		p, err := writeCheck(*out, *framesRoot, r)
		path = p
		return err
	})
	if rec == nil {
		return exitError, err
	}

	written := path
	if written == "" {
		written = "nothing: the write failed"
	}
	c, ib, ob := rec.Ceiling, rec.InputBound, rec.OutputBound
	fmt.Printf("%s: Shape %s ceiling %d: %d of %d sent passed; input bound %d tokens: %d reported, largest %s, %d above; output bound %d tokens: %d reported, largest %s, %d above; %d findings; written %s\n",
		name, rec.Shape, c.Ceiling, c.Passed, c.Sent, ib.BoundTokens, ib.Reported, loadgen.TokenCountText(ib.MaxPromptTokens), ib.Exceeded,
		ob.CeilingTokens, ob.Reported, loadgen.TokenCountText(ob.MaxOutputTokens), ob.Exceeded, len(rec.Findings), written)
	code := checkOutcome(cfg, rec, prior, spent, *byteCap)
	if err != nil {
		if errors.Is(err, errRecordNotWritten) {
			err = errors.Join(err, printRecord(rec))
		}
		return exitError, err
	}
	return code, nil
}

// printRecord writes rec to standard error, synced where it is a file,
// and returns how --recover --record writes it, or why it is not there.
func printRecord(rec *probe.ReleaseRecord) error {
	b, err := probe.Encode(rec)
	if err == nil {
		_, err = os.Stderr.Write(b)
	}
	if fi, serr := os.Stderr.Stat(); err == nil && serr == nil && fi.Mode().IsRegular() {
		err = os.Stderr.Sync()
	}
	if err != nil {
		return fmt.Errorf("ceiling-check: the record is not on standard error either: %w", err)
	}
	return errors.New("ceiling-check: the record is on standard error above, and ceiling-check --recover --record <a file of it> writes it")
}

// frozenVerify is err, a ceiling-check record's Verify error under its
// route's configuration, with the fields that configuration must keep
// from the release's first ceiling check until the schedule where those
// fields decide err (probe.DocsError).
func frozenVerify(err error) error {
	if d := (*probe.DocsError)(nil); !errors.As(err, &d) {
		return err
	}
	return fmt.Errorf("%w; a record is verified under its route's configuration as it now stands, in which target.error_docs, target.billing_refusals and target.probe.account_limit_statuses are frozen from the release's first ceiling check until the schedule, and an edit to one since then fails each record whose results it changes", err)
}

// errRecordNotWritten marks writeCheck's error when the record's write
// failed.
var errRecordNotWritten = errors.New("the record is not written")

// checkOutcome prints the outcome line of rec, cfg's route's check at
// byteCap, after the route's records in prior and the spent byte caps,
// and returns ceiling-check's exit code.
func checkOutcome(cfg *config.Config, rec *probe.ReleaseRecord, prior map[int]priorCheck, spent map[int]*probe.ReleaseRecord, byteCap int) int {
	name := cfg.Target.Name
	c, ib, ob := rec.Ceiling, rec.InputBound, rec.OutputBound
	refusal := modelRefusal(&cfg.Target, rec)
	if refusal != "" {
		fmt.Printf("%s: NOT CHARACTERIZED: run refuses the route, and pages publishes it as excluded (SPEC-HOSTED §2): %s\n", name, refusal)
	}
	switch {
	case rec.Start.Blocked && strings.Contains(rec.Start.Reason, probe.BillingSetting):
		fmt.Printf("%s: BILLING: %s; the check may be sent again once the billing setting is fixed\n", name, rec.Start.Reason)
	case rec.Start.Blocked:
		fmt.Printf("%s: BLOCKED: %s; the check may be sent again once the fault is fixed\n", name, rec.Start.Reason)
	case rec.Outcome() == probe.CheckRefused:
		fmt.Printf("%s: REFUSED: %s\n", name, refusedText(cfg, rec, prior, spent, byteCap))
	case rec.Interrupted != "":
		fmt.Printf("%s: STOPPED: interrupted: %s; the check may be sent again\n", name, rec.Interrupted)
	case rec.Unreached != "":
		fmt.Printf("%s: STOPPED: %s; the check may be sent again\n", name, rec.Unreached)
	case !c.Pass:
		fmt.Printf("%s: FAILED the ceiling check at %d\n", name, c.Ceiling)
	case !ib.Pass:
		fmt.Printf("%s: FAILED the input bound of %d tokens\n", name, ib.BoundTokens)
	case !ob.Pass:
		fmt.Printf("%s: FAILED the output bound of %d tokens, reasoning included\n", name, ob.CeilingTokens)
	case refusal == "":
		return exitOK
	}
	return exitAdverse
}

// checkStepRPS is the lowest ladder step's rate at the rate cfg's ceiling
// check is paced from (admission.CheckRPS), at the route's newest price
// in entries.
func checkStepRPS(cfg *config.Config, entries []admission.Entry) (float64, error) {
	price, err := routePrice(entries, cfg.Target.Name)
	if err != nil {
		return 0, err
	}
	rps, err := admission.CheckRPS(cfg, price)
	if err != nil {
		return 0, err
	}
	if lowest := cfg.Release.Ladder.StepRates(rps)[0]; lowest > 0 {
		return lowest, nil
	}
	return 0, fmt.Errorf("ceiling-check: route %s: its limits admit %g rps, a lowest step of 0 rps", cfg.Target.Name, rps)
}

// sendCeiling admits cfg's ceiling check in ledger on its account group
// at its ceiling and byteCap (admission.Ledger.AdmitCeilingCheck), once
// its input passes probe.CheckReleaseInput and the retained copies in
// snapshots match their hashes (probe.CheckSnapshots), sends it with
// shape's texts cut at byteCap and the release key, its requests at
// least the interval of stepRPS apart, verifying the target's
// certificate against roots where set, records the open line ledger is
// bound to (admission.Ledger.Bind) and the admission's line, passes the
// record to write where set, and settles the requests sent. The ceiling
// is the texts' request-reference seed, so the checks at the two
// ceilings send different texts. It holds the route's lock on the ledger
// (admission.Ledger.LockRoute) throughout. It admits nothing once ctx has
// ended. Where write or the settlement fails it returns the record with
// write's error and the settlement's state; a failed write still settles
// the admission.
func sendCeiling(ctx context.Context, ledger *admission.Ledger, cfg *config.Config, shape pilot.Shape, snapshots string, byteCap int, key *releasekey.Key, stepRPS float64, roots *x509.CertPool, write func(*probe.ReleaseRecord) error) (*probe.ReleaseRecord, error) {
	owner, err := ledger.LockRoute(cfg.Target.Name)
	if err != nil {
		return nil, err
	}
	defer owner.Close()
	texts := make([]string, probe.CeilingRequests)
	for i := range texts {
		texts[i] = shape.UserText(int64(cfg.Release.Shape.CeilingTokens), i)
	}
	in := probe.ReleaseInput{Config: cfg, Texts: texts, SnapshotDir: snapshots, ByteCap: byteCap, Key: key, LowestStepRPS: stepRPS, RootCAs: roots}
	if err := probe.CheckReleaseInput(in); err != nil {
		return nil, err
	}
	if in.Snapshots, err = probe.CheckSnapshots(in); err != nil {
		return nil, fmt.Errorf("ceiling-check: %w", err)
	}
	if ctx.Err() != nil {
		return nil, fmt.Errorf("ceiling-check: route %s: nothing admitted or sent: %s", cfg.Target.Name, probe.CauseText(context.Cause(ctx)))
	}
	a, err := ledger.AdmitCeilingCheck(cfg.Target.Name, cfg.Release.AccountGroup, probe.CeilingRequests, admission.ReleaseProbeTokens(cfg),
		cfg.Release.Shape.CeilingTokens, byteCap)
	if err != nil {
		return nil, err
	}
	rec, err := probe.RunRelease(ctx, in)
	sent := 0
	var werr error
	if rec != nil {
		sent, rec.LedgerSHA256, rec.LedgerSeq = rec.Sent(), ledger.Bound(), a.Seq
		if write != nil {
			werr = write(rec)
		}
	}
	if serr := ledger.Settle(a, admission.Counts{Probe: sent}); serr != nil {
		if werr != nil {
			werr = fmt.Errorf("ceiling-check: %w", werr)
		}
		return rec, errors.Join(err, werr, fmt.Errorf("ceiling-check: ledger line %d is not settled: %w", a.Seq, serr))
	}
	if werr != nil {
		err = errors.Join(err, fmt.Errorf("ceiling-check: %w; ledger line %d is settled at %d requests sent", werr, a.Seq, sent))
	}
	return rec, err
}

// errWithheldNotWritten marks writeCheck's error when the record is
// written and what it withholds is not.
var errWithheldNotWritten = errors.New("the values it withholds are not written")

// writeCheck writes rec under out, and what it withholds under
// framesRoot where set, at ceiling-<UTC time>/<route>-<ceiling>/, the
// time to the nanosecond where that directory exists, and returns the
// record's path, "" with errRecordNotWritten where the record's write
// failed, or the path with errWithheldNotWritten where only the write
// under framesRoot did.
func writeCheck(out, framesRoot string, rec *probe.ReleaseRecord) (string, error) {
	now, name := time.Now().UTC(), fmt.Sprintf("%s-%d", rec.Route, rec.Ceiling.Ceiling)
	rel := filepath.Join("ceiling-"+now.Format("20060102T150405Z"), name)
	if _, err := os.Stat(filepath.Join(out, rel)); err == nil {
		rel = filepath.Join("ceiling-"+now.Format("20060102T150405.000000000Z"), name)
	}
	path, err := probe.Write(filepath.Join(out, rel), rec)
	if err != nil {
		return "", fmt.Errorf("%w (%w)", errRecordNotWritten, err)
	}
	if framesRoot != "" {
		held, err := probe.WriteRetained(filepath.Join(framesRoot, rel), rec)
		if err != nil {
			return path, fmt.Errorf("the record is written at %s; %w under --frames: %w", path, errWithheldNotWritten, err)
		}
		if held != "" {
			fmt.Printf("%s: withheld values written %s\n", rec.Route, held)
		}
	}
	return path, nil
}

// priorCheck is a ceiling-check record ceiling-check wrote, with its
// path.
type priorCheck struct {
	path string
	rec  *probe.ReleaseRecord
}

// routeChecks are the records of all of route.
func routeChecks(all map[int]priorCheck, route string) map[int]priorCheck {
	m := map[int]priorCheck{}
	for seq, p := range all {
		if p.rec.Route == route {
			m[seq] = p
		}
	}
	return m
}

// recordsOf are the records of all.
func recordsOf(all map[int]priorCheck) []*probe.ReleaseRecord {
	var out []*probe.ReleaseRecord
	for _, p := range all {
		out = append(out, p.rec)
	}
	return out
}

// runAdmitted is the first of entries that admits run's traffic: a
// cell, window or replacement admission.
func runAdmitted(entries []admission.Entry) (admission.Entry, bool) {
	if i := slices.IndexFunc(entries, runAdmission); i >= 0 {
		return entries[i], true
	}
	return admission.Entry{}, false
}

// ledgerChecks are the records of every route's ceiling checks admitted
// in the ledger whose open-line SHA-256 is open that ceiling-check wrote
// under out, by their admission's ledger line. Two records of one line
// are refused.
func ledgerChecks(out, open string) (map[int]priorCheck, error) {
	paths, err := filepath.Glob(filepath.Join(out, "ceiling-*", "*", probe.File))
	if err != nil {
		return nil, err
	}
	m := map[int]priorCheck{}
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("ceiling-check: %w", err)
		}
		var rec probe.ReleaseRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return nil, fmt.Errorf("ceiling-check: %s: %w", p, err)
		}
		if rec.LedgerSHA256 != open || rec.LedgerSeq == 0 {
			continue
		}
		if o, ok := m[rec.LedgerSeq]; ok {
			return nil, fmt.Errorf("ceiling-check: %s and %s both record the check admitted at ledger line %d", o.path, p, rec.LedgerSeq)
		}
		m[rec.LedgerSeq] = priorCheck{p, &rec}
	}
	return m, nil
}

// probeAdmissions are the ceiling-check admissions among entries of a
// route in routes, or of every route where routes is nil.
func probeAdmissions(entries []admission.Entry, routes []string) []admission.Entry {
	var out []admission.Entry
	for _, e := range entries {
		if e.Kind == admission.KindAdmit && e.Purpose == admission.PurposeProbe && (routes == nil || slices.Contains(routes, e.Route)) {
			out = append(out, e)
		}
	}
	return out
}

// checkPriorRecords refuses a ceiling check of route while one of its
// ceiling-check admissions among entries has no record in prior.
func checkPriorRecords(route string, entries []admission.Entry, prior map[int]priorCheck) error {
	for _, e := range probeAdmissions(entries, []string{route}) {
		if _, ok := prior[e.Seq]; !ok {
			return fmt.Errorf("ceiling-check: ledger line %d admits a ceiling check of route %s and no record of it is under --out; where its process ended, ceiling-check --recover writes it, and where it printed its record, --recover --record <a file of it>", e.Seq, route)
		}
	}
	return nil
}

// releaseCapCheck refuses a Shape S check of cfg's route at byteCap while
// a Shape S record in all, the records of every route, that was not
// stopped is at another byte cap no check spent (spent): the record
// decides at its cap, and the check is sent there.
func releaseCapCheck(cfg *config.Config, all map[int]priorCheck, spent map[int]*probe.ReleaseRecord, byteCap int) error {
	if cfg.Release.Shape.Name != config.ShapeS {
		return nil
	}
	var seqs []int
	for seq := range all {
		seqs = append(seqs, seq)
	}
	slices.Sort(seqs)
	for _, seq := range seqs {
		r := all[seq].rec
		if r.Shape != config.ShapeS || r.ByteCap == byteCap || spent[r.ByteCap] != nil || r.Outcome() == probe.CheckStopped {
			continue
		}
		return fmt.Errorf("ceiling-check: route %s: --byte-cap %d: the check of route %s at ledger line %d decides at byte cap %d; send the check with --byte-cap %d",
			cfg.Target.Name, byteCap, r.Route, r.LedgerSeq, r.ByteCap, r.ByteCap)
	}
	return nil
}

// repeatCheck refuses a check of cfg's route at its ceiling and byteCap
// at now, after the records of every route in all: at a Shape S byte cap
// at or above one a check spent (spent, probe.Spent), at a ceiling after
// the shape's first unless the ceiling rule over the host group's records
// moves the group there (fallbackCheck), or where one of the route's
// checks there decides (probe.Decisive): one that was not stopped and not
// refused, or a repeat of a refused one. A Shape S check at a spent cap
// above byteCap decides nothing. A refused check is repeated once, and no
// check follows a refused one at the ceiling before probe.RepeatQuiet
// after its end.
func repeatCheck(cfg *config.Config, all map[int]priorCheck, spent map[int]*probe.ReleaseRecord, byteCap int, now time.Time) error {
	name, ceiling := cfg.Target.Name, cfg.Release.Shape.CeilingTokens
	prior := routeChecks(all, name)
	if cfg.Release.Shape.Name == config.ShapeS {
		if c, r := leastSpent(spent); r != nil && byteCap >= c {
			return fmt.Errorf("ceiling-check: route %s: --byte-cap %d: the check of route %s at ledger line %d spent byte cap %d, a prompt above the input bound; a check is sent below %d",
				name, byteCap, r.Route, r.LedgerSeq, c, c)
		}
	}
	if err := fallbackCheck(cfg, all, spent, byteCap); err != nil {
		return err
	}
	var at, refused []*probe.ReleaseRecord
	for _, p := range prior {
		r := p.rec
		if r.Ceiling.Ceiling != ceiling || r.Outcome() == probe.CheckStopped {
			continue
		}
		if r.Outcome() == probe.CheckRefused {
			refused = append(refused, r)
		}
		if !supersededAt(r, spent, byteCap) {
			at = append(at, r)
		}
	}
	slices.SortFunc(at, func(a, b *probe.ReleaseRecord) int { return a.LedgerSeq - b.LedgerSeq })
	if len(at) > 1 || len(at) == 1 && at[0].Outcome() != probe.CheckRefused {
		last := at[len(at)-1]
		return fmt.Errorf("ceiling-check: route %s: the check at %d admitted at ledger line %d decides (%s); it is not repeated", name, last.Ceiling.Ceiling, last.LedgerSeq, last.Outcome())
	}
	quiet := probe.RepeatQuiet(&cfg.Target)
	for _, r := range refused {
		if from := r.EndUTC.Add(quiet); now.Before(from) {
			return fmt.Errorf("ceiling-check: route %s: the check refused at %d (ledger line %d) may be repeated from %s, %s after its end",
				name, ceiling, r.LedgerSeq, wholeSecond(from), quiet)
		}
	}
	return nil
}

// fallbackCheck refuses a check of cfg's route at a ceiling after its
// shape's first (config.Shape.Ceilings) unless the route has a deciding
// record in all at each earlier ceiling (probe.Decisive), neither stopped
// nor superseded at byteCap (supersededAt), and the ceiling rule over
// its host group's records at those ceilings (probe.ChooseCeilingAt)
// moves the group to the route's ceiling with the route not left out.
func fallbackCheck(cfg *config.Config, all map[int]priorCheck, spent map[int]*probe.ReleaseRecord, byteCap int) error {
	name, ceiling, ceilings := cfg.Target.Name, cfg.Release.Shape.CeilingTokens, cfg.Release.Shape.Ceilings()
	i := slices.Index(ceilings, ceiling)
	if i <= 0 {
		return nil
	}
	at := ""
	if cfg.Release.Shape.Name == config.ShapeS {
		at = fmt.Sprintf(" at byte cap %d", byteCap)
	}
	hosts, earlier := hostGroup(cfg), ceilings[:i]
	var recs []*probe.ReleaseRecord
	for _, p := range all {
		if r := p.rec; slices.Contains(hosts, r.Route) && slices.Contains(earlier, r.Ceiling.Ceiling) {
			recs = append(recs, r)
		}
	}
	prev := earlier[len(earlier)-1]
	for _, c := range earlier {
		var own []*probe.ReleaseRecord
		for _, r := range recs {
			if r.Route == name && r.Ceiling.Ceiling == c && r.Outcome() != probe.CheckStopped && !supersededAt(r, spent, byteCap) {
				own = append(own, r)
			}
		}
		if d, err := probe.Decisive(own); err != nil || d == nil {
			return fmt.Errorf("ceiling-check: route %s: the check at %d follows the one at %d, and the route has no check at %d that decides%s",
				name, ceiling, prev, c, at)
		}
	}
	choice, err := probe.ChooseCeilingAt(&cfg.Release.Shape, hosts, append(recs, passing(cfg, hosts, recs, byteCap)...), supersededCaps(spent, byteCap))
	switch {
	case err != nil:
		return fmt.Errorf("ceiling-check: route %s: the check at %d follows the one at %d, and the host group's checks there do not decide%s: %v",
			name, ceiling, prev, at, err)
	case slices.ContainsFunc(choice.LeftOut, func(l probe.LeftOut) bool { return l.Route == name }):
		return fmt.Errorf("ceiling-check: route %s: the check at %d follows the one at %d, and the route is left out on the reported model id: %s",
			name, ceiling, prev, choice.Reason)
	case choice.Excluded:
		return fmt.Errorf("ceiling-check: route %s: the check at %d follows the one at %d, and the host group's checks there exclude it%s: %s",
			name, ceiling, prev, at, choice.Reason)
	case choice.Ceiling != ceiling:
		return fmt.Errorf("ceiling-check: route %s: the check at %d follows the one at %d, and no host of its host group failed the check that decides at %d%s",
			name, ceiling, prev, prev, at)
	}
	return nil
}

// passing are records of cfg's shape at its ceiling and byteCap, one per
// host of hosts, that pass the check and both bounds, each expecting the
// model the host's records in recs expect: over them and recs,
// probe.ChooseCeilingAt chooses cfg's ceiling where recs move the group
// there.
func passing(cfg *config.Config, hosts []string, recs []*probe.ReleaseRecord, byteCap int) []*probe.ReleaseRecord {
	ceiling := cfg.Release.Shape.CeilingTokens
	var out []*probe.ReleaseRecord
	for _, h := range hosts {
		r := &probe.ReleaseRecord{Route: h, Shape: cfg.Release.Shape.Name, ByteCap: byteCap,
			Ceiling:     probe.CeilingCheck{Ceiling: ceiling, Sent: probe.CeilingRequests, Passed: probe.CeilingRequests, Pass: true},
			InputBound:  probe.InputCheck{Reported: probe.CeilingRequests, Pass: true},
			OutputBound: probe.OutputCheck{CeilingTokens: ceiling, Reported: probe.CeilingRequests, Pass: true}}
		if i := slices.IndexFunc(recs, func(x *probe.ReleaseRecord) bool { return x.Route == h }); i >= 0 {
			r.RequestedModel, r.ExpectedModel = recs[i].RequestedModel, recs[i].ExpectedModel
		}
		out = append(out, r)
	}
	return out
}

// supersededCaps are the byte caps in spent above byteCap (supersededAt).
func supersededCaps(spent map[int]*probe.ReleaseRecord, byteCap int) []int {
	var out []int
	for c := range spent {
		if c > byteCap {
			out = append(out, c)
		}
	}
	return out
}

// wholeSecond prints t in RFC 3339 UTC, rounded up to a whole second.
func wholeSecond(t time.Time) string {
	up := t.Truncate(time.Second)
	if up.Before(t) {
		up = up.Add(time.Second)
	}
	return up.UTC().Format(time.RFC3339)
}

// supersededAt reports whether r is a Shape S record at a byte cap spent
// (probe.Spent) above byteCap, which decides nothing.
func supersededAt(r *probe.ReleaseRecord, spent map[int]*probe.ReleaseRecord, byteCap int) bool {
	return r.Shape == config.ShapeS && spent[r.ByteCap] != nil && r.ByteCap > byteCap
}

// leastSpent is the least byte cap in spent, with the record that spent
// it, or nil.
func leastSpent(spent map[int]*probe.ReleaseRecord) (int, *probe.ReleaseRecord) {
	least := 0
	for c := range spent {
		if least == 0 || c < least {
			least = c
		}
	}
	return least, spent[least]
}

// refusedText is the line ceiling-check prints for rec, a refused check
// of cfg's route at byteCap, after the route's records in prior and the
// spent byte caps.
func refusedText(cfg *config.Config, rec *probe.ReleaseRecord, prior map[int]priorCheck, spent map[int]*probe.ReleaseRecord, byteCap int) string {
	why, c := rec.RefusedBy(), rec.Ceiling.Ceiling
	head := refusalHead(why)
	for _, p := range prior {
		r := p.rec
		if r.Ceiling.Ceiling == c && r.Outcome() == probe.CheckRefused && !supersededAt(r, spent, byteCap) {
			return fmt.Sprintf("%s, no documented billing refusal; this check repeats the one refused at ledger line %d and decides: the host group is excluded, ceiling check inconclusive, %s at %s (SPEC-HOSTED HA4, refused checks)",
				head, r.LedgerSeq, why, probe.Number(c))
		}
	}
	return fmt.Sprintf("%s, no documented billing refusal; the check may be repeated once at %s from %s; the last check at that ceiling decides, and a refused one excludes the host group",
		head, probe.Number(c), wholeSecond(rec.EndUTC.Add(probe.RepeatQuiet(&cfg.Target))))
}

// refusalHead says what the request a check is refused by
// (probe.ReleaseRecord.RefusedBy) met.
func refusalHead(why string) string {
	switch {
	case why == "no response":
		return "a request got no response"
	case why == probe.RefusedStreamCut:
		return "a request answered 200 had its stream cut before its terminator"
	case why == probe.RefusedEndedEarly:
		return "a request answered 200 had its stream end before its terminator with no read error"
	case strings.HasSuffix(why, " in-band"):
		return "a request answered 200 carried an in-band error its provider documents as " + strings.TrimSuffix(why, " in-band")
	}
	return "a request was answered " + why
}

// routeShape is the frozen request set of cfg's route (pilot.NewShape):
// Shape S cut at byteCap, which it requires, or a shape with no ticket,
// which takes none.
func routeShape(cmd string, cfg *config.Config, byteCap int) (pilot.Shape, error) {
	name := cfg.Release.Shape.Name
	switch {
	case name == config.ShapeS && byteCap <= 0:
		return nil, fmt.Errorf("%s: --byte-cap %d: the Shape S ticket byte cap is required", cmd, byteCap)
	case name != config.ShapeS && byteCap != 0:
		return nil, fmt.Errorf("%s: --byte-cap %d: Shape %s has no ticket byte cap; leave it out", cmd, byteCap, name)
	}
	return pilot.NewShape(name, byteCap)
}

// modelRefusal is the fault in the model ids rec reports on which run
// refuses t's route (probe.ModelFault), or "". A record reporting no id
// is left to run, which reads it beside the route's other records.
func modelRefusal(t *config.Target, rec *probe.ReleaseRecord) string {
	if len(rec.ReportedModels()) == 0 {
		return ""
	}
	return probe.ModelFault(t, []*probe.ReleaseRecord{rec})
}
