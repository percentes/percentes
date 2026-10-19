// percentes-release runs one release route's cells unattended over the
// committed window (SPEC-HOSTED HA3). At each scheduled start, at the
// route's offset inside the slot, it records the clock offset and the
// endpoint status, runs the route's refresh command and reads the
// operator file, its records and the limits the command prints again,
// records the account balance or quota, stops the route where those
// limits no longer admit its steps at its newest recorded price, blocks
// the cell where the limits and
// model documentation were not refreshed or the balance check does not
// let it start, admits the
// cell in the ledger, sends a one-request probe and runs the ladder up
// to the route's top admitted step with the loopback canary, then writes
// the cell's evidence with a labelled manifest and report pair, also for
// a cell stopped or blocked before its ladder. A missed cell, and one
// whose process ended or failed during it, gets its labelled pair from
// what its directory holds when the process next reads the route's
// records. A cell in which a response reported a model identifier other
// than the expected one is labelled model_mismatch. A cell that does not
// count is re-run on its attempt 2 date within the route's two re-runs.
// After the last cell the refresh command runs once more and the records
// read after it are the route's window-close record.
// Each route of a release runs as its own process on one client host, all
// writing under one --out directory. The key comes from the env file
// named with --env, never from the inherited environment, and is never
// printed.
//
// Before the window, key create writes the release key, under which
// every published fingerprint of a value a confidential route withholds
// is an HMAC-SHA256, and key sum prints a retained copy's; ledger create
// starts the release ledger with the routes' caps, schedule writes the
// committed schedule of the routes whose configurations it is given,
// with each configuration's SHA-256 as published, its HMAC-SHA256 as
// loaded under the release key and its host group, and the ledger's
// open-line SHA-256, which a route's run must match, ledger price
// records a route's prices at the token bounds of its cells and of its
// ceiling check, ceiling-check sends a route's ceiling check, admitted in the ledger,
// and writes the record run reads with --ceiling-probe, and
// public-config writes a route's configuration as published, the file
// the freeze commits, with
// each confidential limit's value withheld. Every command reads the
// configuration as loaded. After
// the window, pages writes the index page and each route's page from the
// cells' records, which must carry the SHA-256 of the schedule file
// given and, across the hosts of a model, one ceiling choice and one set
// of probe records, pack writes a route's evidence pack, its page files
// and cell manifests with an inventory of their SHA-256 hashes, and
// publish-check checks the release publication record against the
// cells, the pages and the files it names, and the publication page it
// gives against the one under --pages.
//
// Before the freeze, rehearse runs the acceptance run: one cell of each
// of six mock routes, served on loopback by this process in the
// release's request forms and credential headers, concurrently at their
// pinned offsets up the whole ladder, and writes the cells' evidence and
// a summary that says whether the rehearsal is green.
//
//	percentes-release key create --key FILE
//	percentes-release key sum --key FILE FILE ...
//	percentes-release ledger create --ledger FILE --config FILE ...
//	percentes-release schedule --master-seed SEED --first DATE --last DATE
//	    --config FILE ... --ledger FILE --release-key FILE --schedule FILE
//	percentes-release ledger price --ledger FILE --config FILE --input USD
//	    --output USD --source URL --retrieved DATE
//	percentes-release ceiling-check --config FILE --env FILE --ledger FILE
//	    --out DIR --byte-cap N [--frames DIR] [--snapshots DIR]
//	    [--release-key FILE]
//	percentes-release public-config --config FILE --out FILE
//	percentes-release run --config FILE --env FILE --ledger FILE
//	    --release-key FILE --schedule FILE --out DIR --frames DIR
//	    --clock-sync CMD
//	    --status-cmd CMD --balance-cmd CMD --refresh-cmd CMD
//	    --operator FILE [--host-config FILE ...] --ceiling-probe FILE ...
//	    --byte-cap N --account-created DATE
//	percentes-release dispatch --out DIR
//	percentes-release pages --out DIR --frames DIR --config FILE ...
//	    --schedule FILE --pages DIR [--release-key FILE]
//	percentes-release pack --out DIR --frames DIR --config FILE ...
//	    --schedule FILE --route NAME --pack FILE [--release-key FILE]
//	percentes-release publish-check --out DIR --frames DIR
//	    --config FILE ... --schedule FILE --pages DIR --record FILE
//	    [--write-page] [--release-key FILE]
//	percentes-release rehearse --out DIR --frames DIR --clock-sync CMD
//	    --byte-cap N [--scale N] [--master-seed SEED] [--source-diff FILE]
//	percentes-release check-cpu
//
// Exit codes: 0 = the route's runs are over and every cell counts, or
// the ceiling check passed with the input bound and run accepts the
// model ids it reported; 2 = the runs are over and a cell does not
// count, or the ceiling check failed, was blocked or reported a model id
// on which run refuses the route;
// 3 = the route stopped (a ledger refusal, a documented billing refusal,
// a clock offset above the pinned bound, limits read at a cell's start
// that no longer admit the route's steps at its newest recorded price, a
// probe 401 or 403 the route's
// previous probed cell also saw, or an operator abort); 1 = execution
// error or refusal to start. rehearse exits 0 when the rehearsal is
// green and 2 when it is not; publish-check exits 0 when the publication
// record is complete and the publication page under --pages is the one
// it gives, and 2 when either is not.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
)

// Exit codes.
const (
	exitOK      = 0
	exitError   = 1
	exitAdverse = 2
	exitStopped = 3
)

const usage = `usage: percentes-release <command> [flags]

commands:
  key create      write the release key
  key sum         print each file's HMAC-SHA256 under the release key
  ledger create   start the release ledger with the routes' caps
  schedule        write the committed schedule of the routes
  ledger price    record a route's prices for its cells and ceiling check
  ceiling-check   send a route's ceiling check and write its record
  public-config   write a route's configuration as published
  run             run one route's cells over the committed window
  dispatch        sum every cell's dispatch series under --out
  pages           write the index page and each route's page
  pack            write a route's evidence pack
  publish-check   check the release publication record
  rehearse        run six mock routes' cells up the ladder at their offsets
  check-cpu       whether this build measures host CPU

Run percentes-release <command> -h for the command's flags.
`

func main() {
	os.Exit(dispatch(os.Args[1:]))
}

func dispatch(args []string) int {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return exitError
	}
	ctx, stop := operatorContext()
	defer stop()
	var (
		code int
		err  error
	)
	switch args[0] {
	case "schedule":
		code, err = scheduleCmd(args[1:])
	case "key":
		code, err = keyCmd(args[1:])
	case "ledger":
		code, err = ledgerCmd(args[1:])
	case "ceiling-check":
		code, err = ceilingCheckCmd(ctx, args[1:])
	case "public-config":
		code, err = publicConfigCmd(args[1:])
	case "run":
		code, err = runCmd(ctx, args[1:])
	case "dispatch":
		code, err = dispatchCmd(args[1:])
	case "pages":
		code, err = pagesCmd(args[1:])
	case "pack":
		code, err = packCmd(args[1:])
	case "publish-check":
		code, err = publishCheckCmd(args[1:])
	case "rehearse":
		code, err = rehearseCmd(ctx, args[1:])
	case "check-cpu":
		code, err = checkCPUCmd(args[1:])
	default:
		fmt.Fprint(os.Stderr, usage)
		return exitError
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "percentes-release: %v\n", err)
		return exitError
	}
	return code
}

// operatorContext is cancelled with loadgen.ErrOperatorAbort on SIGINT or
// SIGTERM, so a cell it stops records an operator abort.
func operatorContext() (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(context.Background())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		select {
		case <-sig:
			cancel(loadgen.ErrOperatorAbort)
		case <-done:
		}
	}()
	return ctx, func() {
		signal.Stop(sig)
		close(done)
		cancel(nil)
	}
}

// listFlag collects a repeated flag's values.
type listFlag []string

func (l *listFlag) String() string { return strings.Join(*l, ",") }

func (l *listFlag) Set(s string) error {
	*l = append(*l, s)
	return nil
}

// loadReleases loads each configuration and refuses one outside the
// release profile.
func loadReleases(paths []string) ([]*config.Config, error) {
	if len(paths) == 0 {
		return nil, errors.New("--config is required")
	}
	var out []*config.Config
	for _, p := range paths {
		cfg, err := config.LoadFile(p)
		if err != nil {
			return nil, err
		}
		if cfg.Profile != config.ProfileRelease || cfg.Release == nil {
			return nil, fmt.Errorf("%s: profile %q, want %q", p, cfg.Profile, config.ProfileRelease)
		}
		out = append(out, cfg)
	}
	return out, nil
}

// replaceJSON writes v, indented, to path through a temporary file, so a
// reader never sees a partial file.
func replaceJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return replaceFile(path, append(b, '\n'))
}

// replaceFile writes b to path through a temporary file, so a reader
// never sees a partial file.
func replaceFile(path string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// checkCPUCmd samples host CPU through the loadgen monitor and prints
// whether a cell's client-validity gate could measure it.
func checkCPUCmd(args []string) (int, error) {
	if len(args) > 0 {
		return exitError, fmt.Errorf("check-cpu takes no arguments")
	}
	ok := loadgen.CPUMeasured(config.PinnedCPUWindowS)
	fmt.Printf("cpu_measured: %t\n", ok)
	if !ok {
		return exitAdverse, nil
	}
	return exitOK, nil
}
