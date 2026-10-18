// percentes-release runs one release route's cells unattended over the
// committed window (SPEC-HOSTED HA3). At each scheduled start, at the
// route's offset inside the slot, it records the clock offset and the
// endpoint status, admits the cell in the ledger, sends a one-request
// probe and runs the ladder up to the route's top admitted step with the
// loopback canary, then writes the cell's evidence with a labelled
// manifest and report pair, also for a cell stopped or blocked before its
// ladder. A cell that does not count is re-run on its attempt 2 date
// within the route's two re-runs.
// Each route of a release runs as its own process on one client host, all
// writing under one --out directory. The key comes from the env file
// named with --env, never from the inherited environment, and is never
// printed.
//
// Before the window, schedule writes the committed schedule of the routes
// whose configurations it is given, ledger create starts the release
// ledger with their caps, ledger price records a route's prices at its
// shape, and ceiling-check sends a route's ceiling check, admitted in the
// ledger, and writes the record run reads with --ceiling-probe. After
// the window, pages writes the index page and each route's page from the
// cells' records.
//
//	percentes-release schedule --master-seed SEED --first DATE --last DATE
//	    --config FILE ... --schedule FILE
//	percentes-release ledger create --ledger FILE --config FILE ...
//	percentes-release ledger price --ledger FILE --config FILE --input USD
//	    --output USD --source URL --retrieved DATE
//	percentes-release ceiling-check --config FILE --env FILE --ledger FILE
//	    --out DIR --byte-cap N [--snapshots DIR]
//	percentes-release run --config FILE --env FILE --ledger FILE
//	    --schedule FILE --out DIR --frames DIR --clock-sync CMD
//	    --status-cmd CMD --operator FILE --ceiling-probe FILE ...
//	    --byte-cap N --account-created DATE
//	percentes-release dispatch --out DIR
//	percentes-release pages --out DIR --frames DIR --config FILE ...
//	    --pages DIR
//	percentes-release check-cpu
//
// Exit codes: 0 = the route's runs are over and every cell counts, or
// the ceiling check passed with the input bound; 2 = the runs are over
// and a cell does not count, or the ceiling check failed or was blocked;
// 3 = the route stopped (a ledger refusal, a documented billing refusal,
// a clock offset above the pinned bound, or an operator abort); 1 =
// execution error or refusal to start.
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
  schedule        write the committed schedule of the routes
  ledger create   start the release ledger with the routes' caps
  ledger price    record a route's prices at its shape
  ceiling-check   send a route's ceiling check and write its record
  run             run one route's cells over the committed window
  dispatch        sum every cell's dispatch series under --out
  pages           write the index page and each route's page
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
	case "ledger":
		code, err = ledgerCmd(args[1:])
	case "ceiling-check":
		code, err = ceilingCheckCmd(ctx, args[1:])
	case "run":
		code, err = runCmd(ctx, args[1:])
	case "dispatch":
		code, err = dispatchCmd(args[1:])
	case "pages":
		code, err = pagesCmd(args[1:])
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
