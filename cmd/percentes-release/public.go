package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/percentes/percentes/internal/config"
)

// publicConfigCmd writes a route's configuration as published
// (config.Config.Published): the file the freeze commits and every cell
// manifest names, with "[retained]" in place of each confidential limit's
// value.
func publicConfigCmd(args []string) (int, error) {
	fs := flag.NewFlagSet("public-config", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "the route's release configuration as loaded (required)")
	out := fs.String("out", "", "the file to write the configuration as published to, other than --config (required)")
	if err := fs.Parse(args); err != nil {
		return exitError, err
	}
	if *cfgPath == "" || *out == "" {
		return exitError, errors.New("public-config: --config and --out are required")
	}
	cfgs, err := loadReleases([]string{*cfgPath})
	if err != nil {
		return exitError, err
	}
	cfg := cfgs[0]
	in, err := os.Stat(*cfgPath)
	if err != nil {
		return exitError, err
	}
	if o, err := os.Stat(*out); err == nil && os.SameFile(in, o) || filepath.Clean(*out) == filepath.Clean(*cfgPath) {
		return exitError, errors.New("public-config: --out is the --config file")
	}
	b, err := cfg.Published()
	if err != nil {
		return exitError, err
	}
	if err := replaceFile(*out, b); err != nil {
		return exitError, err
	}
	n := 0
	for _, l := range cfg.Target.Limits {
		if l.Marking == config.MarkingConfidential {
			n++
		}
	}
	fmt.Printf("%s: confidential limits withheld: %d; written %s\n", cfg.Target.Name, n, *out)
	return exitOK, nil
}
