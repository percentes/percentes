package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/percentes/percentes/internal/releasekey"
)

// keyCmd creates the release key (releasekey.Create), or prints each
// file's HMAC-SHA256 under it, the value a confidential limit's
// hmac_sha256 records for its retained copy. Nothing it prints carries
// the key.
func keyCmd(args []string) (int, error) {
	if len(args) == 0 {
		return exitError, errors.New("key: create or sum")
	}
	fs := flag.NewFlagSet("key "+args[0], flag.ContinueOnError)
	path := fs.String("key", "", "the release key file (required)")
	if err := fs.Parse(args[1:]); err != nil {
		return exitError, err
	}
	if *path == "" {
		return exitError, errors.New("key: --key is required")
	}
	switch args[0] {
	case "create":
		if fs.NArg() > 0 {
			return exitError, fmt.Errorf("key create: unexpected arguments %q", fs.Args())
		}
		if err := releasekey.Create(*path); err != nil {
			return exitError, err
		}
		fmt.Printf("release key written: %s, mode 0600\n", *path)
		return exitOK, nil
	case "sum":
		if fs.NArg() == 0 {
			return exitError, errors.New("key sum: name the files to hash")
		}
		k, err := releasekey.Read(*path)
		if err != nil {
			return exitError, err
		}
		for _, f := range fs.Args() {
			b, err := os.ReadFile(f)
			if err != nil {
				return exitError, err
			}
			fmt.Printf("%s  %s\n", k.Sum(b), f)
		}
		return exitOK, nil
	}
	return exitError, fmt.Errorf("key: unknown action %q", args[0])
}

// readReleaseKey reads the --release-key file.
func readReleaseKey(path string) (*releasekey.Key, error) {
	if path == "" {
		return nil, errors.New("--release-key is required: the file key create wrote")
	}
	return releasekey.Read(path)
}
