package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/percentes/percentes/internal/config"
)

// envName is an environment variable name.
var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// readEnvFile reads NAME=value lines, optionally prefixed "export ",
// from path. Blank lines and lines starting with # are skipped; a value
// wrapped in one pair of matching quotes loses them; nothing is expanded.
// The file must be readable by its owner only. Errors name the file and
// line, never a line's content.
func readEnvFile(path string) (map[string]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("env file: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("env file %s: mode %#o lets other users read it; chmod 600 it", path, info.Mode().Perm())
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("env file: %w", err)
	}
	vars := map[string]string{}
	sc := bufio.NewScanner(bytes.NewReader(b))
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		name, value, ok := strings.Cut(line, "=")
		name = strings.TrimSpace(name)
		if !ok || !envName.MatchString(name) {
			return nil, fmt.Errorf("env file %s line %d: not NAME=value", path, n)
		}
		value = strings.TrimSpace(value)
		if len(value) >= 2 && (value[0] == '"' || value[0] == '\'') && value[len(value)-1] == value[0] {
			value = value[1 : len(value)-1]
		}
		if _, dup := vars[name]; dup {
			return nil, fmt.Errorf("env file %s line %d: %s set twice", path, n, name)
		}
		vars[name] = value
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("env file %s: %w", path, err)
	}
	return vars, nil
}

// loadKeys sets each target's api_key_env from the env files, and refuses
// a variable that is missing, empty or set by two files.
func loadKeys(files []string, targets []*config.Target) error {
	if len(files) == 0 {
		return errors.New("--env is required: the file holding each target's api_key_env")
	}
	from := map[string]string{}
	vars := map[string]string{}
	for _, f := range files {
		got, err := readEnvFile(f)
		if err != nil {
			return err
		}
		for name, value := range got {
			if prev, dup := from[name]; dup {
				return fmt.Errorf("env files %s and %s both set %s", prev, f, name)
			}
			from[name], vars[name] = f, value
		}
	}
	for _, t := range targets {
		v, ok := vars[t.APIKeyEnv]
		if !ok || v == "" {
			return fmt.Errorf("target %s: no env file sets %s", t.Name, t.APIKeyEnv)
		}
		if err := os.Setenv(t.APIKeyEnv, v); err != nil {
			return fmt.Errorf("target %s: %w", t.Name, err)
		}
	}
	return nil
}
