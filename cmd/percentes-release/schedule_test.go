package main

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/percentes/percentes/internal/schedule"
)

// schedule writes the schedule its inputs derive for the configurations'
// routes, which run loads, and never replaces a file.
func TestScheduleCmd(t *testing.T) {
	groq := routeConfigFile(t, "groq", "https://api.example.test/openai/v1/chat/completions", "10")
	baseten := routeConfigFile(t, "baseten", "https://api.example.test/v1/chat/completions", "10")
	path := filepath.Join(t.TempDir(), "schedule.json")
	args := []string{"--master-seed", "percentes-release-test", "--first", "2026-10-17", "--last", "2026-10-26",
		"--config", groq, "--config", baseten, "--schedule", path}
	if code, err := scheduleCmd(args); err != nil || code != exitOK {
		t.Fatalf("schedule: %d, %v", code, err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want, err := testSchedule(t, "groq", "baseten").JSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, want) {
		t.Fatalf("written schedule differs from the derived one:\n%s", b)
	}
	s, err := schedule.Load(b)
	if err != nil || !slices.Equal(s.Input.Routes, []string{"baseten", "groq"}) {
		t.Fatalf("load: %v, routes %v", err, s.Input.Routes)
	}

	other := append(slices.Clone(args[:len(args)-1]), path)
	other[1] = "another-seed"
	if code, err := scheduleCmd(other); err == nil || code != exitError {
		t.Fatalf("an existing schedule replaced: %d, %v", code, err)
	}
	if after, _ := os.ReadFile(path); !bytes.Equal(after, want) {
		t.Fatal("the existing schedule changed")
	}
}

// schedule refuses a window the frame does not allow, a configuration
// outside the release profile and a missing flag, and writes no file.
func TestScheduleCmdRefusals(t *testing.T) {
	groq := routeConfigFile(t, "groq", "https://api.example.test/openai/v1/chat/completions", "10")
	for name, args := range map[string][]string{
		"no weekend":    {"--master-seed", "s", "--first", "2026-10-19", "--last", "2026-10-23", "--config", groq},
		"window length": {"--master-seed", "s", "--first", "2026-10-17", "--last", "2026-10-31", "--config", groq},
		"no seed":       {"--first", "2026-10-17", "--last", "2026-10-26", "--config", groq},
		"pilot profile": {"--master-seed", "s", "--first", "2026-10-17", "--last", "2026-10-26", "--config", "../../configs/pilot.reference.yaml"},
		"no config":     {"--master-seed", "s", "--first", "2026-10-17", "--last", "2026-10-26"},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "schedule.json")
			if code, err := scheduleCmd(append(args, "--schedule", path)); err == nil || code != exitError {
				t.Fatalf("%d, %v", code, err)
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("a refused schedule wrote %s: %v", path, err)
			}
		})
	}
	if code, err := scheduleCmd([]string{"--master-seed", "s", "--config", groq}); err == nil || code != exitError {
		t.Fatalf("no --schedule: %d, %v", code, err)
	}
}
