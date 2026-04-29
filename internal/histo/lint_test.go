package histo

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestForbiddenCorrectionAPIs enforces the SPEC.md §3 recording rule as a
// lint: latencies are re-based to intended dispatch time and recorded via
// plain RecordValue only; HdrHistogram's coordinated-omission correction
// APIs must not appear anywhere in the harness source.
func TestForbiddenCorrectionAPIs(t *testing.T) {
	// Assembled at runtime so this file does not match itself.
	forbidden := []string{
		"RecordCorrected" + "Value",
		"Expected" + "Interval",
	}
	root := "../.."
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if info.Name() == ".git" || info.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "lint_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, f := range forbidden {
			if strings.Contains(string(raw), f) {
				t.Errorf("%s uses forbidden coordinated-omission correction API %q (SPEC.md §3)", path, f)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
