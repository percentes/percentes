package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/percentes/percentes/internal/evidence"
)

// pinRecordTimes sets start_utc and end_utc to at in each clock, status,
// balance and refresh record in dir.
func pinRecordTimes(t *testing.T, dir string, at time.Time) {
	t.Helper()
	for _, name := range []string{evidence.ClockFile, evidence.StatusStartFile, evidence.StatusEndFile, evidence.BalanceStartFile, evidence.BalanceEndFile, evidence.RefreshFile} {
		p := filepath.Join(dir, name)
		b, err := os.ReadFile(p)
		if errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		d := json.NewDecoder(bytes.NewReader(b))
		d.UseNumber()
		if err := d.Decode(&m); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		for _, k := range []string{"start_utc", "end_utc"} {
			if _, ok := m[k]; ok {
				m[k] = at.UTC()
			}
		}
		if err := replaceJSON(p, m); err != nil {
			t.Fatal(err)
		}
	}
}
