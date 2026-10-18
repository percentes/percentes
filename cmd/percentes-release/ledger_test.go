package main

import (
	"math/big"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
)

// ledger create starts a ledger with every route's release caps; ledger
// price records a route's prices at its shape's input bound and ceiling,
// the price run admits the route at.
func TestLedgerCmd(t *testing.T) {
	groq := routeConfigFile(t, "groq", "https://api.example.test/openai/v1/chat/completions", "10")
	baseten := routeConfigFile(t, "baseten", "https://api.example.test/v1/chat/completions", "25")
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	if code, err := ledgerCmd([]string{"create", "--ledger", path, "--config", groq, "--config", baseten}); err != nil || code != exitOK {
		t.Fatalf("create: %d, %v", code, err)
	}
	cfgs, err := loadReleases([]string{groq, baseten})
	if err != nil {
		t.Fatal(err)
	}
	want, err := admission.ReleaseCaps(cfgs...)
	if err != nil {
		t.Fatal(err)
	}
	l, err := admission.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	st, err := l.State()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(st.Caps, want) || st.Caps.RouteLiabilityUSD["baseten"] != "25" || st.Caps.LiabilityUSD != "1300" {
		t.Fatalf("caps %+v, want %+v", st.Caps, want)
	}
	for _, cfg := range cfgs {
		if err := st.CheckRelease(cfg); err != nil {
			t.Fatal(err)
		}
	}
	if code, err := ledgerCmd([]string{"create", "--ledger", path, "--config", groq}); err == nil || code != exitError {
		t.Fatalf("an existing ledger created again: %d, %v", code, err)
	}

	price := []string{"price", "--ledger", path, "--config", groq, "--input", "0.15", "--output", "0.60",
		"--source", "https://example.test/pricing", "--retrieved", "2026-10-06"}
	if code, err := ledgerCmd(price); err != nil || code != exitOK {
		t.Fatalf("price: %d, %v", code, err)
	}
	entries, err := ledgerEntries(path)
	if err != nil {
		t.Fatal(err)
	}
	p, err := routePrice(entries, "groq")
	if err != nil {
		t.Fatal(err)
	}
	if p.InputTokens != 1200 || p.OutputTokens != config.PinnedReleaseCeilings[0] || p.InputUSDPerMTok != "0.15" || p.Source != "https://example.test/pricing" {
		t.Fatalf("price %+v", p)
	}
	if st, err = l.State(); err != nil {
		t.Fatal(err)
	}
	// 1,200 tokens at US$0.15 and 512 at US$0.60 a million.
	if got, want := st.Routes["groq"].MaxPerRequestUSD, big.NewRat(4872, 10_000_000); got.Cmp(want) != 0 {
		t.Fatalf("per-request price %s, want %s", got.FloatString(7), want.FloatString(7))
	}
}

// ledger price refuses a route the ledger does not cap at its
// configuration's caps, more than one configuration and a bad price;
// ledger refuses an unknown action and missing flags.
func TestLedgerCmdRefusals(t *testing.T) {
	groq := routeConfigFile(t, "groq", "https://api.example.test/openai/v1/chat/completions", "10")
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	if code, err := ledgerCmd([]string{"create", "--ledger", path, "--config", groq}); err != nil || code != exitOK {
		t.Fatalf("create: %d, %v", code, err)
	}
	priced := func(cfgs ...string) []string {
		args := []string{"price", "--ledger", path, "--input", "0.15", "--output", "0.60", "--source", "https://example.test/pricing", "--retrieved", "2026-10-06"}
		for _, c := range cfgs {
			args = append(args, "--config", c)
		}
		return args
	}
	for name, args := range map[string][]string{
		"route cap differs": priced(routeConfigFile(t, "groq", "https://api.example.test/openai/v1/chat/completions", "9")),
		"route not capped":  priced(routeConfigFile(t, "baseten", "https://api.example.test/v1/chat/completions", "10")),
		"two routes":        priced(groq, groq),
		"no source":         append(priced(groq), "--source", ""),
		"pilot profile":     priced("../../configs/pilot.reference.yaml"),
		"unknown action":    {"show", "--ledger", path, "--config", groq},
		"no ledger":         {"create", "--config", groq},
		"no config":         {"create", "--ledger", filepath.Join(t.TempDir(), "new.jsonl")},
		"no action":         nil,
	} {
		t.Run(name, func(t *testing.T) {
			if code, err := ledgerCmd(args); err == nil || code != exitError {
				t.Fatalf("%d, %v", code, err)
			}
		})
	}
	entries, err := ledgerEntries(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("refused commands left %d lines", len(entries))
	}
}
