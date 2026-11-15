package report

import (
	"fmt"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/admission"
	"github.com/percentes/percentes/internal/config"
)

// A confidential limit that counts output at generation names one span
// in its step-table entry: the window plus the hard timeout.
func TestConfidentialGenerationLimitNamesSpan(t *testing.T) {
	cfg, _ := stepsConfig(t)
	tpm := &cfg.Target.Limits[1]
	tpm.Value, tpm.Marking, tpm.Source, tpm.SHA256, tpm.HMACSHA256 = 80001, config.MarkingConfidential, "console.png", "", strings.Repeat("ab", 32)
	tpm.OutputAtGeneration = true
	cfg.Target.RetainedHeaders = []string{"x-ratelimit-*"}
	v, err := admission.CeilingRule(cfg, admission.Price{InputUSDPerMTok: "0.15", OutputUSDPerMTok: "0.6", Source: "https://example.test/pricing", Retrieved: "2026-10-07"}, admission.ExpectedPlan(cfg, 1), 1)
	if err != nil {
		t.Fatal(err)
	}
	st, err := NewStepTables(cfg, v, nil)
	if err != nil {
		t.Fatal(err)
	}
	text := st.Text()
	if want := "the rate it admits over 60 s plus the 90 s hard timeout (window test over 60 s plus the 90 s hard timeout: "; !strings.Contains(text, want) {
		t.Errorf("text lacks %q:\n%s", want, text)
	}
	if strings.Contains(text, "admits over 60 s (") {
		t.Errorf("an entry names the window alone:\n%s", text)
	}
}

// The account-group section names one limit counted at generation with
// "counts" and two with "count", and states where a send ends.
func TestAttributionTextAtGeneration(t *testing.T) {
	text := func(dims ...string) string {
		var b strings.Builder
		attributionText(func(f string, a ...any) { fmt.Fprintf(&b, f+"\n", a...) },
			&Attribution{Group: "g", Routes: []string{"azure"}, AtGeneration: dims, HardTimeoutS: 90})
		return b.String()
	}
	one := text(config.LimitTPM)
	for _, want := range []string{
		"tokens_per_minute counts output at generation:",
		"A ladder or warm-up send ends at its done time where it got a status other than 200, completed or returned an empty stream;",
		"a probe or ceiling-check send ends at its done time where it got a status other than 200 or its stream's terminator arrived with no read error.",
		"Any other send ends 90 s after its dispatch, at the hard timeout.",
	} {
		if !strings.Contains(one, want) {
			t.Errorf("missing %q in:\n%s", want, one)
		}
	}
	if strings.Contains(one, "gives up") {
		t.Errorf("text says the client gives up:\n%s", one)
	}
	if two := text(config.LimitOutputTPM, config.LimitTPM); !strings.Contains(two, "output_tokens_per_minute and tokens_per_minute count output at generation:") {
		t.Errorf("two limits:\n%s", two)
	}
}

// A limit counted at generation prints its window tests, and the binding
// limit where it is that limit, over the window plus the hard timeout;
// other limits print the window alone.
func TestStepTablesAtGenerationWindowText(t *testing.T) {
	cfg, _ := stepsConfig(t)
	cfg.Target.Limits[1].OutputAtGeneration = true
	cfg, v := ruled(t, cfg, nil)
	st, err := NewStepTables(cfg, v, []CellRecords{cellOf("weekday-00", outcome{1, LineInTime, 0, 1})})
	if err != nil {
		t.Fatal(err)
	}
	text := st.Text()
	for _, want := range []string{
		"of 80000 (window test over 60 s plus the 90 s hard timeout:",
		"of 1000 (window test over 60 s:",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q in:\n%s", want, text)
		}
	}
	if st.Binding.Limit == 1 && !strings.Contains(text, "binding limit tokens_per_minute over 60 s plus the 90 s hard timeout") {
		t.Errorf("binding:\n%s", text)
	}
	if st.Binding.Limit != 1 {
		t.Errorf("binding limit %+v, want tokens_per_minute", st.Binding)
	}
}
