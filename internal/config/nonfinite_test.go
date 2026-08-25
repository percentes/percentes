package config

import (
	"os"
	"regexp"
	"testing"
)

// NaN makes every ordered comparison false and infinities pass
// positivity checks; neither can hold a pin.
func TestNonFiniteValuesRefuse(t *testing.T) {
	raw, err := os.ReadFile("../../configs/ac.reference.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct{ field, bad string }{
		{"t_inject_offset_s", ".nan"},
		{"fixed_ms", ".nan"},
		{"baseline_s", ".inf"},
	}
	for _, c := range cases {
		re := regexp.MustCompile(c.field + `:\s*[0-9.]+`)
		if !re.Match(raw) {
			t.Fatalf("fixture drift: %s not found in ac.reference.yaml", c.field)
		}
		mutated := re.ReplaceAll(raw, []byte(c.field+": "+c.bad))
		if _, err := Parse(mutated); err == nil {
			t.Errorf("%s: %s accepted; non-finite values must refuse to load", c.field, c.bad)
		}
	}
}
