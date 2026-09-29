package main

import (
	"strings"
	"testing"
)

// The placeholder list names line numbers and any keys; a value never
// reaches it.
func TestPlaceholderLinesNameKeysOnly(t *testing.T) {
	raw := []byte("# PIN-AT-PHASE1 in a comment\n" +
		"target:\n" +
		"  base_url: http://user:SYNTHETIC_PASSWORD_29sep@PIN-AT-PHASE1:8000\n" +
		"  model_name: PIN-AT-PHASE1\n" +
		"  metrics_urls:\n" +
		"    - http://user:SYNTHETIC_PASSWORD_29sep@PIN-AT-PHASE1:8000/metrics\n" +
		"PIN-AT-PHASE1\n" +
		"  admin_url: \"http://user:\\\n" +
		"      SYNTHETIC_PASSWORD_29sep@host.internal:8000/PIN-AT-PHASE1\"\n" +
		"  other: http://\n" +
		"    user:SYNTHETIC_PASSWORD_29sep@PIN-AT-PHASE1\n")
	got := strings.Join(placeholderLines(raw), "\n")
	want := "line 3: base_url\nline 4: model_name\nline 6: list item\nline 7\nline 9\nline 11"
	if got != want {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
	if strings.Contains(got, "SYNTHETIC_PASSWORD") {
		t.Fatalf("a value reached the placeholder list: %s", got)
	}
}
