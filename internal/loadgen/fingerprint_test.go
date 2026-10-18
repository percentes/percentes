package loadgen

import (
	"encoding/json"
	"strings"
	"testing"
)

// The first top-level system_fingerprint is kept; a non-string value is
// ignored and does not make the stream malformed.
func TestSystemFingerprintKept(t *testing.T) {
	frame := func(fp string) string {
		return "data: {\"system_fingerprint\":" + fp + ",\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n"
	}
	for _, c := range []struct {
		name   string
		frames []string
		want   string
	}{
		{"first kept", []string{frame(`"fp_a"`), frame(`"fp_b"`)}, "fp_a"},
		{"absent", []string{answerFrame}, ""},
		{"null then set", []string{frame(`null`), frame(`"fp_b"`)}, "fp_b"},
		{"not a string", []string{frame(`7`)}, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := executeShaped(t, true, nil, append(c.frames, stopFrame, doneFrame)...)
			if r.Outcome != OutcomeCompleted {
				t.Fatalf("got %v/%q", r.Outcome, r.ErrClass)
			}
			if r.SystemFingerprint != c.want {
				t.Fatalf("fingerprint %q, want %q", r.SystemFingerprint, c.want)
			}
			b, _ := json.Marshal(r)
			if strings.Contains(string(b), `"system_fingerprint"`) != (c.want != "") {
				t.Fatalf("system_fingerprint in JSON, want present %v: %s", c.want != "", b)
			}
		})
	}
}
