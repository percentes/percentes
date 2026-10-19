package config

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

// secretValue is a confidential limit's value in these tests; its text
// occurs nowhere else in the reference.
const secretValue = "9876543.25"

// confidentialRef is the release reference with its tokens_per_minute
// limit written as tpm and marked confidential.
func confidentialRef(t *testing.T, tpm string) []byte {
	t.Helper()
	raw, err := os.ReadFile(releaseRef)
	if err != nil {
		t.Fatal(err)
	}
	const block = `    - dimension: tokens_per_minute
      value: 250000
      marking: public
      counting: input_and_output
      source: groq-rate-limits-page.png
      retrieved: 2026-10-07
      sha256: "0000000000000000000000000000000000000000000000000000000000000000"
`
	if !bytes.Contains(raw, []byte(block)) {
		t.Fatal("reference no longer carries the tokens_per_minute block this test edits")
	}
	return bytes.Replace(raw, []byte(block), []byte(tpm), 1)
}

const confidentialTPM = `    - dimension: tokens_per_minute
      value: ` + secretValue + `
      marking: confidential
      counting: input_and_output
      source: groq-console-limits.png
      retrieved: 2026-10-07
      sha256: "abababababababababababababababababababababababababababababababab"
`

// The file as published is the file itself without a confidential
// limit, and otherwise the file with "[retained]" in place of each
// confidential value and every other byte as written, in block and in
// flow style.
func TestPublished(t *testing.T) {
	c := loadRef(t, releaseRef)
	if c.Target.Confidential() {
		t.Fatal("the reference marks a limit confidential")
	}
	if b, err := c.Published(); err != nil || !bytes.Equal(b, c.Raw) {
		t.Fatalf("public limits only: the file as published differs from the file (%v)", err)
	}

	flow := `    - {dimension: tokens_per_minute, source: "é-console.png", value: ` + secretValue + `, marking: confidential, counting: input, retrieved: 2026-10-07, sha256: "abababababababababababababababababababababababababababababababab"}
`
	for name, tpm := range map[string]string{"block": confidentialTPM, "flow, after a two-byte rune": flow} {
		raw := confidentialRef(t, tpm)
		c, err := Parse(raw)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !c.Target.Confidential() || c.Target.Limits[1].Value != 9876543.25 {
			t.Fatalf("%s: limits as loaded %+v", name, c.Target.Limits)
		}
		b, err := c.Published()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if want := bytes.Replace(raw, []byte(secretValue), []byte(`"[retained]"`), 1); !bytes.Equal(b, want) {
			t.Errorf("%s: the file as published:\n%s", name, b)
		}
		if bytes.Contains(b, []byte("9876543")) {
			t.Errorf("%s: the file as published carries the confidential value", name)
		}
		if !bytes.Contains(b, []byte("      value: 1000\n")) {
			t.Errorf("%s: the public value is not published", name)
		}
		if _, err := Parse(b); err == nil {
			t.Errorf("%s: the file as published loads as a run configuration", name)
		}
	}
}

// A confidential value written other than as a plain number in its limit,
// and a comment that could carry it, are refused.
func TestPublishedRefusals(t *testing.T) {
	plain := "      value: " + secretValue + "\n"
	for _, tc := range []struct {
		name, from, to, want string
	}{
		{"tagged", plain, "      value: !!float " + secretValue + "\n", "target.limits[1].value: a confidential value is a plain number"},
		{"anchored", plain, "      value: &v " + secretValue + "\n", "target.limits[1].value: a confidential value is a plain number"},
		{"line comment", plain, "      value: " + secretValue + "  # from the console\n", "target.limits[1]: a confidential limit carries no comment"},
		{"head comment", "    - dimension: tokens_per_minute\n", "    # from the console\n    - dimension: tokens_per_minute\n", "target.limits[1]: a confidential limit carries no comment"},
		{"comment elsewhere", "schema_version: 1\n", "schema_version: 1  # the console showed " + secretValue + " tokens\n", "target.limits[1].value: a comment in the file carries " + secretValue},
	} {
		raw := bytes.Replace(confidentialRef(t, confidentialTPM), []byte(tc.from), []byte(tc.to), 1)
		if !bytes.Contains(raw, []byte(tc.to)) {
			t.Fatalf("%s: the edit did not apply", tc.name)
		}
		c, err := Parse(raw)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if b, err := c.Published(); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: must fail naming %q, got %v and\n%s", tc.name, tc.want, err, b)
		}
	}
}
