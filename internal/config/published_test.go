package config

import (
	"bytes"
	"os"
	"slices"
	"strings"
	"testing"
	"unicode/utf16"
)

// secretValue is a confidential limit's value in these tests; its text
// occurs nowhere else in the reference.
const secretValue = "9876543.25"

// confidentialRef is the release reference with its tokens_per_minute
// limit written as tpm and marked confidential, and its rate-limit
// headers retained.
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
	const headers = "  response_headers: [Retry-After, x-ratelimit-*]\n"
	if !bytes.Contains(raw, []byte(block)) || !bytes.Contains(raw, []byte(headers)) {
		t.Fatal("reference no longer carries the lines this test edits")
	}
	raw = bytes.Replace(raw, []byte(headers), []byte(headers+"  retained_headers: [x-ratelimit-*]\n"), 1)
	return bytes.Replace(raw, []byte(block), []byte(tpm), 1)
}

const confidentialTPM = `    - dimension: tokens_per_minute
      value: ` + secretValue + `
      marking: confidential
      counting: input_and_output
      source: groq-console-limits.png
      retrieved: 2026-10-07
      hmac_sha256: "abababababababababababababababababababababababababababababababab"
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

	flow := `    - {dimension: tokens_per_minute, source: "é-console.png", value: ` + secretValue + `, marking: confidential, counting: input, retrieved: 2026-10-07, hmac_sha256: "abababababababababababababababababababababababababababababababab"}
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
// and a comment or string that could carry it, are refused.
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
		{"naming line", "      - Groq is a trademark", "      - The console showed " + secretValue + " tokens.\n      - Groq is a trademark", "target.limits[1].value: a string in the file carries " + secretValue},
		{"body text", "System text: placeholder", "System text: at most " + secretValue + " tokens a minute", "target.limits[1].value: a string in the file carries " + secretValue},
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

// A file of more than one YAML document, or with text after its
// document-end marker, is refused by Parse, Decode and Published; one
// leading "---" is one document, and a trailing "..." loads.
func TestOneDocument(t *testing.T) {
	const one = "one YAML document required"
	ref := confidentialRef(t, confidentialTPM)
	for name, tail := range map[string]string{
		"a mapping":                       "---\nleak: " + secretValue + "\n",
		"a string":                        "---\nnote: \"tpm " + secretValue + "\"\n",
		"not YAML":                        "---\n: : [\n",
		"a comment":                       "---\n# tokens_per_minute is " + secretValue + "\n",
		"an empty document":               "---\n",
		"after an end marker":             "...\n---\nleak: " + secretValue + "\n",
		"a comment on the end marker":     "... # tpm " + secretValue + "\n",
		"a comment after two markers":     "...\n...\n# tpm " + secretValue + "\n",
		"a comment after the marker":      "...\n# tpm " + secretValue + "\n",
		"a marker after a line separator": "\u2028... # tpm " + secretValue + "\n",
		"a marker after a return":         "\r... # tpm " + secretValue + "\n",
		"a second marker":                 "...\n...",
	} {
		raw := append(slices.Clone(ref), tail...)
		if _, err := Parse(raw); err == nil || !strings.Contains(err.Error(), one) {
			t.Errorf("%s: Parse: %v", name, err)
		}
		if _, err := Decode(raw); err == nil || !strings.Contains(err.Error(), one) {
			t.Errorf("%s: Decode: %v", name, err)
		}
		c, err := Parse(ref)
		if err != nil {
			t.Fatal(err)
		}
		c.Raw = raw
		if b, err := c.Published(); err == nil || !strings.Contains(err.Error(), one) {
			t.Errorf("%s: published %v\n%s", name, err, b)
		}
	}

	c, err := Parse(append([]byte("---\n"), ref...))
	if err != nil {
		t.Fatalf("one leading ---: %v", err)
	}
	if b, err := c.Published(); err != nil || bytes.Contains(b, []byte(secretValue)) || !bytes.HasPrefix(b, []byte("---\n")) {
		t.Fatalf("one leading ---: published %v\n%s", err, b)
	}

	for _, tc := range [][2]string{{"", "..."}, {"", "...\n"}, {"", "... \n\n"}, {"---\n", "...\n"}} {
		raw := append(append([]byte(tc[0]), ref...), tc[1]...)
		c, err := Parse(raw)
		if err != nil {
			t.Fatalf("%q then %q: %v", tc[0], tc[1], err)
		}
		if b, err := c.Published(); err != nil || bytes.Contains(b, []byte(secretValue)) || !bytes.HasSuffix(b, []byte(tc[1])) {
			t.Fatalf("%q then %q: published %v\n%s", tc[0], tc[1], err, b)
		}
	}
}

// A YAML directive or a tag outside the core types is refused by Parse,
// Decode and Published, as is a file not in UTF-8; the reference and a
// core tag written out still load.
func TestPlainYAML(t *testing.T) {
	ref := confidentialRef(t, confidentialTPM)
	const naming = "      - Groq is a trademark"
	for _, tc := range []struct{ name, raw, want string }{
		{"a %TAG directive", "%TAG !x! tag:example.com,2000:" + secretValue + "\n---\n" + string(ref), "a YAML directive"},
		{"a %YAML directive", "%YAML 1.1\n---\n" + string(ref), "a YAML directive"},
		{"a directive after a byte order mark", "\ufeff%YAML 1.1\n---\n" + string(ref), "a YAML directive"},
		{"a verbatim tag", strings.Replace(string(ref), naming, "      - !<tag:x,2000:"+secretValue+"> Groq is a trademark", 1), "a YAML tag outside the core types"},
		{"a local tag", strings.Replace(string(ref), naming, "      - !t"+secretValue+" Groq is a trademark", 1), "a YAML tag outside the core types"},
		{"a binary tag", strings.Replace(string(ref), naming, "      - !!binary OTg3NjU0My4yNQ==\n"+naming, 1), "a YAML tag outside the core types"},
		{"UTF-16", "\xff\xfe" + string(utf16LE(string(ref)+"... # tpm "+secretValue+"\n")), "not UTF-8"},
	} {
		raw := []byte(tc.raw)
		if bytes.Equal(raw, ref) {
			t.Fatalf("%s: the edit did not apply", tc.name)
		}
		if _, err := Parse(raw); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: Parse: %v", tc.name, err)
		}
		if _, err := Decode(raw); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: Decode: %v", tc.name, err)
		}
		c, err := Parse(ref)
		if err != nil {
			t.Fatal(err)
		}
		c.Raw = raw
		if b, err := c.Published(); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: published %v\n%s", tc.name, err, b)
		}
	}

	loadRef(t, releaseRef)
	for _, raw := range [][]byte{ref, bytes.Replace(ref, []byte(naming), []byte("      - !!str Groq is a trademark"), 1)} {
		c, err := Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if b, err := c.Published(); err != nil || bytes.Contains(b, []byte(secretValue)) {
			t.Fatalf("published %v\n%s", err, b)
		}
	}
}

// utf16LE is s in UTF-16 little-endian.
func utf16LE(s string) []byte {
	var b []byte
	for _, u := range utf16.Encode([]rune(s)) {
		b = append(b, byte(u), byte(u>>8))
	}
	return b
}

// An in-band error's stand-in keeps the object's "type" string and
// nothing else, and is its own stand-in.
func TestRetainedError(t *testing.T) {
	for in, want := range map[string]string{
		`{"type":"rate_limit_error","message":"` + secretValue + ` tokens per minute"}`: `{"type":"rate_limit_error"}`,
		`{"message":"` + secretValue + ` tokens per minute"}`:                           `{}`,
		`{"type":null,"limit":` + secretValue + `}`:                                     `{}`,
		`{"type":529}`: `{}`,
		`{"type":"rate_limit_error","message":"` + secretValue: RetainedHeaderValue,
		`limit ` + secretValue + ` reached`:                    RetainedHeaderValue,
		`null`:                                                 RetainedHeaderValue,
	} {
		got := RetainedError(in)
		if got != want || RetainedError(got) != got {
			t.Errorf("%q: stand-in %q (its own %q), want %q", in, got, RetainedError(got), want)
		}
	}
}

// The value is found with its whole part grouped in threes and in its
// decimal form.
func TestPublishedForms(t *testing.T) {
	plain := "      value: " + secretValue + "\n"
	naming := "      - Groq is a trademark"
	for _, tc := range []struct {
		name  string
		edits [][2]string
		want  string
	}{
		{"commas in a naming line", [][2]string{{naming, "      - The console showed 9,876,543.25 tokens\n" + naming}}, "a string in the file carries 9,876,543.25"},
		{"commas in a comment", [][2]string{{"schema_version: 1\n", "schema_version: 1  # the console showed 9,876,543.25\n"}}, "a comment in the file carries 9,876,543.25"},
		{"narrow spaces in the body", [][2]string{{"System text: placeholder", "System text: at most 9 876 543.25 tokens"}}, "a string in the file carries 9 876 543.25"},
		{"before a full stop", [][2]string{{naming, "      - The console showed " + secretValue + ".\n" + naming}}, "a string in the file carries " + secretValue},
		{"a trailing zero in the limit", [][2]string{{plain, "      value: " + secretValue + "0\n"}, {naming, "      - The console showed " + secretValue + " tokens\n" + naming}}, "a string in the file carries " + secretValue},
	} {
		raw := confidentialRef(t, confidentialTPM)
		for _, e := range tc.edits {
			if !bytes.Contains(raw, []byte(e[0])) {
				t.Fatalf("%s: the edit did not apply", tc.name)
			}
			raw = bytes.Replace(raw, []byte(e[0]), []byte(e[1]), 1)
		}
		c, err := Parse(raw)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if b, err := c.Published(); err == nil || !strings.Contains(err.Error(), "target.limits[1].value: "+tc.want) {
			t.Errorf("%s: must fail naming %q, got %v and\n%s", tc.name, tc.want, err, b)
		}
	}
}

// The value's digits inside a hash or a date, or inside a larger number,
// are not refused.
func TestPublishedSkips(t *testing.T) {
	const small = "12"
	plain := "      value: " + secretValue + "\n"
	limitHash := `"abababababababababababababababababababababababababababababababab"`
	naming := "      - Groq is a trademark"
	for _, tc := range []struct {
		name string
		from string
		to   string
	}{
		{"none", naming, naming},
		{"the limit's hmac_sha256", limitHash, `"ab12abababababababababababababababababababababababababababababab"`},
		{"a quoted date", "      source: groq-console-limits.png\n      retrieved: 2026-10-07\n", "      source: groq-console-limits.png\n      retrieved: \"2026-10-12\"\n"},
		{"a date in a comment", "schema_version: 1\n", "schema_version: 1  # retrieved 2026-10-12\n"},
		{"a commit hash in a naming line", naming, "      - Build 12d4e5f60718293a4b5c6d7e8f901a2b3c4d5e6f.\n" + naming},
		{"a larger grouped number", naming, "      - Groq serves 12,000 requests.\n" + naming},
	} {
		raw := bytes.Replace(confidentialRef(t, confidentialTPM), []byte(plain), []byte("      value: "+small+"\n"), 1)
		if !bytes.Contains(raw, []byte(tc.from)) {
			t.Fatalf("%s: the edit did not apply", tc.name)
		}
		raw = bytes.Replace(raw, []byte(tc.from), []byte(tc.to), 1)
		c, err := Parse(raw)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		b, err := c.Published()
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if want := bytes.Replace(raw, []byte("value: "+small+"\n"), []byte(`value: "[retained]"`+"\n"), 1); !bytes.Equal(b, want) {
			t.Errorf("%s: the file as published:\n%s", tc.name, b)
		}
	}
}
