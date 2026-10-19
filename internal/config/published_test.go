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

// The value is found with its whole part grouped in threes, in its
// decimal form, with trailing zeros, with a decimal comma and in its 'E'
// form.
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
		{"trailing zeros in a comment", [][2]string{{"schema_version: 1\n", "schema_version: 1  # the console showed 9,876,543.2500\n"}}, "a comment in the file carries 9,876,543.2500"},
		{"an E form in a naming line", [][2]string{{naming, "      - The console showed 9.87654325E+06 tokens\n" + naming}}, "a string in the file carries 9.87654325E+06"},
		{"a decimal comma in a comment", [][2]string{{"schema_version: 1\n", "schema_version: 1  # the console showed 9.876.543,25\n"}}, "a comment in the file carries 9.876.543,25"},
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

// ValueForms adds zeros after the decimal's last digit up to 17 digits
// in all, groups those forms too, writes each with a fractional part
// with a decimal comma too, and adds the 'e' and 'E' forms; Carries
// finds each such form as a number of its own, and none inside another
// number. A comma before three digits after at most three is left out,
// so "12,500" is not a form of 12.5.
func TestValueForms(t *testing.T) {
	for _, tc := range []struct {
		value     float64
		want      []string
		too       string
		carried   []string
		uncarried []string
	}{
		{300001, []string{"300001", "300001.0", "300001.00000000000", "300,001.00", "300\u202f001.0", "3.00001e+05", "3.00001E+05", "300001,0", "300.001,00", "300\u00a0001,0"}, "300001.000000000000",
			[]string{"TPM 300001.0", "300,001.00/min", "TPM 3.00001E+05.", "about 300001.000 tokens", "TPM 300.001,0"}, []string{"300001.01", "300001.0001", "1300001.0", "3.00001E+056", "300001,01", "1.300.001,0"}},
		{250013.5, []string{"250013.5", "250013.50", "250,013.500", "2.500135e+05", "2.500135E+05", "250013,5", "250.013,5", "250 013,50", "250\u2009013,5"}, "250013.500000000000",
			[]string{"TPM 250013.50", "250 013.5000", "limit 250013,5 tpm", "250.013,5/min"}, []string{"250013.55", "250013.505", "250013,55", "1.250.013,5", "250.013,55"}},
	} {
		forms := ValueForms(tc.value)
		for _, w := range tc.want {
			if !slices.Contains(forms, w) {
				t.Errorf("%v: forms lack %q", tc.value, w)
			}
		}
		if slices.Contains(forms, tc.too) {
			t.Errorf("%v: forms hold %q, past 17 digits", tc.value, tc.too)
		}
		for _, s := range tc.carried {
			if _, ok := Carries(s, forms); !ok {
				t.Errorf("%v: %q not carried", tc.value, s)
			}
		}
		for _, s := range tc.uncarried {
			if f, ok := Carries(s, forms); ok {
				t.Errorf("%v: %q carries %q", tc.value, s, f)
			}
		}
	}
	forms := ValueForms(12.5)
	if !slices.Contains(forms, "12,5") || !slices.Contains(forms, "12,50") || slices.Contains(forms, "12,500") {
		t.Errorf("12.5: forms %q, want 12,5 and 12,50 and not 12,500", forms)
	}
	if f, ok := Carries("12,500 requests", forms); ok {
		t.Errorf("12.5: a number grouped in threes carries %q", f)
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

// A small confidential value inside an identifier the provider fixes, in
// that identifier's own field or as its text in the body or a naming line,
// with or without its provider prefix, does not stop the file loading and
// publishing; a naming line or a comment carrying the value is still
// refused, and so is the value written on from the identifier's last
// digits by a digit, a decimal point, a decimal comma or a space.
func TestPublishedFixedIdentifiers(t *testing.T) {
	const (
		model    = "openai/gpt-oss-120b"
		naming   = "      - Groq is a trademark"
		version  = "schema_version: 1\n"
		provider = `section 1: "Outputs are Customer Data"`
		reported = "  reported_model:\n    name: meta/llama-3.3-70b-versatile\n    source: https://example.test/models\n    retrieved: 2026-10-07\n    sha256: \"0000000000000000000000000000000000000000000000000000000000000000\"\n  limits:\n"
	)
	limit := func(v string) string {
		return `    - dimension: tokens_per_minute
      value: 250000
      marking: public
      counting: input_and_output
      source: groq-rate-limits-page.png
      retrieved: 2026-10-07
      sha256: "0000000000000000000000000000000000000000000000000000000000000000"
    - dimension: concurrent_requests
      value: ` + v + `
      marking: confidential
      source: groq-console-limits.png
      retrieved: 2026-10-07
      hmac_sha256: "abababababababababababababababababababababababababababababababab"
`
	}
	for _, tc := range []struct {
		name, value, line string
		edits             [][2]string
	}{
		{"4 in gpt-4o-mini", "4", "Groq allows 4 requests at once.", [][2]string{{model, "gpt-4o-mini"}}},
		{"1 in an endpoint with /v1/", "1", "Groq allows 1 request at once.", [][2]string{{provider, `section one: "Outputs are Customer Data"`}}},
		{"70 in llama-3.3-70b-versatile", "70", "Groq allows 70 requests at once.", [][2]string{{model, "llama-3.3-70b-versatile"}, {"  limits:\n", reported}}},
		{"3225 beside qwen3-32", "3225", "Served as qwen3-3225.", [][2]string{{model, "qwen3-32"}}},
		{"120 in gpt-oss-120b without its prefix", "120", "Groq allows 120 requests at once.", [][2]string{{naming, "      - Served as gpt-oss-120b.\n" + naming}}},
	} {
		raw := confidentialRef(t, limit(tc.value))
		for _, e := range tc.edits {
			if !bytes.Contains(raw, []byte(e[0])) {
				t.Fatalf("%s: the edit did not apply", tc.name)
			}
			raw = bytes.ReplaceAll(raw, []byte(e[0]), []byte(e[1]))
		}
		c, err := Parse(raw)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		b, err := c.Published()
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
		} else if want := bytes.Replace(raw, []byte("      value: "+tc.value+"\n"), []byte(`      value: "[retained]"`+"\n"), 1); !bytes.Equal(b, want) {
			t.Errorf("%s: the file as published:\n%s", tc.name, b)
		}
		for _, e := range [][2]string{
			{naming, "      - " + tc.line + "\n" + naming},
			{naming, "      - Served as " + c.Target.ModelName + " with " + tc.value + " at once.\n" + naming},
			{version, "schema_version: 1  # " + tc.value + " at once\n"},
		} {
			c, err := Parse(bytes.Replace(raw, []byte(e[0]), []byte(e[1]), 1))
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			want := "target.limits[2].value: a string in the file carries " + tc.value
			if e[0] == version {
				want = "target.limits[2].value: a comment in the file carries " + tc.value
			}
			if b, err := c.Published(); err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("%s: %q must fail naming %q, got %v and\n%s", tc.name, e[1], want, err, b)
			}
		}
	}
	for _, tc := range []struct{ value, line, form string }{
		{"32.5", "Served as qwen3-32.5.", "32.5"},
		{"32000", "Served as qwen3-32,000.", "32,000"},
		{"32000", "Served as qwen3-32 000.", "32 000"},
	} {
		raw := bytes.ReplaceAll(confidentialRef(t, limit(tc.value)), []byte(model), []byte("qwen3-32"))
		c, err := Parse(raw)
		if err != nil {
			t.Fatalf("%s: %v", tc.line, err)
		}
		if _, err := c.Published(); err != nil {
			t.Errorf("%s: without the line: %v", tc.line, err)
		}
		if c, err = Parse(bytes.Replace(raw, []byte(naming), []byte("      - "+tc.line+"\n"+naming), 1)); err != nil {
			t.Fatalf("%s: %v", tc.line, err)
		}
		want := "target.limits[2].value: a string in the file carries " + tc.form
		if b, err := c.Published(); err == nil || !strings.HasSuffix(err.Error(), want) {
			t.Errorf("%q must fail naming %q, got %v and\n%s", tc.line, want, err, b)
		}
	}
}
