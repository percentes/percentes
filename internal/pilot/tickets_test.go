package pilot

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

// Fixture hashes for Seed. A change to any template, fill or draw changes
// them.
const (
	wantMeasuredSHA256    = "e7b317f05202a97f2fc02c96f3e14089595e776010885eb0e8ab8545246f3c00"
	wantDevelopmentSHA256 = "4c2969710097d1ec24d1429aec7311c9f9f52231a54a67c3084d45ab948b4c4f"
)

func generate(t *testing.T) *Fixture {
	t.Helper()
	f, err := Generate(Seed)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func all(f *Fixture) []Ticket {
	return append(append([]Ticket{}, f.Measured...), f.Development...)
}

func sha(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestFixtureIsDeterministic(t *testing.T) {
	a, b := generate(t), generate(t)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("two runs from the same seed differ")
	}
	if got := sha(Encode(a.Measured)); got != wantMeasuredSHA256 {
		t.Errorf("measured sha256 %s, want %s", got, wantMeasuredSHA256)
	}
	if got := sha(Encode(a.Development)); got != wantDevelopmentSHA256 {
		t.Errorf("development sha256 %s, want %s", got, wantDevelopmentSHA256)
	}

	c, err := Generate("another-seed")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(Encode(a.Measured), Encode(c.Measured)) {
		t.Error("a different seed gave the same measured set")
	}
	if _, err := Generate(""); err == nil {
		t.Error("empty seed accepted")
	}
}

func TestFixtureBalance(t *testing.T) {
	f := generate(t)
	for _, set := range []struct {
		name string
		per  int
		ts   []Ticket
	}{{SetMeasured, MeasuredPerLabel, f.Measured}, {SetDevelopment, DevelopmentPerLabel, f.Development}} {
		if len(set.ts) != set.per*len(Labels) {
			t.Fatalf("%s: %d tickets", set.name, len(set.ts))
		}
		for _, label := range Labels {
			var n, second int
			length, tone := map[string]int{}, map[string]int{}
			for _, tk := range set.ts {
				if tk.Label != label {
					continue
				}
				n++
				length[tk.Length]++
				tone[tk.Tone]++
				if tk.Mention != "" || (label == Escalation && tk.Request != "") {
					second++
				}
			}
			if n != set.per {
				t.Errorf("%s %s: %d tickets, want %d", set.name, label, n, set.per)
			}
			for _, l := range lengths {
				if length[l.key] != set.per/3 {
					t.Errorf("%s %s: %d %s tickets, want %d", set.name, label, length[l.key], l.key, set.per/3)
				}
			}
			if second != set.per/3 {
				t.Errorf("%s %s: %d with a second signal, want %d", set.name, label, second, set.per/3)
			}
			for _, tn := range tones {
				if c := tone[tn.key]; c < set.per/len(tones) || c > set.per/len(tones)+1 {
					t.Errorf("%s %s: %d %s tickets", set.name, label, c, tn.key)
				}
			}
		}
	}
}

func TestLabelsFollowConstruction(t *testing.T) {
	f := generate(t)
	var allTriggers []string
	for _, tr := range triggers {
		allTriggers = append(allTriggers, tr.sentences...)
	}
	for _, tk := range all(f) {
		if !tk.Synthetic {
			t.Errorf("%s: not marked synthetic", tk.ID)
		}
		var hasTrigger bool
		for _, s := range allTriggers {
			hasTrigger = hasTrigger || strings.Contains(tk.Text, s)
		}
		switch {
		case tk.Label == Escalation:
			if tk.Trigger == "" || !hasTrigger || tk.Mention != "" {
				t.Errorf("%s: escalation with trigger %q, sentence present %v, mention %q", tk.ID, tk.Trigger, hasTrigger, tk.Mention)
			}
		default:
			if tk.Trigger != "" || hasTrigger || tk.Request != tk.Label {
				t.Errorf("%s: %s ticket with trigger %q, trigger sentence %v, request %q", tk.ID, tk.Label, tk.Trigger, hasTrigger, tk.Request)
			}
		}
		if tk.Request != "" {
			var iss *issue
			for i := range issues[tk.Request] {
				if issues[tk.Request][i].key == tk.Issue {
					iss = &issues[tk.Request][i]
				}
			}
			if iss == nil {
				t.Errorf("%s: issue %q not a %s template", tk.ID, tk.Issue, tk.Request)
				continue
			}
			if fixed, _, _ := strings.Cut(iss.request, "{"); !strings.Contains(tk.Text, fixed) {
				t.Errorf("%s: text lacks the request %q", tk.ID, iss.request)
			}
		}
		if tk.Mention != "" {
			var found bool
			for _, m := range mentions[tk.Mention] {
				found = found || strings.Contains(tk.Text, m)
			}
			if !found {
				t.Errorf("%s: mention %q not in the text", tk.ID, tk.Mention)
			}
		}
		if strings.ContainsAny(tk.Text, "{}") {
			t.Errorf("%s: unfilled placeholder in %q", tk.ID, tk.Text)
		}
	}
}

func TestRoutePrecedence(t *testing.T) {
	for _, c := range []struct{ request, trigger, want string }{
		{Billing, "", Billing},
		{Account, "", Account},
		{Technical, "legal_threat", Escalation},
		{"", "public_exposure", Escalation},
	} {
		if got := Route(c.request, c.trigger); got != c.want {
			t.Errorf("Route(%q, %q) = %q, want %q", c.request, c.trigger, got, c.want)
		}
	}
}

func TestAmbiguousCasesCarryReasons(t *testing.T) {
	f := generate(t)
	var n int
	for _, tk := range all(f) {
		want := tk.Mention != "" ||
			(tk.Trigger != "" && tk.Request != "") ||
			tk.Trigger == "suspected_breach" ||
			(tk.Trigger == "" && tk.Tone == "angry")
		if tk.Ambiguous != want || (tk.Ambiguity != "") != want {
			t.Errorf("%s: ambiguous %v, reason %q, want ambiguous %v", tk.ID, tk.Ambiguous, tk.Ambiguity, want)
		}
		if tk.Ambiguous {
			n++
			if !strings.Contains(tk.Ambiguity, tk.Label) && !strings.Contains(tk.Ambiguity, "escalation trigger") {
				t.Errorf("%s: reason %q does not name the route", tk.ID, tk.Ambiguity)
			}
		}
	}
	if n == 0 {
		t.Fatal("no ambiguous cases")
	}
}

func TestTextsDistinctAcrossSets(t *testing.T) {
	f := generate(t)
	seen := map[string]string{}
	for _, tk := range all(f) {
		if prev, ok := seen[tk.Text]; ok {
			t.Errorf("%s repeats %s", tk.ID, prev)
		}
		seen[tk.Text] = tk.ID
	}
	ids := map[string]bool{}
	for _, tk := range all(f) {
		if ids[tk.ID] {
			t.Errorf("id %s repeated", tk.ID)
		}
		ids[tk.ID] = true
	}
}

func TestByteCap(t *testing.T) {
	if PromptByteCap+TemplateOverheadTokens != InputTokenLimit || InputTokenLimit != 2000 {
		t.Fatalf("prompt cap %d plus overhead %d is not the 2,000-token limit", PromptByteCap, TemplateOverheadTokens)
	}
	if TicketByteCap >= PromptByteCap {
		t.Fatalf("ticket cap %d leaves no room for the system text", TicketByteCap)
	}
	f := generate(t)
	var longest Ticket
	var multibyte bool
	for _, tk := range all(f) {
		if tk.Bytes != len(tk.Text) || tk.Bytes > TicketByteCap {
			t.Errorf("%s: bytes %d, len %d, cap %d", tk.ID, tk.Bytes, len(tk.Text), TicketByteCap)
		}
		if !utf8.ValidString(tk.Text) {
			t.Errorf("%s: invalid UTF-8", tk.ID)
		}
		multibyte = multibyte || utf8.RuneCountInString(tk.Text) < len(tk.Text)
		if tk.Bytes > longest.Bytes {
			longest = tk
		}
	}
	if !multibyte {
		t.Error("no ticket holds a multi-byte character")
	}

	system := strings.Repeat("s", PromptByteCap-longest.Bytes)
	if err := CheckPrompt(system, all(f)); err != nil {
		t.Errorf("prompt at the cap refused: %v", err)
	}
	err := CheckPrompt(system+"s", all(f))
	if err == nil || !strings.Contains(err.Error(), longest.ID) {
		t.Errorf("prompt over the cap: %v, want a refusal naming %s", err, longest.ID)
	}
}

func TestCheckRefuses(t *testing.T) {
	for _, c := range []struct {
		name   string
		mutate func(f *Fixture)
		want   string
	}{
		{"relabelled", func(f *Fixture) { f.Measured[0].Label = otherLabel(f.Measured[0].Label) }, "rubric gives"},
		{"repeated text", func(f *Fixture) {
			f.Measured[1].Text = f.Development[0].Text
			f.Measured[1].Bytes = len(f.Measured[1].Text)
		}, "same text"},
		{"over the cap", func(f *Fixture) {
			f.Measured[2].Text = strings.Repeat("x", TicketByteCap+1)
			f.Measured[2].Bytes = TicketByteCap + 1
		}, "cap"},
		{"bytes wrong", func(f *Fixture) { f.Measured[3].Bytes++ }, "bytes"},
		{"not synthetic", func(f *Fixture) { f.Development[1].Synthetic = false }, "synthetic"},
		{"repeated id", func(f *Fixture) { f.Development[2].ID = f.Measured[0].ID }, "repeated id"},
		{"wrong set", func(f *Fixture) { f.Development[3].Set = SetMeasured }, "set"},
		{"unbalanced", func(f *Fixture) { f.Measured = f.Measured[1:] }, "want 30"},
		{"reason dropped", func(f *Fixture) {
			for i := range f.Measured {
				if f.Measured[i].Ambiguous {
					f.Measured[i].Ambiguous, f.Measured[i].Ambiguity = false, ""
					return
				}
			}
		}, "construction gives"},
		{"reason added", func(f *Fixture) {
			for i := range f.Measured {
				if !f.Measured[i].Ambiguous {
					f.Measured[i].Ambiguous, f.Measured[i].Ambiguity = true, "unclear"
					return
				}
			}
		}, "construction gives"},
		{"mention on escalation", func(f *Fixture) {
			for i := range f.Measured {
				if f.Measured[i].Label == Escalation {
					f.Measured[i].Mention = Billing
					return
				}
			}
		}, "mention"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := generate(t)
			c.mutate(f)
			err := Check(f)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("Check: %v, want an error containing %q", err, c.want)
			}
		})
	}
}

func otherLabel(l string) string {
	if l == Billing {
		return Technical
	}
	return Billing
}

func TestWriteRoundTrip(t *testing.T) {
	f := generate(t)
	dir := t.TempDir()
	sums, err := f.Write(dir)
	if err != nil {
		t.Fatal(err)
	}
	var wantSums strings.Builder
	for _, c := range []struct {
		name string
		ts   []Ticket
	}{{MeasuredFile, f.Measured}, {DevelopmentFile, f.Development}} {
		b, err := os.ReadFile(filepath.Join(dir, c.name))
		if err != nil {
			t.Fatal(err)
		}
		if sha(b) != sums[c.name] {
			t.Errorf("%s: returned sum %s, file hashes to %s", c.name, sums[c.name], sha(b))
		}
		fmt.Fprintf(&wantSums, "%s  %s\n", sha(b), c.name)

		var got []Ticket
		sc := bufio.NewScanner(bytes.NewReader(b))
		for sc.Scan() {
			dec := json.NewDecoder(bytes.NewReader(sc.Bytes()))
			dec.DisallowUnknownFields()
			var tk Ticket
			if err := dec.Decode(&tk); err != nil {
				t.Fatalf("%s line %d: %v", c.name, len(got)+1, err)
			}
			got = append(got, tk)
		}
		if !reflect.DeepEqual(got, c.ts) {
			t.Errorf("%s: decoded tickets differ from the fixture", c.name)
		}
		if !bytes.HasSuffix(b, []byte("}\n")) || bytes.Count(b, []byte("\n")) != len(c.ts) {
			t.Errorf("%s: not one object per newline-terminated line", c.name)
		}
		if bytes.Contains(b, []byte(`\u00`)) {
			t.Errorf("%s: escaped characters in the JSON Lines", c.name)
		}
	}
	list, err := os.ReadFile(filepath.Join(dir, SumsFile))
	if err != nil {
		t.Fatal(err)
	}
	if string(list) != wantSums.String() {
		t.Errorf("%s:\n%s\nwant\n%s", SumsFile, list, wantSums.String())
	}
}
