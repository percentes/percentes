package pilot

import (
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

// Shape S set hash for ShapeSSeed at testCap. A change to the system
// text, a template, a fill or a draw changes it.
const (
	testCap          = 4000
	wantShapeSSHA256 = "c39c3e79da2c5490465c504ce43afc9b585fdd1e4af2c0ba8569b0257c2447d9"
)

func shapeS(t *testing.T, byteCap int) *ShapeS {
	t.Helper()
	s, err := GenerateShapeS(ShapeSSeed, byteCap)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestShapeSIsDeterministic(t *testing.T) {
	a, b := shapeS(t, testCap), shapeS(t, testCap)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("two runs from the same seed differ")
	}
	if got := a.SHA256(); got != wantShapeSSHA256 {
		t.Errorf("set sha256 %s, want %s", got, wantShapeSSHA256)
	}
	c, err := GenerateShapeS("another-seed", testCap)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(a.Tickets, c.Tickets) {
		t.Error("a different seed gave the same tickets")
	}
	if _, err := GenerateShapeS("", testCap); err == nil {
		t.Error("empty seed accepted")
	}
	for _, bc := range []int{0, -1} {
		if _, err := GenerateShapeS(ShapeSSeed, bc); err == nil {
			t.Errorf("byte cap %d accepted", bc)
		}
	}

	f := generate(t)
	if got := sha(Encode(f.Measured)); got != wantMeasuredSHA256 {
		t.Errorf("pilot fixture changed: measured sha256 %s", got)
	}
}

func TestShapeSByteCap(t *testing.T) {
	small, large := shapeS(t, 2000), shapeS(t, testCap)
	var multibyte bool
	for _, s := range []*ShapeS{small, large} {
		if len(s.Tickets) != ShapeSTickets {
			t.Fatalf("cap %d: %d tickets", s.ByteCap, len(s.Tickets))
		}
		var longest int
		for i, tk := range s.Tickets {
			if len(tk) > s.ByteCap || !utf8.ValidString(tk) {
				t.Errorf("cap %d ticket %d: %d bytes, valid UTF-8 %v", s.ByteCap, i, len(tk), utf8.ValidString(tk))
			}
			if strings.ContainsAny(tk, "{}") {
				t.Errorf("cap %d ticket %d: unfilled placeholder", s.ByteCap, i)
			}
			multibyte = multibyte || utf8.RuneCountInString(tk) < len(tk)
			longest = max(longest, len(tk))
		}
		// The cap binds: some ticket comes within 100 bytes of it.
		if longest <= s.ByteCap-100 {
			t.Errorf("cap %d: longest ticket %d bytes", s.ByteCap, longest)
		}
	}
	if !multibyte {
		t.Error("no ticket holds a multi-byte character")
	}
	for i := range small.Tickets {
		if !strings.HasPrefix(large.Tickets[i], small.Tickets[i]) || len(small.Tickets[i]) >= len(large.Tickets[i]) {
			t.Errorf("ticket %d at cap 2000 is not a shorter prefix of the ticket at cap %d", i, testCap)
		}
	}

	_, err := GenerateShapeS(ShapeSSeed, 300)
	if err == nil || !strings.Contains(err.Error(), "header and message take") {
		t.Errorf("cap below a ticket's header and message: %v", err)
	}
}

func TestShapeSText(t *testing.T) {
	if !strings.Contains(ShapeSSystem, ShapeSInstruction) || !strings.Contains(ShapeSInstruction, "two sentences") {
		t.Errorf("system text %q lacks the two-sentence instruction", ShapeSSystem)
	}
	s := shapeS(t, testCap)
	for _, c := range []struct {
		seed  int64
		index int
		want  string
	}{
		{42, 7, "cs-42-000007 " + s.Tickets[7]},
		{42, 123456, "cs-42-123456 " + s.Tickets[123456%ShapeSTickets]},
		{7, ShapeSTickets, "cs-7-000100 " + s.Tickets[0]},
	} {
		if got := s.UserText(c.seed, c.index); got != c.want {
			t.Errorf("UserText(%d, %d) = %.40q, want %.40q", c.seed, c.index, got, c.want)
		}
	}
	for i, tk := range s.Tickets {
		for _, part := range []string{"Subject: ", "\nOrganisation: ", "\n\nWhat we have tried:\n- ", "\n\nSupport wrote:\n> ", "\n\nLog excerpt from "} {
			if !strings.Contains(tk, part) {
				t.Errorf("ticket %d lacks %q", i, part)
			}
		}
	}
}

func TestShapeSHashCoversTheSet(t *testing.T) {
	base := shapeS(t, testCap)
	for _, c := range []struct {
		name   string
		mutate func(s *ShapeS)
	}{
		{"seed", func(s *ShapeS) { s.Seed += "x" }},
		{"byte cap", func(s *ShapeS) { s.ByteCap-- }},
		{"system text", func(s *ShapeS) { s.System += " " }},
		{"ticket", func(s *ShapeS) { s.Tickets[ShapeSTickets-1] += "." }},
		{"order", func(s *ShapeS) { s.Tickets[0], s.Tickets[1] = s.Tickets[1], s.Tickets[0] }},
	} {
		s := shapeS(t, testCap)
		c.mutate(s)
		if s.SHA256() == base.SHA256() {
			t.Errorf("%s changed, hash did not", c.name)
		}
	}
}

func TestShapeSCheckRefuses(t *testing.T) {
	for _, c := range []struct {
		name   string
		mutate func(s *ShapeS)
		want   string
	}{
		{"system text", func(s *ShapeS) { s.System = "Answer in one word." }, "frozen text"},
		{"count", func(s *ShapeS) { s.Tickets = s.Tickets[1:] }, "want 100"},
		{"over the cap", func(s *ShapeS) { s.Tickets[3] = strings.Repeat("x", s.ByteCap+1) }, "cap"},
		{"repeated", func(s *ShapeS) { s.Tickets[5] = s.Tickets[4] }, "same text as ticket 4"},
		{"empty", func(s *ShapeS) { s.Tickets[6] = "" }, "empty"},
		{"invalid UTF-8", func(s *ShapeS) { s.Tickets[7] = "\xff" }, "invalid UTF-8"},
		{"no seed", func(s *ShapeS) { s.Seed = "" }, "empty seed"},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := shapeS(t, testCap)
			c.mutate(s)
			if err := s.Check(); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("Check: %v, want an error containing %q", err, c.want)
			}
		})
	}
}
