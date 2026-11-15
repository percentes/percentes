package pilot

import (
	"reflect"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/config"
)

// Shape R set hash. A change to the system text or the instruction
// changes it.
const wantShapeRSHA256 = "70c4683ee69013c59c535c86f1173bba24f31f5bbbe6be5bd0259a118470511c"

func TestShapeR(t *testing.T) {
	s, err := GenerateShapeR()
	if err != nil {
		t.Fatal(err)
	}
	if got := s.SHA256(); got != wantShapeRSHA256 {
		t.Errorf("set sha256 %s, want %s", got, wantShapeRSHA256)
	}
	if strings.ContainsAny(ShapeRInstruction, "\r\n") || !strings.Contains(ShapeRSystem, "one instruction") {
		t.Errorf("instruction %q, system text %q", ShapeRInstruction, ShapeRSystem)
	}
	for _, c := range []struct {
		seed  int64
		index int
		want  string
	}{
		{42, 7, "cs-42-000007 " + ShapeRInstruction},
		{42, 123456, "cs-42-123456 " + ShapeRInstruction},
		{16, 0, "cs-16-000000 " + ShapeRInstruction},
	} {
		if got := s.UserText(c.seed, c.index); got != c.want {
			t.Errorf("UserText(%d, %d) = %q, want %q", c.seed, c.index, got, c.want)
		}
	}
	for _, c := range []struct {
		name   string
		mutate func(s *ShapeR)
		want   string
	}{
		{"system text", func(s *ShapeR) { s.System += " " }, "system text differs"},
		{"instruction", func(s *ShapeR) { s.Instruction = "Reply with two words." }, "instruction differs"},
	} {
		m := *s
		c.mutate(&m)
		if err := m.Check(); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: Check %v, want an error containing %q", c.name, err, c.want)
		}
		if m.SHA256() == s.SHA256() {
			t.Errorf("%s changed, hash did not", c.name)
		}
	}
}

// The factory builds each release shape's set with its own byte cap
// rule; Shape S is the set GenerateShapeS builds.
func TestNewShape(t *testing.T) {
	sh, err := NewShape(config.ShapeS, testCap)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sh, shapeS(t, testCap)) || sh.SHA256() != wantShapeSSHA256 {
		t.Errorf("Shape S from the factory differs from GenerateShapeS")
	}
	r, err := NewShape(config.ShapeR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.SHA256() != wantShapeRSHA256 || r.UserText(1, 2) != "cs-1-000002 "+ShapeRInstruction {
		t.Errorf("Shape R from the factory: %s, %q", r.SHA256(), r.UserText(1, 2))
	}
	for _, c := range []struct {
		name    string
		byteCap int
		want    string
	}{
		{config.ShapeS, 0, "byte cap 0"},
		{config.ShapeR, testCap, "shape r: byte cap 4000"},
		{"L", 0, `no release request shape "L"`},
	} {
		sh, err := NewShape(c.name, c.byteCap)
		if err == nil || !strings.Contains(err.Error(), c.want) || sh != nil {
			t.Errorf("NewShape(%q, %d) = %v, %v; want a nil shape and an error containing %q", c.name, c.byteCap, sh, err, c.want)
		}
	}
}
