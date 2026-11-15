package pilot

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/percentes/percentes/internal/config"
)

// Shape is a release request shape's frozen set: what every request of a
// route, its ceiling check's included, sends as user text, and the hash
// that freezes it.
type Shape interface {
	// UserText is request index's user text under the request-reference
	// seed.
	UserText(seed int64, index int) string
	SHA256() string
	Check() error
}

// NewShape builds the frozen set of the release shape name: Shape S from
// ShapeSSeed with every ticket at most byteCap bytes, or Shape R, whose
// texts take no byte cap, so byteCap must be 0.
func NewShape(name string, byteCap int) (Shape, error) {
	switch name {
	case config.ShapeS:
		s, err := GenerateShapeS(ShapeSSeed, byteCap)
		if err != nil {
			return nil, err
		}
		return s, nil
	case config.ShapeR:
		if byteCap != 0 {
			return nil, fmt.Errorf("pilot: shape r: byte cap %d; the shape has no ticket to cut", byteCap)
		}
		s, err := GenerateShapeR()
		if err != nil {
			return nil, err
		}
		return s, nil
	}
	return nil, fmt.Errorf("pilot: no release request shape %q", name)
}

// Shape R (SPEC-HOSTED HA4): the frozen one-line instruction after each
// request's reference, under the frozen system text.
const (
	ShapeRInstruction = "Reply with the single word: ready."
	ShapeRSystem      = "You answer requests sent to a software service. The user message is a request reference, a space, and one instruction. " +
		"Follow the instruction and reply in plain text."
)

// ShapeR is Shape R's frozen set: the system text and the instruction.
type ShapeR struct {
	System      string `json:"system"`
	Instruction string `json:"instruction"`
}

// GenerateShapeR builds Shape R's set and checks it.
func GenerateShapeR() (*ShapeR, error) {
	s := &ShapeR{System: ShapeRSystem, Instruction: ShapeRInstruction}
	if err := s.Check(); err != nil {
		return nil, err
	}
	return s, nil
}

// Check verifies a set: the frozen system text and instruction.
func (s *ShapeR) Check() error {
	var errs []error
	if s.System != ShapeRSystem {
		errs = append(errs, errors.New("pilot: shape r: system text differs from the frozen text"))
	}
	if s.Instruction != ShapeRInstruction {
		errs = append(errs, errors.New("pilot: shape r: instruction differs from the frozen text"))
	}
	return errors.Join(errs...)
}

// UserText is request index's user text: the prefix cs-<seed>-<index>
// with a six-digit index, a space, and the instruction.
func (s *ShapeR) UserText(seed int64, index int) string {
	return fmt.Sprintf("cs-%d-%06d %s", seed, index, s.Instruction)
}

// SHA256 is the hex SHA-256 of the set's JSON encoding, HTML escaping off.
func (s *ShapeR) SHA256() string {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
	sum := sha256.Sum256(b.Bytes())
	return hex.EncodeToString(sum[:])
}
