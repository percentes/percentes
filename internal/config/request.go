package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Values of target.auth_header. AuthBearer sends "Authorization: Bearer
// KEY"; the other two send the key alone in the header they name.
const (
	AuthBearer  = "authorization_bearer"
	AuthXAPIKey = "x-api-key"
	AuthAPIKey  = "api-key"
)

var authHeaders = []string{AuthBearer, AuthXAPIKey, AuthAPIKey}

// KeyHeader returns the one header that carries key to t.
func (t *Target) KeyHeader(key string) (name, value string) {
	switch t.AuthHeader {
	case AuthXAPIKey:
		return "X-Api-Key", key
	case AuthAPIKey:
		return "Api-Key", key
	}
	return "Authorization", "Bearer " + key
}

// validateTargetRequest checks auth_header and refuses body: none of
// these profiles sends a literal body.
func validateTargetRequest(v *validator, field string, profile Profile, t *Target) {
	switch {
	case t.AuthHeader == "":
	case !contains(authHeaders, t.AuthHeader):
		v.errf("%s.auth_header: must be one of %q, got %q", field, authHeaders, t.AuthHeader)
	case t.APIKeyEnv == "":
		v.errf("%s.auth_header: set without api_key_env", field)
	}
	if t.Body != "" {
		v.errf("%s.body: a literal request body is not accepted under the %q profile", field, profile)
	}
}

// LiteralBody returns body with PilotCaseSlot replaced by the
// JSON-encoded case text. Every other byte is unchanged.
func LiteralBody(body, caseText string) []byte {
	return bytes.Replace([]byte(body), []byte(PilotCaseSlot), jsonString(caseText), 1)
}

// CheckLiteralBody reports why body is not a literal request body: a JSON
// object with no key repeated in one object, "stream" true, and
// PilotCaseSlot once, as one whole string value.
func CheckLiteralBody(body string) error {
	raw := []byte(body)
	if !json.Valid(raw) {
		return errors.New("not valid JSON")
	}
	if k, err := duplicateKey(json.NewDecoder(bytes.NewReader(raw))); err != nil {
		return err
	} else if k != "" {
		return fmt.Errorf("key %q repeated in one object", k)
	}
	var top map[string]json.RawMessage
	if json.Unmarshal(raw, &top) != nil {
		return errors.New("not a JSON object")
	}
	if string(top["stream"]) != "true" {
		return errors.New("\"stream\" must be true")
	}
	if n := strings.Count(body, PilotCaseSlot); n != 1 {
		return fmt.Errorf("%s must appear once, got %d", PilotCaseSlot, n)
	}
	const marker = "case \"text\"   marker"
	var tree any
	if json.Unmarshal(LiteralBody(body, marker), &tree) != nil || countString(tree, marker) != 1 {
		return fmt.Errorf("%s must be one whole string value", PilotCaseSlot)
	}
	return nil
}

// countString counts the string values in v equal to s; object keys are
// not values.
func countString(v any, s string) int {
	switch x := v.(type) {
	case string:
		if x == s {
			return 1
		}
	case []any:
		n := 0
		for _, e := range x {
			n += countString(e, s)
		}
		return n
	case map[string]any:
		n := 0
		for _, e := range x {
			n += countString(e, s)
		}
		return n
	}
	return 0
}
