package evidence

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/releasekey"
)

// RefreshedLimit is one target.limits entry as a release route's refresh
// command printed it: its value, or why the command could not read it.
// A published record carries no value of a limit marked
// config.MarkingConfidential and, on a target with such a limit, no
// unavailable reason.
type RefreshedLimit struct {
	Dimension    string  `json:"dimension"`
	Value        float64 `json:"value,omitempty"`
	Confidential bool    `json:"confidential,omitempty"`
	Unavailable  string  `json:"unavailable,omitempty"`
}

// ReadRefreshedLimits reads the limits rec, a refresh command's run, printed
// for t. The command must exit 0 and print one JSON object,
// {"limits": [...]}, and nothing else, holding one entry per target.limits
// entry in its order: {"dimension": "<the entry's>", "value": <positive
// number>} or {"dimension": "<the entry's>", "unavailable": "<reason>"}.
// The values are returned as printed, confidential ones included.
func ReadRefreshedLimits(rec *StatusRecord, t *config.Target) ([]RefreshedLimit, error) {
	fail := func(format string, a ...any) ([]RefreshedLimit, error) {
		return nil, fmt.Errorf("refreshed limits: "+format, a...)
	}
	switch {
	case rec.Error != "":
		return fail("the command failed: %s", rec.Error)
	case rec.ExitCode == nil || *rec.ExitCode != 0:
		return fail("the command did not exit 0")
	}
	var out struct {
		Limits []struct {
			Dimension   string   `json:"dimension"`
			Value       *float64 `json:"value"`
			Unavailable *string  `json:"unavailable"`
		} `json:"limits"`
	}
	dec := json.NewDecoder(bytes.NewReader(rec.Stdout.Raw()))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&out); err != nil {
		return fail("the output is not one JSON object with limits: %v", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fail("the output holds more than the JSON object")
	}
	if len(out.Limits) != len(t.Limits) {
		return fail("%d entries, target.limits holds %d", len(out.Limits), len(t.Limits))
	}
	read := make([]RefreshedLimit, len(out.Limits))
	for i, e := range out.Limits {
		l := &t.Limits[i]
		r := RefreshedLimit{Dimension: e.Dimension, Confidential: l.Marking == config.MarkingConfidential}
		switch {
		case e.Dimension != l.Dimension:
			return fail("limits[%d] is %q, target.limits[%d] %q", i, e.Dimension, i, l.Dimension)
		case (e.Value == nil) == (e.Unavailable == nil):
			return fail("limits[%d] holds neither or both of value and unavailable", i)
		case e.Unavailable != nil && strings.TrimSpace(*e.Unavailable) == "":
			return fail("limits[%d]: unavailable gives no reason", i)
		case e.Unavailable != nil:
			r.Unavailable = *e.Unavailable
		case !(*e.Value > 0) || math.IsInf(*e.Value, 0):
			return fail("limits[%d]: value %g is not a positive number", i, *e.Value)
		default:
			r.Value = *e.Value
		}
		read[i] = r
	}
	return read, nil
}

// PublishedRefresh is rec, a release route's refresh record, as it
// publishes, carrying limits without the value of a confidential one. For
// a route with a confidential limit (config.Target.Confidential) every
// limit's unavailable reason and the command's output streams are
// replaced by config.RetainedHeaderValue, and rec as run is kept in the
// retained store (WriteRetainedRefresh).
func PublishedRefresh(rec *StatusRecord, t *config.Target, limits []RefreshedLimit) *StatusRecord {
	pub := *rec
	pub.Limits = nil
	held := t.Confidential()
	for _, l := range limits {
		if l.Confidential {
			l.Value = 0
		}
		if held && l.Unavailable != "" {
			l.Unavailable = config.RetainedHeaderValue
		}
		pub.Limits = append(pub.Limits, l)
	}
	if held {
		pub.Stdout, pub.Stderr = Bytes{Text: config.RetainedHeaderValue}, Bytes{Text: config.RetainedHeaderValue}
	}
	return &pub
}

// WriteRetainedRefresh writes rec, a refresh record as run, to RefreshFile
// in dir, readable by the owner only. It refuses an existing file.
func WriteRetainedRefresh(dir string, rec *StatusRecord) error {
	b, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("evidence: %w", err)
	}
	return writeNew(filepath.Join(dir, RefreshFile), append(b, '\n'))
}

// ReadRetainedRefresh reads a refresh record as run that
// WriteRetainedRefresh wrote to RefreshFile in dir, refusing fields
// StatusRecord does not define and data after it.
func ReadRetainedRefresh(dir string) (*StatusRecord, error) {
	b, err := os.ReadFile(filepath.Join(dir, RefreshFile))
	if err != nil {
		return nil, fmt.Errorf("evidence: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var rec StatusRecord
	if err := dec.Decode(&rec); err != nil {
		return nil, fmt.Errorf("evidence: %s: %w", RefreshFile, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("evidence: %s: data after the record", RefreshFile)
	}
	return &rec, nil
}

// WindowCloseFile is a release route's window-close record, in the route's
// directory under the release output directory.
const WindowCloseFile = "window-close.json"

// WindowClose is a release route's window-close record (SPEC-HOSTED §4
// drift inside the window): the refresh command run once more after the
// route's last cell, as it publishes, and the operator file and its
// records read after it, each limits and eligibility record dated by its
// file's modification time. Copies names the directory of their retained
// copies (WindowCloseDir) from the frame store's root.
type WindowClose struct {
	SchemaVersion int              `json:"schema_version"`
	Route         string           `json:"route"`
	RecordedUTC   time.Time        `json:"recorded_utc"`
	Refresh       *StatusRecord    `json:"refresh"`
	AccountTier   string           `json:"account_tier"`
	Operator      FileRef          `json:"operator"`
	Records       []OperatorRecord `json:"records"`
	Copies        string           `json:"copies"`
}

// NewWindowClose is t's route's window-close record of refresh, as it
// publishes, and op, read after it, at recorded, with op's records keyed
// under key on a target with a confidential limit, as in a cell's
// manifest.
func NewWindowClose(t *config.Target, recorded time.Time, refresh *StatusRecord, op *Operator, key *releasekey.Key) (*WindowClose, error) {
	records, err := op.records(t, key)
	if err != nil {
		return nil, err
	}
	return &WindowClose{SchemaVersion: 1, Route: t.Name, RecordedUTC: recorded.UTC(), Refresh: refresh, AccountTier: op.AccountTier, Operator: op.File, Records: records}, nil
}

// WindowCloseDir is the retained store's directory of route's window-close
// record taken at recorded, under the frame store root: the operator
// copies and, for a route with a confidential limit, the refresh record
// as run.
func WindowCloseDir(root, route string, recorded time.Time) string {
	return filepath.Join(root, "window-close", route, recorded.UTC().Format("20060102T150405.000000000Z"))
}

// ReadWindowClose reads a window-close record, refusing fields WindowClose
// does not define and data after it.
func ReadWindowClose(path string) (*WindowClose, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("evidence: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var w WindowClose
	if err := dec.Decode(&w); err != nil {
		return nil, fmt.Errorf("evidence: %s: %w", WindowCloseFile, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("evidence: %s: data after the record", WindowCloseFile)
	}
	return &w, nil
}
