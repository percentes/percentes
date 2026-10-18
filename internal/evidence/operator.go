package evidence

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"time"
)

// Operator record kinds. The required kinds are the records SPEC-HOSTED §9
// lists for every hosted run; the others are optional.
const (
	RecordLimits           = "limits"
	RecordPrices           = "prices"
	RecordQuota            = "quota"
	RecordTerms            = "terms"
	RecordStatusMapping    = "status_mapping"
	RecordEligibility      = "eligibility"
	RecordDedicatedAccount = "dedicated_account"
	RecordAffiliation      = "affiliation"
	// RecordSourceDiff is the working tree's diff against the build's
	// revision, required when the build is modified.
	RecordSourceDiff = "source_diff"
	RecordCases      = "cases"
	RecordLabels     = "labels"
	RecordRubric     = "rubric"
	RecordScorer     = "scorer"
	RecordPrompt     = "prompt"
	RecordProbe      = "probe"
	RecordSchedule   = "schedule"
	// RecordNotice is a notice sent to the provider with its tables and
	// evidence pack, RecordReplicationPack the AWS replication pack's
	// send, and RecordDisclosure an email or form that described the work
	// to the provider before the notices (SPEC-HOSTED HA3 points 14 and
	// 15).
	RecordNotice          = "notice"
	RecordReplicationPack = "replication_pack"
	RecordDisclosure      = "disclosure"
)

var (
	requiredRecords = []string{RecordLimits, RecordPrices, RecordQuota, RecordTerms, RecordStatusMapping, RecordEligibility, RecordDedicatedAccount, RecordAffiliation}
	optionalRecords = []string{RecordSourceDiff, RecordCases, RecordLabels, RecordRubric, RecordScorer, RecordPrompt, RecordProbe, RecordSchedule, RecordNotice, RecordReplicationPack, RecordDisclosure}
	// urlRecords are the kinds taken from provider documentation.
	urlRecords = []string{RecordLimits, RecordPrices, RecordTerms, RecordStatusMapping}
)

// The retained copies' names in a run's frame directory.
const (
	OperatorCopyFile = "operator.json"
	RecordsCopyDir   = "records"
)

// Operator is a run's operator file, read and checked before the run, with
// every record file it names read and hashed then.
type Operator struct {
	Provider    string
	AccountTier string
	ClientHost  string
	Cell        string
	Roles       *Roles
	Records     []OperatorRecord
	// File is the operator file itself.
	File FileRef
	raw  []byte
}

// Roles are the HA2 pilot roles: who chose the task, who supplied the
// cases and labels, who wrote the rubric, and the interest of the person
// who named the pair.
type Roles struct {
	Task         string `json:"task"`
	CasesLabels  string `json:"cases_labels"`
	Rubric       string `json:"rubric"`
	PairInterest string `json:"pair_interest"`
}

// OperatorRecord is one operator record: a file, named relative to the
// operator file's directory, and its content hash as read before the run.
type OperatorRecord struct {
	Kind string `json:"kind"`
	FileRef
	URL       string `json:"url,omitempty"`
	Retrieved string `json:"retrieved"`
	Note      string `json:"note,omitempty"`
	data      []byte
}

// operatorFile is the operator file's JSON form.
type operatorFile struct {
	Provider    string `json:"provider"`
	AccountTier string `json:"account_tier"`
	ClientHost  string `json:"client_host"`
	Cell        string `json:"cell"`
	Roles       *Roles `json:"roles"`
	Records     []struct {
		Kind      string `json:"kind"`
		Path      string `json:"path"`
		URL       string `json:"url"`
		Retrieved string `json:"retrieved"`
		Note      string `json:"note"`
	} `json:"records"`
}

// LoadOperator strictly decodes the operator file at path and reads every
// record file it names. provider, account_tier and client_host are
// required, and so is a record of each required kind; a record's path is
// local to the file's directory, its retrieved value is a date or an RFC
// 3339 time, and the kinds taken from documentation carry an http or
// https url.
func LoadOperator(path string) (*Operator, error) {
	if path == "" {
		return nil, errors.New("evidence: no operator file")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("evidence: operator file: %w", err)
	}
	fail := func(format string, a ...any) (*Operator, error) {
		return nil, fmt.Errorf("evidence: operator file %s: "+format, append([]any{path}, a...)...)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var f operatorFile
	if err := dec.Decode(&f); err != nil {
		return fail("%v", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return fail("data after the JSON object")
	}
	switch {
	case f.Provider == "":
		return fail("provider: required")
	case f.AccountTier == "":
		return fail("account_tier: required")
	case f.ClientHost == "":
		return fail("client_host: required")
	}
	if r := f.Roles; r != nil && (r.Task == "" || r.CasesLabels == "" || r.Rubric == "" || r.PairInterest == "") {
		return fail("roles: task, cases_labels, rubric and pair_interest are each required")
	}
	op := &Operator{Provider: f.Provider, AccountTier: f.AccountTier, ClientHost: f.ClientHost, Cell: f.Cell, Roles: f.Roles, File: refOf(filepath.Base(path), raw), raw: raw}
	dir := filepath.Dir(path)
	for i, in := range f.Records {
		field := fmt.Sprintf("records[%d]", i)
		switch {
		case !slices.Contains(requiredRecords, in.Kind) && !slices.Contains(optionalRecords, in.Kind):
			return fail("%s.kind: unknown kind %q", field, in.Kind)
		case in.Path == "" || !filepath.IsLocal(in.Path):
			return fail("%s.path: %q is not a path inside the operator file's directory", field, in.Path)
		case !validRetrieved(in.Retrieved):
			return fail("%s.retrieved: %q is not a date (2006-01-02) or an RFC 3339 time", field, in.Retrieved)
		case slices.Contains(urlRecords, in.Kind) && !validDocURL(in.URL):
			return fail("%s.url: an http or https URL is required for %s", field, in.Kind)
		case in.URL != "" && !validDocURL(in.URL):
			return fail("%s.url: %q is not an http or https URL", field, in.URL)
		}
		name := filepath.Clean(in.Path)
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return fail("%s: %v", field, err)
		}
		op.Records = append(op.Records, OperatorRecord{Kind: in.Kind, FileRef: refOf(filepath.ToSlash(name), data), URL: in.URL, Retrieved: in.Retrieved, Note: in.Note, data: data})
	}
	for _, kind := range requiredRecords {
		if op.find(kind) == nil {
			return fail("no %s record", kind)
		}
	}
	if n := op.count(RecordSourceDiff); n > 1 {
		return fail("%d %s records, want at most 1", n, RecordSourceDiff)
	}
	return op, nil
}

func (op *Operator) find(kind string) *OperatorRecord {
	for i := range op.Records {
		if op.Records[i].Kind == kind {
			return &op.Records[i]
		}
	}
	return nil
}

func (op *Operator) count(kind string) int {
	n := 0
	for i := range op.Records {
		if op.Records[i].Kind == kind {
			n++
		}
	}
	return n
}

func validRetrieved(s string) bool {
	if _, err := time.Parse(time.DateOnly, s); err == nil {
		return true
	}
	_, err := time.Parse(time.RFC3339, s)
	return err == nil
}

func validDocURL(s string) bool {
	u, err := url.Parse(s)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

// WriteCopies writes the operator file and the record bytes read by
// LoadOperator into dir, as OperatorCopyFile and under RecordsCopyDir at
// each record's path, readable by the owner only. It refuses an existing
// file.
func (op *Operator) WriteCopies(dir string) error {
	if err := writeNew(filepath.Join(dir, OperatorCopyFile), op.raw); err != nil {
		return err
	}
	done := map[string]bool{}
	for _, r := range op.Records {
		if done[r.Name] {
			continue
		}
		done[r.Name] = true
		if err := writeNew(filepath.Join(dir, RecordsCopyDir, filepath.FromSlash(r.Name)), r.data); err != nil {
			return err
		}
	}
	return nil
}

func writeNew(path string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("evidence: %w", err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("evidence: %w", err)
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return fmt.Errorf("evidence: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("evidence: %w", err)
	}
	return nil
}
