package evidence

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/redact"
	"github.com/percentes/percentes/internal/releasekey"
	"github.com/percentes/percentes/internal/run"
	"github.com/percentes/percentes/internal/validity"
)

// ManifestFile is the run manifest's file name in a run's output
// directory.
const ManifestFile = "manifest.json"

// FileRef names a file and its content's SHA-256 and size or, keyed, its
// content's HMAC-SHA256 under the release key alone: the form of a file
// that can hold a value a route with a confidential limit withholds.
type FileRef struct {
	Name       string `json:"name"`
	SHA256     string `json:"sha256,omitempty"`
	HMACSHA256 string `json:"hmac_sha256,omitempty"`
	// Bytes is the size in decimal digits; a keyed reference has none.
	Bytes json.Number `json:"bytes,omitempty"`
}

// Keyed reports whether ref names its file by HMAC-SHA256.
func (ref FileRef) Keyed() bool {
	return ref.HMACSHA256 != ""
}

func refOf(name string, b []byte) FileRef {
	sum := sha256.Sum256(b)
	return FileRef{Name: name, SHA256: hex.EncodeToString(sum[:]), Bytes: json.Number(strconv.Itoa(len(b)))}
}

func keyedRefOf(name string, b []byte, key *releasekey.Key) FileRef {
	return FileRef{Name: name, HMACSHA256: key.Sum(b)}
}

// HashFile returns the reference of the file at path under name.
func HashFile(path, name string) (FileRef, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return FileRef{}, fmt.Errorf("evidence: %w", err)
	}
	return refOf(name, b), nil
}

// Manifest is the SPEC-HOSTED §9 run manifest, with the HA2 §9 hashes and
// roles. Every file it names is referenced by SHA-256 and size, except
// that on a target with a confidential limit the frame store, the
// operator file and its records are keyed (FileRef); paths are relative
// to the output directory, the frame store root, or the operator file's
// directory.
type Manifest struct {
	SchemaVersion int `json:"schema_version"`
	// StartUTC is the run epoch; for a release cell whose ladder never
	// started, or an interrupted one, the cell's start.
	StartUTC   time.Time      `json:"start_utc"`
	WrittenUTC time.Time      `json:"written_utc"`
	Profile    config.Profile `json:"profile"`
	Cell       string         `json:"cell,omitempty"`
	Seed       int64          `json:"seed"`
	Source     Source         `json:"source"`
	// Capture is what the build keeps of each response body.
	Capture loadgen.Capture `json:"capture"`
	// Release is a release run's cell record.
	Release *Cell `json:"release,omitempty"`
	// Config is the configuration file as published, verbatim
	// (config.Config.Published).
	Config   FileRef  `json:"config"`
	Endpoint Endpoint `json:"endpoint"`
	Client   Client   `json:"client"`
	Clock    ClockRef `json:"clock"`
	// StatusStart and StatusEnd are the endpoint-status records.
	StatusStart FileRef          `json:"status_start"`
	StatusEnd   *FileRef         `json:"status_end,omitempty"`
	Operator    FileRef          `json:"operator"`
	Roles       *Roles           `json:"roles,omitempty"`
	Records     []OperatorRecord `json:"records"`
	Ledger      *LedgerRef       `json:"ledger,omitempty"`
	// BalanceStart and BalanceEnd are the account balance or quota
	// records, read at a release cell's start and end.
	BalanceStart *FileRef `json:"balance_start,omitempty"`
	BalanceEnd   *FileRef `json:"balance_end,omitempty"`
	// Artifacts are the run's other published files.
	Artifacts []FileRef `json:"artifacts"`
	// Frames is the raw-frame store file, named from the store's root.
	Frames         *FileRef `json:"frames,omitempty"`
	RunValid       bool     `json:"run_valid"`
	InvalidReasons []string `json:"invalid_reasons,omitempty"`
	// Gates are the G2 and G6 results; a release cell's are per ladder
	// step (releaseGates).
	Gates []validity.Gate `json:"gates,omitempty"`
	// Replay is what the report pair reads beside the config and the
	// attempts file; absent for a run that ended in an error and for a
	// release cell, whose pair reads its cell record.
	Replay *run.Record `json:"replay,omitempty"`
}

// Artifact returns the reference of the published file name, or nil.
func (m *Manifest) Artifact(name string) *FileRef {
	for i := range m.Artifacts {
		if m.Artifacts[i].Name == name {
			return &m.Artifacts[i]
		}
	}
	return nil
}

// ReadManifest reads a manifest file, refusing fields Manifest does not
// define and data after it.
func ReadManifest(path string) (*Manifest, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("evidence: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var m Manifest
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("evidence: %s: %w", ManifestFile, err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("evidence: %s: data after the manifest", ManifestFile)
	}
	return &m, nil
}

// Source is the instrument build. Diff is the operator's source_diff
// record.
type Source struct {
	Revision string   `json:"revision"`
	Modified bool     `json:"modified"`
	VCSTime  string   `json:"vcs_time,omitempty"`
	Diff     *FileRef `json:"diff,omitempty"`
}

// Endpoint is the target as configured and as the run observed it.
// MaxTokens is the single-target body's ceiling; a pilot target's ceiling
// is in Params.
type Endpoint struct {
	Provider        string         `json:"provider"`
	AccountTier     string         `json:"account_tier"`
	Name            string         `json:"name,omitempty"`
	BaseURL         string         `json:"base_url,omitempty"`
	EndpointURL     string         `json:"endpoint_url,omitempty"`
	RequestedModel  string         `json:"requested_model"`
	ExpectedModel   string         `json:"expected_model,omitempty"`
	ReportedModels  []string       `json:"reported_models,omitempty"`
	ModelMismatches int            `json:"model_mismatches,omitempty"`
	SplitSeries     bool           `json:"split_series,omitempty"`
	Params          *config.Params `json:"params,omitempty"`
	MaxTokens       int            `json:"max_tokens,omitempty"`
	NotApplied      []string       `json:"not_applied,omitempty"`
	// Protocols counts responses by negotiated protocol.
	Protocols map[string]int `json:"protocols"`
}

// Client is the client host: the operator's description of it and the
// build and machine the binary ran on.
type Client struct {
	Host       string `json:"host"`
	OS         string `json:"os"`
	Arch       string `json:"arch"`
	CPUs       int    `json:"cpus"`
	GoVersion  string `json:"go_version"`
	CgoEnabled string `json:"cgo_enabled,omitempty"`
}

// ClockRef is the clock-offset record. ReadBeforeEpochNs is the time from
// the clock-sync command's end to the run epoch on the host's wall clock,
// negative where the epoch is a release cell's start before the clock
// was read.
type ClockRef struct {
	File              FileRef  `json:"file"`
	AbsOffsetS        *float64 `json:"abs_offset_s,omitempty"`
	Flag              string   `json:"flag,omitempty"`
	ReadBeforeEpochNs int64    `json:"read_before_epoch_ns"`
}

// LedgerRef is the admission ledger as read at run start: its hash, size
// and line count.
type LedgerRef struct {
	FileRef
	Lines int `json:"lines"`
}

// ReadLedger reads the ledger file at path.
func ReadLedger(path string) (*LedgerRef, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("evidence: ledger: %w", err)
	}
	return &LedgerRef{FileRef: refOf(filepath.Base(path), b), Lines: bytes.Count(b, []byte("\n"))}, nil
}

// SourceOf reads the revision, modified flag and commit time from a
// build's version-control stamp.
func SourceOf(info *debug.BuildInfo) Source {
	var s Source
	if info != nil {
		for _, kv := range info.Settings {
			switch kv.Key {
			case "vcs.revision":
				s.Revision = kv.Value
			case "vcs.modified":
				s.Modified = kv.Value == "true"
			case "vcs.time":
				s.VCSTime = kv.Value
			}
		}
	}
	return s
}

// CheckSource reports an error when info carries no revision, or when
// the build is modified and op has no source_diff record.
func CheckSource(info *debug.BuildInfo, op *Operator) error {
	s := SourceOf(info)
	if s.Revision == "" {
		return errors.New("evidence: the binary carries no version-control revision (build it inside a git checkout; go run needs -buildvcs=true)")
	}
	if s.Modified && op.find(RecordSourceDiff) == nil {
		return fmt.Errorf("evidence: the binary was built from a modified tree at %s and the operator file has no %s record", s.Revision, RecordSourceDiff)
	}
	return nil
}

// ManifestInput is what a run's manifest is built from. The paths name
// files already written; Gates and Replay are nil for a run that ended in
// an error and for a release cell, whose gates come from Result.
type ManifestInput struct {
	Config *config.Config
	// ConfigName names the configuration file.
	ConfigName string
	Target     *config.Target
	Result     *loadgen.Result
	Build      *debug.BuildInfo
	Operator   *Operator
	Ledger     *LedgerRef
	Clock      *ClockRecord

	ClockPath       string
	StatusStartPath string
	// StatusEndPath is empty when the end status was not recorded.
	StatusEndPath string
	ArtifactPaths []string
	// BalanceStartPath and BalanceEndPath are the balance records; the
	// start record is required under the release profile, and either is
	// empty when not recorded.
	BalanceStartPath, BalanceEndPath string
	// FramesPath is empty when no frame store was written.
	FramesRoot, FramesPath string

	Gates          *validity.Report
	RunValid       bool
	InvalidReasons []string
	Replay         *run.Record
	// Release is the cell record, required under the release profile and
	// refused under any other.
	Release *Cell
	// Key is the release key, which a target with a confidential limit
	// requires.
	Key *releasekey.Key
}

// NewManifest builds the manifest of one hosted run. On a target with a
// confidential limit the frame store, the operator file and its records
// are keyed under in.Key.
func NewManifest(in ManifestInput) (*Manifest, error) {
	cfg, t, res, op := in.Config, in.Target, in.Result, in.Operator
	opFile, records, err := op.published(t, in.Key)
	if err != nil {
		return nil, err
	}
	var gates []validity.Gate
	switch release := cfg.Profile == config.ProfileRelease && cfg.Release != nil; {
	case release && in.Release == nil:
		return nil, errors.New("evidence: a release run's manifest needs its cell record")
	case !release && in.Release != nil:
		return nil, fmt.Errorf("evidence: a cell record under the %q profile", cfg.Profile)
	case release && in.BalanceStartPath == "":
		return nil, errors.New("evidence: a release run's manifest needs its start balance record")
	case release && in.Gates != nil:
		return nil, errors.New("evidence: a release run's gates come from its ladder, not a gate report")
	case release:
		gates = releaseGates(cfg, res)
		if err := checkCell(cfg, t, in.Release); err != nil {
			return nil, err
		}
		if err := in.Release.checkLabel(in.RunValid, in.InvalidReasons, gates, res.ModelMismatches); err != nil {
			return nil, err
		}
		if !in.Release.LadderStarted && len(res.Requests) > 0 {
			return nil, fmt.Errorf("evidence: a release cell whose ladder never started has %d requests", len(res.Requests))
		}
		if in.Release.Outcome == CellInterrupted && len(res.Requests) > 0 {
			return nil, fmt.Errorf("evidence: an interrupted release cell has %d requests", len(res.Requests))
		}
	}
	published, err := cfg.Published()
	if err != nil {
		return nil, fmt.Errorf("evidence: %w", err)
	}
	m := &Manifest{
		SchemaVersion:  1,
		StartUTC:       res.EpochWall.UTC(),
		WrittenUTC:     time.Now().UTC(),
		Profile:        cfg.Profile,
		Cell:           op.Cell,
		Seed:           cfg.Run.Seed,
		Source:         SourceOf(in.Build),
		Capture:        loadgen.CaptureLimits(),
		Release:        in.Release,
		Config:         refOf(in.ConfigName, published),
		Operator:       opFile,
		Roles:          op.Roles,
		Records:        records,
		Ledger:         in.Ledger,
		Artifacts:      []FileRef{},
		RunValid:       in.RunValid,
		InvalidReasons: in.InvalidReasons,
		Gates:          gates,
		Replay:         in.Replay,
	}
	for _, r := range records {
		if r.Kind == RecordSourceDiff {
			ref := r.FileRef
			m.Source.Diff = &ref
			break
		}
	}
	m.Endpoint = Endpoint{
		Provider:        op.Provider,
		AccountTier:     op.AccountTier,
		Name:            t.Name,
		BaseURL:         redact.URL(t.BaseURL),
		EndpointURL:     redact.URL(t.EndpointURL),
		RequestedModel:  t.ModelName,
		ExpectedModel:   t.ExpectedModel(),
		ReportedModels:  res.ReportedModels,
		ModelMismatches: res.ModelMismatches,
		SplitSeries:     res.SplitSeries,
		Params:          t.Params,
		NotApplied:      t.NotApplied,
		Protocols:       map[string]int{},
	}
	if t.Params == nil {
		m.Endpoint.MaxTokens = cfg.Load.MaxTokens
	}
	for i := range res.Requests {
		if p := res.Requests[i].Proto; p != "" {
			m.Endpoint.Protocols[p]++
		}
	}
	m.Client = Client{Host: op.ClientHost, OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU(), GoVersion: runtime.Version()}
	if in.Build != nil {
		for _, kv := range in.Build.Settings {
			if kv.Key == "CGO_ENABLED" {
				m.Client.CgoEnabled = kv.Value
			}
		}
	}
	if m.Clock.File, err = HashFile(in.ClockPath, filepath.Base(in.ClockPath)); err != nil {
		return nil, err
	}
	m.Clock.AbsOffsetS, m.Clock.Flag = in.Clock.AbsOffsetS, in.Clock.Flag
	m.Clock.ReadBeforeEpochNs = res.EpochWall.Round(0).Sub(in.Clock.EndUTC).Nanoseconds()
	if m.StatusStart, err = HashFile(in.StatusStartPath, filepath.Base(in.StatusStartPath)); err != nil {
		return nil, err
	}
	if in.StatusEndPath != "" {
		ref, err := HashFile(in.StatusEndPath, filepath.Base(in.StatusEndPath))
		if err != nil {
			return nil, err
		}
		m.StatusEnd = &ref
	}
	if m.BalanceStart, err = hashOptional(in.BalanceStartPath); err != nil {
		return nil, err
	}
	if m.BalanceEnd, err = hashOptional(in.BalanceEndPath); err != nil {
		return nil, err
	}
	for _, p := range in.ArtifactPaths {
		ref, err := HashFile(p, filepath.Base(p))
		if err != nil {
			return nil, err
		}
		m.Artifacts = append(m.Artifacts, ref)
	}
	if in.FramesPath != "" {
		rel, err := filepath.Rel(in.FramesRoot, in.FramesPath)
		if err != nil {
			return nil, fmt.Errorf("evidence: %w", err)
		}
		b, err := os.ReadFile(in.FramesPath)
		if err != nil {
			return nil, fmt.Errorf("evidence: %w", err)
		}
		ref := refOf(filepath.ToSlash(rel), b)
		if t.Confidential() {
			ref = keyedRefOf(ref.Name, b, in.Key)
		}
		m.Frames = &ref
	}
	if in.Gates != nil {
		for _, g := range in.Gates.Gates {
			if g.ID == "G2" || g.ID == "G6" {
				m.Gates = append(m.Gates, g)
			}
		}
	}
	return m, nil
}

// releaseGates are a release cell's G2 and hosted-G6 results: G2 over
// the whole ladder, then at each attempted step, then hosted-G6 at each
// step its records count (ReleaseG6). A run a stop interrupted has no
// whole-ladder G2, since the requests it left unsent fail that gate and
// its steps' verdicts stand alone. A cell whose ladder never ran has
// none.
func releaseGates(cfg *config.Config, res *loadgen.Result) []validity.Gate {
	l := res.Ladder
	if l == nil {
		return nil
	}
	var out []validity.Gate
	if res.Interrupted == nil {
		out = append(out, validity.G2(0, &res.Gates))
	}
	for k := range l.Steps {
		if l.Steps[k].Attempted {
			out = append(out, validity.G2(k+1, l.Steps[k].Gates))
		}
	}
	recs := make([]Record, len(res.Requests))
	for i := range res.Requests {
		recs[i] = record(&res.Requests[i], cfg, nil)
	}
	return append(out, ReleaseG6(recs, cfg.Release.Shape.DeadlineS)...)
}

// ReleaseG6 is hosted-G6 at each step of a release cell's records, in
// step order: the steps with measured records up to the last step that
// dispatched one, which the cell's per-step table counts as attempted.
// Warm-up records are left out.
func ReleaseG6(recs []Record, deadlineS int) []validity.Gate {
	var scheduled, inTime []int
	last := 0
	for i := range recs {
		r := &recs[i]
		if r.Warmup || r.Step < 1 {
			continue
		}
		for len(scheduled) < r.Step {
			scheduled, inTime = append(scheduled, 0), append(inTime, 0)
		}
		scheduled[r.Step-1]++
		if r.InTime {
			inTime[r.Step-1]++
		}
		if r.DispatchNs != 0 {
			last = max(last, r.Step)
		}
	}
	var out []validity.Gate
	for k := 0; k < last; k++ {
		if scheduled[k] > 0 {
			out = append(out, validity.ReleaseG6(k+1, inTime[k], scheduled[k], deadlineS))
		}
	}
	return out
}

// hashOptional returns the reference of the file at path under its base
// name, or nil for an empty path.
func hashOptional(path string) (*FileRef, error) {
	if path == "" {
		return nil, nil
	}
	ref, err := HashFile(path, filepath.Base(path))
	if err != nil {
		return nil, err
	}
	return &ref, nil
}

// WriteManifest writes m to ManifestFile in dir and returns the file's
// path.
func WriteManifest(dir string, m *Manifest) (string, error) {
	return writeJSON(dir, ManifestFile, m)
}

// writeJSON writes v, indented, to name in dir and returns the file's
// path.
func writeJSON(dir, name string, v any) (string, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return "", fmt.Errorf("evidence: %s: %w", name, err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("evidence: %w", err)
	}
	path := filepath.Join(dir, name)
	if err := ReplaceFile(path, b.Bytes()); err != nil {
		return "", fmt.Errorf("evidence: %w", err)
	}
	return path, nil
}

// ReplaceFile writes b to path through a temporary file in the same
// directory, synced and renamed into place, so a crash leaves the old
// file or the new one. A failed write removes the temporary file.
func ReplaceFile(path string, b []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		_ = os.Remove(tmp)
	}
	return err
}
