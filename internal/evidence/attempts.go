// Package evidence writes a run's per-attempt records and its raw-frame
// store (SPEC-HOSTED §9): JSON Lines files, one JSON object per scheduled
// request, in index order, for every run that reached load generation,
// including a run that failed a run-level check. It also records the
// client clock offset read at run start, the endpoint status at run start
// and end, and the run manifest.
package evidence

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/score"
)

// AttemptsFile is the per-attempt file's name in a run's output
// directory.
const AttemptsFile = "attempts.jsonl"

// Record is one scheduled request. Times are nanosecond offsets from the
// run epoch on the monotonic clock; zero means it never happened, so a
// record with dispatch_ns 0 and no outcome was never sent.
type Record struct {
	Index      int64           `json:"index"`
	IntendedNs int64           `json:"intended_ns"`
	DispatchNs int64           `json:"dispatch_ns"`
	FirstTokNs int64           `json:"first_token_ns"`
	DoneNs     int64           `json:"done_ns"`
	Outcome    loadgen.Outcome `json:"outcome"`
	ErrClass   string          `json:"err_class,omitempty"`
	// Shape and CeilingTokens are a release request's shape and output
	// ceiling, Step its 1-based ladder step and Warmup whether it was sent
	// in the warm-up (SPEC-HOSTED HA3 point 15). InTime and Late split its
	// completions at the deadline.
	Shape         string `json:"shape,omitempty"`
	CeilingTokens int    `json:"ceiling_tokens,omitempty"`
	Step          int    `json:"step,omitempty"`
	Warmup        bool   `json:"warmup,omitempty"`
	InTime        bool   `json:"in_time,omitempty"`
	Late          bool   `json:"late,omitempty"`
	// Protocol and Status are absent when no response arrived.
	Protocol string `json:"protocol,omitempty"`
	Status   int    `json:"status,omitempty"`
	// RetryAfter is the first Retry-After value among Headers, the
	// response headers target.response_headers allows.
	RetryAfter string              `json:"retry_after,omitempty"`
	Headers    map[string][]string `json:"headers,omitempty"`
	ErrText    string              `json:"err_text,omitempty"`
	PeerAddr   string              `json:"peer_addr,omitempty"`
	ConnReused bool                `json:"conn_reused,omitempty"`
	// ContentEvents counts chunks carrying visible content.
	ContentEvents int `json:"content_events"`
	// CompletionTokens is present only when the stream carried a usage
	// object with a completion count.
	CompletionTokens *int `json:"completion_tokens,omitempty"`
	// ReasoningTokens is a release record's integer under a key named
	// reasoning_tokens at any depth of Usage; absent when there is none
	// or two disagree.
	ReasoningTokens   *int            `json:"reasoning_tokens,omitempty"`
	Usage             json.RawMessage `json:"usage,omitempty"`
	FinishReason      string          `json:"finish_reason,omitempty"`
	ReportedModel     string          `json:"reported_model,omitempty"`
	ModelMismatch     []string        `json:"model_mismatch,omitempty"`
	SystemFingerprint string          `json:"system_fingerprint,omitempty"`
	StreamError       string          `json:"stream_error,omitempty"`
	UnknownEvents     int             `json:"unknown_events,omitempty"`
	// Answer is the visible content, present for hosted targets outside
	// the release profile; a release keeps it in the frame store only.
	// Reasoning text is never recorded here.
	Answer           *string `json:"answer,omitempty"`
	Reasoning        bool    `json:"reasoning"`
	ReasoningChunks  int     `json:"reasoning_chunks,omitempty"`
	FirstReasoningNs int64   `json:"first_reasoning_ns,omitempty"`
	// Refusal says whether the stream carried text at a refusal path the
	// target's response shape names, and RefusalBytes is that text's
	// length; Refusal is absent when the shape names none. The text is
	// never recorded here.
	Refusal      *bool `json:"refusal,omitempty"`
	RefusalBytes int   `json:"refusal_bytes,omitempty"`
	// Score is the scorer's output for an attempt with an expected label.
	Score *Score `json:"score,omitempty"`
	// RunFailures are the run's invalid reasons, repeated on every record
	// of the run.
	RunFailures []string `json:"run_failures,omitempty"`
}

// Score is one attempt's task outcome. Error is set, and Class empty,
// when the scorer refused the attempt.
type Score struct {
	Expected string       `json:"expected"`
	Class    score.Class  `json:"class,omitempty"`
	Route    string       `json:"route,omitempty"`
	Reason   score.Reason `json:"reason,omitempty"`
	Late     bool         `json:"late,omitempty"`
	Error    string       `json:"error,omitempty"`
}

// Scoring scores the attempts that have an expected label.
type Scoring struct {
	Scorer *score.Scorer
	// Expected is the frozen label by request index; an index past its
	// end, or with an empty label, is not scored.
	Expected []string
}

// Options carries what a run's records hold beyond its requests.
type Options struct {
	Scoring     *Scoring
	RunFailures []string
}

// Records returns one record per request of res, in index order.
func Records(cfg *config.Config, res *loadgen.Result, opt Options) []Record {
	out := make([]Record, len(res.Requests))
	for i := range res.Requests {
		r := &res.Requests[i]
		out[i] = record(r, cfg)
		rec := &out[i]
		if len(opt.RunFailures) > 0 {
			rec.RunFailures = opt.RunFailures
		}
		if s := opt.Scoring; s != nil && rec.Index >= 0 && rec.Index < int64(len(s.Expected)) && s.Expected[rec.Index] != "" {
			a := rec.Attempt(s.Expected[rec.Index])
			a.Answer, a.Refusal = r.Content, r.Refusal
			rec.Score = scoreOf(s.Scorer, a)
		}
	}
	return out
}

func record(r *loadgen.Request, cfg *config.Config) Record {
	rec := Record{
		Index:             r.Index,
		IntendedNs:        r.IntendedNs,
		DispatchNs:        r.DispatchNs,
		FirstTokNs:        r.FirstTokNs,
		DoneNs:            r.DoneNs,
		Outcome:           r.Outcome,
		ErrClass:          r.ErrClass,
		Step:              r.Step,
		Warmup:            r.Warmup,
		Late:              r.Late,
		Protocol:          r.Proto,
		Status:            r.Status,
		RetryAfter:        http.Header(r.Headers).Get("Retry-After"),
		Headers:           r.Headers,
		ErrText:           r.ErrText,
		PeerAddr:          r.PeerAddr,
		ConnReused:        r.ConnReused,
		ContentEvents:     r.Tokens,
		Usage:             r.Usage,
		FinishReason:      r.FinishReason,
		ReportedModel:     r.ReportedModel,
		ModelMismatch:     r.ModelMismatch,
		SystemFingerprint: r.SystemFingerprint,
		StreamError:       r.StreamError,
		UnknownEvents:     r.UnknownEvents,
		Reasoning:         r.ReasoningChunks > 0,
		ReasoningChunks:   r.ReasoningChunks,
		FirstReasoningNs:  r.FirstReasoningNs,
	}
	if len(cfg.Target.Shape().Refusal) > 0 {
		refused := r.RefusalBytes > 0
		rec.Refusal, rec.RefusalBytes = &refused, r.RefusalBytes
	}
	if r.UsageSeen {
		n := r.CompletionTokens
		rec.CompletionTokens = &n
	}
	if cfg.Profile == config.ProfileRelease && cfg.Release != nil {
		rec.Shape, rec.CeilingTokens = cfg.Release.Shape.Name, cfg.Release.Shape.CeilingTokens
		rec.InTime = r.Outcome == loadgen.OutcomeCompleted && !r.Late
		rec.ReasoningTokens = reasoningTokens(r.Usage)
		return rec
	}
	if cfg.Target.Hosted {
		a := r.Content
		rec.Answer = &a
	}
	return rec
}

// reasoningTokens is the integer under a key named reasoning_tokens at
// any depth of usage, or nil when there is none or two disagree.
func reasoningTokens(usage json.RawMessage) *int {
	var v any
	dec := json.NewDecoder(bytes.NewReader(usage))
	dec.UseNumber()
	if len(usage) == 0 || dec.Decode(&v) != nil {
		return nil
	}
	var found []int64
	var walk func(any)
	walk = func(v any) {
		switch t := v.(type) {
		case map[string]any:
			for k, e := range t {
				if n, ok := e.(json.Number); ok && k == "reasoning_tokens" {
					if i, err := n.Int64(); err == nil {
						found = append(found, i)
					}
				}
				walk(e)
			}
		case []any:
			for _, e := range t {
				walk(e)
			}
		}
	}
	walk(v)
	if len(found) == 0 || slices.Min(found) != slices.Max(found) {
		return nil
	}
	n := int(found[0])
	return &n
}

// Attempt is what the scorer reads from rec. The stream ended when it
// completed, or reached [DONE] with no visible content (empty_stream).
// The refusal text, which rec does not carry, and the incompatible flag
// are left unset.
func (rec *Record) Attempt(expected string) score.Attempt {
	a := score.Attempt{
		Expected:     expected,
		IntendedNs:   rec.IntendedNs,
		Ended:        rec.Outcome == loadgen.OutcomeCompleted || rec.Outcome == loadgen.OutcomeErrored && rec.ErrClass == loadgen.ErrEmptyStream,
		EndNs:        rec.DoneNs,
		FinishReason: rec.FinishReason,
	}
	if rec.Answer != nil {
		a.Answer = *rec.Answer
	}
	return a
}

func scoreOf(s *score.Scorer, a score.Attempt) *Score {
	out := &Score{Expected: a.Expected}
	if s == nil {
		out.Error = "evidence: no scorer"
		return out
	}
	v, err := s.Score(a)
	if err != nil {
		out.Error = err.Error()
		return out
	}
	out.Class, out.Route, out.Reason, out.Late = v.Class, v.Route, v.Reason, v.Late
	return out
}

// Request is the request rec records. The fields the per-attempt record
// does not carry are zero: inter-token gaps, replica, reasoning and
// refusal text, the non-200 body, the frames, and a release request's
// answer.
func (rec *Record) Request() loadgen.Request {
	r := loadgen.Request{
		Index:             rec.Index,
		IntendedNs:        rec.IntendedNs,
		DispatchNs:        rec.DispatchNs,
		FirstTokNs:        rec.FirstTokNs,
		DoneNs:            rec.DoneNs,
		Outcome:           rec.Outcome,
		ErrClass:          rec.ErrClass,
		Tokens:            rec.ContentEvents,
		Step:              rec.Step,
		Warmup:            rec.Warmup,
		Late:              rec.Late,
		Usage:             rec.Usage,
		FinishReason:      rec.FinishReason,
		ReportedModel:     rec.ReportedModel,
		SystemFingerprint: rec.SystemFingerprint,
		ModelMismatch:     rec.ModelMismatch,
		StreamError:       rec.StreamError,
		ReasoningChunks:   rec.ReasoningChunks,
		FirstReasoningNs:  rec.FirstReasoningNs,
		RefusalBytes:      rec.RefusalBytes,
		UnknownEvents:     rec.UnknownEvents,
		Proto:             rec.Protocol,
		Status:            rec.Status,
		Headers:           rec.Headers,
		PeerAddr:          rec.PeerAddr,
		ConnReused:        rec.ConnReused,
		ErrText:           rec.ErrText,
	}
	if rec.CompletionTokens != nil {
		r.CompletionTokens, r.UsageSeen = *rec.CompletionTokens, true
	}
	if rec.Answer != nil {
		r.Content = *rec.Answer
	}
	return r
}

// ReadRecords reads a per-attempt file: one record per line, in index
// order from 0, with no field Record does not define.
func ReadRecords(r io.Reader) ([]Record, error) {
	var out []Record
	sc := bufio.NewScanner(r)
	sc.Buffer(nil, recordLineLimit)
	for sc.Scan() {
		dec := json.NewDecoder(bytes.NewReader(sc.Bytes()))
		dec.DisallowUnknownFields()
		var rec Record
		if err := dec.Decode(&rec); err != nil {
			return nil, fmt.Errorf("evidence: attempts line %d: %w", len(out)+1, err)
		}
		if _, err := dec.Token(); err != io.EOF {
			return nil, fmt.Errorf("evidence: attempts line %d: data after the record", len(out)+1)
		}
		if rec.Index != int64(len(out)) {
			return nil, fmt.Errorf("evidence: attempts line %d: index %d, want %d", len(out)+1, rec.Index, len(out))
		}
		out = append(out, rec)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("evidence: attempts line %d: %w", len(out)+1, err)
	}
	return out, nil
}

// recordLineLimit bounds one per-attempt line.
const recordLineLimit = 16 << 20

// Write writes recs to w, one JSON object per line.
func Write(w io.Writer, recs []Record) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for i := range recs {
		if err := enc.Encode(&recs[i]); err != nil {
			return fmt.Errorf("evidence: record %d: %w", recs[i].Index, err)
		}
	}
	return nil
}

// WriteRun writes the records of res to AttemptsFile in dir and returns
// the file's path. A nil res, a run that never reached load generation,
// writes nothing and returns "".
func WriteRun(dir string, cfg *config.Config, res *loadgen.Result, opt Options) (string, error) {
	if res == nil {
		return "", nil
	}
	var b bytes.Buffer
	if err := Write(&b, Records(cfg, res, opt)); err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("evidence: %w", err)
	}
	path := filepath.Join(dir, AttemptsFile)
	if err := os.WriteFile(path, b.Bytes(), 0o644); err != nil {
		return "", fmt.Errorf("evidence: %w", err)
	}
	return path, nil
}
