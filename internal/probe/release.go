package probe

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/redact"
)

// CeilingRequests is the number of requests in a route's ceiling check
// (SPEC-HOSTED HA3 point 7).
const CeilingRequests = 10

// The finish reason each adapter reports when the output ceiling stopped
// the answer.
const (
	LengthChatCompletions = "length"
	LengthMessages        = "max_tokens"
)

// releaseSpacing is the least time between two dispatches of a release
// probe: the interval of the ladder's lowest step.
var releaseSpacing = time.Duration(float64(time.Second) / config.PinnedReleaseStepsRPS[0])

// ReleaseInput is one release probe of one route.
type ReleaseInput struct {
	// Config is the route's configuration under the release profile; the
	// probe sends its body at its ceiling.
	Config *config.Config
	// Texts are the user texts of the CeilingRequests requests.
	Texts []string
	// SnapshotDir holds the retained copies of the limits whose source is
	// a file name; empty skips their hash check.
	SnapshotDir string
	// ByteCap is the Shape S ticket byte cap the texts were cut at.
	ByteCap int
}

// ReleaseRecord is one release probe: the ceiling and input-bound checks
// over CeilingRequests requests at the configured ceiling, the response
// shape of each, the limits read and the recorded limit snapshots. The
// route, adapter, endpoint, model, body hash and byte cap name the
// request it checked.
type ReleaseRecord struct {
	SchemaVersion  int       `json:"schema_version"`
	Route          string    `json:"route"`
	Adapter        string    `json:"adapter"`
	EndpointURL    string    `json:"endpoint_url"`
	RequestedModel string    `json:"requested_model"`
	StartUTC       time.Time `json:"start_utc"`
	EndUTC         time.Time `json:"end_utc"`
	// BodySHA256 is the hash of the configured body before the user text
	// is placed.
	BodySHA256 string `json:"body_sha256"`
	// ByteCap is the Shape S ticket byte cap the texts were cut at.
	ByteCap    int            `json:"byte_cap"`
	Requests   []CheckRequest `json:"requests"`
	Ceiling    CeilingCheck   `json:"ceiling_check"`
	InputBound InputCheck     `json:"input_bound_check"`
	// RTTNs are the durations of the inference requests' successful TCP
	// connects, each one round trip to the endpoint.
	RTTNs  []int64   `json:"rtt_ns"`
	Limits *Exchange `json:"limits,omitempty"`
	// RateLimitHeaders are the recorded response headers whose names
	// contain "ratelimit", from the last inference request that returned
	// any; LimitsFrom says whether the limits read ("limits_read") or
	// these headers ("response_headers") carry the route's limits, or ""
	// when neither does.
	RateLimitHeaders map[string][]string `json:"rate_limit_headers,omitempty"`
	LimitsFrom       string              `json:"limits_from"`
	Snapshots        []Snapshot          `json:"snapshots"`
	// Findings compare each request's shape with the target's
	// configuration, and report snapshot mismatches. They never gate the
	// start.
	Findings []string `json:"findings,omitempty"`
	Start    Start    `json:"start"`
}

// CheckRequest is one request of the ceiling and input-bound checks.
// Fail names the first reason it failed the ceiling check.
type CheckRequest struct {
	Index            int      `json:"index"`
	Exchange         Exchange `json:"exchange"`
	Shape            *Shape   `json:"shape,omitempty"`
	FinishReason     string   `json:"finish_reason,omitempty"`
	PromptTokens     *int     `json:"prompt_tokens,omitempty"`
	CompletionTokens *int     `json:"completion_tokens,omitempty"`
	Pass             bool     `json:"pass"`
	Fail             string   `json:"fail,omitempty"`
}

// CeilingCheck is the ten-request check at one ceiling: it passes when
// every request returned visible content with a finish reason other than
// the adapter's length value.
type CeilingCheck struct {
	Ceiling     int    `json:"ceiling"`
	LengthValue string `json:"length_value"`
	Sent        int    `json:"sent"`
	Passed      int    `json:"passed"`
	Pass        bool   `json:"pass"`
}

// InputCheck is the input bound: it passes when every request reported a
// prompt token count and none exceeded the bound.
type InputCheck struct {
	BoundTokens     int  `json:"bound_tokens"`
	Reported        int  `json:"reported"`
	MaxPromptTokens int  `json:"max_prompt_tokens"`
	Exceeded        int  `json:"exceeded"`
	Pass            bool `json:"pass"`
}

// Snapshot is one target.limits record. Retained is the hash check of the
// retained copy in ReleaseInput.SnapshotDir: absent when the source is a
// URL or no directory was given.
type Snapshot struct {
	config.Limit
	Retained      *bool  `json:"retained_sha256_match,omitempty"`
	RetainedError string `json:"retained_error,omitempty"`
}

// Values of ReleaseRecord.LimitsFrom.
const (
	LimitsFromRead    = "limits_read"
	LimitsFromHeaders = "response_headers"
)

// RunRelease probes in.Config's route: CeilingRequests requests one after
// another at the configured ceiling, then the limits read when
// configured. A status the start gate blocks on ends the requests. It
// returns an error, before any request, when the key's variable is empty
// or the input is not a release probe with its byte cap.
func RunRelease(ctx context.Context, in ReleaseInput) (*ReleaseRecord, error) {
	cfg := in.Config
	if cfg == nil || cfg.Profile != config.ProfileRelease || cfg.Release == nil {
		return nil, errors.New("probe: a release probe needs a configuration under the release profile")
	}
	if len(in.Texts) != CeilingRequests {
		return nil, fmt.Errorf("probe: %d user texts, want %d", len(in.Texts), CeilingRequests)
	}
	if in.ByteCap <= 0 {
		return nil, fmt.Errorf("probe: byte cap %d: the texts' ticket byte cap is required", in.ByteCap)
	}
	t := &cfg.Target
	key := os.Getenv(t.APIKeyEnv)
	if key == "" {
		return nil, fmt.Errorf("probe: %s: environment variable %s is empty", t.Name, t.APIKeyEnv)
	}
	m := newMasker(key)
	sum := sha256.Sum256([]byte(t.Body))
	rec := &ReleaseRecord{
		SchemaVersion:  1,
		Route:          t.Name,
		Adapter:        t.Adapter,
		EndpointURL:    redact.URL(t.EndpointURL),
		RequestedModel: t.ModelName,
		StartUTC:       time.Now().UTC(),
		BodySHA256:     hex.EncodeToString(sum[:]),
		ByteCap:        in.ByteCap,
		Requests:       []CheckRequest{},
		RTTNs:          []int64{},
		Snapshots:      []Snapshot{},
	}
	transport := newTransport(1)
	defer transport.CloseIdleConnections()
	p := &prober{rt: transport, target: t, mask: m, timeout: time.Duration(cfg.Client.HTTPTimeoutS) * time.Second}
	length := LengthChatCompletions
	if t.Adapter == config.AdapterMessages {
		length = LengthMessages
	}

	for i, text := range in.Texts {
		if i > 0 {
			wait := time.NewTimer(time.Until(rec.Requests[i-1].Exchange.DispatchUTC.Add(releaseSpacing)))
			select {
			case <-wait.C:
			case <-ctx.Done():
				wait.Stop()
			}
			if ctx.Err() != nil {
				break
			}
		}
		r := p.post(ctx, t.EndpointURL, t.ModelName, config.LiteralBody(t.Body, text), key)
		rec.Requests = append(rec.Requests, checkRequest(i, r, length, m))
		if rec.Start = Gate(t, r.ex.Status); rec.Start.Blocked {
			break
		}
	}

	if t.Probe != nil && t.Probe.LimitsURL != "" {
		ex := p.limits(ctx, key)
		rec.Limits = &ex
		if ex.Status == http.StatusOK {
			rec.LimitsFrom = LimitsFromRead
		}
	}
	for _, r := range rec.Requests {
		if h := rateLimitHeaders(r.Exchange.Headers); h != nil {
			rec.RateLimitHeaders = h
		}
		for _, c := range r.Exchange.Connects {
			if c.Error == "" {
				rec.RTTNs = append(rec.RTTNs, c.DurationNs)
			}
		}
		for _, f := range findings(t, r.Exchange, r.Shape, t.ModelName) {
			if !slices.Contains(rec.Findings, f) {
				rec.Findings = append(rec.Findings, f)
			}
		}
	}
	if rec.LimitsFrom == "" && rec.RateLimitHeaders != nil {
		rec.LimitsFrom = LimitsFromHeaders
	}
	rec.Ceiling = ceilingCheck(rec.Requests, cfg.Release.Shape.CeilingTokens, length)
	rec.InputBound = inputCheck(rec.Requests, cfg.Release.Shape.InputBoundTokens)
	for _, l := range t.Limits {
		s := snapshot(l, in.SnapshotDir)
		if s.Retained != nil && !*s.Retained {
			rec.Findings = append(rec.Findings, fmt.Sprintf("retained copy %q does not match its recorded sha256", l.Source))
		}
		if s.RetainedError != "" {
			rec.Findings = append(rec.Findings, fmt.Sprintf("retained copy %q not read: %s", l.Source, s.RetainedError))
		}
		rec.Snapshots = append(rec.Snapshots, s)
	}
	rec.EndUTC = time.Now().UTC()
	return rec, nil
}

// Sent counts the inference requests the probe dispatched. The limits
// read goes to another URL and is not counted.
func (r *ReleaseRecord) Sent() int {
	n := 0
	for _, q := range r.Requests {
		if !q.Exchange.DispatchUTC.IsZero() {
			n++
		}
	}
	return n
}

// checkRequest reads one request's finish reason and token counts and
// applies the ceiling check to it.
func checkRequest(i int, r posted, length string, m masker) CheckRequest {
	c := CheckRequest{Index: i, Exchange: r.ex, Shape: r.shape}
	s := r.shape
	if s != nil {
		c.FinishReason = m.str(s.finish)
		if n, ok := promptTokens(s); ok {
			c.PromptTokens = &n
		}
		for _, k := range []string{"completion_tokens", "output_tokens"} {
			if n, ok := s.tokens(k); ok {
				c.CompletionTokens = &n
				break
			}
		}
	}
	switch {
	case r.ex.Status != http.StatusOK && r.ex.Status != 0:
		c.Fail = fmt.Sprintf("status %d", r.ex.Status)
	case s == nil:
		c.Fail = "no response"
	case s.ReadError != "" || s.DroppedEvents > 0 || s.Undecodable > 0:
		c.Fail = "malformed stream"
	case !s.Done:
		c.Fail = "no terminator"
	case s.Error != "":
		c.Fail = "in-band error"
	case s.ContentEvents == 0:
		c.Fail = "no visible content"
	case s.finish == "":
		c.Fail = "no finish reason"
	case s.finish == length:
		c.Fail = fmt.Sprintf("finish reason %q: stopped at the ceiling", length)
	default:
		c.Pass = true
	}
	return c
}

// promptTokens is the stream's reported prompt token count: a
// chat-completions prompt_tokens, or a Messages input_tokens plus its two
// cache counts, which input_tokens excludes.
func promptTokens(s *Shape) (int, bool) {
	if n, ok := s.tokens("prompt_tokens"); ok {
		return n, true
	}
	n, ok := s.tokens("input_tokens")
	if !ok {
		return 0, false
	}
	for _, k := range []string{"cache_creation_input_tokens", "cache_read_input_tokens"} {
		if c, ok := s.tokens(k); ok {
			n += c
		}
	}
	return n, true
}

func ceilingCheck(reqs []CheckRequest, ceiling int, length string) CeilingCheck {
	c := CeilingCheck{Ceiling: ceiling, LengthValue: length}
	for _, r := range reqs {
		if !r.Exchange.DispatchUTC.IsZero() {
			c.Sent++
		}
		if r.Pass {
			c.Passed++
		}
	}
	c.Pass = c.Passed == CeilingRequests
	return c
}

func inputCheck(reqs []CheckRequest, bound int) InputCheck {
	c := InputCheck{BoundTokens: bound}
	for _, r := range reqs {
		if r.PromptTokens == nil {
			continue
		}
		c.Reported++
		c.MaxPromptTokens = max(c.MaxPromptTokens, *r.PromptTokens)
		if *r.PromptTokens > bound {
			c.Exceeded++
		}
	}
	c.Pass = c.Reported == CeilingRequests && c.Exceeded == 0
	return c
}

// rateLimitHeaders returns the headers whose names contain "ratelimit",
// or nil.
func rateLimitHeaders(h map[string][]string) map[string][]string {
	var out map[string][]string
	for k, v := range h {
		if strings.Contains(strings.ToLower(k), "ratelimit") {
			if out == nil {
				out = map[string][]string{}
			}
			out[k] = v
		}
	}
	return out
}

// Retains reports whether t withholds a probe value: the values of the
// headers target.retained_headers names, or the limits body.
func Retains(t *config.Target) bool {
	return len(t.RetainedHeaders) > 0 || t.Probe != nil && t.Probe.LimitsRetained
}

// Exposed lists what rec, a probe of t's route, carries that t withholds:
// each header target.retained_headers names with a value other than
// config.RetainedHeaderValue, then "limits body" when
// target.probe.limits_retained is set and the limits body is kept.
func Exposed(t *config.Target, rec *ReleaseRecord) []string {
	var out []string
	note := func(h map[string][]string) {
		for name, vs := range h {
			if !t.HeaderRetained(name) || slices.Contains(out, name) {
				continue
			}
			for _, v := range vs {
				if v != config.RetainedHeaderValue {
					out = append(out, name)
					break
				}
			}
		}
	}
	for _, r := range rec.Requests {
		note(r.Exchange.Headers)
	}
	note(rec.RateLimitHeaders)
	if rec.Limits != nil {
		note(rec.Limits.Headers)
	}
	slices.Sort(out)
	if b := rec.Limits; t.Probe != nil && t.Probe.LimitsRetained && b != nil && b.Body != nil && (b.Body.Text != nil || b.Body.Base64 != "") {
		out = append(out, "limits body")
	}
	return out
}

// snapshot checks a file source's retained copy in dir against its
// recorded hash.
func snapshot(l config.Limit, dir string) Snapshot {
	s := Snapshot{Limit: l}
	if dir == "" || strings.Contains(l.Source, "://") {
		return s
	}
	if !filepath.IsLocal(l.Source) {
		s.RetainedError = "the source is not a file name inside the snapshot directory"
		return s
	}
	b, err := os.ReadFile(filepath.Join(dir, l.Source))
	switch {
	case errors.Is(err, fs.ErrNotExist):
		s.RetainedError = "no such file"
		return s
	case err != nil:
		s.RetainedError = redact.ErrorText(err, 0)
		return s
	}
	sum := sha256.Sum256(b)
	match := hex.EncodeToString(sum[:]) == l.SHA256
	s.Retained = &match
	return s
}

// CeilingChoice is the ceiling one model runs at on every host, or its
// exclusion with the reason.
type CeilingChoice struct {
	Ceiling  int    `json:"ceiling,omitempty"`
	Excluded bool   `json:"excluded,omitempty"`
	Reason   string `json:"reason"`
}

// ChooseCeiling applies the ceiling rule (SPEC-HOSTED HA3 point 7) to the
// release probe records of hosts, every route of one model: 512 when
// every host passed the check at 512; otherwise 1,024 when every host
// passed it at 1,024; otherwise the model is excluded. It returns an
// error when a record is of no host, was blocked at the start gate or
// failed the input bound, a host has two records at one ceiling, or a
// record the rule needs is missing.
func ChooseCeiling(hosts []string, recs []*ReleaseRecord) (CeilingChoice, error) {
	low, high := config.PinnedReleaseCeilings[0], config.PinnedReleaseCeilings[1]
	if len(hosts) == 0 {
		return CeilingChoice{}, errors.New("probe: no hosts")
	}
	at := map[int]map[string]*ReleaseRecord{low: {}, high: {}}
	for _, r := range recs {
		if !slices.Contains(hosts, r.Route) {
			return CeilingChoice{}, fmt.Errorf("probe: route %s is not a host of the model (%s)", r.Route, strings.Join(hosts, ", "))
		}
		if r.Start.Blocked {
			return CeilingChoice{}, fmt.Errorf("probe: route %s: the probe was blocked (%s); it is not a ceiling check", r.Route, r.Start.Reason)
		}
		byRoute, ok := at[r.Ceiling.Ceiling]
		if !ok {
			return CeilingChoice{}, fmt.Errorf("probe: route %s: ceiling %d is not one of %v", r.Route, r.Ceiling.Ceiling, config.PinnedReleaseCeilings)
		}
		if ib := r.InputBound; !ib.Pass {
			return CeilingChoice{}, fmt.Errorf("probe: route %s: the input bound failed at ceiling %d: %d of %d requests reported, %d above %d tokens",
				r.Route, r.Ceiling.Ceiling, ib.Reported, CeilingRequests, ib.Exceeded, ib.BoundTokens)
		}
		if byRoute[r.Route] != nil {
			return CeilingChoice{}, fmt.Errorf("probe: route %s: two records at ceiling %d", r.Route, r.Ceiling.Ceiling)
		}
		byRoute[r.Route] = r
	}
	routes := slices.Clone(hosts)
	slices.Sort(routes)
	failed := func(ceiling int) []string {
		var out []string
		for _, rt := range routes {
			if !at[ceiling][rt].Ceiling.Pass {
				out = append(out, rt)
			}
		}
		return out
	}
	need := func(ceiling int) error {
		for _, rt := range routes {
			if at[ceiling][rt] == nil {
				return fmt.Errorf("probe: route %s has no check at %d", rt, ceiling)
			}
		}
		return nil
	}
	if err := need(low); err != nil {
		return CeilingChoice{}, err
	}
	lowFailed := failed(low)
	if len(lowFailed) == 0 {
		return CeilingChoice{Ceiling: low, Reason: fmt.Sprintf("every host passed at %d", low)}, nil
	}
	if err := need(high); err != nil {
		return CeilingChoice{}, fmt.Errorf("%w; %s failed at %d", err, strings.Join(lowFailed, ", "), low)
	}
	if f := failed(high); len(f) > 0 {
		return CeilingChoice{Excluded: true, Reason: fmt.Sprintf("%s failed at %d", strings.Join(f, ", "), high)}, nil
	}
	return CeilingChoice{Ceiling: high, Reason: fmt.Sprintf("%s failed at %d; every host passed at %d", strings.Join(lowFailed, ", "), low, high)}, nil
}
