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

// LengthValue is adapter's finish reason for an answer the output ceiling
// stopped.
func LengthValue(adapter string) string {
	if adapter == config.AdapterMessages {
		return LengthMessages
	}
	return LengthChatCompletions
}

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
// request it checked; ExpectedModel is the route's expected identifier
// at the check (config.Target.ExpectedModel).
type ReleaseRecord struct {
	SchemaVersion  int       `json:"schema_version"`
	Route          string    `json:"route"`
	Adapter        string    `json:"adapter"`
	EndpointURL    string    `json:"endpoint_url"`
	RequestedModel string    `json:"requested_model"`
	ExpectedModel  string    `json:"expected_model,omitempty"`
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

// Snapshot is one target.limits record, without the value of a limit
// marked config.MarkingConfidential. Retained is the hash check of the
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
		ExpectedModel:  t.ExpectedModel(),
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
	length := LengthValue(t.Adapter)

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

// ReportedModels lists each distinct model id the record's responses
// reported, in request order.
func (r *ReleaseRecord) ReportedModels() []string {
	var out []string
	for _, q := range r.Requests {
		if q.Shape == nil {
			continue
		}
		for _, m := range q.Shape.Models {
			if !slices.Contains(out, m) {
				out = append(out, m)
			}
		}
	}
	return out
}

// Expected is the identifier the record's reported model ids are
// compared with: ExpectedModel, or RequestedModel in a record that names
// none.
func (r *ReleaseRecord) Expected() string {
	if r.ExpectedModel != "" {
		return r.ExpectedModel
	}
	return r.RequestedModel
}

// ModelFault says why the reported model ids of recs, the route's own
// ceiling-check records, keep t's provider-model out (SPEC-HOSTED §2
// reported identifier): a reported id other than t.ExpectedModel(), or a
// target.reported_model that no response reported. It is "" when
// neither holds.
func ModelFault(t *config.Target, recs []*ReleaseRecord) string {
	return modelFault(t.ExpectedModel(), t.ReportedModel != nil, recs)
}

// hostModelFault is ModelFault for recs, one host's records, against the
// identifier they expect (ReleaseRecord.Expected), which names a
// target.reported_model where it differs from the requested model. It
// is a fault when the records expect different identifiers.
func hostModelFault(recs []*ReleaseRecord) string {
	want := recs[0].Expected()
	for _, r := range recs[1:] {
		if e := r.Expected(); e != want {
			return fmt.Sprintf("the ceiling-check records expect model %q and %q", want, e)
		}
	}
	return modelFault(want, want != recs[0].RequestedModel, recs)
}

// modelFault is ModelFault against want, the expected identifier, which
// documented says a target.reported_model records.
func modelFault(want string, documented bool, recs []*ReleaseRecord) string {
	var reported []string
	for _, r := range recs {
		for _, m := range r.ReportedModels() {
			if !slices.Contains(reported, m) {
				reported = append(reported, m)
			}
		}
	}
	for _, m := range reported {
		switch {
		case m == want:
		case !documented:
			return fmt.Sprintf("the ceiling check reported model %q, the requested %q, and target.reported_model records no documentation naming both as one build: identifier-not-documented", m, want)
		default:
			return fmt.Sprintf("the ceiling check reported model %q, not target.reported_model %q", m, want)
		}
	}
	if documented && len(reported) == 0 {
		return fmt.Sprintf("target.reported_model %q is recorded and no response of the ceiling check reported a model", want)
	}
	return ""
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
	c.Fail = ceilingFail(r.ex.Status, s, c.FinishReason, length)
	c.Pass = c.Fail == ""
	return c
}

// ceilingFail is the first reason a request with status, stream shape s
// and recorded finish reason fails the ceiling check, or "".
func ceilingFail(status int, s *Shape, finish, length string) string {
	switch {
	case status != http.StatusOK && status != 0:
		return fmt.Sprintf("status %d", status)
	case s == nil:
		return "no response"
	case s.ReadError != "" || s.DroppedEvents > 0 || s.Undecodable > 0:
		return "malformed stream"
	case !s.Done:
		return "no terminator"
	case s.Error != "":
		return "in-band error"
	case s.ContentEvents == 0:
		return "no visible content"
	case finish == "":
		return "no finish reason"
	case finish == length:
		return fmt.Sprintf("finish reason %q: stopped at the ceiling", length)
	}
	return ""
}

// Verify re-derives r's results from its requests: each request's pass
// from its status, shape and finish reason; the start gate from each
// status through Gate(t, status), t being nil for another host's record,
// whose gate is then config.SetupDefectStatuses alone; the ceiling check
// at r's ceiling with length, the adapter's LengthValue; and the input
// bound at bound. It returns why r is refused: a count of requests other
// than CeilingRequests in a record not blocked, a request after the one
// the gate blocks on, a passed request not answered 200, or a recorded
// result other than the re-derived one.
func (r *ReleaseRecord) Verify(t *config.Target, length string, bound int) error {
	if t == nil {
		t = &config.Target{}
	}
	blocked := false
	for i, q := range r.Requests {
		if q.Index != i {
			return fmt.Errorf("request %d has index %d", i, q.Index)
		}
		if blocked {
			return fmt.Errorf("request %d follows the one the start gate blocks on", i)
		}
		fail := ceilingFail(q.Exchange.Status, q.Shape, q.FinishReason, length)
		if q.Pass != (fail == "") || q.Fail != fail {
			return fmt.Errorf("request %d records pass %t (%q); its response gives %t (%q)", i, q.Pass, q.Fail, fail == "", fail)
		}
		if q.Pass && (q.Exchange.Status != http.StatusOK || q.Exchange.DispatchUTC.IsZero()) {
			return fmt.Errorf("request %d passed with status %d, dispatched %t", i, q.Exchange.Status, !q.Exchange.DispatchUTC.IsZero())
		}
		blocked = Gate(t, q.Exchange.Status).Blocked
	}
	switch {
	case blocked && !r.Start.Blocked:
		return fmt.Errorf("request %d's status %d blocks the start; the record is not blocked", len(r.Requests)-1, r.Requests[len(r.Requests)-1].Exchange.Status)
	case !r.Start.Blocked && len(r.Requests) != CeilingRequests:
		return fmt.Errorf("%d requests, want %d", len(r.Requests), CeilingRequests)
	}
	if c := ceilingCheck(r.Requests, r.Ceiling.Ceiling, length); c != r.Ceiling {
		return fmt.Errorf("ceiling check recorded %+v; its requests give %+v", r.Ceiling, c)
	}
	if c := inputCheck(r.Requests, bound); c != r.InputBound {
		return fmt.Errorf("input bound recorded %+v; its requests give %+v", r.InputBound, c)
	}
	return nil
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
// headers target.retained_headers names, the limits body, or, on a target
// with a confidential limit, a non-200 body and the in-band error text.
func Retains(t *config.Target) bool {
	return len(t.RetainedHeaders) > 0 || t.Probe != nil && t.Probe.LimitsRetained || t.Confidential()
}

// Exposed lists what rec, a probe of t's route, carries that t withholds:
// each header target.retained_headers names with a value other than
// config.RetainedHeaderValue, then "limits body" when
// target.probe.limits_retained is set and the limits body is kept, then,
// on a target with a confidential limit, "request <i> body" for each
// request whose body is kept and "request <i> in-band error" for each
// whose in-band error text is not its stand-in (config.RetainedError),
// then "snapshot <i> value" for each snapshot with a value of a limit
// marked config.MarkingConfidential in rec or at index i of
// target.limits.
func Exposed(t *config.Target, rec *ReleaseRecord) []string {
	return exposed(t, rec, t.Confidential())
}

// exposed is Exposed with confidential in place of t.Confidential().
func exposed(t *config.Target, rec *ReleaseRecord, confidential bool) []string {
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
	for _, r := range rec.Requests {
		if b := r.Exchange.Body; confidential && b != nil && (b.Text != nil || b.Base64 != "") {
			out = append(out, fmt.Sprintf("request %d body", r.Index))
		}
		if s := r.Shape; confidential && s != nil && s.Error != "" && s.Error != config.RetainedError(s.Error) {
			out = append(out, fmt.Sprintf("request %d in-band error", r.Index))
		}
	}
	for i, sn := range rec.Snapshots {
		marked := sn.Marking == config.MarkingConfidential || i < len(t.Limits) && t.Limits[i].Marking == config.MarkingConfidential
		if marked && sn.Value != 0 {
			out = append(out, fmt.Sprintf("snapshot %d value", i))
		}
	}
	return out
}

// ExposedByMarking is Exposed for a probe of a route whose configuration
// is not at hand, from the markings its snapshots record: under a
// snapshot marked config.MarkingConfidential, every header whose name
// contains "ratelimit", the limits body, each non-200 body and each
// in-band error text are withheld; the configuration check requires the
// first two of that route (SPEC-HOSTED HA3 point 15).
func ExposedByMarking(rec *ReleaseRecord) []string {
	t := &config.Target{}
	marked := slices.ContainsFunc(rec.Snapshots, func(s Snapshot) bool { return s.Marking == config.MarkingConfidential })
	if marked {
		t.Probe = &config.TargetProbe{LimitsRetained: true}
		names := func(h map[string][]string) {
			for name := range rateLimitHeaders(h) {
				t.RetainedHeaders = append(t.RetainedHeaders, name)
			}
		}
		for _, r := range rec.Requests {
			names(r.Exchange.Headers)
		}
		names(rec.RateLimitHeaders)
		if rec.Limits != nil {
			names(rec.Limits.Headers)
		}
	}
	return exposed(t, rec, marked)
}

// snapshot is l as published, with a file source's retained copy in dir
// checked against its recorded hash.
func snapshot(l config.Limit, dir string) Snapshot {
	if l.Marking == config.MarkingConfidential {
		l.Value = 0
	}
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
// exclusion with the reason. LeftOut lists the hosts the choice was made
// without.
type CeilingChoice struct {
	Ceiling  int       `json:"ceiling,omitempty"`
	Excluded bool      `json:"excluded,omitempty"`
	Reason   string    `json:"reason"`
	LeftOut  []LeftOut `json:"left_out,omitempty"`
}

// LeftOut is a host left out of its model's ceiling choice, with the
// fault in its records' reported model ids (hostModelFault). Its own
// route's run refuses it on that fault (ModelFault).
type LeftOut struct {
	Route string `json:"route"`
	Fault string `json:"model_fault"`
}

// ChooseCeiling applies the ceiling rule (SPEC-HOSTED HA3 point 7) to the
// release probe records of hosts, every route of one model: 512 when
// every host passed the check at 512; otherwise 1,024 when every host
// passed it at 1,024; otherwise the model is excluded. A host whose
// records report a model id other than the one they expect, or expect a
// recorded target.reported_model that none reported, is left out of the
// choice; the model is excluded when every host is. It returns an error
// when a record is of no host, or a record of a host not left out was
// blocked at the start gate or failed the input bound, a host has two
// records at one ceiling, or a record the rule needs is missing.
func ChooseCeiling(hosts []string, recs []*ReleaseRecord) (CeilingChoice, error) {
	low, high := config.PinnedReleaseCeilings[0], config.PinnedReleaseCeilings[1]
	if len(hosts) == 0 {
		return CeilingChoice{}, errors.New("probe: no hosts")
	}
	byHost := map[string][]*ReleaseRecord{}
	for _, r := range recs {
		if !slices.Contains(hosts, r.Route) {
			return CeilingChoice{}, fmt.Errorf("probe: route %s is not a host of the model (%s)", r.Route, strings.Join(hosts, ", "))
		}
		byHost[r.Route] = append(byHost[r.Route], r)
	}
	all := slices.Clone(hosts)
	slices.Sort(all)
	var routes, leftNames []string
	var left []LeftOut
	for _, rt := range all {
		if rs := byHost[rt]; len(rs) > 0 {
			if f := hostModelFault(rs); f != "" {
				left, leftNames = append(left, LeftOut{Route: rt, Fault: f}), append(leftNames, rt)
				continue
			}
		}
		routes = append(routes, rt)
	}
	choice := func(c CeilingChoice) CeilingChoice {
		if len(left) > 0 {
			c.Reason += fmt.Sprintf("; %s left out on the reported model id", strings.Join(leftNames, ", "))
			c.LeftOut = left
		}
		return c
	}
	if len(routes) == 0 {
		return CeilingChoice{Excluded: true, Reason: fmt.Sprintf("%s left out on the reported model id", strings.Join(leftNames, ", ")), LeftOut: left}, nil
	}
	at := map[int]map[string]*ReleaseRecord{low: {}, high: {}}
	for _, r := range recs {
		if !slices.Contains(routes, r.Route) {
			continue
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
		return choice(CeilingChoice{Ceiling: low, Reason: fmt.Sprintf("every host passed at %d", low)}), nil
	}
	if err := need(high); err != nil {
		return CeilingChoice{}, fmt.Errorf("%w; %s failed at %d", err, strings.Join(lowFailed, ", "), low)
	}
	if f := failed(high); len(f) > 0 {
		return choice(CeilingChoice{Excluded: true, Reason: fmt.Sprintf("%s failed at %d", strings.Join(f, ", "), high)}), nil
	}
	return choice(CeilingChoice{Ceiling: high, Reason: fmt.Sprintf("%s failed at %d; every host passed at %d", strings.Join(lowFailed, ", "), low, high)}), nil
}
