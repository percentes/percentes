// Package probe is the hosted pre-run probe (SPEC-HOSTED §5): from the
// client host, DNS resolution, the TLS handshake and one request at the
// session's settings, with the response shape recorded (SPEC-HOSTED §2).
// A 400, 401, 403 or 404 on that request, or a status the target lists as
// an account-side limit the operator set, blocks the window's start; any
// other outcome is recorded and the window starts on schedule. The
// optional wrong-model request and limits read are recorded and never
// gate the start. RunRelease is a release route's probe (SPEC-HOSTED HA3
// point 7): ten requests at the configured ceiling, checked for visible
// content within the ceiling and for the input bound, with the same gate.
package probe

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/percentes/percentes/internal/config"
	"github.com/percentes/percentes/internal/loadgen"
	"github.com/percentes/percentes/internal/redact"
	"github.com/percentes/percentes/internal/releasekey"
)

// File is the probe record's file name in a window's output directory.
const File = "probe.json"

// RetainedFile is the file, outside the published output, holding what a
// probe record withholds.
const RetainedFile = "probe-retained.json"

// requestTimeout bounds each exchange of a pilot probe from its dispatch.
var requestTimeout = time.Duration(config.PinnedClientTimeoutS) * time.Second

// newTransport is the session's transport.
var newTransport = loadgen.NewTransport

// spacing is the least time between the dispatches of the two inference
// requests: the session's interval.
var spacing = time.Duration(float64(time.Second) / config.PinnedPilotRateRPS)

// Bytes kept of a non-200 body and of the limits body.
const (
	errorBodyLimit  = 4096
	limitsBodyLimit = 64 << 10
)

// Ceiling on one scanned line and on the data assembled into one event.
const eventLimit = 1 << 20

// distinctLimit bounds every list and map of distinct values in Shape.
const distinctLimit = 32

// Input is one probe of one pilot target, or of a release route's target
// when Pilot is nil.
type Input struct {
	Pilot  *config.Pilot
	Target *config.Target
	// CaseText is the user text of the probe request.
	CaseText string
	// WrongModel, when set, is sent as the model of a second request, the
	// session's interval after the first; pilot targets only.
	WrongModel string
	// Timeout bounds each exchange from its dispatch; zero is the pilot's
	// requestTimeout.
	Timeout time.Duration
	// Key is the release key, required for a target with a confidential
	// limit: the fingerprints of what its record withholds are keyed
	// hashes under it.
	Key *releasekey.Key
}

// Record is one probe. Fields ending in Ns are nanoseconds on the
// monotonic clock: durations where so named, otherwise offsets from the
// exchange's dispatch.
type Record struct {
	SchemaVersion  int       `json:"schema_version"`
	Target         string    `json:"target"`
	EndpointURL    string    `json:"endpoint_url"`
	RequestedModel string    `json:"requested_model"`
	StartUTC       time.Time `json:"start_utc"`
	EndUTC         time.Time `json:"end_utc"`
	// BodySHA256 and BodyBytes identify the probe request body.
	BodySHA256 string    `json:"body_sha256"`
	BodyBytes  int       `json:"body_bytes"`
	Request    Exchange  `json:"request"`
	Shape      *Shape    `json:"shape,omitempty"`
	Limits     *Exchange `json:"limits,omitempty"`
	WrongModel *Exchange `json:"wrong_model,omitempty"`
	// Findings compare the observed shape with the target's configured
	// response shape. They never gate the start.
	Findings []string `json:"findings,omitempty"`
	Start    Start    `json:"start"`
}

// Start is the start gate's decision.
type Start struct {
	Blocked bool   `json:"blocked"`
	Reason  string `json:"reason,omitempty"`
}

// Exchange is one HTTP request and what came back.
type Exchange struct {
	Method string `json:"method"`
	URL    string `json:"url"`
	// Model is the model the request named, when it carried one.
	Model       string    `json:"model,omitempty"`
	DispatchUTC time.Time `json:"dispatch_utc"`
	// DNS is absent when the host is an address literal.
	DNS      *DNS      `json:"dns,omitempty"`
	Connects []Connect `json:"connects,omitempty"`
	TLS      *TLS      `json:"tls,omitempty"`
	PeerAddr string    `json:"peer_addr,omitempty"`
	Proto    string    `json:"proto,omitempty"`
	Status   int       `json:"status,omitempty"`
	// Headers holds the response headers target.response_headers allows,
	// with config.RetainedHeaderValue in place of each value of a header
	// target.retained_headers names; HeaderNames lists every response
	// header name.
	Headers     map[string][]string `json:"headers,omitempty"`
	HeaderNames []string            `json:"header_names,omitempty"`
	FirstByteNs int64               `json:"first_byte_ns,omitempty"`
	DoneNs      int64               `json:"done_ns"`
	// Body is a non-200 body or the limits body, cut at its limit; a
	// target with a confidential limit withholds a non-200 body (withhold).
	Body *Body `json:"body,omitempty"`
	// Error is the redacted Go error of a failure before or during the
	// response.
	Error string `json:"error,omitempty"`
	// Retained holds what the record withholds, for RetainedFile.
	Retained *Retained `json:"-"`
}

// Retained is what one exchange's record withholds: the values of the
// headers target.retained_headers names, the limits body when
// target.probe.limits_retained is set, and on a target with a
// confidential limit a non-200 body and the in-band error text.
type Retained struct {
	Headers     map[string][]string `json:"headers,omitempty"`
	Body        *Body               `json:"body,omitempty"`
	StreamError string              `json:"stream_error,omitempty"`
}

// DNS is the resolution of the request's host.
type DNS struct {
	Host       string   `json:"host"`
	Addrs      []string `json:"addrs,omitempty"`
	DurationNs int64    `json:"duration_ns"`
	Error      string   `json:"error,omitempty"`
}

// Connect is one dial attempt.
type Connect struct {
	Addr       string `json:"addr"`
	DurationNs int64  `json:"duration_ns"`
	Error      string `json:"error,omitempty"`
}

// TLS is the handshake's outcome.
type TLS struct {
	Version     string `json:"version,omitempty"`
	CipherSuite string `json:"cipher_suite,omitempty"`
	// ALPN is the negotiated application protocol ("" = none).
	ALPN        string       `json:"alpn"`
	ServerName  string       `json:"server_name,omitempty"`
	Certificate *Certificate `json:"certificate,omitempty"`
	DurationNs  int64        `json:"duration_ns"`
	Error       string       `json:"error,omitempty"`
}

// Certificate names the server's leaf certificate.
type Certificate struct {
	Subject  string    `json:"subject"`
	Issuer   string    `json:"issuer"`
	NotAfter time.Time `json:"not_after"`
}

// Body is response bytes: text when valid UTF-8, base64 otherwise.
// Truncated says the body ran past the limit and the rest was not read.
// A retained body carries only a hash of the bytes kept: on a target
// with a confidential limit their HMAC-SHA256 under the release key,
// otherwise their SHA-256.
type Body struct {
	Text       *string `json:"text,omitempty"`
	Base64     string  `json:"base64,omitempty"`
	Truncated  bool    `json:"truncated,omitempty"`
	Retained   bool    `json:"retained,omitempty"`
	SHA256     string  `json:"sha256,omitempty"`
	HMACSHA256 string  `json:"hmac_sha256,omitempty"`
}

func bodyOf(b []byte, truncated bool) *Body {
	if utf8.Valid(b) {
		s := string(b)
		return &Body{Text: &s, Truncated: truncated}
	}
	return &Body{Base64: base64.StdEncoding.EncodeToString(b), Truncated: truncated}
}

// raw returns the bytes bodyOf was given.
func (b *Body) raw() []byte {
	if b.Text != nil {
		return []byte(*b.Text)
	}
	r, _ := base64.StdEncoding.DecodeString(b.Base64)
	return r
}

// Gate returns the start decision for a probe request's status.
func Gate(t *config.Target, status int) Start {
	switch {
	case slices.Contains(config.SetupDefectStatuses, status):
		return Start{Blocked: true, Reason: fmt.Sprintf("probe request returned %d, a setup defect (SPEC-HOSTED §5); the window does not start", status)}
	case t.Probe != nil && slices.Contains(t.Probe.AccountLimitStatuses, status):
		return Start{Blocked: true, Reason: fmt.Sprintf("probe request returned %d, which the target lists as an account-side limit the operator set (SPEC-HOSTED §5); the window does not start", status)}
	}
	return Start{}
}

// Run probes in.Target: the request, then the limits read when
// configured, then the wrong-model request when in.WrongModel is set. A
// release target's request is its literal body with the case text in
// place. It returns an error, before any request, when the key's variable
// is empty or a target with a confidential limit has no release key.
func Run(ctx context.Context, in Input) (*Record, error) {
	t := in.Target
	if in.Pilot == nil && (t.Body == "" || in.WrongModel != "") {
		return nil, fmt.Errorf("probe: %s: without a pilot body, a target body is required and no wrong-model request is sent", t.Name)
	}
	if t.Confidential() && in.Key == nil {
		return nil, fmt.Errorf("probe: %s: a target with a confidential limit needs the release key", t.Name)
	}
	key := os.Getenv(t.APIKeyEnv)
	if key == "" {
		return nil, fmt.Errorf("probe: %s: environment variable %s is empty", t.Name, t.APIKeyEnv)
	}
	m := newMasker(key)
	var body []byte
	if in.Pilot != nil {
		body = in.Pilot.RequestBody(t, in.CaseText)
	} else {
		body = config.LiteralBody(t.Body, in.CaseText)
	}
	timeout := requestTimeout
	if in.Timeout > 0 {
		timeout = in.Timeout
	}
	sum := sha256.Sum256(body)
	rec := &Record{
		SchemaVersion:  1,
		Target:         t.Name,
		EndpointURL:    redact.URL(t.EndpointURL),
		RequestedModel: t.ModelName,
		StartUTC:       time.Now().UTC(),
		BodySHA256:     hex.EncodeToString(sum[:]),
		BodyBytes:      len(body),
	}
	transport := newTransport(1)
	defer transport.CloseIdleConnections()
	p := &prober{rt: transport, target: t, mask: m, timeout: timeout, key: in.Key}

	dispatched := time.Now()
	req := p.post(ctx, t.EndpointURL, t.ModelName, body, key)
	rec.Request, rec.Shape = req.ex, req.shape
	rec.Start = Gate(t, rec.Request.Status)

	if t.Probe != nil && t.Probe.LimitsURL != "" {
		ex := p.limits(ctx, key)
		rec.Limits = &ex
	}

	if in.WrongModel != "" {
		wt := *t
		wt.ModelName = in.WrongModel
		wait := time.NewTimer(time.Until(dispatched.Add(spacing)))
		select {
		case <-wait.C:
			wr := p.post(ctx, t.EndpointURL, in.WrongModel, in.Pilot.RequestBody(&wt, in.CaseText), key)
			rec.WrongModel = &wr.ex
		case <-ctx.Done():
			wait.Stop()
			rec.WrongModel = &Exchange{Method: http.MethodPost, URL: redact.URL(t.EndpointURL), Model: m.str(in.WrongModel), Error: "not sent: " + ctx.Err().Error()}
		}
	}
	rec.Findings = findings(t, rec.Request, rec.Shape, rec.RequestedModel)
	rec.EndUTC = time.Now().UTC()
	return rec, nil
}

// Encode returns rec, a *Record or a *ReleaseRecord, as Write writes it.
func Encode(rec any) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rec); err != nil {
		return nil, fmt.Errorf("probe: %w", err)
	}
	return b.Bytes(), nil
}

// Write writes rec, a *Record or a *ReleaseRecord, indented, to File in
// dir and returns the file's path.
func Write(dir string, rec any) (string, error) {
	b, err := Encode(rec)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("probe: %w", err)
	}
	path := filepath.Join(dir, File)
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return "", fmt.Errorf("probe: %w", err)
	}
	return path, nil
}

// RetainedRecord is what a probe record withholds, by exchange:
// "request", "limits" and "wrong_model" of a Record, "requests.<index>"
// and "limits" of a ReleaseRecord.
type RetainedRecord struct {
	SchemaVersion int                  `json:"schema_version"`
	Target        string               `json:"target"`
	StartUTC      time.Time            `json:"start_utc"`
	Exchanges     map[string]*Retained `json:"exchanges"`
}

// Withheld returns what rec, a *Record or a *ReleaseRecord, withholds, or
// nil when it withholds nothing.
func Withheld(rec any) (*RetainedRecord, error) {
	out := &RetainedRecord{SchemaVersion: 1, Exchanges: map[string]*Retained{}}
	add := func(name string, ex *Exchange) {
		if ex != nil && ex.Retained != nil {
			out.Exchanges[name] = ex.Retained
		}
	}
	switch r := rec.(type) {
	case *Record:
		out.Target, out.StartUTC = r.Target, r.StartUTC
		add("request", &r.Request)
		add("limits", r.Limits)
		add("wrong_model", r.WrongModel)
	case *ReleaseRecord:
		out.Target, out.StartUTC = r.Route, r.StartUTC
		for i := range r.Requests {
			add(fmt.Sprintf("requests.%d", r.Requests[i].Index), &r.Requests[i].Exchange)
		}
		add("limits", r.Limits)
	default:
		return nil, fmt.Errorf("probe: %T is not a probe record", rec)
	}
	if len(out.Exchanges) == 0 {
		return nil, nil
	}
	return out, nil
}

// WriteRetained writes what rec withholds (Withheld) to RetainedFile in
// dir, readable by the owner only, and returns the file's path, or ""
// when rec withholds nothing. It refuses an existing file.
func WriteRetained(dir string, rec any) (string, error) {
	held, err := Withheld(rec)
	if err != nil || held == nil {
		return "", err
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(held); err != nil {
		return "", fmt.Errorf("probe: %w", err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("probe: %w", err)
	}
	path := filepath.Join(dir, RetainedFile)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", fmt.Errorf("probe: %w", err)
	}
	if _, err := f.Write(b.Bytes()); err != nil {
		f.Close()
		return "", fmt.Errorf("probe: %w", err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("probe: %w", err)
	}
	return path, nil
}

type prober struct {
	rt     http.RoundTripper
	target *config.Target
	mask   masker
	// timeout bounds each exchange from its dispatch.
	timeout time.Duration
	// key hashes what the record withholds on a target with a
	// confidential limit.
	key *releasekey.Key
}

type posted struct {
	ex    Exchange
	shape *Shape
}

// post sends one inference request in the target's adapter shape and
// reads a 200 stream to its terminator through the shape reader.
func (p *prober) post(ctx context.Context, url, model string, body []byte, key string) posted {
	var out posted
	name, value := p.target.KeyHeader(key)
	headers := [][2]string{{name, value}}
	if p.target.Adapter == config.AdapterMessages {
		headers = append(headers, [2]string{"anthropic-version", config.MessagesVersion})
	}
	out.ex = p.exchange(ctx, http.MethodPost, url, body, headers, errorBodyLimit, func(r io.Reader, since func() int64) {
		out.shape = readShape(r, p.target.Adapter, p.target.Shape(), p.mask, since, p.timeout)
	})
	out.ex.Model = p.mask.str(model)
	if p.target.Confidential() {
		withhold(&out.ex, out.shape, p.key)
	}
	return out
}

// withhold moves ex's non-200 body and s's in-band error text into
// ex.Retained, leaving a stand-in with the HMAC-SHA256 of each under key.
func withhold(ex *Exchange, s *Shape, key *releasekey.Key) {
	held := func() *Retained {
		if ex.Retained == nil {
			ex.Retained = &Retained{}
		}
		return ex.Retained
	}
	if b := ex.Body; b != nil {
		held().Body = b
		ex.Body = &Body{Truncated: b.Truncated, Retained: true, HMACSHA256: key.Sum(b.raw())}
	}
	if s != nil && s.Error != "" {
		held().StreamError = s.Error
		s.Error, s.ErrorHMACSHA256 = config.RetainedError(s.Error), key.Sum([]byte(s.Error))
	}
}

// exchange sends one request with the run's transport, never following
// a redirect or retrying, and records it. headers are names and values
// set on the request, the credential header first. A 200 body goes to
// stream when set; any other body is kept up to limit bytes.
func (p *prober) exchange(ctx context.Context, method, url string, body []byte, headers [][2]string, limit int, stream func(io.Reader, func() int64)) Exchange {
	ex := Exchange{Method: method, URL: redact.URL(url), DispatchUTC: time.Now().UTC()}
	start := time.Now()
	since := func() int64 { return time.Since(start).Nanoseconds() }
	timeout := p.timeout
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// A dial can outlive the request; closed stops its callbacks writing
	// to the returned record.
	var mu sync.Mutex
	var closed bool
	var dnsStart, tlsStart int64
	connStart := map[string]int64{}
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		DNSStart: func(info httptrace.DNSStartInfo) {
			mu.Lock()
			defer mu.Unlock()
			if closed {
				return
			}
			dnsStart = since()
			ex.DNS = &DNS{Host: info.Host}
		},
		DNSDone: func(info httptrace.DNSDoneInfo) {
			mu.Lock()
			defer mu.Unlock()
			if closed {
				return
			}
			if ex.DNS == nil {
				return
			}
			ex.DNS.DurationNs = since() - dnsStart
			for _, a := range info.Addrs {
				ex.DNS.Addrs = append(ex.DNS.Addrs, a.String())
			}
			if info.Err != nil {
				ex.DNS.Error = redact.ErrorText(info.Err, timeout)
			}
		},
		ConnectStart: func(_, addr string) {
			mu.Lock()
			defer mu.Unlock()
			if closed {
				return
			}
			connStart[addr] = since()
		},
		ConnectDone: func(_, addr string, err error) {
			mu.Lock()
			defer mu.Unlock()
			if closed {
				return
			}
			c := Connect{Addr: addr, DurationNs: since() - connStart[addr]}
			if err != nil {
				c.Error = redact.ErrorText(err, timeout)
			}
			ex.Connects = append(ex.Connects, c)
		},
		TLSHandshakeStart: func() {
			mu.Lock()
			defer mu.Unlock()
			if closed {
				return
			}
			tlsStart = since()
		},
		TLSHandshakeDone: func(st tls.ConnectionState, err error) {
			mu.Lock()
			defer mu.Unlock()
			if closed {
				return
			}
			ex.TLS = tlsOf(st, err, since()-tlsStart, timeout)
		},
		GotConn: func(info httptrace.GotConnInfo) {
			mu.Lock()
			defer mu.Unlock()
			if closed {
				return
			}
			ex.PeerAddr = info.Conn.RemoteAddr().String()
		},
		GotFirstResponseByte: func() {
			mu.Lock()
			defer mu.Unlock()
			if closed {
				return
			}
			ex.FirstByteNs = since()
		},
	})
	done := func() Exchange {
		mu.Lock()
		defer mu.Unlock()
		closed = true
		ex.DoneNs = since()
		return ex
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		ex.Error = redact.ErrorText(err, timeout)
		return done()
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	} else {
		req.Header.Set("Accept", "application/json")
	}
	for _, h := range headers {
		req.Header.Set(h[0], h[1])
	}
	req.GetBody = nil
	resp, err := p.rt.RoundTrip(req)
	if err != nil {
		mu.Lock()
		ex.Error = redact.ErrorText(err, timeout)
		mu.Unlock()
		return done()
	}
	defer resp.Body.Close()
	mu.Lock()
	ex.Proto, ex.Status = resp.Proto, resp.StatusCode
	var held map[string][]string
	ex.Headers, ex.HeaderNames, held = p.headers(resp.Header)
	if held != nil {
		ex.Retained = &Retained{Headers: held}
	}
	mu.Unlock()
	if resp.StatusCode == http.StatusOK && stream != nil {
		stream(resp.Body, since)
		return done()
	}
	// The bytes past limit let a key that starts inside it be masked whole.
	b, rerr := io.ReadAll(io.LimitReader(resp.Body, int64(limit+p.mask.longest()+1)))
	b = p.mask.bytes(b)
	truncated := len(b) > limit
	if truncated {
		b = b[:limit]
	}
	mu.Lock()
	ex.Body = bodyOf(b, truncated)
	if rerr != nil {
		ex.Error = redact.ErrorText(rerr, timeout)
	}
	mu.Unlock()
	return done()
}

// headers returns the allowlisted headers, any value carrying the key
// dropped and each retained value replaced by config.RetainedHeaderValue,
// every header name, and the retained values. Names arrive case-folded,
// so they are masked without regard to case.
func (p *prober) headers(h http.Header) (map[string][]string, []string, map[string][]string) {
	var out, held map[string][]string
	names := make([]string, 0, len(h))
	for name, vs := range h {
		masked := p.mask.fold(name)
		names = append(names, masked)
		if !config.HeaderAllowed(p.target.ResponseHeaders, name) || masked != name {
			continue
		}
		retained := p.target.HeaderRetained(name)
		for _, v := range vs {
			if p.mask.str(v) != v {
				continue
			}
			if out == nil {
				out = map[string][]string{}
			}
			if retained {
				if held == nil {
					held = map[string][]string{}
				}
				held[name] = append(held[name], v)
				v = config.RetainedHeaderValue
			}
			out[name] = append(out[name], v)
		}
	}
	sort.Strings(names)
	return out, names, held
}

// limits reads target.probe.limits_url with the key after the configured
// Authorization scheme. A retained body leaves its HMAC-SHA256 under the
// release key on a target with a confidential limit, otherwise its
// SHA-256.
func (p *prober) limits(ctx context.Context, key string) Exchange {
	t := p.target
	ex := p.exchange(ctx, http.MethodGet, t.Probe.LimitsURL, nil, [][2]string{{"Authorization", t.Probe.LimitsAuthScheme + " " + key}}, limitsBodyLimit, nil)
	if t.Probe.LimitsRetained && ex.Body != nil {
		if ex.Retained == nil {
			ex.Retained = &Retained{}
		}
		ex.Retained.Body = ex.Body
		stand := &Body{Truncated: ex.Body.Truncated, Retained: true}
		if t.Confidential() {
			stand.HMACSHA256 = p.key.Sum(ex.Body.raw())
		} else {
			sum := sha256.Sum256(ex.Body.raw())
			stand.SHA256 = hex.EncodeToString(sum[:])
		}
		ex.Body = stand
	}
	return ex
}

func tlsOf(st tls.ConnectionState, err error, ns int64, timeout time.Duration) *TLS {
	t := &TLS{DurationNs: ns}
	if err != nil {
		t.Error = redact.ErrorText(err, timeout)
		return t
	}
	t.Version = tls.VersionName(st.Version)
	t.CipherSuite = tls.CipherSuiteName(st.CipherSuite)
	t.ALPN = st.NegotiatedProtocol
	t.ServerName = st.ServerName
	if len(st.PeerCertificates) > 0 {
		c := st.PeerCertificates[0]
		t.Certificate = &Certificate{Subject: c.Subject.String(), Issuer: c.Issuer.String(), NotAfter: c.NotAfter.UTC()}
	}
	return t
}

// masker holds the key and every run of redact.SecretMin bytes taken
// from it, the whole key first.
type masker []string

func newMasker(key string) masker {
	m := masker{key}
	for i := 0; i+redact.SecretMin <= len(key); i++ {
		m = append(m, key[i:i+redact.SecretMin])
	}
	return m
}

func (m masker) bytes(b []byte) []byte {
	for _, s := range m {
		b = bytes.ReplaceAll(b, []byte(s), bytes.Repeat([]byte("*"), len(s)))
	}
	return b
}

func (m masker) str(s string) string {
	for _, sec := range m {
		s = strings.ReplaceAll(s, sec, strings.Repeat("*", len(sec)))
	}
	return s
}

// fold masks every match found without regard to ASCII case.
func (m masker) fold(s string) string {
	b := []byte(s)
	lower := strings.ToLower(s)
	for _, sec := range m {
		sec = strings.ToLower(sec)
		for i := 0; sec != "" && i+len(sec) <= len(lower); i++ {
			if lower[i:i+len(sec)] == sec {
				copy(b[i:i+len(sec)], strings.Repeat("*", len(sec)))
			}
		}
	}
	return string(b)
}

func (m masker) longest() int {
	if len(m) == 0 {
		return 0
	}
	return len(m[0])
}

// findings compares the observed shape with the target's configuration.
func findings(t *config.Target, ex Exchange, s *Shape, requested string) []string {
	var f []string
	if ex.Proto != "" && ex.Proto != "HTTP/1.1" {
		f = append(f, fmt.Sprintf("response over %s, not HTTP/1.1", ex.Proto))
	}
	if s == nil {
		return f
	}
	if t.Adapter == config.AdapterMessages {
		return append(f, messagesFindings(s, requested)...)
	}
	sh := t.Shape()
	if !s.Done {
		f = append(f, "the stream ended without data: [DONE]")
	}
	if s.Undecodable > 0 {
		f = append(f, fmt.Sprintf("%d events did not decode as a JSON object", s.Undecodable))
	}
	if s.ContentEvents == 0 {
		f = append(f, fmt.Sprintf("no event carried text in the configured content key %q", sh.Content))
	}
	for _, k := range sortedKeys(s.DeltaKeys) {
		if s.DeltaKeys[k].StringBytes > 0 && k != sh.Content && k != "role" && !slices.Contains(sh.Reasoning, k) &&
			!slices.Contains(sh.Refusal, "choices.0.delta."+k) {
			f = append(f, fmt.Sprintf("delta key %q carries text and is neither the content key nor a configured reasoning key", k))
		}
	}
	for _, u := range s.Usage {
		if !slices.Contains(sh.Usage, u.Path) {
			f = append(f, fmt.Sprintf("usage object at %q, not a configured usage path", u.Path))
		}
	}
	if len(s.Usage) == 0 {
		f = append(f, "no usage object in the stream")
	}
	for _, r := range s.Refusals {
		if !slices.Contains(sh.Refusal, r) {
			f = append(f, fmt.Sprintf("refusal field at %q, not a configured refusal path", r))
		}
	}
	if len(s.FinishReasons) == 0 {
		f = append(f, "no finish_reason in the stream")
	}
	if len(s.Models) == 0 {
		f = append(f, fmt.Sprintf("no model id at the configured path %q", sh.Model))
	}
	return append(f, sharedFindings(s, requested)...)
}

// messagesEvents and messagesDeltas are the event and delta types the
// Messages adapter reads.
var (
	messagesEvents = []string{"message_start", "content_block_start", "content_block_delta", "content_block_stop", "message_delta", "ping", "error"}
	messagesDeltas = []string{"text_delta", "thinking_delta", "signature_delta"}
)

func messagesFindings(s *Shape, requested string) []string {
	var f []string
	if !s.Done {
		f = append(f, "the stream ended without message_stop")
	}
	if s.Undecodable > 0 {
		f = append(f, fmt.Sprintf("%d events did not decode as a JSON object", s.Undecodable))
	}
	if s.ContentEvents == 0 {
		f = append(f, "no text_delta carried text")
	}
	for _, k := range sortedCounts(s.EventTypes) {
		if !slices.Contains(messagesEvents, k) {
			f = append(f, fmt.Sprintf("event type %q is not one the Messages adapter reads", k))
		}
	}
	for _, k := range sortedCounts(s.DeltaTypes) {
		if !strings.HasPrefix(k, "block:") && !slices.Contains(messagesDeltas, k) {
			f = append(f, fmt.Sprintf("delta type %q is not one the Messages adapter reads", k))
		}
	}
	if len(s.Usage) == 0 {
		f = append(f, "no usage object in the stream")
	}
	for _, r := range s.Refusals {
		f = append(f, fmt.Sprintf("refusal field at %q", r))
	}
	if len(s.FinishReasons) == 0 {
		f = append(f, "no stop_reason in the stream")
	}
	if len(s.Models) == 0 {
		f = append(f, "no model id in message_start")
	}
	return append(f, sharedFindings(s, requested)...)
}

func sharedFindings(s *Shape, requested string) []string {
	var f []string
	for _, mdl := range s.Models {
		if mdl != requested {
			f = append(f, fmt.Sprintf("reported model %q differs from the requested %q", mdl, requested))
		}
	}
	if s.Error != "" {
		f = append(f, "the stream carried an in-band error object")
	}
	return f
}

func sortedCounts(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
