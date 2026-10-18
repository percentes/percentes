// Package admission is the pilot's request and liability ledger: one
// append-only JSON Lines file holding every inference request sent to a
// pilot endpoint (probe, development, warm-up, measured and replacement
// traffic), the per-request prices and the hosting costs the operator
// entered. A window starts only when the ledger plus WindowRequests stays
// within EndpointRequestCap for its endpoint and TotalRequestCap in all,
// and the projected liability stays within LiabilityCapUSD.
//
// An admission reserves its requests before any is sent and counts at the
// reservation until settled with the requests actually sent, so a run cut
// short still holds its reservation. Each line carries its sequence number
// and the SHA-256 of every byte before it; a ledger whose chain or last
// line does not check admits nothing.
package admission

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/url"
	"os"
	"regexp"
	"syscall"
	"time"

	"github.com/percentes/percentes/internal/config"
)

// Pilot caps and the per-request token bounds the price is taken at.
const (
	EndpointRequestCap = 1024
	TotalRequestCap    = 2048
	LiabilityCapUSD    = "40"
	MaxInputTokens     = 2000
	MaxOutputTokens    = config.PinnedPilotCompletionCeiling
)

// WindowRequests is one window's reservation: the session schedule's
// requests plus the window's pre-run probe request.
const WindowRequests = int(config.PinnedPilotRateRPS*(config.PinnedPilotWarmupS+config.PinnedPilotMeasuredS)) + 1

// SchemaVersion is the ledger format's version.
const SchemaVersion = 1

// Entry kinds.
const (
	KindOpen    = "open"
	KindPrice   = "price"
	KindHosting = "hosting"
	KindAdmit   = "admit"
	KindRefused = "refused"
	KindSettle  = "settle"
)

// Admission purposes. A window or replacement reserves WindowRequests; a
// probe or development admission reserves the count it names.
const (
	PurposeProbe       = "probe"
	PurposeDevelopment = "development"
	PurposeWindow      = "window"
	PurposeReplacement = "replacement"
)

// Entry is one ledger line. Seq counts lines from 1; Prev is the hex
// SHA-256 of every byte of the file before the line.
type Entry struct {
	Seq  int       `json:"seq"`
	Prev string    `json:"prev"`
	Time time.Time `json:"time"`
	Kind string    `json:"kind"`

	// open
	SchemaVersion int   `json:"schema_version,omitempty"`
	Caps          *Caps `json:"caps,omitempty"`

	// price, admit, refused, settle
	Endpoint string `json:"endpoint,omitempty"`

	// price: list prices in US$ per million tokens, and where they were
	// read. PerRequestUSD is the price at MaxInputTokens and
	// MaxOutputTokens.
	InputUSDPerMTok  string `json:"input_usd_per_mtok,omitempty"`
	OutputUSDPerMTok string `json:"output_usd_per_mtok,omitempty"`
	PerRequestUSD    string `json:"per_request_usd,omitempty"`
	Source           string `json:"source,omitempty"`
	Retrieved        string `json:"retrieved,omitempty"`

	// hosting
	USD  string `json:"usd,omitempty"`
	Note string `json:"note,omitempty"`

	// admit, refused
	Purpose string    `json:"purpose,omitempty"`
	Reserve int       `json:"reserve,omitempty"`
	Before  *Position `json:"before,omitempty"`
	After   *Position `json:"after,omitempty"`
	Reason  string    `json:"reason,omitempty"`

	// settle: the admit line it settles and the requests sent.
	Admit   int     `json:"admit,omitempty"`
	Sent    *Counts `json:"sent,omitempty"`
	Overrun bool    `json:"overrun,omitempty"`
}

// Caps are the limits in force when the ledger was created.
type Caps struct {
	EndpointRequests int    `json:"endpoint_requests"`
	TotalRequests    int    `json:"total_requests"`
	LiabilityUSD     string `json:"liability_usd"`
	WindowRequests   int    `json:"window_requests"`
	MaxInputTokens   int    `json:"max_input_tokens"`
	MaxOutputTokens  int    `json:"max_output_tokens"`
}

func currentCaps() *Caps {
	return &Caps{EndpointRequestCap, TotalRequestCap, LiabilityCapUSD, WindowRequests, MaxInputTokens, MaxOutputTokens}
}

// Position is the ledger's charge for the endpoint, in all, and the
// projected liability.
type Position struct {
	EndpointRequests int    `json:"endpoint_requests"`
	TotalRequests    int    `json:"total_requests"`
	LiabilityUSD     string `json:"liability_usd"`
}

// Counts are requests sent, by traffic class.
type Counts struct {
	Probe       int `json:"probe"`
	Development int `json:"development"`
	Warmup      int `json:"warmup"`
	Measured    int `json:"measured"`
}

func (c Counts) total() int { return c.Probe + c.Development + c.Warmup + c.Measured }

// Usage is one endpoint's charge. A replacement window's warm-up and
// measured requests count as Replacement; its probe request as Probe.
// Reserved is the reservation of admissions not yet settled.
type Usage struct {
	Probe       int
	Development int
	Warmup      int
	Measured    int
	Replacement int
	Reserved    int
	// MaxPerRequestUSD is the highest per-request price recorded for the
	// endpoint (nil = none).
	MaxPerRequestUSD *big.Rat
}

// Charged is every request sent plus every unsettled reservation.
func (u *Usage) Charged() int {
	return u.Probe + u.Development + u.Warmup + u.Measured + u.Replacement + u.Reserved
}

// State is the ledger read and checked.
type State struct {
	Lines      int
	Endpoints  map[string]*Usage
	HostingUSD *big.Rat
	// Open maps each unsettled admit line to its admission.
	Open map[int]Admission
	// order lists endpoints in first-seen order.
	order []string
}

// Admission is one admitted reservation.
type Admission struct {
	Seq      int
	Endpoint string
	Purpose  string
	Reserve  int
}

// Total is the charge across endpoints.
func (s *State) Total() int {
	n := 0
	for _, u := range s.Endpoints {
		n += u.Charged()
	}
	return n
}

// Liability is every endpoint's charge at its highest recorded
// per-request price plus the hosting costs. An endpoint with a charge
// and no price is an error.
func (s *State) Liability() (*big.Rat, error) {
	return s.liability("", 0)
}

func (s *State) liability(endpoint string, extra int) (*big.Rat, error) {
	sum := new(big.Rat).Set(s.HostingUSD)
	for _, e := range s.order {
		u := s.Endpoints[e]
		n := u.Charged()
		if e == endpoint {
			n += extra
		}
		if n == 0 {
			continue
		}
		if u.MaxPerRequestUSD == nil {
			return nil, fmt.Errorf("no price recorded for %s", e)
		}
		sum.Add(sum, new(big.Rat).Mul(u.MaxPerRequestUSD, new(big.Rat).SetInt64(int64(n))))
	}
	return sum, nil
}

func (s *State) usage(endpoint string) *Usage {
	u, ok := s.Endpoints[endpoint]
	if !ok {
		u = &Usage{}
		s.Endpoints[endpoint] = u
		s.order = append(s.order, endpoint)
	}
	return u
}

// Refusal is an admission the caps refused; the ledger records it.
type Refusal struct {
	Endpoint string
	Purpose  string
	Reason   string
}

func (r *Refusal) Error() string {
	return fmt.Sprintf("admission: %s for %s refused: %s", r.Purpose, r.Endpoint, r.Reason)
}

// Ledger is the ledger file at a path. Every operation reads and checks
// the whole file under an exclusive lock before it appends.
type Ledger struct {
	path string
	now  func() time.Time
}

// Create starts a new ledger at path; the file must not exist.
func Create(path string) (*Ledger, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("admission: create ledger: %w", err)
	}
	l := &Ledger{path: path, now: time.Now}
	err = l.append(f, nil, &Entry{Kind: KindOpen, SchemaVersion: SchemaVersion, Caps: currentCaps()})
	if cerr := f.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("admission: create ledger: %w", cerr)
	}
	if err != nil {
		return nil, err
	}
	return l, nil
}

// Open opens an existing ledger and checks it.
func Open(path string) (*Ledger, error) {
	l := &Ledger{path: path, now: time.Now}
	if _, err := l.State(); err != nil {
		return nil, err
	}
	return l, nil
}

// State reads and checks the ledger.
func (l *Ledger) State() (*State, error) {
	f, err := os.Open(l.path)
	if err != nil {
		return nil, fmt.Errorf("admission: ledger: %w", err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH); err != nil {
		return nil, fmt.Errorf("admission: lock ledger: %w", err)
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("admission: ledger: %w", err)
	}
	return Parse(b)
}

// update runs fn on the checked ledger under an exclusive lock and appends
// the entry it returns. A Refusal from fn is appended as a refused entry
// and returned.
func (l *Ledger) update(fn func(*State) (*Entry, error)) (*Entry, error) {
	f, err := os.OpenFile(l.path, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		return nil, fmt.Errorf("admission: ledger: %w", err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return nil, fmt.Errorf("admission: lock ledger: %w", err)
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("admission: ledger: %w", err)
	}
	st, err := Parse(b)
	if err != nil {
		return nil, err
	}
	e, ferr := fn(st)
	var ref *Refusal
	if ferr != nil && !errors.As(ferr, &ref) {
		return nil, ferr
	}
	if e == nil {
		return nil, ferr
	}
	if err := l.append(f, b, e); err != nil {
		return nil, err
	}
	return e, ferr
}

// append writes e after prior, the file's whole content, and syncs.
func (l *Ledger) append(f *os.File, prior []byte, e *Entry) error {
	sum := sha256.Sum256(prior)
	e.Seq = bytes.Count(prior, []byte("\n")) + 1
	e.Prev = hex.EncodeToString(sum[:])
	e.Time = l.now().UTC()
	line, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("admission: ledger: %w", err)
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("admission: ledger write: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("admission: ledger sync: %w", err)
	}
	return nil
}

var (
	decimalRe = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]+)?$`)
	dateRe    = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`)
)

func decimal(s string) (*big.Rat, error) {
	if !decimalRe.MatchString(s) {
		return nil, fmt.Errorf("%q is not a decimal number", s)
	}
	r, _ := new(big.Rat).SetString(s)
	return r, nil
}

// decimalString writes r, a terminating decimal, in full.
func decimalString(r *big.Rat) string {
	for p := 0; p < 64; p++ {
		s := r.FloatString(p)
		if v, ok := new(big.Rat).SetString(s); ok && v.Cmp(r) == 0 {
			return s
		}
	}
	return r.RatString()
}

// perRequest is the price of one request at the token bounds.
func perRequest(in, out *big.Rat) *big.Rat {
	r := new(big.Rat).Mul(in, big.NewRat(MaxInputTokens, 1))
	r.Add(r, new(big.Rat).Mul(out, big.NewRat(MaxOutputTokens, 1)))
	return r.Quo(r, big.NewRat(1_000_000, 1))
}

// Price is a provider's list price for an endpoint, as read.
type Price struct {
	InputUSDPerMTok  string
	OutputUSDPerMTok string
	// Source is the URL the prices were read from; Retrieved its date
	// (2006-01-02).
	Source    string
	Retrieved string
}

func checkPrice(endpoint string, p Price) (*big.Rat, error) {
	if endpoint == "" {
		return nil, errors.New("empty endpoint")
	}
	in, err := decimal(p.InputUSDPerMTok)
	if err != nil {
		return nil, fmt.Errorf("input price: %w", err)
	}
	out, err := decimal(p.OutputUSDPerMTok)
	if err != nil {
		return nil, fmt.Errorf("output price: %w", err)
	}
	if u, err := url.Parse(p.Source); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("source %q is not an http or https URL", p.Source)
	}
	if _, err := time.Parse("2006-01-02", p.Retrieved); err != nil || !dateRe.MatchString(p.Retrieved) {
		return nil, fmt.Errorf("retrieved %q is not a 2006-01-02 date", p.Retrieved)
	}
	return perRequest(in, out), nil
}

// RecordPrice appends an endpoint's prices. Admission uses the highest
// per-request price ever recorded for the endpoint.
func (l *Ledger) RecordPrice(endpoint string, p Price) error {
	_, err := l.update(func(*State) (*Entry, error) {
		r, err := checkPrice(endpoint, p)
		if err != nil {
			return nil, fmt.Errorf("admission: price: %w", err)
		}
		return &Entry{Kind: KindPrice, Endpoint: endpoint, InputUSDPerMTok: p.InputUSDPerMTok, OutputUSDPerMTok: p.OutputUSDPerMTok,
			PerRequestUSD: decimalString(r), Source: p.Source, Retrieved: p.Retrieved}, nil
	})
	return err
}

// RecordHosting appends a hosting cost in US$. Entries add up and are
// never withdrawn.
func (l *Ledger) RecordHosting(usd, note string) error {
	_, err := l.update(func(*State) (*Entry, error) {
		r, err := decimal(usd)
		if err != nil || r.Sign() == 0 {
			return nil, fmt.Errorf("admission: hosting: %q is not a positive decimal number", usd)
		}
		if note == "" {
			return nil, errors.New("admission: hosting: empty note")
		}
		return &Entry{Kind: KindHosting, USD: usd, Note: note}, nil
	})
	return err
}

// Admit reserves n requests to endpoint for purpose. A window or
// replacement must reserve exactly WindowRequests. The admission is
// refused, and the refusal recorded, when the endpoint's charge plus n
// exceeds EndpointRequestCap, the total plus n exceeds TotalRequestCap, or
// the projected liability exceeds LiabilityCapUSD.
func (l *Ledger) Admit(endpoint, purpose string, n int) (Admission, error) {
	var a Admission
	_, err := l.update(func(st *State) (*Entry, error) {
		if endpoint == "" {
			return nil, errors.New("admission: empty endpoint")
		}
		switch purpose {
		case PurposeWindow, PurposeReplacement:
			if n != WindowRequests {
				return nil, fmt.Errorf("admission: a %s reserves %d requests, not %d", purpose, WindowRequests, n)
			}
		case PurposeProbe, PurposeDevelopment:
			if n < 1 {
				return nil, fmt.Errorf("admission: a %s reserves at least one request, not %d", purpose, n)
			}
		default:
			return nil, fmt.Errorf("admission: unknown purpose %q", purpose)
		}
		before, err := st.position(endpoint, 0)
		var after *Position
		if err == nil {
			after, err = st.position(endpoint, n)
		}
		if err != nil {
			return &Entry{Kind: KindRefused, Endpoint: endpoint, Purpose: purpose, Reserve: n, Reason: err.Error()},
				&Refusal{endpoint, purpose, err.Error()}
		}
		reason := ""
		switch liab, _ := decimal(after.LiabilityUSD); {
		case after.EndpointRequests > EndpointRequestCap:
			reason = fmt.Sprintf("%d requests for the endpoint exceed the cap of %d", after.EndpointRequests, EndpointRequestCap)
		case after.TotalRequests > TotalRequestCap:
			reason = fmt.Sprintf("%d requests in total exceed the cap of %d", after.TotalRequests, TotalRequestCap)
		case liab.Cmp(liabilityCap) > 0:
			reason = fmt.Sprintf("projected liability US$%s exceeds US$%s", after.LiabilityUSD, LiabilityCapUSD)
		}
		if reason != "" {
			return &Entry{Kind: KindRefused, Endpoint: endpoint, Purpose: purpose, Reserve: n, Before: before, After: after, Reason: reason},
				&Refusal{endpoint, purpose, reason}
		}
		a = Admission{Seq: st.Lines + 1, Endpoint: endpoint, Purpose: purpose, Reserve: n}
		return &Entry{Kind: KindAdmit, Endpoint: endpoint, Purpose: purpose, Reserve: n, Before: before, After: after}, nil
	})
	if err != nil {
		return Admission{}, err
	}
	return a, nil
}

var liabilityCap, _ = new(big.Rat).SetString(LiabilityCapUSD)

// position is the charge with extra more requests to endpoint. It fails
// when the liability cannot be projected.
func (st *State) position(endpoint string, extra int) (*Position, error) {
	if extra > 0 {
		if u, ok := st.Endpoints[endpoint]; !ok || u.MaxPerRequestUSD == nil {
			return nil, fmt.Errorf("no price recorded for %s", endpoint)
		}
	}
	liab, err := st.liability(endpoint, extra)
	if err != nil {
		return nil, err
	}
	n := extra
	if u, ok := st.Endpoints[endpoint]; ok {
		n += u.Charged()
	}
	return &Position{EndpointRequests: n, TotalRequests: st.Total() + extra, LiabilityUSD: decimalString(liab)}, nil
}

// Settle records the requests a's traffic sent, replacing its
// reservation. A probe admission sends probe requests only, a development
// admission development requests only, a window or replacement probe,
// warm-up and measured requests. Sent counts above the reservation are
// recorded as an overrun and returned as an error after the write.
func (l *Ledger) Settle(a Admission, sent Counts) error {
	e, err := l.update(func(st *State) (*Entry, error) {
		open, ok := st.Open[a.Seq]
		if !ok {
			return nil, fmt.Errorf("admission: settle: line %d is not an unsettled admission", a.Seq)
		}
		if open != a {
			return nil, fmt.Errorf("admission: settle: line %d is %+v, not %+v", a.Seq, open, a)
		}
		if err := checkCounts(a.Purpose, sent); err != nil {
			return nil, fmt.Errorf("admission: settle: %w", err)
		}
		s := sent
		return &Entry{Kind: KindSettle, Endpoint: a.Endpoint, Admit: a.Seq, Sent: &s, Overrun: sent.total() > a.Reserve}, nil
	})
	if err != nil {
		return err
	}
	if e.Overrun {
		return fmt.Errorf("admission: settle of line %d: %d requests sent against a reservation of %d", a.Seq, sent.total(), a.Reserve)
	}
	return nil
}

func checkCounts(purpose string, c Counts) error {
	if c.Probe < 0 || c.Development < 0 || c.Warmup < 0 || c.Measured < 0 {
		return fmt.Errorf("negative count %+v", c)
	}
	var stray bool
	switch purpose {
	case PurposeProbe:
		stray = c.Development+c.Warmup+c.Measured > 0
	case PurposeDevelopment:
		stray = c.Probe+c.Warmup+c.Measured > 0
	case PurposeWindow, PurposeReplacement:
		stray = c.Development > 0
	default:
		return fmt.Errorf("unknown purpose %q", purpose)
	}
	if stray {
		return fmt.Errorf("a %s admission cannot send %+v", purpose, c)
	}
	return nil
}

// Parse checks a ledger's bytes and returns its state.
func Parse(b []byte) (*State, error) {
	st := &State{Endpoints: map[string]*Usage{}, HostingUSD: new(big.Rat), Open: map[int]Admission{}}
	if len(b) == 0 {
		return nil, errors.New("admission: ledger is empty")
	}
	if b[len(b)-1] != '\n' {
		return nil, errors.New("admission: ledger's last line is incomplete")
	}
	h := sha256.New()
	for rest := b; len(rest) > 0; {
		i := bytes.IndexByte(rest, '\n')
		line := rest[:i]
		rest = rest[i+1:]
		seq := st.Lines + 1
		var e Entry
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&e); err != nil {
			return nil, fmt.Errorf("admission: ledger line %d: %w", seq, err)
		}
		if dec.More() {
			return nil, fmt.Errorf("admission: ledger line %d: trailing data", seq)
		}
		if e.Seq != seq {
			return nil, fmt.Errorf("admission: ledger line %d: seq %d", seq, e.Seq)
		}
		if want := hex.EncodeToString(h.Sum(nil)); e.Prev != want {
			return nil, fmt.Errorf("admission: ledger line %d: prev %s does not match the preceding bytes (%s)", seq, e.Prev, want)
		}
		if err := st.apply(&e); err != nil {
			return nil, fmt.Errorf("admission: ledger line %d: %w", seq, err)
		}
		h.Write(line)
		h.Write([]byte{'\n'})
		st.Lines = seq
	}
	return st, nil
}

// apply adds one checked entry to the state.
func (st *State) apply(e *Entry) error {
	if (e.Kind == KindOpen) != (e.Seq == 1) {
		return fmt.Errorf("kind %q on line %d", e.Kind, e.Seq)
	}
	switch e.Kind {
	case KindOpen:
		if e.SchemaVersion != SchemaVersion || e.Caps == nil {
			return fmt.Errorf("schema_version %d, caps %v", e.SchemaVersion, e.Caps)
		}
	case KindPrice:
		r, err := checkPrice(e.Endpoint, Price{e.InputUSDPerMTok, e.OutputUSDPerMTok, e.Source, e.Retrieved})
		if err != nil {
			return err
		}
		if e.PerRequestUSD != decimalString(r) {
			return fmt.Errorf("per_request_usd %q, prices give %s", e.PerRequestUSD, decimalString(r))
		}
		u := st.usage(e.Endpoint)
		if u.MaxPerRequestUSD == nil || r.Cmp(u.MaxPerRequestUSD) > 0 {
			u.MaxPerRequestUSD = r
		}
	case KindHosting:
		r, err := decimal(e.USD)
		if err != nil {
			return err
		}
		st.HostingUSD.Add(st.HostingUSD, r)
	case KindAdmit:
		switch {
		case e.Endpoint == "" || e.Reserve < 1:
			return fmt.Errorf("admit %q reserve %d", e.Endpoint, e.Reserve)
		case (e.Purpose == PurposeWindow || e.Purpose == PurposeReplacement) && e.Reserve != WindowRequests:
			return fmt.Errorf("%s reserve %d", e.Purpose, e.Reserve)
		}
		if err := checkCounts(e.Purpose, Counts{}); err != nil {
			return err
		}
		st.usage(e.Endpoint).Reserved += e.Reserve
		st.Open[e.Seq] = Admission{Seq: e.Seq, Endpoint: e.Endpoint, Purpose: e.Purpose, Reserve: e.Reserve}
	case KindRefused:
	case KindSettle:
		a, ok := st.Open[e.Admit]
		if !ok || e.Sent == nil || a.Endpoint != e.Endpoint {
			return fmt.Errorf("settle of line %d does not match an unsettled admission", e.Admit)
		}
		if err := checkCounts(a.Purpose, *e.Sent); err != nil {
			return err
		}
		if e.Overrun != (e.Sent.total() > a.Reserve) {
			return fmt.Errorf("overrun %t for %d sent against %d", e.Overrun, e.Sent.total(), a.Reserve)
		}
		delete(st.Open, e.Admit)
		u := st.usage(a.Endpoint)
		u.Reserved -= a.Reserve
		c := e.Sent
		u.Probe += c.Probe
		u.Development += c.Development
		if a.Purpose == PurposeReplacement {
			u.Replacement += c.Warmup + c.Measured
		} else {
			u.Warmup += c.Warmup
			u.Measured += c.Measured
		}
	default:
		return fmt.Errorf("unknown kind %q", e.Kind)
	}
	return nil
}
