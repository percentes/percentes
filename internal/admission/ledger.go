// Package admission is the request and liability ledger of the pilot and
// the release, and the release's ceiling rule. The ledger is one
// append-only JSON Lines file holding every inference request sent to a
// route (probe, development, warm-up, measured and replacement traffic),
// the per-request prices and the hosting costs the operator entered. Its
// first line fixes the caps it enforces: the pilot's (PilotCaps) or a
// release's, read from each route's configuration (ReleaseCaps). An
// admission names the token bounds of its requests and counts them at the
// highest per-request price recorded for the route at those bounds. It is
// refused when the route has no price at them, or when it would take a
// route's or the total request count, or a route's or the global
// projected liability, past its cap.
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

// The pilot's caps and the per-request token bounds its prices are taken
// at.
const (
	PilotRouteRequestCap = 1024
	PilotTotalRequestCap = 2048
	PilotLiabilityCapUSD = "40"
	PilotMaxInputTokens  = 2000
	PilotMaxOutputTokens = config.PinnedPilotCompletionCeiling
)

// PilotTokens are the token bounds of every pilot request.
var PilotTokens = Tokens{Input: PilotMaxInputTokens, Output: PilotMaxOutputTokens}

// Tokens are the per-request input and output token bounds a price is
// taken at and an admission's requests are counted at.
type Tokens struct {
	Input, Output int
}

// WindowRequests is one pilot window's reservation: the session
// schedule's requests plus the window's pre-run probe request.
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

// Admission purposes. A window or replacement reserves the ledger's
// window_requests; a probe, development or cell admission reserves the
// count it names.
const (
	PurposeProbe       = "probe"
	PurposeDevelopment = "development"
	PurposeWindow      = "window"
	PurposeReplacement = "replacement"
	PurposeCell        = "cell"
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
	Route string `json:"route,omitempty"`

	// price: list prices in US$ per million tokens, and where they were
	// read. PerRequestUSD is the price at InputTokens and OutputTokens.
	// admit and refused: the requests' token bounds; admit: the price
	// they are counted at.
	InputUSDPerMTok  string `json:"input_usd_per_mtok,omitempty"`
	OutputUSDPerMTok string `json:"output_usd_per_mtok,omitempty"`
	InputTokens      int    `json:"input_tokens,omitempty"`
	OutputTokens     int    `json:"output_tokens,omitempty"`
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

// Caps are the limits the ledger enforces, fixed when it is created.
// LiabilityUSD is the global cap and RouteLiabilityUSD each route's;
// where RouteLiabilityUSD is set, a route it does not list admits
// nothing. A zero request cap sets none. WindowRequests is the size of
// every window and replacement admission (zero admits none).
// MaxInputTokens and MaxOutputTokens are the token bounds of a price
// that names none (zero: every price names its own).
type Caps struct {
	RouteRequests     int               `json:"route_requests"`
	TotalRequests     int               `json:"total_requests"`
	LiabilityUSD      string            `json:"liability_usd"`
	RouteLiabilityUSD map[string]string `json:"route_liability_usd,omitempty"`
	WindowRequests    int               `json:"window_requests"`
	MaxInputTokens    int               `json:"max_input_tokens"`
	MaxOutputTokens   int               `json:"max_output_tokens"`
}

// PilotCaps are the pilot's caps.
func PilotCaps() Caps {
	return Caps{RouteRequests: PilotRouteRequestCap, TotalRequests: PilotTotalRequestCap, LiabilityUSD: PilotLiabilityCapUSD,
		WindowRequests: WindowRequests, MaxInputTokens: PilotMaxInputTokens, MaxOutputTokens: PilotMaxOutputTokens}
}

// ReleaseCaps are the caps of a ledger shared by the release routes
// whose configurations are given: each route's release.caps.route_usd
// under its target name, and the release.caps.global_usd they all
// carry. A release ledger caps liability only; every price names the
// route's shape.
func ReleaseCaps(cfgs ...*config.Config) (Caps, error) {
	if len(cfgs) == 0 {
		return Caps{}, errors.New("admission: release caps: no route configurations")
	}
	c := Caps{RouteLiabilityUSD: map[string]string{}}
	var global *big.Rat
	for _, cfg := range cfgs {
		if cfg.Profile != config.ProfileRelease || cfg.Release == nil {
			return Caps{}, fmt.Errorf("admission: release caps: %q is not a release configuration", cfg.Target.Name)
		}
		name, rc := cfg.Target.Name, cfg.Release.Caps
		if _, dup := c.RouteLiabilityUSD[name]; dup || name == "" {
			return Caps{}, fmt.Errorf("admission: release caps: route name %q empty or listed twice", name)
		}
		g, err := decimal(rc.GlobalUSD)
		if err != nil {
			return Caps{}, fmt.Errorf("admission: release caps: %s global_usd: %w", name, err)
		}
		if global == nil {
			global, c.LiabilityUSD = g, rc.GlobalUSD
		} else if g.Cmp(global) != 0 {
			return Caps{}, fmt.Errorf("admission: release caps: %s global_usd %s, another route's %s", name, rc.GlobalUSD, c.LiabilityUSD)
		}
		c.RouteLiabilityUSD[name] = rc.RouteUSD
	}
	if _, _, err := checkCaps(&c); err != nil {
		return Caps{}, fmt.Errorf("admission: release caps: %w", err)
	}
	return c, nil
}

// checkCaps returns the global and per-route liability caps.
func checkCaps(c *Caps) (*big.Rat, map[string]*big.Rat, error) {
	switch {
	case c.RouteRequests < 0 || c.TotalRequests < 0 || c.WindowRequests < 0:
		return nil, nil, fmt.Errorf("negative request cap in %+v", *c)
	case c.MaxInputTokens < 0 || c.MaxOutputTokens < 0 || (c.MaxInputTokens == 0) != (c.MaxOutputTokens == 0):
		return nil, nil, fmt.Errorf("token bounds %d and %d: both positive or both zero", c.MaxInputTokens, c.MaxOutputTokens)
	}
	global, err := decimal(c.LiabilityUSD)
	if err != nil || global.Sign() == 0 {
		return nil, nil, fmt.Errorf("liability_usd %q is not a positive decimal number", c.LiabilityUSD)
	}
	routes := map[string]*big.Rat{}
	for name, usd := range c.RouteLiabilityUSD {
		r, err := decimal(usd)
		switch {
		case name == "":
			return nil, nil, errors.New("route_liability_usd: empty route name")
		case err != nil || r.Sign() == 0:
			return nil, nil, fmt.Errorf("route_liability_usd for %s: %q is not a positive decimal number", name, usd)
		case r.Cmp(global) > 0:
			return nil, nil, fmt.Errorf("route_liability_usd for %s: US$%s exceeds the global US$%s", name, usd, c.LiabilityUSD)
		}
		routes[name] = r
	}
	return global, routes, nil
}

// Position is the ledger's charge for the route, in all, and the
// projected liability in all and, where the ledger caps it, for the
// route.
type Position struct {
	RouteRequests     int    `json:"route_requests"`
	TotalRequests     int    `json:"total_requests"`
	LiabilityUSD      string `json:"liability_usd"`
	RouteLiabilityUSD string `json:"route_liability_usd,omitempty"`

	liab, routeLiab *big.Rat
}

// Counts are requests sent, by traffic class.
type Counts struct {
	Probe       int `json:"probe"`
	Development int `json:"development"`
	Warmup      int `json:"warmup"`
	Measured    int `json:"measured"`
}

func (c Counts) total() int { return c.Probe + c.Development + c.Warmup + c.Measured }

// Usage is one route's charge. A replacement window's warm-up and
// measured requests count as Replacement; its probe request as Probe.
// Reserved is the reservation of admissions not yet settled.
type Usage struct {
	Probe       int
	Development int
	Warmup      int
	Measured    int
	Replacement int
	Reserved    int
	// Prices holds the highest per-request price recorded for the route at
	// each pair of token bounds.
	Prices map[Tokens]*big.Rat
	// charged counts the requests sent and reserved at each pair of token
	// bounds.
	charged map[Tokens]int
}

// Charged is every request sent plus every unsettled reservation.
func (u *Usage) Charged() int {
	return u.Probe + u.Development + u.Warmup + u.Measured + u.Replacement + u.Reserved
}

// State is the ledger read and checked.
type State struct {
	Lines int
	// OpenSHA256 is the hex SHA-256 of the open line with its newline,
	// which carries the ledger's creation time.
	OpenSHA256 string
	// Caps are the caps the ledger was created with.
	Caps       Caps
	Routes     map[string]*Usage
	HostingUSD *big.Rat
	// Open maps each unsettled admit line to its admission.
	Open map[int]Admission
	// order lists routes in first-seen order.
	order []string
	// liabilityCap and routeCaps are Caps' liability caps.
	liabilityCap *big.Rat
	routeCaps    map[string]*big.Rat
}

// Admission is one admitted reservation.
type Admission struct {
	Seq     int
	Route   string
	Purpose string
	Reserve int
	Tokens  Tokens
}

// Total is the charge across routes.
func (s *State) Total() int {
	n := 0
	for _, u := range s.Routes {
		n += u.Charged()
	}
	return n
}

// Liability is every route's charge, each request at the highest
// per-request price recorded for the route at its token bounds, plus the
// hosting costs. A charge with no price is an error.
func (s *State) Liability() (*big.Rat, error) {
	return s.liability("", Tokens{}, 0)
}

func (s *State) liability(route string, tok Tokens, extra int) (*big.Rat, error) {
	sum := new(big.Rat).Set(s.HostingUSD)
	for _, r := range s.order {
		c, err := s.routeLiability(r, route, tok, extra)
		if err != nil {
			return nil, err
		}
		sum.Add(sum, c)
	}
	return sum, nil
}

// routeLiability is route r's charge, with extra more requests at tok when
// r is route.
func (s *State) routeLiability(r, route string, tok Tokens, extra int) (*big.Rat, error) {
	charged := map[Tokens]int{}
	if u := s.Routes[r]; u != nil {
		for t, n := range u.charged {
			charged[t] = n
		}
	}
	if r == route && extra > 0 {
		charged[tok] += extra
	}
	sum := new(big.Rat)
	for t, n := range charged {
		if n == 0 {
			continue
		}
		p := s.price(r, t)
		if p == nil {
			return nil, fmt.Errorf("no price recorded for %s at %d input and %d output tokens", r, t.Input, t.Output)
		}
		sum.Add(sum, new(big.Rat).Mul(p, big.NewRat(int64(n), 1)))
	}
	return sum, nil
}

// price is the highest per-request price recorded for route at tok (nil
// = none).
func (s *State) price(route string, tok Tokens) *big.Rat {
	if u := s.Routes[route]; u != nil {
		return u.Prices[tok]
	}
	return nil
}

// ReservationUSD is n requests to route at tok, each at the highest
// per-request price recorded for the route at tok: the liability their
// admission adds, as a decimal number of US dollars.
func (s *State) ReservationUSD(route string, n int, tok Tokens) (string, error) {
	p := s.price(route, tok)
	if p == nil {
		return "", fmt.Errorf("admission: no price recorded for %s at %d input and %d output tokens", route, tok.Input, tok.Output)
	}
	return decimalString(new(big.Rat).Mul(p, big.NewRat(int64(n), 1))), nil
}

func (s *State) usage(route string) *Usage {
	u, ok := s.Routes[route]
	if !ok {
		u = &Usage{Prices: map[Tokens]*big.Rat{}, charged: map[Tokens]int{}}
		s.Routes[route] = u
		s.order = append(s.order, route)
	}
	return u
}

// CheckRelease reports whether the ledger caps cfg's route at cfg's
// release.caps: the same global cap, and the route's cap under its
// target name.
func (s *State) CheckRelease(cfg *config.Config) error {
	if cfg.Release == nil {
		return errors.New("admission: not a release configuration")
	}
	name, rc := cfg.Target.Name, cfg.Release.Caps
	g, err := decimal(rc.GlobalUSD)
	if err != nil || g.Cmp(s.liabilityCap) != 0 {
		return fmt.Errorf("admission: ledger global cap US$%s, %s's configuration %q", s.Caps.LiabilityUSD, name, rc.GlobalUSD)
	}
	have, ok := s.routeCaps[name]
	r, err := decimal(rc.RouteUSD)
	if !ok || err != nil || r.Cmp(have) != 0 {
		return fmt.Errorf("admission: ledger cap for route %s %q, its configuration %q", name, s.Caps.RouteLiabilityUSD[name], rc.RouteUSD)
	}
	return nil
}

// Refusal is an admission the caps refused; the ledger records it.
type Refusal struct {
	Route   string
	Purpose string
	Reason  string
}

func (r *Refusal) Error() string {
	return fmt.Sprintf("admission: %s for %s refused: %s", r.Purpose, r.Route, r.Reason)
}

// Ledger is the ledger file at a path. Every operation reads and checks
// the whole file under an exclusive lock before it appends.
type Ledger struct {
	path string
	now  func() time.Time
}

// Create starts a new ledger at path under caps; the file must not
// exist.
func Create(path string, caps Caps) (*Ledger, error) {
	if _, _, err := checkCaps(&caps); err != nil {
		return nil, fmt.Errorf("admission: create ledger: %w", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("admission: create ledger: %w", err)
	}
	l := &Ledger{path: path, now: time.Now}
	err = l.append(f, nil, &Entry{Kind: KindOpen, SchemaVersion: SchemaVersion, Caps: &caps})
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

// perRequest is the price of one request at inTok input and outTok
// output tokens.
func perRequest(in, out *big.Rat, inTok, outTok int) *big.Rat {
	r := new(big.Rat).Mul(in, big.NewRat(int64(inTok), 1))
	r.Add(r, new(big.Rat).Mul(out, big.NewRat(int64(outTok), 1)))
	return r.Quo(r, big.NewRat(1_000_000, 1))
}

// Price is a provider's list price for a route, as read.
type Price struct {
	InputUSDPerMTok  string
	OutputUSDPerMTok string
	// InputTokens and OutputTokens are the per-request token bounds the
	// price is taken at: a release route's input bound and ceiling. Zero
	// takes the ledger's max_input_tokens and max_output_tokens.
	InputTokens  int
	OutputTokens int
	// Source is the URL the prices were read from; Retrieved its date
	// (2006-01-02).
	Source    string
	Retrieved string
}

func checkPrice(route string, p Price) (*big.Rat, error) {
	if route == "" {
		return nil, errors.New("empty route")
	}
	if p.InputTokens < 1 || p.OutputTokens < 1 {
		return nil, fmt.Errorf("token bounds %d and %d: both must be positive", p.InputTokens, p.OutputTokens)
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
	return perRequest(in, out, p.InputTokens, p.OutputTokens), nil
}

// RecordPrice appends a route's prices. An admission at the price's token
// bounds counts its requests at the highest per-request price ever
// recorded for the route at those bounds.
func (l *Ledger) RecordPrice(route string, p Price) error {
	_, err := l.update(func(st *State) (*Entry, error) {
		if p.InputTokens == 0 && p.OutputTokens == 0 {
			p.InputTokens, p.OutputTokens = st.Caps.MaxInputTokens, st.Caps.MaxOutputTokens
		}
		r, err := checkPrice(route, p)
		if err != nil {
			return nil, fmt.Errorf("admission: price: %w", err)
		}
		return &Entry{Kind: KindPrice, Route: route, InputUSDPerMTok: p.InputUSDPerMTok, OutputUSDPerMTok: p.OutputUSDPerMTok,
			InputTokens: p.InputTokens, OutputTokens: p.OutputTokens, PerRequestUSD: decimalString(r), Source: p.Source, Retrieved: p.Retrieved}, nil
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

// Admit reserves n requests to route for purpose, each at most tok's
// input and output tokens. A window or replacement must reserve exactly
// the ledger's window_requests. The admission is refused, and the refusal
// recorded, when the route has no price at tok, the route's charge plus n
// exceeds the route request cap, the total plus n exceeds the total
// request cap, or the projected liability exceeds the route's or the
// global liability cap. The admit line carries the price its requests
// count at.
func (l *Ledger) Admit(route, purpose string, n int, tok Tokens) (Admission, error) {
	var a Admission
	_, err := l.update(func(st *State) (*Entry, error) {
		if route == "" {
			return nil, errors.New("admission: empty route")
		}
		if tok.Input < 1 || tok.Output < 1 {
			return nil, fmt.Errorf("admission: token bounds %d and %d: both must be positive", tok.Input, tok.Output)
		}
		switch w := st.Caps.WindowRequests; purpose {
		case PurposeWindow, PurposeReplacement:
			if w == 0 {
				return nil, fmt.Errorf("admission: this ledger admits no %s", purpose)
			}
			if n != w {
				return nil, fmt.Errorf("admission: a %s reserves %d requests, not %d", purpose, w, n)
			}
		case PurposeProbe, PurposeDevelopment, PurposeCell:
			if n < 1 {
				return nil, fmt.Errorf("admission: a %s reserves at least one request, not %d", purpose, n)
			}
		default:
			return nil, fmt.Errorf("admission: unknown purpose %q", purpose)
		}
		refusal := func(before, after *Position, reason string) (*Entry, error) {
			return &Entry{Kind: KindRefused, Route: route, InputTokens: tok.Input, OutputTokens: tok.Output, Purpose: purpose, Reserve: n,
				Before: before, After: after, Reason: reason}, &Refusal{route, purpose, reason}
		}
		before, err := st.position(route, tok, 0)
		var after *Position
		if err == nil {
			after, err = st.position(route, tok, n)
		}
		if err != nil {
			return refusal(nil, nil, err.Error())
		}
		c := st.Caps
		reason := ""
		switch {
		case c.RouteRequests > 0 && after.RouteRequests > c.RouteRequests:
			reason = fmt.Sprintf("%d requests for the route exceed the cap of %d", after.RouteRequests, c.RouteRequests)
		case c.TotalRequests > 0 && after.TotalRequests > c.TotalRequests:
			reason = fmt.Sprintf("%d requests in total exceed the cap of %d", after.TotalRequests, c.TotalRequests)
		case st.routeCaps[route] != nil && after.routeLiab.Cmp(st.routeCaps[route]) > 0:
			reason = fmt.Sprintf("projected liability for the route US$%s exceeds US$%s", after.RouteLiabilityUSD, c.RouteLiabilityUSD[route])
		case after.liab.Cmp(st.liabilityCap) > 0:
			reason = fmt.Sprintf("projected liability US$%s exceeds US$%s", after.LiabilityUSD, c.LiabilityUSD)
		}
		if reason != "" {
			return refusal(before, after, reason)
		}
		a = Admission{Seq: st.Lines + 1, Route: route, Purpose: purpose, Reserve: n, Tokens: tok}
		return &Entry{Kind: KindAdmit, Route: route, InputTokens: tok.Input, OutputTokens: tok.Output, PerRequestUSD: decimalString(st.price(route, tok)),
			Purpose: purpose, Reserve: n, Before: before, After: after}, nil
	})
	if err != nil {
		return Admission{}, err
	}
	return a, nil
}

// position is the charge with extra more requests at tok to route. It
// fails when the route has no price at tok, when the liability cannot be
// projected, or when the ledger caps routes and not this one.
func (st *State) position(route string, tok Tokens, extra int) (*Position, error) {
	if len(st.routeCaps) > 0 && st.routeCaps[route] == nil {
		return nil, fmt.Errorf("no liability cap for route %s", route)
	}
	if st.price(route, tok) == nil {
		return nil, fmt.Errorf("no price recorded for %s at %d input and %d output tokens", route, tok.Input, tok.Output)
	}
	liab, err := st.liability(route, tok, extra)
	if err != nil {
		return nil, err
	}
	n := extra
	if u, ok := st.Routes[route]; ok {
		n += u.Charged()
	}
	p := &Position{RouteRequests: n, TotalRequests: st.Total() + extra, LiabilityUSD: decimalString(liab), liab: liab}
	if len(st.routeCaps) > 0 {
		if p.routeLiab, err = st.routeLiability(route, route, tok, extra); err != nil {
			return nil, err
		}
		p.RouteLiabilityUSD = decimalString(p.routeLiab)
	}
	return p, nil
}

// Settle records the requests a's traffic sent, replacing its
// reservation. A probe admission sends probe requests only, a development
// admission development requests only, a window, replacement or cell
// probe, warm-up and measured requests. Sent counts above the
// reservation are recorded as an overrun and returned as an error after
// the write.
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
		return &Entry{Kind: KindSettle, Route: a.Route, Admit: a.Seq, Sent: &s, Overrun: sent.total() > a.Reserve}, nil
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
	case PurposeWindow, PurposeReplacement, PurposeCell:
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
	st := &State{Routes: map[string]*Usage{}, HostingUSD: new(big.Rat), Open: map[int]Admission{}}
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
		if seq == 1 {
			st.OpenSHA256 = hex.EncodeToString(h.Sum(nil))
		}
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
		global, routes, err := checkCaps(e.Caps)
		if err != nil {
			return err
		}
		st.Caps, st.liabilityCap, st.routeCaps = *e.Caps, global, routes
	case KindPrice:
		r, err := checkPrice(e.Route, Price{InputUSDPerMTok: e.InputUSDPerMTok, OutputUSDPerMTok: e.OutputUSDPerMTok,
			InputTokens: e.InputTokens, OutputTokens: e.OutputTokens, Source: e.Source, Retrieved: e.Retrieved})
		if err != nil {
			return err
		}
		if e.PerRequestUSD != decimalString(r) {
			return fmt.Errorf("per_request_usd %q, prices give %s", e.PerRequestUSD, decimalString(r))
		}
		u, tok := st.usage(e.Route), Tokens{e.InputTokens, e.OutputTokens}
		if p := u.Prices[tok]; p == nil || r.Cmp(p) > 0 {
			u.Prices[tok] = r
		}
	case KindHosting:
		r, err := decimal(e.USD)
		if err != nil {
			return err
		}
		st.HostingUSD.Add(st.HostingUSD, r)
	case KindAdmit:
		switch {
		case e.Route == "" || e.Reserve < 1:
			return fmt.Errorf("admit %q reserve %d", e.Route, e.Reserve)
		case (e.Purpose == PurposeWindow || e.Purpose == PurposeReplacement) && e.Reserve != st.Caps.WindowRequests:
			return fmt.Errorf("%s reserve %d", e.Purpose, e.Reserve)
		}
		if err := checkCounts(e.Purpose, Counts{}); err != nil {
			return err
		}
		tok := Tokens{e.InputTokens, e.OutputTokens}
		if p := st.price(e.Route, tok); p == nil || e.PerRequestUSD != decimalString(p) {
			return fmt.Errorf("admit at %d input and %d output tokens: per_request_usd %q, not the route's highest price there", tok.Input, tok.Output, e.PerRequestUSD)
		}
		u := st.usage(e.Route)
		u.Reserved += e.Reserve
		u.charged[tok] += e.Reserve
		st.Open[e.Seq] = Admission{Seq: e.Seq, Route: e.Route, Purpose: e.Purpose, Reserve: e.Reserve, Tokens: tok}
	case KindRefused:
	case KindSettle:
		a, ok := st.Open[e.Admit]
		if !ok || e.Sent == nil || a.Route != e.Route {
			return fmt.Errorf("settle of line %d does not match an unsettled admission", e.Admit)
		}
		if err := checkCounts(a.Purpose, *e.Sent); err != nil {
			return err
		}
		if e.Overrun != (e.Sent.total() > a.Reserve) {
			return fmt.Errorf("overrun %t for %d sent against %d", e.Overrun, e.Sent.total(), a.Reserve)
		}
		delete(st.Open, e.Admit)
		u := st.usage(a.Route)
		u.Reserved -= a.Reserve
		u.charged[a.Tokens] += e.Sent.total() - a.Reserve
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
