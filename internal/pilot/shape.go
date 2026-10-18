package pilot

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// Shape S (SPEC-HOSTED HA3 point 7). The system text carries the
// instruction. The probe checks the input bound on reported prompt tokens.
const (
	ShapeSSeed        = "percentes-shape-s-1"
	ShapeSTickets     = 100
	ShapeSInstruction = "Summarise the ticket in two sentences: what the customer reports, and what they ask for."
	ShapeSSystem      = "You read support tickets sent to a software company. The user message is a request reference, a space, and one ticket. " +
		ShapeSInstruction + " Reply in plain text."
)

// ShapeS is Shape S's frozen set: the system text and the tickets
// generated from Seed under ByteCap.
type ShapeS struct {
	Seed    string   `json:"seed"`
	ByteCap int      `json:"byte_cap"`
	System  string   `json:"system"`
	Tickets []string `json:"tickets"`
}

var subjects = map[string][]string{
	Billing:   {"Question about our invoice", "Billing problem", "Payment query"},
	Technical: {"Problem with {product}", "{Product} not working", "Error in {product}"},
	Account:   {"Change to our account", "Help with our account", "Account access"},
}

var tried = []string{
	"Signed out and back in.",
	"Cleared the browser cache and cookies.",
	"Restarted the device.",
	"Tried a second user on the same account.",
	"Checked your status page, which showed no incidents.",
	"Reinstalled {product}.",
	"Tried again from a different network.",
	"Read the help article on this topic.",
	"Asked our admin to check the account settings.",
	"Waited a day in case it was temporary.",
}

var supportReplies = []string{
	"Thanks for getting in touch. Could you tell us which version you are using and send a screenshot of the problem?",
	"Sorry for the trouble. We have passed this to the team that looks after {product} and will update you when we hear back.",
	"Thanks for the details. Could you try signing out and back in, and let us know whether that helps?",
	"We have checked your account and cannot see anything unusual. Could you tell us when this first happened?",
}

var customerReplies = []string{
	"We tried that and it made no difference.",
	"The screenshot is attached. It first happened on {weekday}.",
	"We are on the latest version, updated last week.",
	"It still happens, and it is now affecting the rest of the team.",
}

var logLevels = []string{"INFO", "WARN", "ERROR"}

var logMessages = []string{
	"request to /api/records failed with status 502",
	"sync stopped: batch %d of 48 not acknowledged",
	"retrying upload, attempt %d",
	"session token refreshed",
	"export wrote 0 rows",
	"queue depth %d above threshold",
	"worker restarted after %d s",
}

var shapeFills = map[string][]string{
	"{company}": {"Northfield Joinery", "Bluefin Logistics", "Ostara Clinics", "Marlow Analytics", "Tidewater Schools", "Kestrel Print Works"},
	"{plan}":    {"Starter", "Team", "Business"},
}

var shapeFillOrder = []string{"{company}", "{plan}"}

const (
	threadRounds = 2
	logLines     = 64
)

// GenerateShapeS builds Shape S's set from seed with every ticket at most
// byteCap bytes of UTF-8, and checks it. A ticket ends at its last log
// line where the cap allows the whole of it.
func GenerateShapeS(seed string, byteCap int) (*ShapeS, error) {
	if seed == "" {
		return nil, errors.New("pilot: empty seed")
	}
	if byteCap <= 0 {
		return nil, fmt.Errorf("pilot: byte cap %d", byteCap)
	}
	d := drawer(seed)
	s := &ShapeS{Seed: seed, ByteCap: byteCap, System: ShapeSSystem}
	labels := d.spread("s/label", ShapeSTickets, len(business))
	toneIx := d.spread("s/tone", ShapeSTickets, len(tones))
	prodIx := d.spread("s/product", ShapeSTickets, len(products))
	seen := map[string]bool{}
	for i := 0; i < ShapeSTickets; i++ {
		text, err := d.longTicket(fmt.Sprintf("s/%d", i), business[labels[i]], tones[toneIx[i]], products[prodIx[i]], byteCap, seen)
		if err != nil {
			return nil, fmt.Errorf("pilot: shape s ticket %d: %w", i, err)
		}
		s.Tickets = append(s.Tickets, text)
	}
	if err := s.Check(); err != nil {
		return nil, err
	}
	return s, nil
}

// longTicket assembles one ticket: a header and a message from the pilot
// templates, then the steps tried, the thread so far and a log excerpt,
// stopping before the first piece that would take it over byteCap.
func (d drawer) longTicket(key, label string, tn tone, product string, byteCap int, seen map[string]bool) (string, error) {
	phs := append(append([]string{}, fillOrder...), shapeFillOrder...)
	fill := func(k, s string) string {
		s = strings.ReplaceAll(s, "{Product}", capitalise(product))
		s = strings.ReplaceAll(s, "{product}", product)
		for _, ph := range phs {
			if !strings.Contains(s, ph) {
				continue
			}
			list := fills[ph]
			if list == nil {
				list = shapeFills[ph]
			}
			s = strings.ReplaceAll(s, ph, list[d.n(k+"/"+ph, len(list))])
		}
		return s
	}
	pick := func(k string, list []string) string { return list[d.n(key+"/"+k, len(list))] }

	iss := issues[label][d.n(key+"/issue", len(issues[label]))]
	msg, err := d.text(key+"/message", Ticket{Product: product}, iss, nil, "", tn, 3, seen)
	if err != nil {
		return "", err
	}
	head := fill(key+"/head", "Subject: "+pick("subject", subjects[label])+
		"\nOrganisation: {company}\nPlan: {plan}, {seats} seats, customer since {year}\nPlatform: {os}")
	text := head + "\n\n" + msg
	if len(text) > byteCap {
		return "", fmt.Errorf("header and message take %d bytes, cap %d", len(text), byteCap)
	}

	var pieces []string
	order := make([]int, len(tried))
	for i := range order {
		order[i] = i
	}
	d.shuffle(key+"/tried", len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	for i, ix := range order[:4] {
		lead := "\n- "
		if i == 0 {
			lead = "\n\nWhat we have tried:\n- "
		}
		pieces = append(pieces, lead+fill(fmt.Sprintf("%s/tried/%d", key, i), tried[ix]))
	}
	for r := 0; r < threadRounds; r++ {
		k := fmt.Sprintf("%s/thread/%d", key, r)
		pieces = append(pieces,
			"\n\nSupport wrote:\n> "+fill(k+"/support", pick(fmt.Sprintf("thread/%d/support", r), supportReplies)),
			"\n\nWe replied:\n> "+fill(k+"/customer", pick(fmt.Sprintf("thread/%d/customer", r), customerReplies)))
	}
	at := time.Date(2026, time.September, 1+d.n(key+"/log/day", 28), d.n(key+"/log/hour", 24), d.n(key+"/log/minute", 60), 0, 0, time.UTC)
	code := pick("log/code", fills["{code}"])
	for l := 0; l < logLines; l++ {
		k := fmt.Sprintf("%s/log/%d", key, l)
		at = at.Add(time.Duration(1+d.n(k+"/gap", 90)) * time.Second)
		m := logMessages[d.n(k+"/message", len(logMessages))]
		if strings.Contains(m, "%d") {
			m = fmt.Sprintf(m, 1+d.n(k+"/value", 48))
		}
		lead := "\n"
		if l == 0 {
			lead = "\n\nLog excerpt from " + product + ":\n"
		}
		pieces = append(pieces, fmt.Sprintf("%s%s %-5s %s %s", lead, at.Format(time.RFC3339), logLevels[d.n(k+"/level", len(logLevels))], code, m))
	}
	for _, p := range pieces {
		if len(text)+len(p) > byteCap {
			break
		}
		text += p
	}
	return text, nil
}

// Check verifies a set: the frozen system text, the ticket count, each
// ticket non-empty, valid UTF-8 and within ByteCap, and no ticket repeated.
func (s *ShapeS) Check() error {
	var errs []error
	if s.Seed == "" {
		errs = append(errs, errors.New("pilot: shape s: empty seed"))
	}
	if s.System != ShapeSSystem {
		errs = append(errs, errors.New("pilot: shape s: system text differs from the frozen text"))
	}
	if len(s.Tickets) != ShapeSTickets {
		errs = append(errs, fmt.Errorf("pilot: shape s: %d tickets, want %d", len(s.Tickets), ShapeSTickets))
	}
	seen := map[string]int{}
	for i, t := range s.Tickets {
		switch {
		case t == "":
			errs = append(errs, fmt.Errorf("pilot: shape s ticket %d: empty", i))
		case !utf8.ValidString(t):
			errs = append(errs, fmt.Errorf("pilot: shape s ticket %d: invalid UTF-8", i))
		case len(t) > s.ByteCap:
			errs = append(errs, fmt.Errorf("pilot: shape s ticket %d: %d bytes, cap %d", i, len(t), s.ByteCap))
		}
		if j, ok := seen[t]; ok {
			errs = append(errs, fmt.Errorf("pilot: shape s ticket %d: same text as ticket %d", i, j))
		}
		seen[t] = i
	}
	return errors.Join(errs...)
}

// UserText is request index's user text: the prefix cs-<seed>-<index>
// with a six-digit index, a space, and the set's tickets in turn.
func (s *ShapeS) UserText(seed int64, index int) string {
	return fmt.Sprintf("cs-%d-%06d %s", seed, index, s.Tickets[index%len(s.Tickets)])
}

// SHA256 is the hex SHA-256 of the set's JSON encoding, HTML escaping off.
func (s *ShapeS) SHA256() string {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
	sum := sha256.Sum256(b.Bytes())
	return hex.EncodeToString(sum[:])
}
