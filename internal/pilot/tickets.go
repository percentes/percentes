// Package pilot holds the pilot's task data: the synthetic support-ticket
// fixture (SPEC-HOSTED HA2 terms: 120 measured cases and 24 development
// cases, each with one frozen label).
//
// Every ticket is assembled from templates, and its label is Route of the
// request and trigger it was assembled with. The templates, names,
// amounts and products are invented.
package pilot

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Route labels.
const (
	Billing    = "billing"
	Technical  = "technical"
	Account    = "account"
	Escalation = "escalation"
)

// Labels lists the route labels in rubric order.
var Labels = []string{Billing, Technical, Account, Escalation}

// business are the labels a ticket's requested action can carry.
var business = []string{Billing, Technical, Account}

// Sets and their sizes per label.
const (
	SetMeasured    = "measured"
	SetDevelopment = "development"

	MeasuredPerLabel    = 30
	DevelopmentPerLabel = 6
)

// Seed is the fixture's seed. Every draw is the first 8 bytes of
// SHA-256(seed + "/" + key), big-endian, modulo the choice count.
const Seed = "percentes-pilot-tickets-1"

// Input bound. A text's UTF-8 byte length is taken as an upper bound on
// its token count. TemplateOverheadTokens is the stated worst case for the
// tokens a provider adds around the system and user text.
const (
	InputTokenLimit        = 2000
	TemplateOverheadTokens = 400
	PromptByteCap          = InputTokenLimit - TemplateOverheadTokens
	TicketByteCap          = 600
)

// Fixture file names written by Write.
const (
	MeasuredFile    = "tickets-measured.jsonl"
	DevelopmentFile = "tickets-development.jsonl"
	SumsFile        = "tickets.sha256"
)

// Ticket is one case. Label is Route(Request, Trigger); the other fields
// record how Text was built.
type Ticket struct {
	ID        string `json:"id"`
	Set       string `json:"set"`
	Label     string `json:"label"`
	Text      string `json:"text"`
	Synthetic bool   `json:"synthetic"`
	Bytes     int    `json:"bytes"`
	Product   string `json:"product"`
	// Request is the business label whose action the ticket asks for,
	// and Issue the template that asks it. Both are empty on an
	// escalation ticket that asks for no business action.
	Request string `json:"request,omitempty"`
	Issue   string `json:"issue,omitempty"`
	// Mention is a business label the ticket raises without asking for
	// its action (business tickets only).
	Mention   string `json:"mention,omitempty"`
	Trigger   string `json:"trigger,omitempty"`
	Tone      string `json:"tone"`
	Length    string `json:"length"`
	Ambiguous bool   `json:"ambiguous"`
	Ambiguity string `json:"ambiguity,omitempty"`
}

// Route is the rubric's precedence rule: an escalation trigger decides
// the route; otherwise the requested action does.
func Route(request, trigger string) string {
	if trigger != "" {
		return Escalation
	}
	return request
}

// Fixture is the generator's output.
type Fixture struct {
	Seed        string
	Measured    []Ticket
	Development []Ticket
}

type issue struct {
	key, symptom, request string
}

var issues = map[string][]issue{
	Billing: {
		{"duplicate_charge", "We were charged twice for the {month} renewal, {amount} each time.", "Please refund the duplicate payment."},
		{"missing_vat_number", "Invoice {invoice} is missing our VAT number.", "Could you reissue it with the VAT number added?"},
		{"card_expiring", "The card we pay with expires at the end of this month.", "How do we switch to a new card before the next charge?"},
		{"wrong_plan_price", "We moved to the Starter plan in {month} but are still being billed {amount} for the Team plan.", "Please correct the bill and credit the difference."},
		{"cancel_renewal", "Our annual subscription renews next week and we no longer need it.", "Please cancel the renewal so we are not charged."},
		{"discount_missing", "The discount we agreed with your sales team was not applied to the {month} invoice.", "Can you apply it and send a corrected invoice?"},
		{"receipts", "Our finance team needs receipts for the last three payments.", "Please send copies to our billing contact."},
		{"billing_currency", "We are billed in US dollars, but our contract is in pounds.", "Please switch our billing currency to GBP."},
	},
	Technical: {
		{"crash_on_open", "{Product} closes as soon as it opens, since the last update.", "Can you tell us how to get it working again?"},
		{"sync_error", "Sync in {product} stops at 80% and shows error {code}.", "What does the error mean, and how do we fix it?"},
		{"empty_export", "Exports from {product} come out as empty CSV files.", "How can we get our data out?"},
		{"slow_reports", "{Product} now takes over two minutes to load a report.", "Is there anything we can change to speed it up?"},
		{"notifications_stopped", "Email notifications from {product} stopped arriving on {weekday}.", "Can you check why they are not being sent?"},
		{"upload_timeout", "Uploading files over 5 MB in {product} fails with a timeout.", "Is there a limit, or is this a bug you can fix?"},
		{"schedule_late", "Scheduled jobs in {product} have run an hour late since the clocks changed.", "Please help us get them back on time."},
		{"save_error", "{Product} shows error {code} every time we save a record.", "Could you look into what is causing it?"},
	},
	Account: {
		{"change_login_email", "I've changed jobs and need the account moved to my new email address.", "Please update the login email for me."},
		{"lost_2fa_device", "I lost the phone that has my two-factor codes.", "How can I get back into my account?"},
		{"transfer_owner", "Our account owner has left the company.", "Please make {colleague} the new owner."},
		{"close_account", "We have stopped using the service.", "Please close our account and delete our data."},
		{"merge_accounts", "I accidentally signed up twice with two different email addresses.", "Can you merge the two accounts into one?"},
		{"rename_organisation", "Our company has changed its name.", "Please update the organisation name on our account."},
		{"editor_access", "My colleague {colleague} can view our projects but cannot edit them.", "Please give them editor access."},
		{"locked_out", "My account was locked after too many wrong password attempts.", "Please unlock it."},
	},
}

// mentions raise a business label without asking for its action.
var mentions = map[string][]string{
	Billing: {
		"Our last invoice also looked higher than usual, but our finance team is checking that separately.",
		"Separately, I noticed a small extra charge last month, but that can wait.",
	},
	Technical: {
		"The app has also been a bit slow this week, though that is not why I'm writing.",
		"We've had the odd sync error too, but nothing urgent.",
	},
	Account: {
		"I'll need to add a new colleague at some point as well, but not yet.",
		"My email address is changing next month too, but I'll raise that later.",
	},
}

type trigger struct {
	key       string
	sentences []string
	// ambiguity is set where the trigger's wording also fits a business
	// label.
	ambiguity string
}

var triggers = []trigger{
	{key: "legal_threat", sentences: []string{
		"If this is not resolved by Friday, our solicitor will be in touch.",
		"We have taken legal advice and will start proceedings if we do not hear back this week.",
	}},
	{key: "suspected_breach", sentences: []string{
		"Someone signed in to our workspace from a location we do not recognise and exported our customer list.",
		"There are logins and changes on our account that none of us made, and I think our data has been accessed.",
	}, ambiguity: "describes unauthorised access to the account, which reads as an account matter; a suspected breach is an escalation trigger"},
	{key: "manager_after_repeat_contact", sentences: []string{
		"This is the fourth time I have written about this without a fix, and I want a manager to call me today.",
		"I have contacted support three times this week and nobody has replied, so I want this passed to a manager now.",
	}},
	{key: "regulator_complaint", sentences: []string{
		"I am making a formal complaint to the data protection regulator about how you have handled my personal data.",
		"I will be reporting this to the consumer regulator unless I get a proper answer.",
	}},
	{key: "public_exposure", sentences: []string{
		"If I do not get a proper answer, I will post this whole thread publicly and contact a journalist.",
		"I am going to share this experience on social media and with the press.",
	}},
}

type tone struct {
	key       string
	openings  []string
	asides    []string
	closings  []string
	ambiguity bool
}

var tones = []tone{
	{key: "polite", openings: []string{"Hello,", "Good morning,", "Dear support team,"},
		closings: []string{"Many thanks,\n{name}", "Kind regards,\n{name}"}},
	{key: "neutral", openings: []string{"Hi,", "Hi there,"},
		closings: []string{"Thanks,\n{name}", "Cheers,\n{name}"}},
	{key: "terse", openings: []string{""},
		closings: []string{"{name}", ""}},
	{key: "frustrated", openings: []string{"Hi,"},
		asides:   []string{"I'm getting quite frustrated with this.", "This has wasted most of my morning."},
		closings: []string{"Please sort this out.\n{name}", "{name}"}},
	{key: "angry", openings: []string{""},
		asides:   []string{"This is completely unacceptable.", "Honestly, your service has been a shambles lately."},
		closings: []string{"Sort it out.", "{name}"}, ambiguity: true},
}

// Lengths add context sentences: the first before the ticket's subject,
// the others after it.
var lengths = []struct {
	key     string
	context int
}{{"short", 0}, {"medium", 1}, {"long", 3}}

var products = []string{
	"the desktop app", "the mobile app", "the tablet app",
	"the web dashboard", "the browser extension", "the reporting module",
}

var openContext = []string{
	"We use {product} every day across a team of {seats}.",
	"We've been customers since {year} and mostly use {product}.",
}

var moreContext = []string{
	"I've checked our settings and can't see anything wrong.",
	"We're on {os}, fully up to date.",
	"Nothing has changed on our side as far as I know.",
	"Our admin is away this week, so I'm writing instead.",
}

var fills = map[string][]string{
	"{name}":      {"Zoë", "José", "Siobhán", "Anaïs", "Bartłomiej", "Priya", "Tom", "Wei", "Chloé", "Kwame", "Ingrid", "Mateus"},
	"{colleague}": {"Dana Okafor", "Lukas Brandt", "Aoife Byrne", "Ravi Menon", "Mei Tanaka", "Sam Ellis"},
	"{month}":     {"January", "February", "March", "April", "May", "June", "July", "August", "September"},
	"{amount}":    {"£49.00", "£120.00", "£18.50", "£240.00", "€75.00", "£96.00"},
	"{invoice}":   {"INV-20431", "INV-31877", "INV-40912", "INV-52260", "INV-68014"},
	"{code}":      {"E-104", "E-221", "E-317", "E-409", "E-512"},
	"{weekday}":   {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday"},
	"{seats}":     {"4", "12", "25", "40", "150"},
	"{year}":      {"2019", "2020", "2021", "2022", "2023"},
	"{os}":        {"Windows 11", "macOS 15", "Ubuntu 24.04", "ChromeOS"},
}

// fillOrder fixes the order placeholders are drawn in.
var fillOrder = []string{"{name}", "{colleague}", "{month}", "{amount}", "{invoice}", "{code}", "{weekday}", "{seats}", "{year}", "{os}"}

const maxRedraws = 64

type drawer string

func (d drawer) n(key string, k int) int {
	sum := sha256.Sum256([]byte(string(d) + "/" + key))
	return int(binary.BigEndian.Uint64(sum[:8]) % uint64(k))
}

func (d drawer) shuffle(key string, n int, swap func(i, j int)) {
	for i := n - 1; i > 0; i-- {
		swap(i, d.n(fmt.Sprintf("%s/%d", key, i), i+1))
	}
}

// spread returns n entries cycling through k choices, shuffled.
func (d drawer) spread(key string, n, k int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i % k
	}
	d.shuffle(key, n, func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// Generate builds the fixture from seed and checks it.
func Generate(seed string) (*Fixture, error) {
	if seed == "" {
		return nil, errors.New("pilot: empty seed")
	}
	d := drawer(seed)
	f := &Fixture{Seed: seed}
	seen := map[string]bool{}
	for _, set := range []struct {
		name   string
		per    int
		prefix string
		width  int
		out    *[]Ticket
	}{
		{SetDevelopment, DevelopmentPerLabel, "d", 2, &f.Development},
		{SetMeasured, MeasuredPerLabel, "m", 3, &f.Measured},
	} {
		var ts []Ticket
		for _, label := range Labels {
			g, err := d.group(set.name, label, set.per, seen)
			if err != nil {
				return nil, err
			}
			ts = append(ts, g...)
		}
		d.shuffle(set.name+"/order", len(ts), func(i, j int) { ts[i], ts[j] = ts[j], ts[i] })
		for i := range ts {
			ts[i].ID = fmt.Sprintf("%s%0*d", set.prefix, set.width, i+1)
		}
		*set.out = ts
	}
	if err := Check(f); err != nil {
		return nil, err
	}
	return f, nil
}

// group builds one label's tickets for one set. A third carry a second
// signal (a mention on a business ticket, a business request on an
// escalation ticket); lengths split in thirds.
func (d drawer) group(set, label string, per int, seen map[string]bool) ([]Ticket, error) {
	key := set + "/" + label
	lens := d.spread(key+"/length", per, len(lengths))
	second := d.spread(key+"/second", per, 3)
	toneIx := d.spread(key+"/tone", per, len(tones))
	prodIx := d.spread(key+"/product", per, len(products))
	var subj, sub2 []int
	if label == Escalation {
		subj = d.spread(key+"/trigger", per, len(triggers))
		sub2 = d.spread(key+"/request", per, len(business))
	} else {
		subj = d.spread(key+"/issue", per, len(issues[label]))
		sub2 = d.spread(key+"/mention", per, 2)
	}
	out := make([]Ticket, 0, per)
	for i := 0; i < per; i++ {
		t := Ticket{
			Set:       set,
			Synthetic: true,
			Product:   products[prodIx[i]],
			Tone:      tones[toneIx[i]].key,
			Length:    lengths[lens[i]].key,
		}
		var iss issue
		var trig *trigger
		var mention string
		hasSecond := second[i] == 0
		if label == Escalation {
			trig = &triggers[subj[i]]
			t.Trigger = trig.key
			if hasSecond {
				t.Request = business[sub2[i]]
				list := issues[t.Request]
				iss = list[d.n(fmt.Sprintf("%s/%d/issue", key, i), len(list))]
			}
		} else {
			t.Request = label
			iss = issues[label][subj[i]]
			if hasSecond {
				t.Mention = otherBusiness(label)[sub2[i]]
				ms := mentions[t.Mention]
				mention = ms[d.n(fmt.Sprintf("%s/%d/mention-text", key, i), len(ms))]
			}
		}
		t.Issue = iss.key
		t.Label = Route(t.Request, t.Trigger)
		t.Ambiguity = ambiguity(t)
		t.Ambiguous = t.Ambiguity != ""

		var err error
		t.Text, err = d.text(fmt.Sprintf("%s/%d", key, i), t, iss, trig, mention, tones[toneIx[i]], lengths[lens[i]].context, seen)
		if err != nil {
			return nil, err
		}
		t.Bytes = len(t.Text)
		out = append(out, t)
	}
	return out, nil
}

func otherBusiness(label string) []string {
	var out []string
	for _, b := range business {
		if b != label {
			out = append(out, b)
		}
	}
	return out
}

// ambiguity names the reasons a reader might route t other than by its
// label: a second signal, a trigger whose wording fits a business label,
// or a hostile tone with no trigger. Empty means none.
func ambiguity(t Ticket) string {
	var rs []string
	if t.Mention != "" {
		rs = append(rs, fmt.Sprintf("raises a second topic (%s) without asking for it; the requested action is %s", t.Mention, t.Request))
	}
	if t.Trigger != "" && t.Request != "" {
		rs = append(rs, fmt.Sprintf("the requested action is %s, and the %s escalation trigger overrides it", t.Request, t.Trigger))
	}
	for _, tr := range triggers {
		if tr.key == t.Trigger && tr.ambiguity != "" {
			rs = append(rs, tr.ambiguity)
		}
	}
	for _, tn := range tones {
		if tn.key == t.Tone && tn.ambiguity && t.Trigger == "" {
			rs = append(rs, fmt.Sprintf("hostile tone with no escalation trigger; the requested action (%s) decides", t.Request))
		}
	}
	return strings.Join(rs, "; ")
}

// text assembles a ticket, redrawing its free choices until the text is
// new to the fixture.
func (d drawer) text(key string, t Ticket, iss issue, trig *trigger, mention string, tn tone, context int, seen map[string]bool) (string, error) {
	for a := 0; a < maxRedraws; a++ {
		k := fmt.Sprintf("%s/%d", key, a)
		pick := func(what string, list []string) string {
			return list[d.n(k+"/"+what, len(list))]
		}
		var body []string
		if context > 0 {
			body = append(body, pick("context0", openContext))
		}
		if iss.key != "" {
			body = append(body, iss.symptom)
		}
		if context > 1 {
			first := d.n(k+"/context1", len(moreContext))
			second := (first + 1 + d.n(k+"/context2", len(moreContext)-1)) % len(moreContext)
			body = append(body, moreContext[first], moreContext[second])
		}
		if len(tn.asides) > 0 {
			body = append(body, pick("aside", tn.asides))
		}
		if mention != "" {
			body = append(body, mention)
		}
		if iss.key != "" {
			body = append(body, iss.request)
		}
		if trig != nil {
			body = append(body, pick("trigger", trig.sentences))
		}
		var parts []string
		if o := pick("opening", tn.openings); o != "" {
			parts = append(parts, o)
		}
		parts = append(parts, strings.Join(body, " "))
		if c := pick("closing", tn.closings); c != "" {
			parts = append(parts, c)
		}
		s := strings.Join(parts, "\n")
		s = strings.ReplaceAll(s, "{Product}", capitalise(t.Product))
		s = strings.ReplaceAll(s, "{product}", t.Product)
		for _, ph := range fillOrder {
			if strings.Contains(s, ph) {
				s = strings.ReplaceAll(s, ph, pick(ph, fills[ph]))
			}
		}
		if !seen[s] {
			seen[s] = true
			return s, nil
		}
	}
	return "", fmt.Errorf("pilot: %s: no distinct text in %d draws", key, maxRedraws)
}

func capitalise(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// Check verifies a fixture: per-label balance, labels by Route,
// ambiguity reasons by construction, distinct texts and IDs across both
// sets, the byte cap, and the synthetic flag.
func Check(f *Fixture) error {
	var errs []error
	texts, ids := map[string]string{}, map[string]bool{}
	for _, set := range []struct {
		name string
		per  int
		ts   []Ticket
	}{
		{SetMeasured, MeasuredPerLabel, f.Measured},
		{SetDevelopment, DevelopmentPerLabel, f.Development},
	} {
		count := map[string]int{}
		for _, t := range set.ts {
			where := fmt.Sprintf("%s %s", set.name, t.ID)
			count[t.Label]++
			switch {
			case t.Set != set.name:
				errs = append(errs, fmt.Errorf("%s: set %q", where, t.Set))
			case t.ID == "" || ids[t.ID]:
				errs = append(errs, fmt.Errorf("%s: empty or repeated id", where))
			case !t.Synthetic:
				errs = append(errs, fmt.Errorf("%s: not marked synthetic", where))
			case t.Label != Route(t.Request, t.Trigger):
				errs = append(errs, fmt.Errorf("%s: label %q, rubric gives %q", where, t.Label, Route(t.Request, t.Trigger)))
			case t.Bytes != len(t.Text):
				errs = append(errs, fmt.Errorf("%s: bytes %d, text has %d", where, t.Bytes, len(t.Text)))
			case len(t.Text) > TicketByteCap:
				errs = append(errs, fmt.Errorf("%s: %d bytes, cap %d", where, len(t.Text), TicketByteCap))
			case t.Mention != "" && (t.Trigger != "" || t.Mention == t.Request):
				errs = append(errs, fmt.Errorf("%s: mention %q on a ticket requesting %q with trigger %q", where, t.Mention, t.Request, t.Trigger))
			case t.Ambiguity != ambiguity(t) || t.Ambiguous != (t.Ambiguity != ""):
				errs = append(errs, fmt.Errorf("%s: ambiguity %q, construction gives %q", where, t.Ambiguity, ambiguity(t)))
			}
			if prev, ok := texts[t.Text]; ok {
				errs = append(errs, fmt.Errorf("%s: same text as %s", where, prev))
			}
			texts[t.Text] = where
			ids[t.ID] = true
		}
		for _, l := range Labels {
			if count[l] != set.per {
				errs = append(errs, fmt.Errorf("%s: %d %s tickets, want %d", set.name, count[l], l, set.per))
			}
		}
		if n := len(set.ts); n != set.per*len(Labels) {
			errs = append(errs, fmt.Errorf("%s: %d tickets, want %d", set.name, n, set.per*len(Labels)))
		}
	}
	return errors.Join(errs...)
}

// CheckPrompt verifies that system plus each ticket's text stays within
// PromptByteCap.
func CheckPrompt(system string, ts []Ticket) error {
	var errs []error
	for _, t := range ts {
		if n := len(system) + len(t.Text); n > PromptByteCap {
			errs = append(errs, fmt.Errorf("pilot: %s: prompt %d bytes, cap %d", t.ID, n, PromptByteCap))
		}
	}
	return errors.Join(errs...)
}

// Encode returns ts as JSON Lines, one ticket per line.
func Encode(ts []Ticket) []byte {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	for i := range ts {
		_ = enc.Encode(&ts[i])
	}
	return b.Bytes()
}

// Write writes both sets to dir as JSON Lines, and their SHA-256 sums in
// sha256sum format, and returns the sums by file name.
func (f *Fixture) Write(dir string) (map[string]string, error) {
	sums := map[string]string{}
	var list bytes.Buffer
	for _, out := range []struct {
		name string
		ts   []Ticket
	}{{MeasuredFile, f.Measured}, {DevelopmentFile, f.Development}} {
		b := Encode(out.ts)
		sum := sha256.Sum256(b)
		sums[out.name] = hex.EncodeToString(sum[:])
		fmt.Fprintf(&list, "%s  %s\n", sums[out.name], out.name)
		if err := os.WriteFile(filepath.Join(dir, out.name), b, 0o644); err != nil {
			return nil, fmt.Errorf("pilot: %w", err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, SumsFile), list.Bytes(), 0o644); err != nil {
		return nil, fmt.Errorf("pilot: %w", err)
	}
	return sums, nil
}
