package evidence

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/percentes/percentes/internal/config"
)

// NoticeWorkingDays is the least number of working days after a notice's
// date up to its window's close (SPEC-HOSTED HA3 point 14).
const NoticeWorkingDays = 4

// The ways a disclosure reached a provider.
const (
	DisclosureEmail = "email"
	DisclosureForm  = "form"
)

// Publication is the release publication record (SPEC-HOSTED HA3 point
// 14), a file apart from the cell manifests, which it names by hash: the
// manifests, pages and window-close records the notices carried, each
// route's notice,
// responses, naming permission and replication pack, every email and
// form that described the work to a provider before the notices, and
// every pre-release copy. Files it names are relative to its own
// directory. NonWorkingDays are weekdays the notice windows do not count.
type Publication struct {
	Manifests      []PublishedFile    `json:"manifests"`
	Pages          []PublishedFile    `json:"pages"`
	WindowCloses   []PublishedFile    `json:"window_closes"`
	Routes         []RoutePublication `json:"routes"`
	Disclosures    []Disclosure       `json:"disclosures"`
	Prerelease     []Prerelease       `json:"prerelease"`
	NonWorkingDays []string           `json:"non_working_days"`
	dir            string
}

// PublishedFile is a file by its path and SHA-256.
type PublishedFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

// RoutePublication is one route's notice and what followed it.
// Responses is an empty list when none arrived.
type RoutePublication struct {
	Route           string         `json:"route"`
	Notice          *Notice        `json:"notice"`
	Responses       []Response     `json:"responses"`
	Permission      *PublishedFile `json:"permission,omitempty"`
	ReplicationPack *Send          `json:"replication_pack,omitempty"`
}

// Notice is the notice sent to a route's provider: the date, the
// recipient, the account identifier it gave, the route's evidence pack
// (BuildPack), and the date its factual-accuracy window closes.
type Notice struct {
	Sent         string         `json:"sent"`
	Recipient    string         `json:"recipient"`
	Account      string         `json:"account"`
	Pack         *PublishedFile `json:"pack"`
	WindowCloses string         `json:"window_closes"`
}

// Send is a file sent to a provider on a date.
type Send struct {
	Sent      string         `json:"sent"`
	Recipient string         `json:"recipient"`
	Pack      *PublishedFile `json:"pack"`
}

// Response is a provider's response to its notice. A response publishes
// verbatim only with the written consent kept in Consent; otherwise the
// page carries its date and Corrections, an empty list when none were
// made.
type Response struct {
	Received    string         `json:"received"`
	Verbatim    bool           `json:"verbatim"`
	Text        *PublishedFile `json:"text,omitempty"`
	Consent     *PublishedFile `json:"consent,omitempty"`
	Corrections []string       `json:"corrections"`
}

// Disclosure is an email or form that described the work to a provider
// before the notices.
type Disclosure struct {
	Provider  string         `json:"provider"`
	How       string         `json:"how"`
	Date      string         `json:"date"`
	Recipient string         `json:"recipient"`
	Copy      *PublishedFile `json:"copy,omitempty"`
}

// Prerelease is a copy of the results sent before publication, with the
// routes whose figures it withheld.
type Prerelease struct {
	Date       string         `json:"date"`
	Recipients string         `json:"recipients"`
	Copy       *PublishedFile `json:"copy"`
	Withheld   []string       `json:"withheld"`
}

// RouteDuties is what one route's publication requires: its name, its
// naming permission (config Naming.PermissionRequired), a replication
// pack with any disclosure of its figures, and its window-close record
// where it has a page.
type RouteDuties struct {
	Name            string
	Permission      bool
	ReplicationPack bool
	WindowClose     bool
}

// WithheldValue is a confidential limit's value on a route, as its
// configuration records it or as a refresh command read it.
type WithheldValue struct {
	Route     string
	Dimension string
	Value     float64
}

// PublicationState is the release as it stands: each cell manifest's
// and each window-close record's bytes by its slash path under the
// release output directory, each page the cells generate by its slash
// path under PagesDir, each route's evidence pack as BuildPack builds it
// from those pages and manifests, by route, the routes, the values of
// its confidential limits, and the time of the check.
type PublicationState struct {
	Manifests    map[string][]byte
	WindowCloses map[string][]byte
	Pages        map[string][]byte
	PagesDir     string
	Packs        map[string][]byte
	Routes       []RouteDuties
	Withheld     []WithheldValue
	Now          time.Time
}

var sha256Hex = regexp.MustCompile(`^[0-9a-f]{64}$`)

// LoadPublication strictly decodes the publication record at path.
func LoadPublication(path string) (*Publication, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("evidence: publication record: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var p Publication
	if err := dec.Decode(&p); err != nil {
		return nil, fmt.Errorf("evidence: publication record %s: %w", path, err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("evidence: publication record %s: data after the JSON object", path)
	}
	p.dir = filepath.Dir(path)
	return &p, nil
}

// Check returns nil when the record is complete for s: it names every
// cell manifest, every window-close record and every page by the hash of
// its bytes, and each page is on disk with those bytes; every route with
// the WindowClose duty has its record at <route>/WindowCloseFile; every
// route has one entry, with a notice whose window of at least
// NoticeWorkingDays working days closed before s.Now, sent no earlier
// than the UTC date of the latest start among the route's cell manifests
// or of its window-close record, and whose pack is the route's pack in
// s.Packs byte for byte, s.Packs holding one for the route, its
// responses, its naming
// permission where required and its replication pack, sent no later than
// the notice and carrying every file of the route's pack (PackCarries),
// where required; every disclosure predates
// the first notice; every pre-release copy withholds the routes that need
// a replication pack; and every file the record names has the SHA-256 it
// records. A verbatim response's text must be UTF-8, and each correction
// and each disclosure's provider one line, as Page prints them. No text
// Page prints from the record (a verbatim response's text, a correction
// or a disclosure's provider) may hold any notice's account or recipient,
// in any letter case or spacing, or a value in s.Withheld written in a
// form config.ValueForms gives as a number of its own (config.Carries).
// Otherwise it returns an error naming every missing item and every
// mismatch, and neither such an identifier nor such a value.
func (p *Publication) Check(s PublicationState) error {
	var problems []string
	fail := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }
	date := func(field, v string) (time.Time, bool) {
		d, err := time.Parse(time.DateOnly, v)
		if err != nil {
			fail("%s: %q is not a date (2006-01-02)", field, v)
			return time.Time{}, false
		}
		return d, true
	}
	text := func(field, v string) {
		if strings.TrimSpace(v) == "" {
			fail("%s: required", field)
		}
	}
	line := func(field, v string) {
		text(field, v)
		if strings.ContainsAny(v, "\r\n") {
			fail("%s: one line required", field)
		}
	}
	type identifier struct{ name, folded string }
	var ids []identifier
	for _, r := range p.Routes {
		if n := r.Notice; n != nil {
			ids = append(ids, identifier{"route " + r.Route + "'s notice.account", fold(n.Account)}, identifier{"route " + r.Route + "'s notice.recipient", fold(n.Recipient)})
		}
	}
	printed := func(field, v string) {
		var held []string
		folded := fold(v)
		for _, id := range ids {
			if h := "holds " + id.name; id.folded != "" && strings.Contains(folded, id.folded) && !slices.Contains(held, h) {
				held = append(held, h)
			}
		}
		for _, w := range s.Withheld {
			h := fmt.Sprintf("holds the value of route %s's confidential %s limit", w.Route, w.Dimension)
			if _, ok := config.Carries(v, config.ValueForms(w.Value)); ok && !slices.Contains(held, h) {
				held = append(held, h)
			}
		}
		for _, h := range held {
			fail("%s: %s", field, h)
		}
	}

	p.fileSet("manifests", p.Manifests, s.Manifests, fail)
	p.fileSet("window_closes", p.WindowCloses, s.WindowCloses, fail)
	p.fileSet("pages", p.Pages, s.Pages, fail)
	for _, name := range sortedKeys(s.Pages) {
		b, err := os.ReadFile(filepath.Join(s.PagesDir, filepath.FromSlash(name)))
		switch {
		case errors.Is(err, os.ErrNotExist):
			fail("pages: %s is missing under %s", name, s.PagesDir)
		case err != nil:
			fail("pages: %v", err)
		case !bytes.Equal(b, s.Pages[name]):
			fail("pages: %s under %s is not the page the cells generate", name, s.PagesDir)
		}
	}
	nonWorking := map[string]bool{}
	for i, v := range p.NonWorkingDays {
		if d, ok := date(fmt.Sprintf("non_working_days[%d]", i), v); ok {
			nonWorking[d.Format(time.DateOnly)] = true
		}
	}

	duties := map[string]RouteDuties{}
	for _, r := range s.Routes {
		duties[r.Name] = r
	}
	seen := map[string]bool{}
	var firstNotice time.Time
	for i, r := range p.Routes {
		d, ok := duties[r.Route]
		switch {
		case !ok:
			fail("routes[%d]: %q is not a route of the release", i, r.Route)
			continue
		case seen[r.Route]:
			fail("routes[%d]: route %s recorded twice", i, r.Route)
			continue
		}
		seen[r.Route] = true
		field := "route " + r.Route
		var sent time.Time
		if n := r.Notice; n == nil {
			fail("%s: notice: required", field)
		} else {
			text(field+": notice.recipient", n.Recipient)
			text(field+": notice.account", n.Account)
			pack, packed := s.Packs[r.Route]
			if !packed {
				fail("%s: notice.pack: the release has no evidence pack for the route", field)
			}
			if b := p.file(field+": notice.pack", n.Pack, fail); b != nil && packed && !bytes.Equal(b, pack) {
				fail("%s: notice.pack: %s is not the evidence pack the route's pages and manifests give (percentes-release pack)", field, n.Pack.Path)
			}
			var okSent, okClose bool
			var closes time.Time
			sent, okSent = date(field+": notice.sent", n.Sent)
			closes, okClose = date(field+": notice.window_closes", n.WindowCloses)
			if okSent && (firstNotice.IsZero() || sent.Before(firstNotice)) {
				firstNotice = sent
			}
			if okSent {
				if started := routeStarted(s.Manifests, r.Route, field, fail); sent.Before(dayOf(started)) {
					fail("%s: notice.sent: %s, before the route's last cell in its pack started at %s", field, n.Sent, started.UTC().Format(time.RFC3339Nano))
				}
				if b, ok := s.WindowCloses[r.Route+"/"+WindowCloseFile]; ok {
					var w WindowClose
					if err := json.Unmarshal(b, &w); err != nil {
						fail("%s: notice.sent: %s/%s: %v", field, r.Route, WindowCloseFile, err)
					} else if sent.Before(dayOf(w.RecordedUTC)) {
						fail("%s: notice.sent: %s, before the route's window-close record of %s", field, n.Sent, w.RecordedUTC.UTC().Format(time.RFC3339Nano))
					}
				}
			}
			if okSent && okClose {
				if days := workingDays(sent, closes, nonWorking); days < NoticeWorkingDays {
					fail("%s: notice: the window closes %s, %d working days after the notice of %s, want at least %d", field, n.WindowCloses, days, n.Sent, NoticeWorkingDays)
				}
				if s.Now.Before(closes.AddDate(0, 0, 1)) {
					fail("%s: notice: the window is open until the end of %s UTC", field, n.WindowCloses)
				}
			}
		}
		if r.Responses == nil {
			fail("%s: responses: required (an empty list when none arrived)", field)
		}
		for j, resp := range r.Responses {
			rf := fmt.Sprintf("%s: responses[%d]", field, j)
			if got, ok := date(rf+".received", resp.Received); ok && !sent.IsZero() && got.Before(sent) {
				fail("%s.received: %s, before the notice of %s", rf, resp.Received, r.Notice.Sent)
			}
			if resp.Verbatim {
				if b := p.file(rf+".text", resp.Text, fail); b != nil {
					if !utf8.Valid(b) {
						fail("%s.text: %s is not UTF-8 text", rf, resp.Text.Path)
					}
					printed(rf+".text", string(b))
				}
				p.file(rf+".consent", resp.Consent, fail)
			} else if resp.Corrections == nil {
				fail("%s.corrections: required when the response does not publish verbatim (an empty list when none were made)", rf)
			}
			for k, c := range resp.Corrections {
				line(fmt.Sprintf("%s.corrections[%d]", rf, k), c)
				printed(fmt.Sprintf("%s.corrections[%d]", rf, k), c)
			}
		}
		switch {
		case r.Permission != nil:
			p.file(field+": permission", r.Permission, fail)
		case d.Permission:
			fail("%s: permission: required (naming.permission_required)", field)
		}
		switch rp := r.ReplicationPack; {
		case rp != nil:
			text(field+": replication_pack.recipient", rp.Recipient)
			if b := p.file(field+": replication_pack.pack", rp.Pack, fail); b != nil {
				if err := PackCarries(b, s.Packs[r.Route]); err != nil {
					fail("%s: replication_pack.pack: %s: %v", field, rp.Pack.Path, err)
				}
			}
			if got, ok := date(field+": replication_pack.sent", rp.Sent); ok && !sent.IsZero() && got.After(sent) {
				fail("%s: replication_pack.sent: %s, after the notice of %s", field, rp.Sent, r.Notice.Sent)
			}
		case d.ReplicationPack:
			fail("%s: replication_pack: required", field)
		}
	}
	for _, r := range s.Routes {
		if !seen[r.Name] {
			fail("route %s: not in the record", r.Name)
		}
		if _, ok := s.WindowCloses[r.Name+"/"+WindowCloseFile]; r.WindowClose && !ok {
			fail("route %s: window close: no %s; a route with a page needs its window-close record", r.Name, WindowCloseFile)
		}
	}

	if p.Disclosures == nil {
		fail("disclosures: required (an empty list when none were made)")
	}
	for i, d := range p.Disclosures {
		field := fmt.Sprintf("disclosures[%d]", i)
		line(field+".provider", d.Provider)
		printed(field+".provider", d.Provider)
		text(field+".recipient", d.Recipient)
		if d.How != DisclosureEmail && d.How != DisclosureForm {
			fail("%s.how: %q, want %s or %s", field, d.How, DisclosureEmail, DisclosureForm)
		}
		if got, ok := date(field+".date", d.Date); ok && !firstNotice.IsZero() && !got.Before(firstNotice) {
			fail("%s.date: %s, not before the first notice of %s", field, d.Date, firstNotice.Format(time.DateOnly))
		}
		if d.Copy != nil {
			p.file(field+".copy", d.Copy, fail)
		}
	}

	if p.Prerelease == nil {
		fail("prerelease: required (an empty list when no copy was sent)")
	}
	for i, c := range p.Prerelease {
		field := fmt.Sprintf("prerelease[%d]", i)
		date(field+".date", c.Date)
		text(field+".recipients", c.Recipients)
		p.file(field+".copy", c.Copy, fail)
		for j, w := range c.Withheld {
			if _, ok := duties[w]; !ok {
				fail("%s.withheld[%d]: %q is not a route of the release", field, j, w)
			}
		}
		for _, r := range s.Routes {
			if r.ReplicationPack && !slices.Contains(c.Withheld, r.Name) {
				fail("%s.withheld: route %s's figures go with its replication pack, which the copy lacks", field, r.Name)
			}
		}
	}

	if len(problems) > 0 {
		return fmt.Errorf("evidence: publication refused: %s", strings.Join(problems, "; "))
	}
	return nil
}

// ToldBefore lists each disclosure as the page names it: the provider,
// how and the date, in date order.
func (p *Publication) ToldBefore() []string {
	ds := p.toldBefore()
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.String()
	}
	return out
}

// toldBefore is each disclosure without its recipient and copy, in date
// order, then by provider.
func (p *Publication) toldBefore() []PageDisclosure {
	out := make([]PageDisclosure, len(p.Disclosures))
	for i, d := range p.Disclosures {
		out[i] = PageDisclosure{Provider: d.Provider, How: d.How, Date: d.Date}
	}
	slices.SortStableFunc(out, func(a, b PageDisclosure) int {
		if c := strings.Compare(a.Date, b.Date); c != 0 {
			return c
		}
		return strings.Compare(a.Provider, b.Provider)
	})
	return out
}

// fileSet checks that recorded names each file of have once, by the
// SHA-256 of its bytes, and nothing else.
func (p *Publication) fileSet(field string, recorded []PublishedFile, have map[string][]byte, fail func(string, ...any)) {
	named := map[string]bool{}
	for _, f := range recorded {
		b, ok := have[f.Path]
		switch {
		case named[f.Path]:
			fail("%s: %s recorded twice", field, f.Path)
		case !ok:
			fail("%s: %s is not among the release's %s", field, f.Path, field)
		case refOf(f.Path, b).SHA256 != f.SHA256:
			fail("%s: %s has SHA-256 %s, the record %s", field, f.Path, refOf(f.Path, b).SHA256, f.SHA256)
		}
		named[f.Path] = true
	}
	for _, name := range sortedKeys(have) {
		if !named[name] {
			fail("%s: %s is not in the record", field, name)
		}
	}
}

// file checks that f is given and names a file under the record's
// directory with the SHA-256 it records, and returns the file's bytes
// when it does.
func (p *Publication) file(field string, f *PublishedFile, fail func(string, ...any)) []byte {
	if f == nil {
		fail("%s: required", field)
		return nil
	}
	name := filepath.FromSlash(f.Path)
	switch {
	case f.Path == "" || !filepath.IsLocal(name):
		fail("%s.path: %q is not a path inside the record's directory", field, f.Path)
		return nil
	case !sha256Hex.MatchString(f.SHA256):
		fail("%s.sha256: %q is not a lowercase hex SHA-256", field, f.SHA256)
		return nil
	}
	b, err := os.ReadFile(filepath.Join(p.dir, name))
	switch {
	case errors.Is(err, os.ErrNotExist):
		fail("%s: %s is missing", field, f.Path)
	case err != nil:
		fail("%s: %v", field, err)
	case refOf(f.Path, b).SHA256 != f.SHA256:
		fail("%s: %s has SHA-256 %s, the record %s", field, f.Path, refOf(f.Path, b).SHA256, f.SHA256)
	default:
		return b
	}
	return nil
}

// routeStarted returns the latest start among route's cell manifests,
// the manifests its pack carries, or the zero time when it has none.
func routeStarted(manifests map[string][]byte, route, field string, fail func(string, ...any)) time.Time {
	var latest time.Time
	for _, name := range sortedKeys(manifests) {
		if !strings.HasPrefix(name, route+"/") {
			continue
		}
		var m Manifest
		if err := json.Unmarshal(manifests[name], &m); err != nil {
			fail("%s: notice.sent: %s: %v", field, name, err)
			continue
		}
		if m.StartUTC.After(latest) {
			latest = m.StartUTC
		}
	}
	return latest
}

// dayOf is the start of t's UTC date.
func dayOf(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// workingDays counts the weekdays after from up to and including to
// that nonWorking does not hold.
func workingDays(from, to time.Time, nonWorking map[string]bool) int {
	n := 0
	for d := from.AddDate(0, 0, 1); !d.After(to); d = d.AddDate(0, 0, 1) {
		if wd := d.Weekday(); wd != time.Saturday && wd != time.Sunday && !nonWorking[d.Format(time.DateOnly)] {
			n++
		}
	}
	return n
}

// fold is s in lower case with each run of white space one space.
func fold(s string) string {
	return strings.Join(strings.Fields(strings.ToLower(s)), " ")
}

func sortedKeys(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
