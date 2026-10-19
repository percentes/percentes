package evidence

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// publicationFixture is a complete release publication: three routes,
// azure needing its naming permission, bedrock its replication pack and
// groq, the route with a page, its window-close record, with every file
// the record names written under dir and the pages under the state's
// PagesDir. Each notice carries its route's pack and bedrock's
// replication pack carries its pack's files and one more. groq withholds
// a tokens_per_minute limit of 250,013.5, which no text holds.
type publicationFixture struct {
	dir string
	p   Publication
	s   PublicationState
}

func shaOf(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// put writes content at the slash path name under dir and returns it as
// a published file.
func put(t *testing.T, dir, name, content string) *PublishedFile {
	t.Helper()
	p := filepath.Join(dir, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return &PublishedFile{Path: name, SHA256: shaOf([]byte(content))}
}

func newPublicationFixture(t *testing.T) *publicationFixture {
	t.Helper()
	f := &publicationFixture{dir: t.TempDir()}
	f.s = PublicationState{
		Manifests: map[string][]byte{"azure/c01-a1/manifest.json": []byte(`{"start_utc": "2026-10-20T09:00:00Z"}`),
			"bedrock/c01-a1/manifest.json": []byte(`{"start_utc": "2026-10-20T10:00:00Z"}`), "groq/c01-a1/manifest.json": []byte(`{"start_utc": "2026-10-21T09:00:00Z"}`)},
		WindowCloses: map[string][]byte{"groq/" + WindowCloseFile: []byte(`{"recorded_utc": "2026-10-26T12:00:00Z"}`)},
		Pages:        map[string][]byte{"index.md": []byte("index"), "routes/groq.txt": []byte("groq page")},
		PagesDir:     t.TempDir(),
		Routes:       []RouteDuties{{Name: "groq", WindowClose: true}, {Name: "azure", Permission: true}, {Name: "bedrock", ReplicationPack: true}},
		Withheld:     []WithheldValue{{Route: "groq", Dimension: "tokens_per_minute", Value: 250013.5}},
		Now:          time.Date(2026, 11, 3, 9, 0, 0, 0, time.UTC),
	}
	for name, b := range f.s.Pages {
		put(t, f.s.PagesDir, name, string(b))
		f.p.Pages = append(f.p.Pages, PublishedFile{Path: name, SHA256: shaOf(b)})
	}
	for name, b := range f.s.Manifests {
		f.p.Manifests = append(f.p.Manifests, PublishedFile{Path: name, SHA256: shaOf(b)})
	}
	for name, b := range f.s.WindowCloses {
		f.p.WindowCloses = append(f.p.WindowCloses, PublishedFile{Path: name, SHA256: shaOf(b)})
	}
	f.s.Packs = map[string][]byte{}
	for _, r := range f.s.Routes {
		f.s.Packs[r.Name] = packOf(t, r.Name, f.s)
	}
	notice := func(route string) *Notice {
		return &Notice{Sent: "2026-10-27", Recipient: route + " support", Account: route + "-account", Pack: put(t, f.dir, "packs/"+route+".tar", string(f.s.Packs[route])), WindowCloses: "2026-11-02"}
	}
	replication := [][2]string{{"replication/README", "how to run it"}}
	packed, err := ReadPack(f.s.Packs["bedrock"])
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range sortedKeys(packed) {
		replication = append(replication, [2]string{name, string(packed[name])})
	}
	f.p.Routes = []RoutePublication{
		{Route: "groq", Notice: notice("groq"), Responses: []Response{
			{Received: "2026-10-29", Verbatim: true, Text: put(t, f.dir, "responses/groq.txt", "looks right"), Consent: put(t, f.dir, "responses/groq-consent.eml", "yes, verbatim")},
		}},
		{Route: "azure", Notice: notice("azure"), Responses: []Response{{Received: "2026-10-30", Corrections: []string{"2026-10-31: model version spelt as documented"}}},
			Permission: put(t, f.dir, "naming/openai-permission.eml", "permission")},
		{Route: "bedrock", Notice: notice("bedrock"), Responses: []Response{},
			ReplicationPack: &Send{Sent: "2026-10-27", Recipient: "aws benchmarks", Pack: put(t, f.dir, "packs/aws-replication.tar", string(tarOf(t, replication...)))}},
	}
	f.p.Disclosures = []Disclosure{
		{Provider: "OpenAI", How: DisclosureEmail, Date: "2026-10-07", Recipient: "permissions"},
		{Provider: "Groq", How: DisclosureEmail, Date: "2026-09-15", Recipient: "support", Copy: put(t, f.dir, "told/groq.eml", "email")},
		{Provider: "AWS", How: DisclosureForm, Date: "2026-10-07", Recipient: "Bedrock FTU form"},
	}
	f.p.Prerelease = []Prerelease{{Date: "2026-10-30", Recipients: "named recipients", Copy: put(t, f.dir, "prerelease.pdf", "copy"), Withheld: []string{"bedrock"}}}
	f.p.NonWorkingDays = []string{}
	return f
}

// packOf builds route's pack from its pages and manifests in s.
func packOf(t *testing.T, route string, s PublicationState) []byte {
	t.Helper()
	files := map[string][]byte{}
	for name, b := range s.Pages {
		if strings.HasPrefix(name, "routes/"+route+".") {
			files["pages/"+name] = b
		}
	}
	for name, b := range s.Manifests {
		if strings.HasPrefix(name, route+"/") {
			files["out/"+name] = b
		}
	}
	b, err := BuildPack(route, files)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// load writes the record as JSON and loads it.
func (f *publicationFixture) load(t *testing.T) *Publication {
	t.Helper()
	b, err := json.Marshal(f.p)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.dir, "publication.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := LoadPublication(path)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// check loads the record and checks it.
func (f *publicationFixture) check(t *testing.T) error {
	t.Helper()
	return f.load(t).Check(f.s)
}

// A complete record passes, and lists the providers told before the
// notices in date order; a notice dated the day of its route's last cell
// start or window-close record passes.
func TestPublicationComplete(t *testing.T) {
	f := newPublicationFixture(t)
	if err := f.check(t); err != nil {
		t.Fatal(err)
	}
	f.p.Routes[0].Notice.Sent, f.p.Routes[1].Notice.Sent = "2026-10-26", "2026-10-20"
	if err := f.check(t); err != nil {
		t.Fatalf("notices on the days of the window-close record and the last cell start: %v", err)
	}
	want := []string{"Groq, by email, 2026-09-15", "AWS, by form, 2026-10-07", "OpenAI, by email, 2026-10-07"}
	if got := f.p.ToldBefore(); !reflect.DeepEqual(got, want) {
		t.Errorf("told before %q, want %q", got, want)
	}
	if n := workingDays(time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC), time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC), nil); n != 4 {
		t.Errorf("27 October to 2 November: %d working days, want 4", n)
	}
}

// The check refuses each missing item and each mismatch, naming it.
func TestPublicationRefuses(t *testing.T) {
	route := func(f *publicationFixture, name string) *RoutePublication {
		for i := range f.p.Routes {
			if f.p.Routes[i].Route == name {
				return &f.p.Routes[i]
			}
		}
		panic(name)
	}
	for _, c := range []struct {
		name string
		edit func(*testing.T, *publicationFixture)
		want string
	}{
		{"manifest unrecorded", func(_ *testing.T, f *publicationFixture) {
			f.s.Manifests["groq/c02-a1/manifest.json"] = []byte("m2")
		}, "manifests: groq/c02-a1/manifest.json is not in the record"},
		{"manifest changed", func(_ *testing.T, f *publicationFixture) {
			f.s.Manifests["groq/c01-a1/manifest.json"] = []byte("m-groq changed")
		}, "manifests: groq/c01-a1/manifest.json has SHA-256 " + shaOf([]byte("m-groq changed"))},
		{"manifest not in the release", func(_ *testing.T, f *publicationFixture) {
			delete(f.s.Manifests, "azure/c01-a1/manifest.json")
		}, "manifests: azure/c01-a1/manifest.json is not among the release's manifests"},
		{"manifest twice", func(_ *testing.T, f *publicationFixture) {
			f.p.Manifests = append(f.p.Manifests, f.p.Manifests[0])
		}, "recorded twice"},
		{"no window close", func(_ *testing.T, f *publicationFixture) {
			delete(f.s.WindowCloses, "groq/window-close.json")
			f.p.WindowCloses = nil
		}, "route groq: window close: no window-close.json; a route with a page needs its window-close record"},
		{"window close unrecorded", func(_ *testing.T, f *publicationFixture) { f.p.WindowCloses = nil },
			"window_closes: groq/window-close.json is not in the record"},
		{"window close changed", func(_ *testing.T, f *publicationFixture) {
			f.s.WindowCloses["groq/window-close.json"] = []byte("wc-groq edited")
		}, "window_closes: groq/window-close.json has SHA-256 " + shaOf([]byte("wc-groq edited"))},
		{"window close not in the release", func(_ *testing.T, f *publicationFixture) {
			f.p.WindowCloses = append(f.p.WindowCloses, PublishedFile{Path: "azure/window-close.json", SHA256: shaOf([]byte("wc-azure"))})
		}, "window_closes: azure/window-close.json is not among the release's window_closes"},
		{"page unrecorded", func(t *testing.T, f *publicationFixture) {
			f.s.Pages["routes/groq.json"] = []byte("{}")
			put(t, f.s.PagesDir, "routes/groq.json", "{}")
		}, "pages: routes/groq.json is not in the record"},
		{"page regenerated differently", func(_ *testing.T, f *publicationFixture) {
			f.s.Pages["index.md"] = []byte("index, later cells")
		}, "pages: index.md has SHA-256"},
		{"page edited on disk", func(t *testing.T, f *publicationFixture) {
			put(t, f.s.PagesDir, "routes/groq.txt", "groq page, edited")
		}, "pages: routes/groq.txt under " + "%DIR%" + " is not the page the cells generate"},
		{"page missing on disk", func(t *testing.T, f *publicationFixture) {
			if err := os.Remove(filepath.Join(f.s.PagesDir, "index.md")); err != nil {
				t.Fatal(err)
			}
		}, "pages: index.md is missing under"},
		{"route unrecorded", func(_ *testing.T, f *publicationFixture) { f.p.Routes = f.p.Routes[:2] }, "route bedrock: not in the record"},
		{"route unknown", func(_ *testing.T, f *publicationFixture) {
			f.p.Routes = append(f.p.Routes, RoutePublication{Route: "baseten"})
		}, `routes[3]: "baseten" is not a route of the release`},
		{"route twice", func(_ *testing.T, f *publicationFixture) {
			f.p.Routes = append(f.p.Routes, f.p.Routes[0])
		}, "routes[3]: route groq recorded twice"},
		{"no notice", func(_ *testing.T, f *publicationFixture) { route(f, "groq").Notice = nil }, "route groq: notice: required"},
		{"no recipient", func(_ *testing.T, f *publicationFixture) { route(f, "groq").Notice.Recipient = " " }, "route groq: notice.recipient: required"},
		{"no account", func(_ *testing.T, f *publicationFixture) { route(f, "groq").Notice.Account = "" }, "route groq: notice.account: required"},
		{"no pack", func(_ *testing.T, f *publicationFixture) { route(f, "groq").Notice.Pack = nil }, "route groq: notice.pack: required"},
		{"pack changed", func(t *testing.T, f *publicationFixture) {
			put(t, f.dir, "packs/groq.tar", "groq pack, rebuilt")
		}, "route groq: notice.pack: packs/groq.tar has SHA-256 " + shaOf([]byte("groq pack, rebuilt"))},
		{"pack missing", func(t *testing.T, f *publicationFixture) {
			if err := os.Remove(filepath.Join(f.dir, "packs", "groq.tar")); err != nil {
				t.Fatal(err)
			}
		}, "route groq: notice.pack: packs/groq.tar is missing"},
		{"pack outside", func(_ *testing.T, f *publicationFixture) { route(f, "groq").Notice.Pack.Path = "../groq.tar" }, `notice.pack.path: "../groq.tar" is not a path inside`},
		{"pack of other text", func(t *testing.T, f *publicationFixture) {
			route(f, "groq").Notice.Pack = put(t, f.dir, "packs/groq.tar", "tables and evidence")
		}, "route groq: notice.pack: packs/groq.tar is not the evidence pack the route's pages and manifests give (percentes-release pack)"},
		{"pack before a later cell", func(t *testing.T, f *publicationFixture) {
			m := []byte(`{"start_utc": "2026-10-22T09:00:00Z"}`)
			f.s.Manifests["groq/c02-a1/manifest.json"] = m
			f.p.Manifests = append(f.p.Manifests, PublishedFile{Path: "groq/c02-a1/manifest.json", SHA256: shaOf(m)})
			f.s.Packs["groq"] = packOf(t, "groq", f.s)
		}, "route groq: notice.pack: packs/groq.tar is not the evidence pack"},
		{"notice before a later cell in its pack", func(t *testing.T, f *publicationFixture) {
			m := []byte(`{"start_utc": "2026-10-28T09:00:00Z"}`)
			f.s.Manifests["groq/c02-a1/manifest.json"] = m
			f.p.Manifests = append(f.p.Manifests, PublishedFile{Path: "groq/c02-a1/manifest.json", SHA256: shaOf(m)})
			f.s.Packs["groq"] = packOf(t, "groq", f.s)
			route(f, "groq").Notice.Pack = put(t, f.dir, "packs/groq.tar", string(f.s.Packs["groq"]))
		}, "evidence: publication refused: route groq: notice.sent: 2026-10-27, before the route's last cell in its pack started at 2026-10-28T09:00:00Z"},
		{"notice before the last cell", func(_ *testing.T, f *publicationFixture) { route(f, "azure").Notice.Sent = "2026-10-19" },
			"route azure: notice.sent: 2026-10-19, before the route's last cell in its pack started at 2026-10-20T09:00:00Z"},
		{"notice before the window-close record", func(_ *testing.T, f *publicationFixture) { route(f, "groq").Notice.Sent = "2026-10-23" },
			"evidence: publication refused: route groq: notice.sent: 2026-10-23, before the route's window-close record of 2026-10-26T12:00:00Z"},
		{"manifest not JSON", func(_ *testing.T, f *publicationFixture) {
			f.s.Manifests["groq/c01-a1/manifest.json"] = []byte("m-groq")
		}, "route groq: notice.sent: groq/c01-a1/manifest.json: invalid character"},
		{"no pack for the route", func(_ *testing.T, f *publicationFixture) { delete(f.s.Packs, "groq") },
			"route groq: notice.pack: the release has no evidence pack for the route"},
		{"empty pack, no pack for the route", func(t *testing.T, f *publicationFixture) {
			route(f, "groq").Notice.Pack = put(t, f.dir, "packs/groq.tar", "")
			delete(f.s.Packs, "groq")
		}, "route groq: notice.pack: the release has no evidence pack for the route"},
		{"pack hash form", func(_ *testing.T, f *publicationFixture) { route(f, "groq").Notice.Pack.SHA256 = "ABC" }, `notice.pack.sha256: "ABC" is not a lowercase hex SHA-256`},
		{"bad date", func(_ *testing.T, f *publicationFixture) { route(f, "groq").Notice.Sent = "27/10/2026" }, `route groq: notice.sent: "27/10/2026" is not a date`},
		{"short window", func(_ *testing.T, f *publicationFixture) { route(f, "groq").Notice.WindowCloses = "2026-10-30" },
			"route groq: notice: the window closes 2026-10-30, 3 working days after the notice of 2026-10-27, want at least 4"},
		{"non-working day", func(_ *testing.T, f *publicationFixture) { f.p.NonWorkingDays = []string{"2026-10-29"} },
			"route groq: notice: the window closes 2026-11-02, 3 working days after the notice of 2026-10-27"},
		{"window open", func(_ *testing.T, f *publicationFixture) { f.s.Now = time.Date(2026, 11, 2, 23, 0, 0, 0, time.UTC) },
			"route groq: notice: the window is open until the end of 2026-11-02 UTC"},
		{"no responses", func(_ *testing.T, f *publicationFixture) { route(f, "bedrock").Responses = nil }, "route bedrock: responses: required"},
		{"verbatim without consent", func(_ *testing.T, f *publicationFixture) { route(f, "groq").Responses[0].Consent = nil },
			"route groq: responses[0].consent: required"},
		{"verbatim without text", func(_ *testing.T, f *publicationFixture) { route(f, "groq").Responses[0].Text = nil },
			"route groq: responses[0].text: required"},
		{"consent changed", func(t *testing.T, f *publicationFixture) { put(t, f.dir, "responses/groq-consent.eml", "no") },
			"route groq: responses[0].consent: responses/groq-consent.eml has SHA-256"},
		{"no corrections", func(_ *testing.T, f *publicationFixture) { route(f, "azure").Responses[0].Corrections = nil },
			"route azure: responses[0].corrections: required when the response does not publish verbatim"},
		{"empty correction", func(_ *testing.T, f *publicationFixture) { route(f, "azure").Responses[0].Corrections = []string{""} },
			"route azure: responses[0].corrections[0]: required"},
		{"correction of two lines", func(_ *testing.T, f *publicationFixture) {
			route(f, "azure").Responses[0].Corrections = []string{"model version\n- spelt as documented"}
		}, "route azure: responses[0].corrections[0]: one line required"},
		{"verbatim text not UTF-8", func(t *testing.T, f *publicationFixture) {
			route(f, "groq").Responses[0].Text = put(t, f.dir, "responses/groq.txt", "looks \xffright")
		}, "route groq: responses[0].text: responses/groq.txt is not UTF-8 text"},
		{"response before notice", func(_ *testing.T, f *publicationFixture) { route(f, "azure").Responses[0].Received = "2026-10-26" },
			"route azure: responses[0].received: 2026-10-26, before the notice of 2026-10-27"},
		{"no permission", func(_ *testing.T, f *publicationFixture) { route(f, "azure").Permission = nil },
			"route azure: permission: required (naming.permission_required)"},
		{"no replication pack", func(_ *testing.T, f *publicationFixture) { route(f, "bedrock").ReplicationPack = nil },
			"route bedrock: replication_pack: required"},
		{"replication pack late", func(_ *testing.T, f *publicationFixture) { route(f, "bedrock").ReplicationPack.Sent = "2026-10-28" },
			"route bedrock: replication_pack.sent: 2026-10-28, after the notice of 2026-10-27"},
		{"replication pack changed", func(t *testing.T, f *publicationFixture) { put(t, f.dir, "packs/aws-replication.tar", "other") },
			"route bedrock: replication_pack.pack: packs/aws-replication.tar has SHA-256"},
		{"replication pack inventory differs", func(t *testing.T, f *publicationFixture) {
			other := f.s
			other.Manifests = map[string][]byte{"bedrock/c01-a1/manifest.json": []byte("m-bedrock, earlier")}
			route(f, "bedrock").ReplicationPack.Pack = put(t, f.dir, "packs/aws-replication.tar", string(packOf(t, "bedrock", other)))
		}, "route bedrock: replication_pack.pack: packs/aws-replication.tar: evidence: pack: [inventory.json out/bedrock/c01-a1/manifest.json] differ from the route's evidence pack"},
		{"replication pack of other text", func(t *testing.T, f *publicationFixture) {
			route(f, "bedrock").ReplicationPack.Pack = put(t, f.dir, "packs/aws-replication.tar", "replication")
		}, "route bedrock: replication_pack.pack: packs/aws-replication.tar: evidence: pack: "},
		{"replication pack without the inventory", func(t *testing.T, f *publicationFixture) {
			route(f, "bedrock").ReplicationPack.Pack = put(t, f.dir, "packs/aws-replication.tar", string(tarOf(t, [2]string{"replication/README", "how to run it"})))
		}, "route bedrock: replication_pack.pack: packs/aws-replication.tar: evidence: pack: holds no [inventory.json out/bedrock/c01-a1/manifest.json] of the route's evidence pack"},
		{"no disclosures", func(_ *testing.T, f *publicationFixture) { f.p.Disclosures = nil }, "disclosures: required"},
		{"disclosure after the notices", func(_ *testing.T, f *publicationFixture) { f.p.Disclosures[0].Date = "2026-10-27" },
			"disclosures[0].date: 2026-10-27, not before the first notice of 2026-10-27"},
		{"disclosure how", func(_ *testing.T, f *publicationFixture) { f.p.Disclosures[0].How = "phone" }, `disclosures[0].how: "phone", want email or form`},
		{"disclosure recipient", func(_ *testing.T, f *publicationFixture) { f.p.Disclosures[0].Recipient = "" }, "disclosures[0].recipient: required"},
		{"disclosure provider of two lines", func(_ *testing.T, f *publicationFixture) { f.p.Disclosures[0].Provider = "OpenAI\r\nAWS" },
			"disclosures[0].provider: one line required"},
		{"disclosure copy changed", func(t *testing.T, f *publicationFixture) { put(t, f.dir, "told/groq.eml", "edited") },
			"disclosures[1].copy: told/groq.eml has SHA-256"},
		{"no prerelease", func(_ *testing.T, f *publicationFixture) { f.p.Prerelease = nil }, "prerelease: required"},
		{"prerelease without copy", func(_ *testing.T, f *publicationFixture) { f.p.Prerelease[0].Copy = nil }, "prerelease[0].copy: required"},
		{"prerelease shows bedrock", func(_ *testing.T, f *publicationFixture) { f.p.Prerelease[0].Withheld = nil },
			"prerelease[0].withheld: route bedrock's figures go with its replication pack, which the copy lacks"},
		{"prerelease withholds an unknown route", func(_ *testing.T, f *publicationFixture) {
			f.p.Prerelease[0].Withheld = append(f.p.Prerelease[0].Withheld, "baseten")
		}, `prerelease[0].withheld[1]: "baseten" is not a route of the release`},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newPublicationFixture(t)
			c.edit(t, f)
			want := strings.ReplaceAll(c.want, "%DIR%", f.s.PagesDir)
			if err := f.check(t); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("got %v, want %q", err, want)
			}
		})
	}
}

// The check refuses a text the page prints that holds a notice's account
// or recipient, in any letter case and with or without white space, or a
// withheld value as a number of its own, naming the field and neither the
// identifier nor the value: a consented reply quoting its account, its
// recipient and a limit, a correction holding the account, and a
// disclosure's provider holding another route's account, also with format
// characters inside them or a decimal comma in the value. A longer number
// holding a value's digits passes, and so does another value written with
// a decimal comma.
func TestPublicationWithheld(t *testing.T) {
	for _, c := range []struct {
		name string
		edit func(*testing.T, *publicationFixture)
		want []string
	}{
		{"consented reply quoting the account and a limit", func(t *testing.T, f *publicationFixture) {
			f.s.Withheld = append(f.s.Withheld, WithheldValue{Route: "groq", Dimension: "requests_per_minute", Value: 9999})
			f.p.Routes[0].Responses[0].Text = put(t, f.dir, "responses/groq.txt", "account groq-account (sent to groq support); limit is 9999 rpm, tpm 250,013.5")
		}, []string{"route groq: responses[0].text: holds route groq's notice.account", "route groq: responses[0].text: holds route groq's notice.recipient",
			"route groq: responses[0].text: holds the value of route groq's confidential tokens_per_minute limit",
			"route groq: responses[0].text: holds the value of route groq's confidential requests_per_minute limit"}},
		{"reply quoting the recipient over a line break", func(t *testing.T, f *publicationFixture) {
			f.p.Routes[0].Responses[0].Text = put(t, f.dir, "responses/groq.txt", "Write to GROQ\r\n  Support.\r\n")
		}, []string{"route groq: responses[0].text: holds route groq's notice.recipient"}},
		{"correction holding the account", func(_ *testing.T, f *publicationFixture) {
			f.p.Routes[1].Responses[0].Corrections = []string{"tier of account Azure-Account spelt as documented"}
		}, []string{"route azure: responses[0].corrections[0]: holds route azure's notice.account"}},
		{"disclosure provider holding another route's account", func(_ *testing.T, f *publicationFixture) {
			f.p.Disclosures[0].Provider = "AWS (bedrock-account)"
		}, []string{"disclosures[0].provider: holds route bedrock's notice.account"}},
		{"reply with format characters inside the account, the recipient and a limit", func(t *testing.T, f *publicationFixture) {
			f.p.Routes[0].Responses[0].Text = put(t, f.dir, "responses/groq.txt", "account groq\u200b-acc\u2060ount (sent to Groq\u00adsupport); tpm 250\ufeff013,5")
		}, []string{"route groq: responses[0].text: holds route groq's notice.account", "route groq: responses[0].text: holds route groq's notice.recipient",
			"route groq: responses[0].text: holds the value of route groq's confidential tokens_per_minute limit"}},
		{"correction with a decimal comma and dot grouping", func(_ *testing.T, f *publicationFixture) {
			f.p.Routes[1].Responses[0].Corrections = []string{"tpm corrected to 250.013,5"}
		}, []string{"route azure: responses[0].corrections[0]: holds the value of route groq's confidential tokens_per_minute limit"}},
		{"disclosure provider with a soft hyphen in another route's account", func(_ *testing.T, f *publicationFixture) {
			f.p.Disclosures[0].Provider = "AWS (Bedrock\u00ad-Account)"
		}, []string{"disclosures[0].provider: holds route bedrock's notice.account"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newPublicationFixture(t)
			c.edit(t, f)
			err := f.check(t)
			if err == nil {
				t.Fatal("passed")
			}
			if want := "evidence: publication refused: " + strings.Join(c.want, "; "); err.Error() != want {
				t.Errorf("got %v\nwant %s", err, want)
			}
			for _, s := range []string{"groq-account", "support", "Azure-Account", "bedrock-account", "9999", "250,013.5", "250013.5", "250013,5", "013,5"} {
				if strings.Contains(err.Error(), s) {
					t.Errorf("the refusal carries %q", s)
				}
			}
		})
	}
	f := newPublicationFixture(t)
	f.s.Withheld = append(f.s.Withheld, WithheldValue{Route: "groq", Dimension: "requests_per_minute", Value: 9999})
	f.p.Routes[0].Responses[0].Text = put(t, f.dir, "responses/groq.txt", "batch 99990 and 2502013.5 accepted for the groq route")
	f.p.Routes[1].Responses[0].Corrections = []string{"9999.5 tokens, version 1.9999", "9.999,5 tokens from groq\u00a0route support"}
	if err := f.check(t); err != nil {
		t.Errorf("longer numbers, another decimal comma value and the route name: %v", err)
	}
}

// The publication page holds each route's notice and window-close
// dates, a consented response's text, another response's date and
// corrections, the corrections log, the providers told before the
// notices and the fixed lines, and no recipient, account identifier or
// file path; a fence outruns the backticks in a verbatim text; a
// carriage return, alone or before a line feed, prints as a line feed;
// and the page is refused when a verbatim text changed after the check.
func TestPublicationPage(t *testing.T) {
	f := newPublicationFixture(t)
	p := f.load(t)
	if err := p.Check(f.s); err != nil {
		t.Fatal(err)
	}
	pg, err := p.Page()
	if err != nil {
		t.Fatal(err)
	}
	const want = "# Publication record\n\n## Notices\n\n" +
		"Each notice carried its route's evidence pack. The factual-accuracy window closed at the end of the date shown, UTC.\n\n" +
		"| Route | Notice sent | Window closed | Named with written permission |\n| --- | --- | --- | --- |\n" +
		"| azure | 2026-10-27 | 2026-11-02 | yes |\n| bedrock | 2026-10-27 | 2026-11-02 | no |\n| groq | 2026-10-27 | 2026-11-02 | no |\n\n" +
		"## Responses\n\n### azure\n\nReceived 2026-10-30; corrections made: 1, in the corrections log.\n\n" +
		"### bedrock\n\nNo response.\n\n" +
		"### groq\n\nReceived 2026-10-29; published verbatim with the provider's written consent; no corrections made:\n\n```text\nlooks right\n```\n\n" +
		"## Corrections log\n\n- azure, response of 2026-10-30: 2026-10-31: model version spelt as documented\n\n" +
		"## Told of the measurement before the notices\n\n- Groq, by email, 2026-09-15\n- AWS, by form, 2026-10-07\n- OpenAI, by email, 2026-10-07\n\n" +
		"## Naming, accounts and funding\n\n" +
		"Each provider is named by its word mark in plain text. A provider whose terms permit naming only with its written permission is named with that permission, which the publication record keeps.\n\n" +
		"Each route ran through an ordinary paid account with its provider.\n\nNo provider funded the measurement.\n"
	md := pg.Markdown()
	if md != want {
		t.Errorf("markdown:\n%s\nwant:\n%s", md, want)
	}
	js, err := json.Marshal(pg)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{`"route":"groq","notice_sent":"2026-10-27","window_closes":"2026-11-02","named_with_permission":false`,
		`{"received":"2026-10-29","verbatim":true,"text":"looks right","corrections":[]}`,
		`{"received":"2026-10-30","verbatim":false,"corrections":["2026-10-31: model version spelt as documented"]}`,
		`"corrections_log":[{"route":"azure","received":"2026-10-30","correction":"2026-10-31: model version spelt as documented"}]`,
		`{"provider":"Groq","how":"email","date":"2026-09-15"}`, `"statements":["Each provider is named`} {
		if !strings.Contains(string(js), s) {
			t.Errorf("JSON lacks %s:\n%s", s, js)
		}
	}
	for _, s := range []string{"support", "-account", "permissions", "FTU", "benchmarks", "named recipients", "responses/", "packs/", "told/", "naming/", "prerelease"} {
		if strings.Contains(md, s) || strings.Contains(string(js), s) {
			t.Errorf("the page carries %q", s)
		}
	}

	f = newPublicationFixture(t)
	f.p.Routes[0].Responses[0].Text = put(t, f.dir, "responses/groq.txt", "see ```go\nx\n```` here\n")
	p = f.load(t)
	if err := p.Check(f.s); err != nil {
		t.Fatal(err)
	}
	if pg, err = p.Page(); err != nil {
		t.Fatal(err)
	}
	if md := pg.Markdown(); !strings.Contains(md, "\n\n`````text\nsee ```go\nx\n```` here\n`````\n\n") {
		t.Errorf("fence:\n%s", md)
	}
	f = newPublicationFixture(t)
	f.p.Routes[0].Responses[0].Text = put(t, f.dir, "responses/groq.txt", "line one\r\nline two\rline three\r\n")
	p = f.load(t)
	if err := p.Check(f.s); err != nil {
		t.Fatal(err)
	}
	if pg, err = p.Page(); err != nil {
		t.Fatal(err)
	}
	if js, err = json.Marshal(pg); err != nil {
		t.Fatal(err)
	}
	if md := pg.Markdown(); !strings.Contains(md, "\n\n```text\nline one\nline two\nline three\n```\n\n") || strings.Contains(md, "\r") ||
		!strings.Contains(string(js), `"text":"line one\nline two\nline three\n"`) {
		t.Errorf("CRLF text:\n%q\n%s", md, js)
	}
	put(t, f.dir, "responses/groq.txt", "edited")
	if _, err := p.Page(); err == nil || !strings.Contains(err.Error(), "evidence: publication page: route groq: responses[0].text: responses/groq.txt has SHA-256 "+shaOf([]byte("edited"))) {
		t.Errorf("text edited after the check: %v", err)
	}
}

// The record decodes strictly.
func TestLoadPublicationStrict(t *testing.T) {
	dir := t.TempDir()
	for _, c := range []struct{ body, want string }{
		{`{"manifests": [], "notices": []}`, `unknown field "notices"`},
		{`{"manifests": []} {}`, "data after the JSON object"},
		{`{"routes": [{"route": "groq", "notice": {"sent": "2026-10-27", "window": "2026-11-02"}}]}`, `unknown field "window"`},
	} {
		path := filepath.Join(dir, "publication.json")
		if err := os.WriteFile(path, []byte(c.body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadPublication(path); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want %q", c.body, err, c.want)
		}
	}
	if _, err := LoadPublication(filepath.Join(dir, "absent.json")); err == nil {
		t.Error("an absent record loaded")
	}
}
