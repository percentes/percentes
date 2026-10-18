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
// azure needing its naming permission and bedrock its replication pack,
// with every file the record names written under dir and the pages
// under the state's PagesDir.
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
		Manifests: map[string][]byte{"azure/c01-a1/manifest.json": []byte("m-azure"), "bedrock/c01-a1/manifest.json": []byte("m-bedrock"), "groq/c01-a1/manifest.json": []byte("m-groq")},
		Pages:     map[string][]byte{"index.md": []byte("index"), "routes/groq.txt": []byte("groq page")},
		PagesDir:  t.TempDir(),
		Routes:    []RouteDuties{{Name: "groq"}, {Name: "azure", Permission: true}, {Name: "bedrock", ReplicationPack: true}},
		Now:       time.Date(2026, 11, 3, 9, 0, 0, 0, time.UTC),
	}
	for name, b := range f.s.Pages {
		put(t, f.s.PagesDir, name, string(b))
		f.p.Pages = append(f.p.Pages, PublishedFile{Path: name, SHA256: shaOf(b)})
	}
	for name, b := range f.s.Manifests {
		f.p.Manifests = append(f.p.Manifests, PublishedFile{Path: name, SHA256: shaOf(b)})
	}
	notice := func(route string) *Notice {
		return &Notice{Sent: "2026-10-27", Recipient: route + " support", Account: route + "-account", Pack: put(t, f.dir, "packs/"+route+".tar", route+" pack"), WindowCloses: "2026-11-02"}
	}
	f.p.Routes = []RoutePublication{
		{Route: "groq", Notice: notice("groq"), Responses: []Response{
			{Received: "2026-10-29", Verbatim: true, Text: put(t, f.dir, "responses/groq.txt", "looks right"), Consent: put(t, f.dir, "responses/groq-consent.eml", "yes, verbatim")},
		}},
		{Route: "azure", Notice: notice("azure"), Responses: []Response{{Received: "2026-10-30", Corrections: []string{"2026-10-31: model version spelt as documented"}}},
			Permission: put(t, f.dir, "naming/openai-permission.eml", "permission")},
		{Route: "bedrock", Notice: notice("bedrock"), Responses: []Response{},
			ReplicationPack: &Send{Sent: "2026-10-27", Recipient: "aws benchmarks", Pack: put(t, f.dir, "packs/aws-replication.tar", "replication")}},
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

// check writes the record as JSON, loads it and checks it.
func (f *publicationFixture) check(t *testing.T) error {
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
	return p.Check(f.s)
}

// A complete record passes, and lists the providers told before the
// notices in date order.
func TestPublicationComplete(t *testing.T) {
	f := newPublicationFixture(t)
	if err := f.check(t); err != nil {
		t.Fatal(err)
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
		{"no disclosures", func(_ *testing.T, f *publicationFixture) { f.p.Disclosures = nil }, "disclosures: required"},
		{"disclosure after the notices", func(_ *testing.T, f *publicationFixture) { f.p.Disclosures[0].Date = "2026-10-27" },
			"disclosures[0].date: 2026-10-27, not before the first notice of 2026-10-27"},
		{"disclosure how", func(_ *testing.T, f *publicationFixture) { f.p.Disclosures[0].How = "phone" }, `disclosures[0].how: "phone", want email or form`},
		{"disclosure recipient", func(_ *testing.T, f *publicationFixture) { f.p.Disclosures[0].Recipient = "" }, "disclosures[0].recipient: required"},
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
