package admission

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// A ceiling check is admitted with its account group on the line. While
// one is unsettled no other check on the group, or of the route, is
// admitted and no line is written; checks on another group are, and
// once the check settles the group admits the next. A non-probe
// admission that names an account group does not parse.
func TestAdmitCheck(t *testing.T) {
	l, path := releaseLedger(t, routeConfig(t, "azure", "10", "1300"), routeConfig(t, "azure-R", "10", "1300"), routeConfig(t, "gemini", "10", "1300"))
	for _, r := range []string{"azure", "azure-R", "gemini"} {
		must(t, l.RecordPrice(r, shapePrice(groqPrice, 1024)))
	}
	a, err := l.AdmitCheck("azure", "azure", 10, at1024)
	if err != nil {
		t.Fatal(err)
	}
	if a.Group != "azure" || a.Purpose != PurposeProbe {
		t.Fatalf("%+v", a)
	}
	st, err := l.State()
	if err != nil {
		t.Fatal(err)
	}
	lines := st.Lines
	for _, c := range []struct{ route, group string }{{"azure-R", "azure"}, {"azure", "azure"}} {
		if _, err := l.AdmitCheck(c.route, c.group, 10, at1024); err == nil || !strings.Contains(err.Error(), "holds an unsettled ceiling check of route azure on account group azure") {
			t.Fatalf("%s on %s while azure's check is unsettled: %v", c.route, c.group, err)
		}
	}
	if st, err = l.State(); err != nil || st.Lines != lines {
		t.Fatalf("a refused check wrote a line: %d, want %d (%v)", st.Lines, lines, err)
	}
	g, err := l.AdmitCheck("gemini", "gemini", 10, at1024)
	if err != nil {
		t.Fatalf("another group's check: %v", err)
	}
	must(t, l.Settle(a, Counts{Probe: 10}))
	r, err := l.AdmitCheck("azure-R", "azure", 10, at1024)
	if err != nil {
		t.Fatalf("after the settlement: %v", err)
	}
	must(t, l.Settle(r, Counts{Probe: 3}))
	must(t, l.Settle(g, Counts{Probe: 10}))
	if _, err := l.AdmitCheck("azure", "", 10, at1024); err == nil {
		t.Fatal("a check with no account group")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(b), `"account_group":"azure"`); n != 2 {
		t.Fatalf("%d admit lines name account group azure:\n%s", n, b)
	}
	bad := strings.Replace(string(b), `"purpose":"probe","account_group":"gemini"`, `"purpose":"development","account_group":"gemini"`, 1)
	if bad == string(b) {
		t.Fatalf("no gemini check line:\n%s", b)
	}
	if _, err := Parse([]byte(bad)); err == nil || !strings.Contains(err.Error(), "names account group") {
		t.Fatalf("a development admission naming a group: %v", err)
	}
}

// A ceiling check's admit line records its ceiling and byte cap, which
// parse back; a line without them, as AdmitCheck writes it, parses too. A
// check needs a positive ceiling, and a line naming a ceiling on another
// purpose does not parse.
func TestAdmitCeilingCheckLine(t *testing.T) {
	l, path := releaseLedger(t, routeConfig(t, "azure", "10", "1300"), routeConfig(t, "gemini", "10", "1300"))
	for _, r := range []string{"azure", "gemini"} {
		must(t, l.RecordPrice(r, shapePrice(groqPrice, 1024)))
	}
	if _, err := l.AdmitCeilingCheck("azure", "azure", 10, at1024, 0, 2000); err == nil {
		t.Fatal("a check at ceiling 0")
	}
	a, err := l.AdmitCeilingCheck("azure", "azure", 10, at1024, 512, 2000)
	if err != nil {
		t.Fatal(err)
	}
	must(t, l.Settle(a, Counts{Probe: 10}))
	old, err := l.AdmitCheck("gemini", "gemini", 10, at1024)
	if err != nil {
		t.Fatal(err)
	}
	must(t, l.Settle(old, Counts{Probe: 10}))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Parse(b); err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(b), []byte("\n"))
	var e, o Entry
	if err := json.Unmarshal(lines[a.Seq-1], &e); err != nil || e.CheckCeiling != 512 || e.ByteCap != 2000 {
		t.Fatalf("admit line %+v, %v", e, err)
	}
	if err := json.Unmarshal(lines[old.Seq-1], &o); err != nil || o.CheckCeiling != 0 || o.ByteCap != 0 || bytes.Contains(lines[old.Seq-1], []byte("check_ceiling")) {
		t.Fatalf("line without a ceiling %s, %v", lines[old.Seq-1], err)
	}
	bad := strings.Replace(string(b), `"purpose":"probe","account_group":"azure"`, `"purpose":"development","account_group":""`, 1)
	if bad == string(b) {
		t.Fatalf("no azure check line:\n%s", b)
	}
	if _, err := Parse([]byte(bad)); err == nil || !strings.Contains(err.Error(), "a development admission names ceiling 512 and byte cap 2000") {
		t.Fatalf("a development admission naming a ceiling: %v", err)
	}
}
