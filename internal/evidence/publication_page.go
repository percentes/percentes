package evidence

import (
	"fmt"
	"slices"
	"strings"
)

// PublicationStatements are the publication page's fixed lines
// (SPEC-HOSTED §9): the naming rules, the accounts' paid status and the
// absence of provider funding.
var PublicationStatements = []string{
	"Each provider is named by its word mark in plain text. A provider whose terms permit naming only with its written permission is named with that permission, which the publication record keeps.",
	"Each route ran through an ordinary paid account with its provider.",
	"No provider funded the measurement.",
}

// PublicationPage is what the release publishes from its publication
// record (SPEC-HOSTED §9): each route's notice and window dates and its
// responses, the corrections log, the providers told of the measurement
// before the notices and PublicationStatements. Recipients, account
// identifiers and the files the record keeps are left out, except the
// text of a response published verbatim with the provider's consent.
type PublicationPage struct {
	Routes         []PageRoute      `json:"routes"`
	CorrectionsLog []PageCorrection `json:"corrections_log"`
	ToldBefore     []PageDisclosure `json:"told_before"`
	Statements     []string         `json:"statements"`
}

// PageRoute is one route's notice and its responses in date order.
// NamedWithPermission marks a route whose record carries the provider's
// naming permission.
type PageRoute struct {
	Route               string         `json:"route"`
	NoticeSent          string         `json:"notice_sent"`
	WindowCloses        string         `json:"window_closes"`
	NamedWithPermission bool           `json:"named_with_permission"`
	Responses           []PageResponse `json:"responses"`
}

// PageResponse is a response's date and corrections, with its text when
// it publishes verbatim.
type PageResponse struct {
	Received    string   `json:"received"`
	Verbatim    bool     `json:"verbatim"`
	Text        string   `json:"text,omitempty"`
	Corrections []string `json:"corrections"`
}

// PageCorrection is one correction, by route and the date of the
// response that led to it.
type PageCorrection struct {
	Route      string `json:"route"`
	Received   string `json:"received"`
	Correction string `json:"correction"`
}

// PageDisclosure is an email or form that described the work to a
// provider before the notices.
type PageDisclosure struct {
	Provider string `json:"provider"`
	How      string `json:"how"`
	Date     string `json:"date"`
}

func (d PageDisclosure) String() string {
	return fmt.Sprintf("%s, by %s, %s", d.Provider, d.How, d.Date)
}

// Page builds the publication page from a record that passed Check:
// routes in name order, each route's responses by date received, and
// the corrections log in the same order. It reads each verbatim
// response's text again and refuses one that no longer has its recorded
// SHA-256.
func (p *Publication) Page() (*PublicationPage, error) {
	var problems []string
	fail := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }
	pg := &PublicationPage{Routes: []PageRoute{}, CorrectionsLog: []PageCorrection{}, ToldBefore: p.toldBefore(),
		Statements: slices.Clone(PublicationStatements)}
	routes := slices.Clone(p.Routes)
	slices.SortStableFunc(routes, func(a, b RoutePublication) int { return strings.Compare(a.Route, b.Route) })
	for _, r := range routes {
		if r.Notice == nil {
			return nil, fmt.Errorf("evidence: publication page: route %s has no notice", r.Route)
		}
		pr := PageRoute{Route: r.Route, NoticeSent: r.Notice.Sent, WindowCloses: r.Notice.WindowCloses,
			NamedWithPermission: r.Permission != nil, Responses: []PageResponse{}}
		resps := slices.Clone(r.Responses)
		slices.SortStableFunc(resps, func(a, b Response) int { return strings.Compare(a.Received, b.Received) })
		for j, resp := range resps {
			out := PageResponse{Received: resp.Received, Verbatim: resp.Verbatim, Corrections: []string{}}
			if resp.Verbatim {
				b := p.file(fmt.Sprintf("route %s: responses[%d].text", r.Route, j), resp.Text, fail)
				out.Text = string(b)
			}
			out.Corrections = append(out.Corrections, resp.Corrections...)
			for _, c := range resp.Corrections {
				pg.CorrectionsLog = append(pg.CorrectionsLog, PageCorrection{Route: r.Route, Received: resp.Received, Correction: c})
			}
			pr.Responses = append(pr.Responses, out)
		}
		pg.Routes = append(pg.Routes, pr)
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("evidence: publication page: %s", strings.Join(problems, "; "))
	}
	return pg, nil
}

// Markdown renders the page, one paragraph per block. A verbatim
// response prints in a fenced block longer than any run of backticks in
// its text.
func (pg *PublicationPage) Markdown() string {
	var paras []string
	add := func(format string, a ...any) { paras = append(paras, fmt.Sprintf(format, a...)) }
	add("# Publication record")
	add("## Notices")
	add("Each notice carried its route's evidence pack. The factual-accuracy window closed at the end of the date shown, UTC.")
	table := "| Route | Notice sent | Window closed | Named with written permission |\n| --- | --- | --- | --- |"
	for _, r := range pg.Routes {
		named := "no"
		if r.NamedWithPermission {
			named = "yes"
		}
		table += fmt.Sprintf("\n| %s | %s | %s | %s |", r.Route, r.NoticeSent, r.WindowCloses, named)
	}
	add("%s", table)
	add("## Responses")
	for _, r := range pg.Routes {
		add("### %s", r.Route)
		if len(r.Responses) == 0 {
			add("No response.")
		}
		for _, resp := range r.Responses {
			corrections := "no corrections made"
			if n := len(resp.Corrections); n > 0 {
				corrections = fmt.Sprintf("corrections made: %d, in the corrections log", n)
			}
			if !resp.Verbatim {
				add("Received %s; %s.", resp.Received, corrections)
				continue
			}
			add("Received %s; published verbatim with the provider's written consent; %s:", resp.Received, corrections)
			fence := "```"
			for strings.Contains(resp.Text, fence) {
				fence += "`"
			}
			add("%stext\n%s\n%s", fence, strings.TrimSuffix(resp.Text, "\n"), fence)
		}
	}
	add("## Corrections log")
	if len(pg.CorrectionsLog) == 0 {
		add("No corrections.")
	}
	var log []string
	for _, c := range pg.CorrectionsLog {
		log = append(log, fmt.Sprintf("- %s, response of %s: %s", c.Route, c.Received, c.Correction))
	}
	if len(log) > 0 {
		add("%s", strings.Join(log, "\n"))
	}
	add("## Told of the measurement before the notices")
	if len(pg.ToldBefore) == 0 {
		add("No provider was told before the notices.")
	}
	var told []string
	for _, d := range pg.ToldBefore {
		told = append(told, "- "+d.String())
	}
	if len(told) > 0 {
		add("%s", strings.Join(told, "\n"))
	}
	add("## Naming, accounts and funding")
	paras = append(paras, pg.Statements...)
	return strings.Join(paras, "\n\n") + "\n"
}
