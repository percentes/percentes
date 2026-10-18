package score

import "testing"

var labels = []string{"billing", "technical", "account", "escalation"}

func contract(t *testing.T) *Contract {
	t.Helper()
	c, err := NewContract(labels)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestParseAcceptsTheContract(t *testing.T) {
	c := contract(t)
	for _, c2 := range []struct{ in, want string }{
		{`{"route":"billing"}`, "billing"},
		{`{"route":"escalation"}`, "escalation"},
		{"  \n\t{ \"route\" : \"technical\" }\r\n ", "technical"},
		{`{"r\u006fute":"acc\u006funt"}`, "account"},
	} {
		got := c.Parse(c2.in)
		if !got.OK() || got.Route != c2.want {
			t.Errorf("Parse(%q) = %+v, want route %q", c2.in, got, c2.want)
		}
	}
}

func TestParseNamesEachFailure(t *testing.T) {
	c := contract(t)
	for _, c2 := range []struct {
		in   string
		want Reason
	}{
		{"{\"route\":\"billing\xff\"}", InvalidUTF8},
		{"", Empty},
		{" \n\t\r", Empty},
		{"```json\n{\"route\":\"billing\"}\n```", CodeFence},
		{"```\n{\"route\":\"billing\"}\n```", CodeFence},
		{`Route: {"route":"billing"}`, NotObject},
		{`billing`, NotObject},
		{`"billing"`, NotObject},
		{`[{"route":"billing"}]`, NotObject},
		{"\u00a0{\"route\":\"billing\"}", NotObject},
		{"\ufeff{\"route\":\"billing\"}", NotObject},
		{`{"route":"billing"`, InvalidJSON},
		{`{"route":"billing",}`, InvalidJSON},
		{`{"route" "billing"}`, InvalidJSON},
		{`{"route":"billing" "x":1}`, InvalidJSON},
		{`{route:"billing"}`, InvalidJSON},
		{`{'route':'billing'}`, InvalidJSON},
		{`{"route":"billing","x":[1,}`, InvalidJSON},
		{`{"route":"billing"} thanks`, TrailingText},
		{"{\"route\":\"billing\"}\n```", TrailingText},
		{`{"route":"billing"}{"route":"billing"}`, TrailingText},
		{`{"route":"billing"},`, TrailingText},
		{`{"route":"billing","route":"escalation"}`, DuplicateKey},
		{`{"route":"billing","route":"billing"}`, DuplicateKey},
		{`{"route":"billing","r\u006fute":"billing"}`, DuplicateKey},
		{`{"route":"nope","route":"billing"}`, DuplicateKey},
		{`{"route":"billing","reason":"invoice"}`, ExtraKey},
		{`{"reason":"invoice","route":"billing"}`, ExtraKey},
		{`{"Route":"billing"}`, ExtraKey},
		{`{"x":1,"x":2,"route":"billing"}`, DuplicateKey},
		{`{}`, MissingRoute},
		{" { } ", MissingRoute},
		{`{"route":null}`, WrongType},
		{`{"route":1}`, WrongType},
		{`{"route":true}`, WrongType},
		{`{"route":["billing"]}`, WrongType},
		{`{"route":{"label":"billing"}}`, WrongType},
		{`{"route":"Billing"}`, UnknownLabel},
		{`{"route":" billing"}`, UnknownLabel},
		{`{"route":""}`, UnknownLabel},
		{`{"route":"refund"}`, UnknownLabel},
		{`{"route":"\ud800"}`, UnknownLabel},
	} {
		got := c.Parse(c2.in)
		if got.OK() || got.Reason != c2.want || got.Route != "" {
			t.Errorf("Parse(%q) = %+v, want reason %q", c2.in, got, c2.want)
		}
	}
}

func TestNewContractRefusesBadLabels(t *testing.T) {
	for _, in := range [][]string{
		nil,
		{},
		{"billing", ""},
		{"billing", "billing"},
		{"billing", "bad\xff"},
	} {
		if _, err := NewContract(in); err == nil {
			t.Errorf("NewContract(%q) accepted", in)
		}
	}
}

func TestParseUsesTheConfiguredLabels(t *testing.T) {
	c, err := NewContract([]string{"refund"})
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Parse(`{"route":"refund"}`); !got.OK() || got.Route != "refund" {
		t.Errorf("configured label: got %+v", got)
	}
	if got := c.Parse(`{"route":"billing"}`); got.Reason != UnknownLabel {
		t.Errorf("unconfigured label: got %+v, want %q", got, UnknownLabel)
	}
}
