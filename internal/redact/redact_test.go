package redact

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

const secret = "AIzaSyURLSECRET123"

func TestURLStripsCredentialsAndKeepsTheHost(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"https://host/v1/chat/completions", "https://host/v1/chat/completions"},
		{"https://user:" + secret + "@host/v1", "https://host/v1"},
		{"https://host/v1?key=" + secret + "&n=1", "https://host/v1"},
		{"https://host/v1#" + secret, "https://host/v1"},
		{"http://10.0.0.122:8000", "http://10.0.0.122:8000"},
		// url.Parse rejects a bad escape, a space in the host and a control
		// character; the whole string is replaced.
		{"https://user:" + secret + "@host/v1/%zz", unparseable},
		{"https://user:" + secret + "@ho st/v1", unparseable},
		{"https://host/v1?key=" + secret + "\x01", unparseable},
	} {
		if got := URL(c.in); got != c.want {
			t.Errorf("URL(%q) = %q, want %q", c.in, got, c.want)
		}
		if strings.Contains(URL(c.in), secret) {
			t.Errorf("URL(%q) carries the credential", c.in)
		}
	}
}

func TestSecretsHonoursTheFloor(t *testing.T) {
	got := Secrets("https://user:" + secret + "@host/v1?key=" + secret + "x&n=1&api-version=2024-06-01")
	want := map[string]bool{secret: true, secret + "x": true, "2024-06-01": true}
	if len(got) != len(want) {
		t.Fatalf("got %q, want %d values", got, len(want))
	}
	for _, s := range got {
		if !want[s] {
			t.Errorf("unexpected secret %q", s)
		}
	}
	if Secrets("https://user:"+secret+"@host/%zz") != nil {
		t.Error("an unparseable endpoint yields secrets")
	}
}

func TestErrorTextIsAllowlisted(t *testing.T) {
	for _, c := range []struct {
		name    string
		err     error
		want    string
		exclude string
	}{
		{"network operation", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, "dial tcp: connection refused", ""},
		{"deadline", context.DeadlineExceeded, "timed out after 5s", ""},
		{"free text", errors.New("Location: /%zz?key=" + secret), "*errors.errorString, text withheld", secret},
		// A dial failure prints its host; the host is not a credential.
		{"dns error under a dial", &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", Name: "h.invalid"}}, "dial tcp: lookup h.invalid: no such host", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := ErrorText(c.err, 5*time.Second)
			if got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
			if c.exclude != "" && strings.Contains(got, c.exclude) {
				t.Errorf("text carries withheld bytes: %q", got)
			}
		})
	}
}

func TestWrapKeepsTheCauseAndHidesTheText(t *testing.T) {
	cause := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	err := Wrap("scrape", cause, time.Second)
	if err.Error() != "scrape: dial tcp: connection refused" {
		t.Errorf("text %q", err)
	}
	if !errors.Is(err, cause) {
		t.Error("cause is not reachable through errors.Is")
	}
	leaky := Wrap("scrape", errors.New("Get \"http://h/?token="+secret+"\": refused"), time.Second)
	if strings.Contains(leaky.Error(), secret) {
		t.Errorf("wrapped text carries the credential: %q", leaky)
	}
	if !errors.Is(Wrap("x", context.DeadlineExceeded, time.Second), context.DeadlineExceeded) {
		t.Error("deadline cause lost")
	}
}
