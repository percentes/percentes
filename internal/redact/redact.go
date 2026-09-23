// Package redact keeps credentials and response bytes out of anything
// printed or written. URL strips the parts of an endpoint that carry a
// credential, and ErrorText prints an error in full only when its Go type
// cannot carry response bytes.
package redact

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"time"
)

// unparseable replaces an endpoint that url.Parse rejects.
const unparseable = "<unparseable endpoint>"

// URL returns s without userinfo, query or fragment. A string that does not
// parse is replaced whole.
func URL(s string) string {
	u, err := url.Parse(s)
	if err != nil {
		return unparseable
	}
	u.User = nil
	u.RawQuery = ""
	u.ForceQuery = false
	u.Fragment = ""
	u.RawFragment = ""
	return u.String()
}

// SecretMin is the shortest endpoint value treated as a credential.
const SecretMin = 8

// Secrets returns the endpoint's username, password and query values of at
// least SecretMin bytes, for a replacer.
func Secrets(s string) []string {
	u, err := url.Parse(s)
	if err != nil {
		return nil
	}
	var out []string
	add := func(v string) {
		if len(v) >= SecretMin {
			out = append(out, v)
		}
	}
	add(u.User.Username())
	if p, ok := u.User.Password(); ok {
		add(p)
	}
	for _, vs := range u.Query() {
		for _, v := range vs {
			add(v)
		}
	}
	return out
}

// ErrorText prints err in full only when its type cannot carry response
// bytes; any other error is named by type. timeout is reported for a
// deadline or a timing-out net.Error.
func ErrorText(err error, timeout time.Duration) string {
	var op *net.OpError
	var cert *tls.CertificateVerificationError
	var ne net.Error
	switch {
	case errors.As(err, &op):
		return op.Error()
	case errors.As(err, &cert):
		return cert.Error()
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Sprintf("timed out after %s", timeout)
	case errors.As(err, &ne) && ne.Timeout():
		return fmt.Sprintf("timed out after %s", timeout)
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "connection closed"
	}
	return fmt.Sprintf("%T, text withheld", err)
}

// Wrap returns an error whose text is prefix and the allowlisted text of
// err, and whose cause is err, so errors.Is still sees it.
func Wrap(prefix string, err error, timeout time.Duration) error {
	return &wrapped{cause: err, text: prefix + ": " + ErrorText(err, timeout)}
}

type wrapped struct {
	cause error
	text  string
}

func (w *wrapped) Error() string { return w.text }
func (w *wrapped) Unwrap() error { return w.cause }
