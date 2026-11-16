package main

import (
	"context"
	"strings"
	"testing"

	"github.com/percentes/percentes/internal/probe"
)

// A ceiling check of a route refuses to send while another holds the
// route, and goes ahead once the first ends.
func TestCeilingCheckOneOwner(t *testing.T) {
	first := make(chan struct{})
	release := make(chan struct{})
	e := newEndpoint(t, func(n int64) (int, string) {
		if n == 1 {
			close(first)
			<-release
		}
		return always200(n)
	})
	r, _ := newTestRoute(t, releaseConfig(t, e.url()), "+0.005")
	check := func() (*probe.ReleaseRecord, error) {
		return sendCeiling(context.Background(), r.ledger, r.cfg, r.shape, "", 2000, r.key, 2000, nil, nil)
	}
	done := make(chan error, 1)
	go func() { _, err := check(); done <- err }()
	<-first
	second, err := check()
	hits := e.hits.Load()
	close(release)
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if err == nil || second != nil || hits != 1 || !strings.Contains(err.Error(), "another process holds it") {
		t.Fatalf("overlapping check: record %t, %v; %d requests before the first was released", second != nil, err, hits)
	}
	if _, err := check(); err != nil {
		t.Fatalf("check after the first: %v", err)
	}
}
