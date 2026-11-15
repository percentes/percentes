package mock

import (
	"sync"
	"time"
)

// rateLimit refuses a request that arrives when requests or more were
// admitted in the window before it, a sliding log of admissions.
type rateLimit struct {
	mu       sync.Mutex
	requests int
	window   time.Duration
	admitted []time.Time
}

// SetRateLimit makes the server refuse with 429 each inference request
// that arrives when requests or more were admitted in the window before
// it; requests 0 removes the limit. Refused requests do not count
// toward it.
func (s *Server) SetRateLimit(requests int, window time.Duration) {
	s.limit.mu.Lock()
	defer s.limit.mu.Unlock()
	s.limit.requests, s.limit.window, s.limit.admitted = requests, window, nil
}

// admit reports whether a request arriving at now is admitted.
func (l *rateLimit) admit(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.requests <= 0 {
		return true
	}
	i := 0
	for i < len(l.admitted) && !l.admitted[i].After(now.Add(-l.window)) {
		i++
	}
	l.admitted = l.admitted[i:]
	if len(l.admitted) >= l.requests {
		return false
	}
	l.admitted = append(l.admitted, now)
	return true
}
