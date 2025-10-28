// pkg/protocol/common/inflight.go
package common

import (
	"sync"
	"time"
)

// ExpiredRec describes a timed-out ping.
type ExpiredRec struct {
	Nonce uint64
	Addr  string // peer UDP "ip:port" we pinged
}

type Inflight struct {
	mu sync.Mutex
	m  map[uint64]struct {
		dl   time.Time
		addr string
	}
}

func NewInflight() *Inflight {
	return &Inflight{
		m: make(map[uint64]struct {
			dl   time.Time
			addr string
		}),
	}
}

// Add registers a nonce with a deadline and target address.
func (i *Inflight) Add(nonce uint64, ttl time.Duration, addr string) {
	i.mu.Lock()
	i.m[nonce] = struct {
		dl   time.Time
		addr string
	}{dl: time.Now().Add(ttl), addr: addr}
	i.mu.Unlock()
}

// Done removes the nonce (ACK received). Returns true if existed.
func (i *Inflight) Done(nonce uint64) bool {
	i.mu.Lock()
	_, ok := i.m[nonce]
	delete(i.m, nonce)
	i.mu.Unlock()
	return ok
}

// Expired returns expired records and prunes them.
func (i *Inflight) Expired(now time.Time) []ExpiredRec {
	i.mu.Lock()
	defer i.mu.Unlock()
	var out []ExpiredRec
	for n, rec := range i.m {
		if now.After(rec.dl) {
			out = append(out, ExpiredRec{Nonce: n, Addr: rec.addr})
			delete(i.m, n)
		}
	}
	return out
}

// Reset clears all inflight nonces (e.g., when switching modes).
func (i *Inflight) Reset() {
	i.mu.Lock()
	i.m = make(map[uint64]struct {
		dl   time.Time
		addr string
	})
	i.mu.Unlock()
}
