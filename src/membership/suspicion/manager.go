package suspicion

import (
	"hydfs-g33/membership/protocol/common"
	"hydfs-g33/membership/store"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Manager struct {
	mu             sync.RWMutex // protects TSuspect, TFail, TCleanup
	St             *store.Store
	TSuspect       time.Duration    // time since last Alive before we mark Suspect
	TFail          time.Duration    // time since Suspect before we mark Failed
	TCleanup       time.Duration    // time since Failed/Left before we delete
	UsePingTimeout bool             // only handle inflight expirations in PingAck mode
	Inflight       *common.Inflight // shared inflight (can be nil)
	stop           chan struct{}

	failMarksThisMinute int64 // per-minute fail mark counter
}

func (m *Manager) Run() {
	m.stop = make(chan struct{})

	tickerInterval := 100 * time.Millisecond

	// m.mu.RLock()
	ticker := time.NewTicker(tickerInterval) // based on initial config
	// m.mu.RUnlock()
	defer ticker.Stop()

	rateTick := time.NewTicker(time.Minute) // once/minute meter
	defer rateTick.Stop()

	for {
		//log.Printf("Scanner: %s", time.Now().String())
		select {
		case <-m.stop:
			return
		case <-ticker.C:
			now := time.Now()

			// Snapshot values under read lock
			m.mu.RLock()
			suspect := m.TSuspect
			fail := m.TFail
			cleanup := m.TCleanup
			useTO := m.UsePingTimeout
			m.mu.RUnlock()

			m.scanOnce(now, suspect, fail, cleanup)

			// also handle inflight ping timeouts (only in PingAck)
			if useTO && m.Inflight != nil {
				m.reapInflight(now)
			}

		case <-rateTick.C: // report and reset
			// n := atomic.SwapInt64(&m.failMarksThisMinute, 0)
			// log.Printf("[suspicion] fail_marks=%d /min", n)
		}
	}
}

func (m *Manager) Close() { close(m.stop) }

// scanOnce now takes the config values explicitly, so we don’t race on fields
func (m *Manager) scanOnce(now time.Time, tSuspect, tFail, tCleanup time.Duration) {
	entries, selfID := m.St.Snapshot() // deep copy; safe to iterate without locks

	// self state becomes Left, skip processing
	for _, e := range entries {
		if e.ID == selfID && e.State == store.StateLeft {
			age := now.Sub(e.LastUpdate)
			if age >= tCleanup {
				//m.St.RemoveEntry(e.ID)
				os.Exit(0) // self left; exit immediately
			}
			return
		}
	}

	// Iterate over all entries and update states as necessary

	for _, e := range entries {
		age := now.Sub(e.LastUpdate)

		if (e.State == store.StateLeft) && (age >= tCleanup) {
			m.St.RemoveEntry(e.ID)
			continue
		}

		if e.ID == m.St.SelfID() {
			continue // skip self
		}

		if tSuspect != 0 {
			// Suspicion enabled
			// Checking states as well so that we don't repeatedly set the same state again and again
			if age >= tSuspect+tFail+tCleanup {
				m.St.RemoveEntry(e.ID)
			} else if (age >= tSuspect+tFail) && (e.State == store.StateAlive || e.State == store.StateSuspect) {
				m.St.ApplyLocked(e.ID, e.Incarnation, store.StateFailed)
				atomic.AddInt64(&m.failMarksThisMinute, 1) // count transition to Failed
			} else if (age >= tSuspect) && (e.State == store.StateAlive) {
				m.St.ApplyLocked(e.ID, e.Incarnation, store.StateSuspect)
			}
		} else {
			// Suspicion disabled
			if age >= tFail+tCleanup {
				m.St.RemoveEntry(e.ID)
			} else if (age >= tFail) && (e.State != store.StateFailed) {
				m.St.ApplyLocked(e.ID, e.Incarnation, store.StateFailed)
				atomic.AddInt64(&m.failMarksThisMinute, 1) // count transition to Failed
			}
		}
	}
}

// reapInflight marks peers Suspect when their ping nonce expired (no ACK).
func (m *Manager) reapInflight(now time.Time) {
	exp := m.Inflight.Expired(now)
	if len(exp) == 0 {
		return
	}

	// Snapshot whether suspicion is disabled
	m.mu.RLock()
	suspicionDisabled := (m.TSuspect == 0)
	m.mu.RUnlock()

	// We have target addresses; find matching entries and mark Suspect.
	entries, _ := m.St.Snapshot()
	for _, rec := range exp {
		for _, e := range entries {
			if e.ID.EP.EndpointToString() == rec.Addr {
				if suspicionDisabled {
					m.St.ApplyLocked(e.ID, e.Incarnation, store.StateFailed)
					atomic.AddInt64(&m.failMarksThisMinute, 1) // count transition to Failed
				} else {
					m.St.ApplyLocked(e.ID, e.Incarnation, store.StateSuspect)
				}
				break
			}
		}
	}
}

// Reconfigure allows updating thresholds safely
func (m *Manager) Reconfigure(suspect, fail, cleanup time.Duration, usePingTimeout bool) {
	m.mu.Lock()
	m.TSuspect = suspect
	m.TFail = fail
	m.TCleanup = cleanup
	m.UsePingTimeout = usePingTimeout
	m.mu.Unlock()
}
