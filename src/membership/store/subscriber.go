package store

import (
	"context"
	"time"
)

// ChangeEvent is sent whenever membership makes an effective change.
type ChangeEvent struct {
	Version uint64    // membership version after the change
	When    time.Time // emission time on this node
	Reason  string
}

type watcher struct {
	id int
	ch chan ChangeEvent // buffered, non-blocking fanout
}

// Subscribe registers a subscriber and returns a buffered, receive-only channel,
// plus a cancel func to stop receiving events. The optional ctx auto-cancels.
func (s *Store) Subscribe(ctx context.Context, buf int) (<-chan ChangeEvent, func()) {
	if buf <= 0 {
		buf = 16
	}
	c := make(chan ChangeEvent, buf)

	s.wmu.Lock()
	id := s.nextWatch
	s.nextWatch++
	s.watchers[id] = watcher{id: id, ch: c}
	s.wmu.Unlock()

	// bootstrap: send current version immediately (non-blocking)
	ver := s.Version()
	select {
	case c <- ChangeEvent{Version: ver, When: time.Now(), Reason: "bootstrap"}:
	default:
	}

	cancel := func() {
		s.wmu.Lock()
		if w, ok := s.watchers[id]; ok {
			delete(s.watchers, id)
			close(w.ch)
		}
		s.wmu.Unlock()
	}
	if ctx != nil {
		go func() {
			<-ctx.Done()
			cancel()
		}()
	}
	return c, cancel
}

// notifyLocked emits a change event to all watchers. Call while holding s.mu or
// immediately after a change; it takes care of watcher lock and non-blocking sends.
func (s *Store) notifyLocked(reason string) {
	ev := ChangeEvent{Version: s.version, When: time.Now(), Reason: reason}
	s.wmu.Lock()
	for _, w := range s.watchers {
		select {
		case w.ch <- ev:
		default:
			// slow subscriber; drop to avoid blocking membership path
		}
	}
	s.wmu.Unlock()
}
