// pkg/store/membership.go
package store

import (
	"fmt"
	"hydfs-g33/config"
	nodeid "hydfs-g33/membership/node"
	colors "hydfs-g33/membership/utils"
	generic_utils "hydfs-g33/utils"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// State models liveness state for a given NodeID *within a boot*.
type State uint8

const (
	StateUnknown State = iota
	StateAlive   State = 1
	StateSuspect State = 2
	StateFailed  State = 3
	StateLeft    State = 4
)

func (s State) StateToString() string {
	switch s {
	case StateAlive:
		return "Alive"
	case StateSuspect:
		return "Suspect"
	case StateFailed:
		return "Failed"
	case StateLeft:
		return "Left"
	default:
		return "Unknown"
	}
}

// MemberEntry represents one row in the full membership list.
// Incarnation is the SWIM-style epoch within this boot (NOT in NodeID).
type MemberEntry struct {
	ID          nodeid.NodeID
	Incarnation uint64
	State       State
	LastUpdate  time.Time // Pseudo-heartbeat
}

// Store owns the full membership list and conflict resolution.
type Store struct {
	mu      sync.RWMutex
	entries map[string]*MemberEntry // key = ID.EP.EndpointToString() -> Only storing Endpoint as key without Timestamp
	version uint64                  // monotonically increases on any effective change
	selfID  nodeid.NodeID           // Node ID of the local node
	config  config.Config

	wmu       sync.Mutex
	watchers  map[int]watcher
	nextWatch int
	closed    atomic.Bool
}

// New creates an empty Store with the given self node id.
// Seed the table with self as Alive(inc=0) if you like.
func New(self nodeid.NodeID, cfg config.Config) *Store {
	selfEntry := &MemberEntry{
		ID:          self,
		Incarnation: 100,
		State:       StateAlive,
		LastUpdate:  time.Now(),
	}

	store := &Store{
		entries:  make(map[string]*MemberEntry),
		version:  1,
		selfID:   self,
		config:   cfg,
		watchers: make(map[int]watcher),
	}

	k := self.EP.EndpointToString()
	store.entries[k] = selfEntry

	log.Printf("Store initialized with self entry: %+v\n", *selfEntry)

	return store
}

// --------------------------- Queries ---------------------------

// Snapshot returns a deep copy of the full membership list and the current version.
// Use this to send the entire list over the wire.
func (s *Store) Snapshot() (list []MemberEntry, selfID nodeid.NodeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entryMap := s.entries
	list = make([]MemberEntry, 0, len(entryMap))
	for _, entry := range entryMap { // entry is a pointer
		if entry.ID == s.selfID && entry.State != StateLeft {
			// Set last uptime for self entry to time.Now() before sending out membership list
			entry.LastUpdate = time.Now()
		}
		list = append(list, *entry) // dereferencing pointer to copy value
	}
	return list, s.selfID
}

func (s *Store) SnapshotWithoutLock() (list []MemberEntry, selfID nodeid.NodeID) {
	entryMap := s.entries
	list = make([]MemberEntry, 0, len(entryMap))
	for _, entry := range entryMap { // entry is a pointer
		if entry.ID == s.selfID && entry.State != StateLeft {
			// Set last uptime for self entry to time.Now() before sending out membership list
			entry.LastUpdate = time.Now()
		}
		list = append(list, *entry) // dereferencing pointer to copy value
	}
	return list, s.selfID
}

func (s *Store) SelfID() nodeid.NodeID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.selfID
}

// Size returns number of entries.
func (s *Store) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	log.Print(("Size: "), len(s.entries), "\n")
	return len(s.entries)
}

// Version returns a monotonically increasing counter incremented on effective changes.
// Peers can compare versions to decide whether to re-sync (optional optimization).
func (s *Store) Version() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	log.Print(("Version: "), s.version, "\n")
	return s.version
}

// Get returns a copy of the entry for id if present.
func (s *Store) Get(id nodeid.NodeID) (MemberEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	lookupKey := id.EP.EndpointToString()
	if entry, ok := s.entries[lookupKey]; ok {
		log.Print(("Get: "), *entry, "\n")
		return *entry, true // dereference pointer to return value copy
	} else {
		log.Print(("Get: not found\n"))
	}
	return MemberEntry{}, false
}

// -------------------- Idempotent Apply APIs --------------------
//
// Conflict resolution policy (within the same NodeID / boot):
//   1) Higher Incarnation wins.
//   2) For equal Incarnation: Alive < Suspect < Failed < Left. (Left is terminal.)
// Across different NodeIDs (same endpoint, different boots):
//   - Prefer newer boot (new boot eclipses old).
//   - Older boot entry should be removed or tombstoned.
//
// All Apply* must be idempotent. If a call makes an effective change, bump s.version.

func (s *Store) apply(incomingNodeID nodeid.NodeID, incomingIncNumber uint64, state State, updateVersion bool) {
	lookupKey := incomingNodeID.EP.EndpointToString()
	now := time.Now()
	if entry, ok := s.entries[lookupKey]; ok {
		if state == StateAlive || state == StateLeft {
			// Change last update time if state is alive since last update time is now a pesudo-heartbeat
			entry.LastUpdate = now
		}
		entry.State = state
		entry.Incarnation = incomingIncNumber
		if updateVersion {
			s.version++
			switch state {
			case StateSuspect:
				log.Printf(colors.Orange+"Apply: %s updated existing entry: %+v\n"+colors.Reset, state.StateToString(), *entry)
			case StateFailed:
				log.Printf(colors.Red+"Apply: %s updated existing entry: %+v\n"+colors.Reset, state.StateToString(), *entry)
			case StateLeft:
				// Update ring when a member voluntarily leaves
				// list, _ := s.SnapshotWithoutLock()
				// ring.UpdateRingFromMembership(list)
				s.notifyLocked("Member Left: updating ring")

				log.Printf(colors.Cyan+"Apply: %s updated existing entry: %+v\n"+colors.Reset, state.StateToString(), *entry)
			default:
				log.Printf(colors.Underline+"Apply: %s updated existing entry: %+v\n"+colors.Reset, state.StateToString(), *entry)
			}
		}
	} else {
		log.Print("Entry not found \n")
	}
}

func (s *Store) ApplyLocked(incomingNodeID nodeid.NodeID, incomingIncNumber uint64, state State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.apply(incomingNodeID, incomingIncNumber, state, true)
	s.PrintSnapshot()
}

// Delete removes an entry (used after suspicion/leave grace period).
func (s *Store) RemoveEntry(incomingNodeID nodeid.NodeID) {
	lookupKey := incomingNodeID.EP.EndpointToString()
	if entry, ok := s.entries[lookupKey]; ok {
		delete(s.entries, lookupKey)
		s.version++
		log.Print(colors.DarkRed+("Entry deleted: "), *entry, "\n"+colors.Reset)
		s.PrintSnapshot()

		// Update ring on member removal
		// list, _ := s.SnapshotWithoutLock()
		// ring.UpdateRingFromMembership(list)
		s.notifyLocked("Member Removed: updating ring")
	} else {
		log.Print("Entry not found \n")
	}
}

// NormalizeBoot resolves cases where two NodeIDs for the same endpoint exist.
// Keep the newer boot and drop the older.
func (s *Store) normalizeBoot(newMemberEntry MemberEntry) {
	//Since lookupkey for both entries will be same, we don't need to delete previous entry, instead just replace it with new entry
	lookupKey := newMemberEntry.ID.EP.EndpointToString()
	newMemberEntry.LastUpdate = time.Now()
	s.entries[lookupKey] = &newMemberEntry
	s.version++
	log.Print("Boot of incoming entry node is newer, replacing old entry with the new one \n")
}

// ----------------------- Self operations -----------------------

// SelfBumpIncarnation increments this node’s incarnation to refute suspicion.
// Returns the new incarnation.
func (s *Store) selfBumpIncarnation() {
	lookupKey := s.selfID.EP.EndpointToString()
	if entry, ok := s.entries[lookupKey]; ok {
		entry.Incarnation++
		entry.LastUpdate, entry.State = time.Now(), StateAlive // Refuting suspicion, mark self as Alive
		s.version++
		log.Print((colors.Pink + "SelfBumpIncarnation: new incarnation: "), entry, "\n"+colors.Reset)
	} else {
		log.Print("Self entry not found \n")
	}
}

// SetSelfState allows voluntary leave, etc., for the local node.
func (s *Store) SetSelfState(st State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lookupKey := s.selfID.EP.EndpointToString()
	if entry, ok := s.entries[lookupKey]; ok {
		entry.State, entry.LastUpdate = st, time.Now()
		s.version++
		log.Print(("SetSelfState: new state: "), entry.State, "\n")
	} else {
		log.Print("Self entry not found \n")
	}
}

// ------------------- Incoming entry handler -------------------

func (s *Store) updateMemberEntry(incomingMemberEntry MemberEntry) {
	// First compare incarnation numbers (higher incarnation wins)
	// If incarnation numbers are equal, use state precedence: Failed > Suspect > Alive
	tSuspect := s.config.TSuspect

	lookupKey := incomingMemberEntry.ID.EP.EndpointToString()
	if existingEntry, ok := s.entries[lookupKey]; ok {
		// Checking for stale entry in membership list
		if incomingMemberEntry.ID.TimeStamp > existingEntry.ID.TimeStamp {
			// Same endpoint but different boot times, use NormalizeBoot to insert newer boot
			s.normalizeBoot(incomingMemberEntry)
			return
		}
		if incomingMemberEntry.ID.TimeStamp < existingEntry.ID.TimeStamp {
			// Existing entry is from a newer boot, ignore incoming entry
			return
		}
		// If suspicion is enabled
		//log.Printf("tSuspect"+" value: %v\n", tSuspect)
		if tSuspect != 0 {
			if incomingMemberEntry.Incarnation > existingEntry.Incarnation {
				s.entries[lookupKey] = &incomingMemberEntry
				s.version++
				s.PrintStateChange(*existingEntry, incomingMemberEntry)
				return
			} else if incomingMemberEntry.Incarnation == existingEntry.Incarnation {
				if incomingMemberEntry.ID == s.selfID && (incomingMemberEntry.State == StateSuspect || incomingMemberEntry.State == StateFailed) {
					// If the incoming entry is for self, bump incarnation to refute suspicion/failed allegation
					s.selfBumpIncarnation()
					return
				} else {
					if incomingMemberEntry.LastUpdate.After(existingEntry.LastUpdate) {
						// If incoming entry has higher "heartbeat counter" -> latest news
						s.entries[lookupKey] = &incomingMemberEntry
						s.version++
						s.PrintStateChange(*existingEntry, incomingMemberEntry)
						return
					} else if incomingMemberEntry.LastUpdate.Equal(existingEntry.LastUpdate) {
						return // Both entries are identical, ignore incoming entry // TODO: Check if this is causing an edge case
					} else if incomingMemberEntry.LastUpdate.Before(existingEntry.LastUpdate) {
						return // Existing entry has higher "heartbeat counter", ignore incoming entry
					}
				}
			} else {
				return // Existing entry has higher incarnation, ignore incoming entry
			}
		} else { // Suspicion is disabled
			if incomingMemberEntry.LastUpdate.After(existingEntry.LastUpdate) {
				// If incoming entry has higher "heartbeat counter" -> latest news
				s.entries[lookupKey] = &incomingMemberEntry
				s.version++
				s.PrintStateChange(*existingEntry, incomingMemberEntry)
				return
			} else if incomingMemberEntry.LastUpdate.Equal(existingEntry.LastUpdate) {
				return // Both entries are identical, ignore incoming entry // TODO: Check if this is causing an edge case
			} else if incomingMemberEntry.LastUpdate.Before(existingEntry.LastUpdate) {
				return // Existing entry has higher "heartbeat counter", ignore incoming entry
			}
		}
	} else {
		// New entry, add it to the membership list
		log.Print(colors.Yellow+"New entry, adding to membership list", incomingMemberEntry, "\n"+colors.Reset)
		// incomingMemberEntry.LastUpdate = time.Now()
		s.entries[lookupKey] = &incomingMemberEntry
		s.version++
		s.PrintSnapshot()

		// Update ring on new member addition
		// list, _ := s.SnapshotWithoutLock()
		// ring.UpdateRingFromMembership(list)
		s.notifyLocked("New Member Added: updating ring")
		return
	}
}

// MergeFullList merges an incoming full membership list into the local store
func (s *Store) MergeFullList(incomingList []MemberEntry, from string) {
	s.PrintMemberEntry(incomingList, fmt.Sprintf("Mem. Entry received before merge called from %s\n", from))
	s.mu.Lock()
	for _, incomingEntry := range incomingList {
		s.updateMemberEntry(incomingEntry)
	}
	s.mu.Unlock()
	s.PrintTable(fmt.Sprintf("Mem. List after merge called from %s\n", from))
}

func (s *Store) Reconfigure(newConfig config.Config) {
	s.mu.Lock()
	s.config = newConfig
	s.mu.Unlock()
}

func (s *Store) PrintTable(tag string) {
	//membershipList, _ := s.Snapshot()
	//list := membershipList
	//log.Printf(colors.Purple + "-----------------------------------------------------------------------")
	//log.Printf("== %s @ %s ==\n", tag, time.Now().Format("2006-01-02 15:04:05"))
	//for _, m := range list {
	//	log.Printf("  %s  inc=%d  state=%v  at=%s\n",
	//		m.ID.NodeIDToString(), m.Incarnation, m.State, m.LastUpdate.Format("2006-01-02 15:04:05"))
	//}
	//if len(list) == 0 {
	//	log.Println("  (empty)")
	//}
	//log.Printf("-----------------------------------------------------------------------\n" + colors.Reset)

}

func (s *Store) PrintMemberEntry(list []MemberEntry, tag string) {
	//log.Printf(colors.Purple + "-----------------------------------------------------------------------")
	//log.Printf("== %s @ %s ==\n", tag, time.Now().Format("2006-01-02 15:04:05"))
	//for _, m := range list {
	//	log.Printf("  %s  inc=%d  state=%v  at=%s\n",
	//		m.ID.NodeIDToString(), m.Incarnation, m.State, m.LastUpdate.Format("2006-01-02 15:04:05"))
	//}
	//if len(list) == 0 {
	//	log.Println("  (empty)")
	//}
	//log.Printf("-----------------------------------------------------------------------\n" + colors.Reset)

}

func (s *Store) PrintSnapshot() {
	entryMap := s.entries
	membershipList := make([]MemberEntry, 0, len(entryMap))
	for _, entry := range entryMap { // entry is a pointer
		membershipList = append(membershipList, *entry) // dereferencing pointer to copy value
	}

	log.Printf(colors.Purple + "-----------------------------------------------------------------------")
	log.Printf("== %s @ %s  -> Total Count: %d ==\n", "Membership Table", time.Now().Format("2006-01-02 15:04:05"), len(membershipList))
	for _, m := range membershipList {
		log.Printf("  %s  inc=%d  state=%v  at=%s\n",
			generic_utils.ResolveDNSFromIP(m.ID.NodeIDToString()), m.Incarnation, m.State, m.LastUpdate.Format("2006-01-02 15:04:05"))
	}
	if len(membershipList) == 0 {
		log.Println("  (empty)")
	}
	log.Printf("-----------------------------------------------------------------------\n" + colors.Reset)

}

func (s *Store) PrintStateChange(prev, next MemberEntry) {
	if prev.State != next.State {
		switch next.State {
		case StateSuspect:
			log.Printf(colors.Orange+"Apply: %s updated existing entry: %+v\n"+colors.Reset, next.State.StateToString(), next)
		case StateFailed:
			log.Printf(colors.Red+"Apply: %s updated existing entry: %+v\n"+colors.Reset, next.State.StateToString(), next)
		case StateLeft:
			log.Printf(colors.Cyan+"Apply: %s updated existing entry: %+v\n"+colors.Reset, next.State.StateToString(), next)
		default:
			log.Printf(colors.Underline+"Apply: %s updated existing entry: %+v\n"+colors.Reset, next.State.StateToString(), next)
		}

		s.PrintSnapshot()
	}
}
