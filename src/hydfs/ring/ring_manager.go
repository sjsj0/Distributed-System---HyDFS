package ring

import (
	"context"
	"fmt"
	ascii_ring "hydfs-g33/hydfs/utils"
	ids "hydfs-g33/hydfs/utils"
	store "hydfs-g33/membership/store"
	generic_utils "hydfs-g33/utils"
	"sync"
)

// Manager owns the current ring view and (optionally) runs local rebalancing.
type Manager struct {
	mu     sync.RWMutex
	cur    *Ring
	rf     int
	cancel func()
	mover  *HTTPMover
}

func NewManager(ctx context.Context, st *store.Store, rf int) *Manager {
	m := &Manager{rf: rf}
	events, cancel := st.Subscribe(ctx, 32) // buffered; non-blocking
	m.cancel = cancel

	// initial build
	m.UpdateRingFromMembership(st)

	// Keeps running in the background to update ring on membership changes
	go func() {
		for range events {
			m.UpdateRingFromMembership(st)
		}
	}()
	return m
}

// SetMover provides the HTTP mover so this node will actually push/GC on ring changes.
// TODO: might not be needed
func (m *Manager) SetMover(hm *HTTPMover) {
	m.mu.Lock()
	m.mover = hm
	m.mu.Unlock()
}

func (m *Manager) UpdateRingFromMembership(st *store.Store) {
	// Snapshot membership and identify self
	list, selfID := st.Snapshot()

	// Build new ring + locate "self" as a ring.Node
	nodes := make([]Node, 0, len(list))
	var selfNode *Node
	for _, e := range list {
		token := ids.NodeToken64(e.ID)
		n := Node{NodeID: e.ID, Token: token}
		nodes = append(nodes, n)
		if e.ID == selfID {
			// capture self for local planning
			nCopy := n
			selfNode = &nCopy
		}
	}

	newRing := NewRing(nodes)

	// Swap rings (remember the old one for planning)
	m.mu.Lock()
	oldRing := m.cur
	m.cur = newRing
	httpMover := m.mover // read the mover under the same lock
	m.mu.Unlock()

	fmt.Printf("[ring] updated: %d nodes\n", newRing.Len())

	// Pretty print ring
	nodeIPs := make([]string, 0, newRing.Len())
	nodeIDs := make([]string, 0, newRing.Len())
	nodeTokens := make([]uint64, 0, newRing.Len())
	for _, n := range newRing.nodes {
		nodeIPs = append(nodeIPs, generic_utils.ResolveDNSFromIP(n.NodeID.NodeIDToString()))
		nodeIDs = append(nodeIDs, n.NodeID.NodeIDToString())
		nodeTokens = append(nodeTokens, n.Token)
	}
	ascii_ring.PrintAsciiRingWithLegend(nodeIPs, nodeIDs, nodeTokens)
	fmt.Println()

	// Nothing else to do if we don't have an old ring yet (first build) or no self
	if oldRing == nil || selfNode == nil {
		return
	}

	// ---- Local rebalancing for THIS node ----
	rm := NewReplicaManager(m.rf)
	localPlan := rm.ReconcileLocal(*selfNode, oldRing, newRing)

	// Log the plan so you can see what would happen
	for _, p := range localPlan.Pushes {
		fmt.Printf("[rebalance] PUSH  %s -> %s  range %s  (%s)\n",
			p.From.NodeID.NodeIDToString(), p.To.NodeID.NodeIDToString(), p.Range, p.Note)
	}
	for _, g := range localPlan.GCs {
		fmt.Printf("[rebalance] GC    on %s      range %s  (%s)\n",
			g.On.NodeID.NodeIDToString(), g.Range, g.Note)
	}

	// If a mover is installed, execute the plan now (pushes -> GCs).
	if httpMover != nil {
		if err := rm.ExecuteLocal(httpMover, localPlan); err != nil {
			fmt.Printf("[rebalance] error: %v\n", err)
		}
	}
}

func (m *Manager) Ring() *Ring {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cur
}

func (m *Manager) RF() int { return m.rf }

func (m *Manager) Close() {
	if m.cancel != nil {
		m.cancel()
	}
}
