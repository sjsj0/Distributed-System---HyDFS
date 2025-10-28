package ring

import (
	"fmt"
	"hydfs-g33/config"
	nodeid "hydfs-g33/membership/node"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)

// ───────────────────────────────────────────────────────────────────────────────
// Token ranges + plans
// ───────────────────────────────────────────────────────────────────────────────

// TokenRange is a clockwise half-open interval on the ring: (Start, End] with wrap.
type TokenRange struct {
	Start uint64 // exclusive
	End   uint64 // inclusive
}

func (tr TokenRange) String() string { return fmt.Sprintf("(%d, %d]", tr.Start, tr.End) }

// LocalPlan: what THIS node must do (pushes + GCs) to converge after a ring change.
type LocalPlan struct {
	Pushes []TransferPlan // copy from self -> peer
	GCs    []GCPlan       // delete on self
}

// TransferPlan: copy all files with token in Range from From → To
type TransferPlan struct {
	From  Node
	To    Node
	Range TokenRange
	Note  string
}

// GCPlan: delete all files with token in Range on node On (self)
type GCPlan struct {
	On    Node
	Range TokenRange
	Note  string
}

// ───────────────────────────────────────────────────────────────────────────────
// ReplicaManager (local, per-node planner)
// ───────────────────────────────────────────────────────────────────────────────

type ReplicaManager struct {
	rf int
}

func NewReplicaManager(rf int) *ReplicaManager {
	if rf < 1 {
		rf = 1
	}
	return &ReplicaManager{rf: rf}
}

// ReconcileLocal computes only the actions this node (self) must perform when
// moving from oldRing to newRing.
func (rm *ReplicaManager) ReconcileLocal(self Node, oldRing, newRing *Ring) LocalPlan {
	var lp LocalPlan
	if oldRing == nil || oldRing.Len() == 0 || newRing == nil || newRing.Len() == 0 {
		return lp
	}

	joined, left := diffRings(oldRing, newRing)

	fmt.Printf("Joined nodes: %v\n", joined)
	fmt.Printf("Left nodes: %v\n", left)

	// JOINs (computed on NEW ring)
	for _, joinedNode := range joined {
		rm.localPlanJoin(&lp, self, newRing, joinedNode)
	}

	// LEAVEs (need OLD + NEW rings)
	for _, leftNode := range left {
		rm.localPlanLeave(&lp, self, oldRing, newRing, leftNode)
	}

	return lp
}

// localPlanJoin: if self is succ(J) in NEW ring, self pushes U=(pred^{rf-1}(J), J] to J.
// If self is one of the RF-1 evicted nodes, self GCs its sub-range.
func (rm *ReplicaManager) localPlanJoin(lp *LocalPlan, self Node, newRing *Ring, joinedNode Node) {
	replicationFactor := rm.rf
	if newRing.Len() == 0 {
		return
	}

	fmt.Print("Calculating join plan for node: ", joinedNode.NodeID.NodeIDToString(), "\n")

	// Special case: if newRing has <= RF nodes, only the node with smallest timestamp
	// pushes full data to the new node.
	if newRing.Len() <= replicationFactor {
		// Find the node with the smallest timestamp in the new ring
		var donor Node
		var minTimestamp uint64 = ^uint64(0) // max uint64
		for _, n := range newRing.nodes {
			timeStamp := n.NodeID.GetTimestamp()
			if timeStamp < minTimestamp {
				minTimestamp = timeStamp
				donor = n
			}
		}

		if sameNode(donor, self) {
			lp.Pushes = append(lp.Pushes, TransferPlan{
				From:  self,
				To:    joinedNode,
				Range: TokenRange{Start: 0, End: ^uint64(0)}, // full range
				Note:  "join: donor(self)->new for union U",
			})
		}
		return
	}

	donor := newRing.SuccNode(joinedNode)                      // succ(J) on NEW ring
	uStart := newRing.PredNodeN(joinedNode, replicationFactor) // pred^{k-1}(J)

	U := TokenRange{Start: uStart.Token, End: joinedNode.Token} // Calculating range of tokens that need to be transfered

	// If I'm the donor, I push U -> J
	if sameNode(donor, self) {
		lp.Pushes = append(lp.Pushes, TransferPlan{
			From:  self,
			To:    joinedNode,
			Range: U,
			Note:  "join: donor(self)->new for union U",
		})
	}

	// If I'm one of the evicted RF-1 nodes, I GC my sub-range.
	for r := 0; r <= replicationFactor-1; r++ {
		on := newRing.SuccNodeN(donor, r)
		if !sameNode(on, self) {
			continue
		}
		left := newRing.PredNodeN(joinedNode, replicationFactor-r)
		right := newRing.PredNodeN(joinedNode, replicationFactor-1-r)
		Rr := TokenRange{Start: left.Token, End: right.Token}
		lp.GCs = append(lp.GCs, GCPlan{
			On:    self,
			Range: Rr,
			Note:  fmt.Sprintf("join: GC sub-range r=%d", r),
		})
	}
}

// localPlanLeave: if self is succ(L) in OLD ring, self pushes R=(pred(L), L] to new farthest.
func (rm *ReplicaManager) localPlanLeave(lp *LocalPlan, self Node, oldRing, newRing *Ring, leftNode Node) {
	replicationFactor := rm.rf
	if newRing.Len() == 0 {
		return
	}

	for r := replicationFactor - 1; r >= -1; r-- {
		if r == 0 {
			continue // skip r=0 to avoid pushing from node that already left
		}

		on := oldRing.PredNodeN(leftNode, r)
		if !sameNode(on, self) {
			continue // not my job
		}
		left := newRing.PredNode(on)

		R := TokenRange{}
		targetNew := Node{}

		if r >= 0 {
			R = TokenRange{Start: left.Token, End: on.Token}
			targetNew = oldRing.SuccNodeN(on, replicationFactor)
		} else if r == -1 {
			R = TokenRange{Start: left.Token, End: leftNode.Token}
			targetNew = oldRing.SuccNodeN(on, replicationFactor-1)
		}

		lp.Pushes = append(lp.Pushes, TransferPlan{
			From:  self,
			To:    targetNew,
			Range: R,
			Note:  "leave: donor(self)->new farthest",
		})
	}
	// No survivor GC for R.
}

// ───────────────────────────────────────────────────────────────────────────────
// Local execution via HTTP to /v1/replica/create_with_data
// ───────────────────────────────────────────────────────────────────────────────

// FileMeta represents a file owned/stored by this node (by token).
type FileMeta struct {
	Name  string
	Token uint64
}

// FileEnumerator is the minimal surface the mover needs from your storage layer.
type FileEnumerator interface {
	// List returns files THIS node currently stores whose tokens are within (r.Start, r.End].
	List(r TokenRange) ([]FileMeta, error)
	// Open returns a reader for the full logical bytes of the file (manifest→chunks streamed).
	Open(fileName string) (io.ReadCloser, int64, error)
	// Delete removes file (manifest + chunks) from this node.
	Delete(fileName string) error
}

// HTTPMover sends files to peers using the replica-only endpoint and deletes locally for GC.
type HTTPMover struct {
	Client        *http.Client
	Config        config.Config
	Index         FileEnumerator
	SelfClientSeq *uint64
	SelfClientID  string
}

// CopyRange implements the "From(self) -> To(peer)" transfer by enumerating local files in r
// and POSTing each to peer /v1/replica/create_with_data.
func (m *HTTPMover) CopyRange(from Node, to Node, r TokenRange) error {
	if m.Client == nil || m.Index == nil {
		return fmt.Errorf("HTTPMover: missing client or index")
	}
	files, err := m.Index.List(r)
	if err != nil {
		return err
	}

	replicaEndpoint, err := m.resolveReplicaEndpoint(to.NodeID)
	base := "http://" + replicaEndpoint
	now := time.Now().UnixNano()

	for _, f := range files {
		rc, _, err := m.Index.Open(f.Name)
		if err != nil {
			return fmt.Errorf("open %s: %w", f.Name, err)
		}

		// /v1/replica/create_with_data?file_name=...&client_id=...&seq=0&ts_ns=...
		q := url.Values{}
		q.Set("hydfs_file_name", f.Name)
		if m.SelfClientID != "" {
			q.Set("client_id", m.SelfClientID)
		} else {
			q.Set("client_id", "rebalance")
		}
		q.Set("seq", fmt.Sprintf("%d", atomic.LoadUint64(m.SelfClientSeq)))
		q.Set("ts_ns", fmt.Sprintf("%d", now))

		u := base + "/v1/replica/create_with_data?" + q.Encode()
		req, err := http.NewRequest("POST", u, rc)
		if err != nil {
			rc.Close()
			return err
		}
		req.Header.Set("Content-Type", "application/octet-stream")

		resp, err := m.Client.Do(req)
		rc.Close()
		if err != nil {
			return fmt.Errorf("POST %s: %w", u, err)
		}
		// 200 OK → created/updated; 409 Conflict → peer already has it (idempotent ok).
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return fmt.Errorf("peer %s: %s", to.NodeID.EP.EndpointToString(), string(b))
		}
		resp.Body.Close()
	}
	return nil
}

// DeleteRange enumerates local files in r and deletes them locally.
func (m *HTTPMover) DeleteRange(on Node, r TokenRange) error {
	if m.Index == nil {
		return fmt.Errorf("HTTPMover: missing index")
	}
	files, err := m.Index.List(r)
	if err != nil {
		return err
	}
	for _, f := range files {
		if err := m.Index.Delete(f.Name); err != nil {
			return fmt.Errorf("delete %s: %w", f.Name, err)
		}
	}
	return nil
}

// ExecuteLocal runs a local plan with the mover: pushes first, then GCs.
func (rm *ReplicaManager) ExecuteLocal(mover *HTTPMover, lp LocalPlan) error {
	for _, p := range lp.Pushes {
		if err := mover.CopyRange(p.From, p.To, p.Range); err != nil {
			return err
		}
	}
	for _, g := range lp.GCs {
		if err := mover.DeleteRange(g.On, g.Range); err != nil {
			return err
		}
	}
	return nil
}

// ───────────────────────────────────────────────────────────────────────────────
// Ring helpers
// ───────────────────────────────────────────────────────────────────────────────

func sameNode(a, b Node) bool { return a.NodeID.NodeIDToString() == b.NodeID.NodeIDToString() }

// diffRings returns (joined, left) as nodes present only in new or only in old.
func diffRings(oldR, newR *Ring) (joined []Node, left []Node) {
	oldMap := make(map[string]Node, oldR.Len())
	for _, n := range oldR.nodes {
		oldMap[n.NodeID.NodeIDToString()] = n
	}
	newMap := make(map[string]Node, newR.Len())
	for _, n := range newR.nodes {
		newMap[n.NodeID.NodeIDToString()] = n
	}

	for k, n := range newMap {
		if _, ok := oldMap[k]; !ok {
			joined = append(joined, n)
		}
	}
	for k, n := range oldMap {
		if _, ok := newMap[k]; !ok {
			left = append(left, n)
		}
	}
	sort.Slice(joined, func(i, j int) bool { return joined[i].Token < joined[j].Token })
	sort.Slice(left, func(i, j int) bool { return left[i].Token < left[j].Token })
	return
}

// Pred/Succ helpers (indices assume ring.nodes sorted by token then NodeID)
func (r *Ring) PredNode(x Node) Node {
	if r.Len() == 0 {
		return Node{}
	}
	i := r.indexOfToken(x.Token)
	if i == 0 {
		return r.nodes[len(r.nodes)-1]
	}
	return r.nodes[i-1]
}

func (r *Ring) PredNodeN(x Node, n int) Node {
	if r.Len() == 0 {
		return Node{}
	}
	i := r.indexOfToken(x.Token)
	i = (i - n) % r.Len()
	if i < 0 {
		i += r.Len()
	}
	return r.nodes[i]
}

func (r *Ring) SuccNode(x Node) Node {
	if r.Len() == 0 {
		return Node{}
	}
	i := r.indexOfToken(x.Token)
	i = (i + 1) % r.Len()
	return r.nodes[i]
}

func (r *Ring) SuccNodeN(x Node, n int) Node {
	if r.Len() == 0 {
		return Node{}
	}
	i := r.indexOfToken(x.Token)
	i = (i + n) % r.Len()
	return r.nodes[i]
}

func (r *Ring) indexOfToken(tok uint64) int {
	i := sort.Search(len(r.nodes), func(i int) bool {
		return r.nodes[i].Token >= tok
	})
	if i < len(r.nodes) && r.nodes[i].Token == tok {
		return i
	}
	if i == len(r.nodes) {
		return 0
	}
	return i
}

func (m *HTTPMover) resolveReplicaEndpoint(rp nodeid.NodeID) (string, error) {

	replicaEndpoint := rp.EP.EndpointToString()
	fmt.Println("Resolving replica endpoint for:", replicaEndpoint)

	host, _, err := net.SplitHostPort(replicaEndpoint)
	if err != nil {
		log.Printf("invalid replica endpoint %q: %v", replicaEndpoint, err)
		return "", err
	}
	fmt.Println("Host extracted:", host)

	if m.Config.Env == "dev" {
		port := rp.EP.Port

		_, bindPortStr, err := net.SplitHostPort(m.Config.BindAddr)
		if err != nil {
			log.Printf("invalid bind address %q: %v", m.Config.BindAddr, err)
			return "", err
		}
		bindPort, _ := strconv.Atoi(bindPortStr)

		diff := port - uint16(bindPort)

		hydfsHTTPPort, _ := strconv.Atoi(m.Config.HydfsHTTP[1:]) // skip leading ':'
		fmt.Println("Calculated hydfsHTTPPort:", hydfsHTTPPort)

		replicaEndpoint = fmt.Sprintf("%s:%d", host, hydfsHTTPPort+int(diff))
	} else {
		// assuming s.Config.HydfsHTTP already includes :port or prefix
		replicaEndpoint = fmt.Sprintf("%s%s", host, m.Config.HydfsHTTP)
	}

	fmt.Println("Resolved replica endpoint to:", replicaEndpoint)

	return replicaEndpoint, nil
}
