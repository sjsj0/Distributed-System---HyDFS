package ring

import (
	"fmt"
	nodeid "hydfs-g33/membership/node"
	generic_utils "hydfs-g33/utils"
	"sort"
)

// Node represents a live node on the ring.
type Node struct {
	NodeID nodeid.NodeID `json:"node_id"`
	Token  uint64        `json:"token"`
}

// Ring holds a sorted list of nodes (by token).
type Ring struct {
	nodes []Node
}

func NewRing(newNodes []Node) *Ring {
	nodes := append([]Node(nil), newNodes...)

	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].Token == nodes[j].Token {
			return nodes[i].NodeID.NodeIDToString() < nodes[j].NodeID.NodeIDToString()
		}
		return nodes[i].Token < nodes[j].Token
	})

	return &Ring{nodes: nodes}
}

// Calculate length of the ring.
func (r *Ring) Len() int {
	return len(r.nodes)
}

// Nodes returns a copy of all nodes in the ring.
func (r *Ring) Nodes() []Node {
	out := make([]Node, len(r.nodes))
	copy(out, r.nodes)
	return out
}

// Successors returns the first k successors after token (wraps around).
func (r *Ring) Successors(token uint64, ReplicationFactor int) []Node {
	n := len(r.nodes)
	fmt.Println("Ring Successors called with token:", token, "ReplicationFactor:", ReplicationFactor, "n:", n)
	if n == 0 || ReplicationFactor <= 0 {
		return nil
	}
	i := sort.Search(n, func(i int) bool { return r.nodes[i].Token >= token })
	if i == n {
		// Wrap around — token is larger than all existing tokens, so pick the first node
		i = 0
	}
	fmt.Println("Primary replica found at:", r.nodes[i], "->", generic_utils.ResolveDNSFromIP(r.nodes[i].NodeID.NodeIDToString()))

	// Check if number of nodes is less than ReplicationFactor
	if n < ReplicationFactor {
		ReplicationFactor = n
	}

	out := make([]Node, 0, ReplicationFactor)
	for j := 0; j < ReplicationFactor; j++ {
		out = append(out, r.nodes[(i+j)%n])
	}
	fmt.Println("Secondary replica found at:", out[1], "->", generic_utils.ResolveDNSFromIP(out[1].NodeID.NodeIDToString()))
	fmt.Println("Tertiary replica found at:", out[2], "->", generic_utils.ResolveDNSFromIP(out[2].NodeID.NodeIDToString()))
	fmt.Println("Successors returned:", out)
	return out
}
