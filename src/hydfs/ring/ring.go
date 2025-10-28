package ring

import (
	"fmt"
	nodeid "hydfs-g33/membership/node"
	"sort"
)

// Node represents a live node on the ring.
type Node struct {
	NodeID nodeid.NodeID
	Token  uint64
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

// Successors returns the first k successors after token (wraps around).
func (r *Ring) Successors(token uint64, ReplicationFactor int) []Node {
	n := len(r.nodes)
	fmt.Println("Ring Successors called with token:", token, "ReplicationFactor:", ReplicationFactor, "n:", n)
	if n == 0 || ReplicationFactor <= 0 {
		return nil
	}
	i := sort.Search(n, func(i int) bool { return r.nodes[i].Token >= token })
	fmt.Println("Primary replica found at:", i)

	// Check if number of nodes is less than ReplicationFactor
	if n < ReplicationFactor {
		ReplicationFactor = n
	}

	out := make([]Node, 0, ReplicationFactor)
	for j := 0; j < ReplicationFactor; j++ {
		out = append(out, r.nodes[(i+j)%n])
	}
	fmt.Println("Successors returned:", out)
	return out
}
