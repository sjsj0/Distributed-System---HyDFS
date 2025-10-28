// pkg/protocol/common/peers.go
package common

import (
	"fmt"
	nodeid "hydfs-g33/membership/node"
	"hydfs-g33/membership/store"
)

//// NextPeer returns a random peer address string (e.g., "10.0.0.2:5000").
//// Returns empty string if peers is empty.
//func NextPeer(peers []string) string {
//	if len(peers) == 0 {
//		return ""
//	}
//	return peers[rand.Intn(len(peers))]
//}
//
//// KDistinct returns up to k distinct peer addresses (random order).
//func KDistinct(peers []string, k int) []string {
//	if k <= 0 || len(peers) == 0 {
//		return nil
//	}
//	if k > len(peers) {
//		k = len(peers)
//	}
//	perm := rand.Perm(len(peers))
//	out := make([]string, 0, k)
//	for i := 0; i < k; i++ {
//		out = append(out, peers[perm[i]])
//	}
//	return out
//}
//
//// MustResolve converts "ip:port" to *net.UDPAddr; returns nil on error.
//func MustResolve(addr string) *net.UDPAddr {
//	udp, _ := net.ResolveUDPAddr("udp", addr)
//	return udp
//}

func ResolvePeersFromMemberList(list []store.MemberEntry, selfID nodeid.NodeID) []string {
	peers := make([]string, 0, len(list))

	for _, me := range list {
		if me.ID == selfID {
			continue
		}
		peers = append(peers, fmt.Sprintf("%s", me.ID.EP.EndpointToString()))
	}
	//log.Printf("Resolved peers from member list: %v", peers)
	return peers
}
