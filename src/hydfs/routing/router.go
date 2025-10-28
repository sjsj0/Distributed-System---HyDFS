// hydfs/routing/router.go
package routing

import (
	"fmt"
	"hydfs-g33/hydfs/ring"
	ids "hydfs-g33/hydfs/utils"
)

type Router struct{ manager *ring.Manager }

func NewRouter(manager *ring.Manager) *Router { return &Router{manager: manager} }

// ReplicaSet returns the nodes responsible for storing the replicas of the given file.
func (r *Router) ReplicaSet(fileName string) []ring.Node {
	ring := r.manager.Ring()
	if ring == nil || ring.Len() == 0 {
		return nil
	}
	token := ids.FileToken64(fileName)
	fmt.Println("Calculating replicas for file:", fileName, "with token:", token)
	nodes := ring.Successors(token, r.manager.RF())
	return nodes
}
