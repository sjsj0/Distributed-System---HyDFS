package merge

import (
	"hydfs-g33/hydfs/internal/storage"
	"sort"
)

func SortOps(ops []storage.AppendOp) {
	sort.SliceStable(ops, func(i, j int) bool {
		a, b := ops[i], ops[j]
		if !a.Timestamp.Equal(b.Timestamp) {
			return a.Timestamp.Before(b.Timestamp)
		}
		if a.ClientID != b.ClientID {
			return a.ClientID < b.ClientID
		}
		if a.ClientSeq != b.ClientSeq {
			return a.ClientSeq < b.ClientSeq
		}
		return a.OpID < b.OpID
	})
}
