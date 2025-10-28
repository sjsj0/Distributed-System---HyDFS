package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"
)

type AppendOp struct {
	ChunkIDs  []string  `json:"chunk_ids"`  // logically ordered list of chunk IDs (don't sort!)
	Timestamp time.Time `json:"timestamp"`  // unix nanotime
	ClientID  string    `json:"client_id"`  // unique ID of the client who issued the op
	ClientSeq uint64    `json:"client_seq"` // sequence number of the client's operation
	OpID      string    `json:"op_id"`      // hash of (file_id, client_id, client_seq, timestamp, chunk_ids)
	TotalSize int64     `json:"total_size"` // sum of chunk sizes
}

type Manifest struct {
	FileToken  string     `json:"file_token"`  // HyDFS file token
	FileName   string     `json:"file_name"`   // HyDFS file name
	Version    uint64     `json:"version"`     // incremented on each change
	Ops        []AppendOp `json:"ops"`         // ordered list of append operations
	Created    time.Time  `json:"created"`     // manifest file creation time
	LastUpdate time.Time  `json:"last_update"` // last manifest update time
}

func LoadManifest(manifestFilePath string) (*Manifest, error) {
	f, err := os.Open(manifestFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var m Manifest
	if err := json.NewDecoder(f).Decode(&m); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	return &m, nil
}

func (m *Manifest) NewManifest(manifestFilePath string) error {
	tmp := manifestFilePath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(m); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	return os.Rename(tmp, manifestFilePath) // rename from manifest.json.tmp to manifest.json
}

func (m *Manifest) AddOp(op AppendOp) {
	m.Version++
	m.Ops = append(m.Ops, op)
}

// SortOps ensures deterministic ordering (merge rule).
func (m *Manifest) SortOps() {
	sort.SliceStable(m.Ops, func(i, j int) bool {
		a, b := m.Ops[i], m.Ops[j]
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
