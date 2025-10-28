package utils

import (
	"crypto/sha256"
	"encoding/hex"
	nodeid "hydfs-g33/membership/node"
	"time"
)

// -----------------------------------------------------------------------------
// FILE IDENTIFIERS
//
// HyDFS needs a stable, deterministic way to map each file to a location
// on the consistent hashing ring. To achieve this, every file is assigned a
// unique and immutable FileID derived from its *logical name* (the filename
// or path provided by the user).
//
// Hashing method:
//   - We use SHA-256 to get a 256-bit (32-byte) digest of the file name.
//   - This digest is represented as a lowercase 64-character hexadecimal string.
//   - It’s collision-resistant and uniform for consistent hashing placement.
//
// The same digest is also used to derive a 64-bit numeric token used by
// the consistent hashing ring (first 8 bytes of the digest, big-endian order).
// -----------------------------------------------------------------------------

// FileIDHex returns a 64-character lowercase hexadecimal string
// representing SHA-256(fileName). This serves as the file's unique ID.
// func FileIDHex(fileName string) string {
// 	sum := sha256.Sum256([]byte(fileName))
// 	return hex.EncodeToString(sum[:]) // 256-bit, uniform, stable ID
// }

// FileToken64 derives a 64-bit ring token from the same SHA-256 digest.
// The token is used when placing files and nodes on the consistent hash ring.
// It’s the big-endian interpretation of the first 8 bytes of the SHA-256 hash.
func FileToken64(fileName string) uint64 {
	sum := sha256.Sum256([]byte(fileName))
	return uint64(sum[0])<<56 | uint64(sum[1])<<48 | uint64(sum[2])<<40 | uint64(sum[3])<<32 |
		uint64(sum[4])<<24 | uint64(sum[5])<<16 | uint64(sum[6])<<8 | uint64(sum[7])
}

// NodeToken64: stable ring position for a node (include incarnation to avoid reuse after restart).
func NodeToken64(nodeID nodeid.NodeID) uint64 {
	sum := sha256.Sum256([]byte(nodeID.NodeIDToString())) // "ip:port@timestamp"
	return uint64(sum[0])<<56 | uint64(sum[1])<<48 | uint64(sum[2])<<40 | uint64(sum[3])<<32 |
		uint64(sum[4])<<24 | uint64(sum[5])<<16 | uint64(sum[6])<<8 | uint64(sum[7])
}

func AppendOpID(fileToken string, clientID string, clientSeq uint64, ts time.Time, chunkIDs []string) string {
	h := sha256.New()

	// add fileToken and clientID with separators to avoid ambiguity
	h.Write([]byte(fileToken))
	h.Write([]byte{0})
	h.Write([]byte(clientID))
	h.Write([]byte{0})

	// encode sequence (8 bytes, big-endian)
	var b [8]byte
	for i := 0; i < 8; i++ {
		b[7-i] = byte(clientSeq >> (8 * i))
	}
	h.Write(b[:])

	// encode timestamp (nanoseconds)
	ns := ts.UnixNano()
	for i := 0; i < 8; i++ {
		b[7-i] = byte(uint64(ns) >> (8 * i))
	}
	h.Write(b[:])

	// hash all chunk IDs in order, separated by 0x00
	for _, cid := range chunkIDs {
		h.Write([]byte(cid))
		h.Write([]byte{0})
	}

	return hex.EncodeToString(h.Sum(nil))
}
