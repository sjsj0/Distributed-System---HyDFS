package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const MaxChunkSize = 64 * 1024 * 1024 // 64 MB

// WriteChunks splits data into ≤64 MB pieces and writes them as hashed chunk files.
// Returns slice of chunkIDs.
func WriteChunks(chunksDir string, r io.Reader) ([]string, int64, error) {
	if err := os.MkdirAll(chunksDir, 0755); err != nil {
		return nil, 0, fmt.Errorf("mkdir %s: %w", chunksDir, err)
	}
	var chunkIDs []string
	var total int64
	buf := make([]byte, MaxChunkSize)
	for {
		n, err := io.ReadFull(r, buf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if n == 0 {
				break
			}
			// handle last partial chunk
			chunkID, size, werr := writeChunk(chunksDir, buf[:n])
			if werr != nil {
				return nil, total, werr
			}
			chunkIDs = append(chunkIDs, chunkID)
			total += size
			break
		}
		if err != nil {
			return nil, total, err
		}
		chunkID, size, werr := writeChunk(chunksDir, buf[:n])
		if werr != nil {
			return nil, total, werr
		}
		chunkIDs = append(chunkIDs, chunkID)
		total += size
	}
	return chunkIDs, total, nil
}

func writeChunk(chunksDir string, data []byte) (string, int64, error) {
	sum := sha256.Sum256(data)
	chunkID := hex.EncodeToString(sum[:])
	tmp := filepath.Join(chunksDir, chunkID+".tmp")
	final := filepath.Join(chunksDir, chunkID)

	f, err := os.Create(tmp)
	if err != nil {
		return "", 0, err
	}
	// defer f.Close()
	if _, err := f.Write(data); err != nil { // write chunk data to temp file
		return "", 0, err
	}
	if err := f.Sync(); err != nil { // ensure data is flushed to disk
		return "", 0, err
	}
	f.Close()
	if err := os.Rename(tmp, final); err != nil { // rename temp file to chunkID
		return "", 0, err
	}
	fmt.Print("Renamed file!")
	return chunkID, int64(len(data)), nil
}

// ReadChunks concatenates the content of all given chunkIDs.
func ReadChunks(chunksDir string, chunkIDs []string, w io.Writer) (int64, error) {
	var total int64
	for _, id := range chunkIDs {
		path := filepath.Join(chunksDir, id)
		f, err := os.Open(path)
		if err != nil {
			return total, err
		}
		n, err := io.Copy(w, f)
		f.Close()
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}
