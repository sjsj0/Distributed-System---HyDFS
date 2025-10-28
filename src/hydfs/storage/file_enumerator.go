package storage

import (
	"fmt"
	"hydfs-g33/hydfs/ring"
	ids "hydfs-g33/hydfs/utils"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

// fileIndex implements the ring.HTTPMover's FileEnumerator interface
// by scanning the on-disk layout:
//
// <DataDir>/files/<fileID>/manifest.json
// <DataDir>/files/<fileID>/chunks/<chunkID>
type fileIndex struct {
	Paths FSPaths
}

func NewFileEnumerator(fs *FileStore) *fileIndex {
	return &fileIndex{
		Paths: *fs.Paths,
	}
}

// inRangeHalfOpen reports whether tok ∈ (start, end] on a modulo ring (with wrap).
func inRangeHalfOpen(start, end, tok uint64) bool {
	if start < end {
		return tok > start && tok <= end
	}
	// wrap-around case
	return tok > start || tok <= end
}

// List enumerates files that THIS node currently stores whose tokens are in (r.Start, r.End].
// We derive the token from the manifest's logical FileName (same hash used for placement).
func (fi *fileIndex) List(r ring.TokenRange) ([]ring.FileMeta, error) {
	root := fi.Paths.FilesDir
	filesDir, err := os.ReadDir(root)
	if err != nil {
		// If directory does not exist yet, treat as empty.
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("readdir %s: %w", root, err)
	}

	var out []ring.FileMeta
	for _, fileTokenDir := range filesDir {
		if !fileTokenDir.IsDir() {
			continue
		}

		fileTokenStr := fileTokenDir.Name()
		fileToken, err := strconv.ParseUint(fileTokenStr, 10, 64)
		mPath := fi.Paths.ManifestPath(fileToken)
		m, err := LoadManifest(mPath)
		if err != nil || m == nil {
			// Skip unreadable/missing manifests; continue scanning.
			continue
		}
		if inRangeHalfOpen(r.Start, r.End, fileToken) {
			out = append(out, ring.FileMeta{
				Name:  m.FileName,
				Token: fileToken,
			})
		}
	}
	return out, nil
}

// Open returns a streaming reader for the full logical bytes of the file
// by walking manifest ops and chunk files in order. It also returns total size.
func (fi *fileIndex) Open(fileName string) (io.ReadCloser, int64, error) {

	fileToken := ids.FileToken64(fileName)
	mPath := fi.Paths.ManifestPath(fileToken)
	m, err := LoadManifest(mPath)
	if err != nil {
		return nil, 0, fmt.Errorf("load manifest: %w", err)
	}
	if m == nil {
		return nil, 0, fmt.Errorf("file not found: %s", fileName)
	}
	// Ensure deterministic read order.
	m.SortOps()

	// Compute total logical size from manifest metadata (cheapest).
	var total int64
	for _, op := range m.Ops {
		total += op.TotalSize
	}

	// Build a streaming reader using an io.Pipe; a goroutine
	// sequentially reads chunk files and feeds the pipe.
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		buf := make([]byte, 1<<20) // 1 MiB copy buffer

		chunksDir := fi.Paths.ChunksDir(fileToken)
		for _, op := range m.Ops {
			for _, cid := range op.ChunkIDs {
				p := filepath.Join(chunksDir, cid)
				f, err := os.Open(p)
				if err != nil {
					_ = pw.CloseWithError(fmt.Errorf("open chunk %s: %w", cid, err))
					return
				}
				for {
					n, er := f.Read(buf)
					if n > 0 {
						if _, werr := pw.Write(buf[:n]); werr != nil {
							_ = f.Close()
							_ = pw.CloseWithError(werr)
							return
						}
					}
					if er == io.EOF {
						break
					}
					if er != nil {
						_ = f.Close()
						_ = pw.CloseWithError(fmt.Errorf("read chunk %s: %w", cid, er))
						return
					}
				}
				_ = f.Close()
			}
		}
	}()
	return pr, total, nil
}

// Delete removes the file's local data (manifest + chunks dir).
// This is used by GC after successful rebalancing.
func (fi *fileIndex) Delete(fileName string) error {
	fileToken := ids.FileToken64(fileName)
	dir := fi.Paths.FileDir(fileToken) // .../files/<fileToken>
	// Remove the entire per-file directory.
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("remove %s: %w", dir, err)
	}
	return nil
}
