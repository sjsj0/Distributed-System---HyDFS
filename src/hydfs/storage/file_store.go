package storage

import (
	"fmt"
	ids "hydfs-g33/hydfs/utils"
	nodeid "hydfs-g33/membership/node"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type FileOpResult struct {
	FileName  string    `json:"file_name"`
	FileToken string    `json:"file_token"`
	Version   uint64    `json:"version"`
	Bytes     int64     `json:"bytes"`
	OpID      string    `json:"op_id"`
	Timestamp time.Time `json:"timestamp"`
	ClientID  string    `json:"client_id"`
	ClientSeq uint64    `json:"client_seq"`
}

type GetResult struct {
	FileName  string `json:"file_name"`
	FileToken string `json:"file_token"`
	Version   uint64 `json:"version"`
	Bytes     int64  `json:"bytes"`
}

type FileStore struct {
	Paths  *FSPaths               // paths to data directories
	NodeID nodeid.NodeID          // placeholder for membership integration
	mu     sync.Mutex             // protects fileMu map
	fileMu map[string]*sync.Mutex // fileToken -> lock
}

func NewFileStore(paths *FSPaths, nodeID nodeid.NodeID) *FileStore {
	return &FileStore{
		Paths:  paths,
		NodeID: nodeID,
		fileMu: make(map[string]*sync.Mutex), // per file lock
	}
}

func (fs *FileStore) lockFile(fileToken string) func() {
	fs.mu.Lock()
	m := fs.fileMu[fileToken]
	if m == nil {
		m = &sync.Mutex{}
		fs.fileMu[fileToken] = m
	}
	fs.mu.Unlock()
	m.Lock()
	return m.Unlock
}

func (fs *FileStore) Create(fileName string, r io.Reader, clientID string, clientSeq uint64, ts time.Time) (*FileOpResult, error) {
	fileToken := ids.FileToken64(fileName)
	fileTokenStr := strconv.FormatUint(fileToken, 10)

	// checks if file already exists, if so, no-op
	ok, err := fs.Paths.FileDirsExist(fileToken)
	if err == nil {
		fmt.Errorf("file already exists: %s", fileTokenStr)
		return nil, os.ErrExist
	}
	if ok {
		fmt.Errorf("file already exists: %s", fileTokenStr)
		return nil, os.ErrExist
	}

	// create the file directory
	if err := fs.Paths.CreateFileDirs(fileToken); err != nil {
		fmt.Printf("error creating file dirs: %s\n", err)
		return nil, err
	}

	fmt.Printf("create successful: %s\n", fileTokenStr)
	manifestPath := fs.Paths.ManifestPath(fileToken) // path to manifest.json for this fileToken
	if _, err := os.Stat(manifestPath); err == nil { // check if manifest already exists
		fmt.Printf("manifest file already exists: %s\n", fileTokenStr)
		return nil, os.ErrExist
	}

	m := &Manifest{
		FileName:   fileName,
		FileToken:  fileTokenStr,
		Version:    1,
		Ops:        []AppendOp{},
		Created:    time.Now(),
		LastUpdate: time.Now(),
	}

	if err := m.NewManifest(manifestPath); err != nil { // actually create the manifest file
		return nil, err
	}

	res, err := fs.Append(fileName, r, clientID, clientSeq, ts)
	if err != nil {
		return nil, err
	}

	fmt.Printf("create successful: %s (version: %d, bytes: %d)\n", fileName, res.Version, res.Bytes)
	return res, nil
}

func (fs *FileStore) Append(fileName string, r io.Reader, clientID string, clientSeq uint64, ts time.Time) (*FileOpResult, error) {
	fileToken := ids.FileToken64(fileName)
	fileTokenStr := strconv.FormatUint(fileToken, 10)
	fmt.Printf("append received: %s (client: %s)\n", fileTokenStr, clientID)

	// ensure dirs exist
	ok, err := fs.Paths.FileDirsExist(fileToken)
	if err != nil {
		fmt.Printf("file dir does not exist: %s\n", fileTokenStr)
		return nil, os.ErrNotExist
	}
	if !ok {
		fmt.Printf("file does not exist: %s\n", fileTokenStr)
		return nil, os.ErrNotExist
	}

	manifestPath := fs.Paths.ManifestPath(fileToken)
	if _, err := os.Stat(manifestPath); err != nil { // check if manifest already exists
		fmt.Printf("manifest file doesn't exist: %s\n", fileTokenStr)
		return nil, os.ErrNotExist
	}

	// write chunk files first
	chunkIDs, total, err := WriteChunks(fs.Paths.ChunksDir(fileToken), r)
	if err != nil {
		return nil, err
	}

	// TODO: remove locking in favour of per write pending manifest
	// critical section: manifest load -> modify -> save
	unlock := fs.lockFile(fileTokenStr)
	defer unlock()

	m, err := LoadManifest(manifestPath)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, fmt.Errorf("manifest file not found: %s", fileName)
	}

	opID := ids.AppendOpID(fileTokenStr, clientID, clientSeq, ts, chunkIDs)
	op := AppendOp{
		ChunkIDs:  chunkIDs,
		Timestamp: ts,
		ClientID:  clientID,
		ClientSeq: clientSeq,
		OpID:      opID,
		TotalSize: total,
	}
	m.AddOp(op)
	m.Version++
	m.FileSize += total
	m.LastUpdate = time.Now()
	m.SortOps()

	if err := m.NewManifest(manifestPath); err != nil {
		return nil, err
	}
	fmt.Printf("append successful: %s (version: %d, bytes: %d)\n", fileName, m.Version, total)

	return &FileOpResult{
		FileName:  fileName,
		FileToken: fileTokenStr,
		Version:   m.Version,
		Bytes:     total,
		OpID:      opID,
		Timestamp: ts,
		ClientID:  clientID,
		ClientSeq: clientSeq,
	}, nil
}

func (fs *FileStore) GetFile(fileName string, w io.Writer) (*GetResult, error) {
	fmt.Printf("get received: %s\n", fileName)
	fileToken := ids.FileToken64(fileName)
	fileTokenStr := strconv.FormatUint(fileToken, 10)

	// checks if file exists, if not, no-op
	ok, err := fs.Paths.FileDirsExist(fileToken)
	if err != nil {
		fmt.Printf("file dir does not exist: %s\n", fileTokenStr)
		return nil, os.ErrNotExist
	}
	if !ok {
		return nil, os.ErrNotExist
	}

	manifestPath := fs.Paths.ManifestPath(fileToken) // path to manifest.json for this fileToken
	if _, err := os.Stat(manifestPath); err != nil { // check if manifest already exists
		fmt.Printf("manifest file does not exist: %s\n", fileTokenStr)
		return nil, os.ErrNotExist
	}

	mPath := fs.Paths.ManifestPath(fileToken)
	m, err := LoadManifest(mPath)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, fmt.Errorf("manifest file not found: %s", fileName)
	}
	// m.SortOps()
	var total int64
	for _, op := range m.Ops {
		n, err := ReadChunks(fs.Paths.ChunksDir(fileToken), op.ChunkIDs, w)
		if err != nil {
			return nil, err
		}
		total += n
	}
	fmt.Printf("get successful: %s (version: %d, bytes: %d)\n", fileName, m.Version, total)

	return &GetResult{
		FileName:  fileName,
		FileToken: fileTokenStr,
		Version:   m.Version,
		Bytes:     total,
	}, nil
}

func (fs *FileStore) GetManifest(fileName string) (*Manifest, error) {
	fileToken := ids.FileToken64(fileName)
	fmt.Printf("get manifest received: %s\n", fileName)

	manifestPath := fs.Paths.ManifestPath(fileToken)
	m, err := LoadManifest(manifestPath)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, fmt.Errorf("manifest file not found: %s", fileName)
	}
	fmt.Printf("get manifest successful: %s (version: %d)\n", fileName, m.Version)
	return m, nil
}

// storage/file_store_local.go
func (fs *FileStore) CreateLocalFile(localFileName string, r io.Reader) error {
	fmt.Printf("create local file received: %s\n", localFileName)
	localFilePath := fs.Paths.LocalFilePath(localFileName)

	// ensure parent dir exists
	if err := os.MkdirAll(filepath.Dir(localFilePath), 0o755); err != nil {
		return err
	}

	f, err := os.Create(localFilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	n, err := io.Copy(f, r) // copy INTO the file
	fmt.Printf("create local file successful: %s (bytes: %d)\n", localFileName, n)
	return err
}
