package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

// FSPaths defines the directory layout for all stored files.
type FSPaths struct {
	HyDFSDir   string // root directory for HyDFS data
	FilesDir   string // hyDFSDir/files/
	LocalDir   string // sibling to hyDFSDir for local files
	DatasetDir string // directory for datasets
}

// NewFSPaths creates the base directory and ensures the "files" folder exists.
func NewFSPaths(hyDFSDir string, localDir string, datasetDir string) (*FSPaths, error) {
	filesDir := filepath.Join(hyDFSDir, "files")

	// Check if directories already exist, if they do, delete them and recreate else just create them
	if _, err := os.Stat(hyDFSDir); err == nil {
		if err := os.RemoveAll(hyDFSDir); err != nil {
			return nil, fmt.Errorf("error in remove dir %s: %w", hyDFSDir, err)
		}
	}
	if _, err := os.Stat(localDir); err == nil {
		if err := os.RemoveAll(localDir); err != nil {
			return nil, fmt.Errorf("error in remove dir %s: %w", localDir, err)
		}
	}

	for _, dir := range []string{hyDFSDir, filesDir, localDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("create dir %s: %w", dir, err)
		}
	}

	if err := copyTxtFiles(datasetDir, localDir); err != nil {
		return nil, fmt.Errorf("copy txt: %w", err)
	}

	return &FSPaths{
		HyDFSDir:   hyDFSDir,
		FilesDir:   filesDir,
		LocalDir:   localDir,
		DatasetDir: datasetDir,
	}, nil
}

// LocalFilePath returns the full path for a local file.
func (p *FSPaths) LocalFilePath(localFileName string) string {
	return filepath.Join(p.LocalDir, localFileName)
}

// FileDir returns the directory path for a specific fileToken.
func (p *FSPaths) FileDir(fileToken uint64) string {
	return filepath.Join(p.FilesDir, strconv.FormatUint(fileToken, 10)) // hyDFSDir/files/<fileToken>
}

// ManifestPath returns the path to manifest.json for a file.
func (p *FSPaths) ManifestPath(fileToken uint64) string {
	return filepath.Join(p.FileDir(fileToken), "manifest.json") // hyDFSDir/files/<fileToken>/manifest.json
}

// TempManifestPath returns the temp path used for atomic manifest writes.
func (p *FSPaths) TempManifestPath(fileToken uint64) string {
	return p.ManifestPath(fileToken) + ".tmp" // hyDFSDir/files/<fileToken>/manifest.json.tmp
}

// ChunksDir returns the chunks directory for a specific fileToken.
func (p *FSPaths) ChunksDir(fileToken uint64) string {
	return filepath.Join(p.FileDir(fileToken), "chunks") // hyDFSDir/files/<fileToken>/chunks
}

// FileDirsExist checks whether the file and chunks directories exist.
// Returns true only if both directories already exist.
func (p *FSPaths) FileDirsExist(fileToken uint64) (bool, error) {
	fileDir := p.FileDir(fileToken)
	chunksDir := p.ChunksDir(fileToken)

	for _, dir := range []string{fileDir, chunksDir} {
		info, err := os.Stat(dir)
		if err != nil {
			if os.IsNotExist(err) {
				return false, err // missing dir
			}
			return false, fmt.Errorf("stat dir %s: %w", dir, err)
		}
		if !info.IsDir() {
			return false, fmt.Errorf("%s exists but is not a directory", dir)
		}
	}
	return true, nil
}

// CreateFileDirs creates the file and chunks directories for the given fileToken.
// It does not check if they already exist.
func (p *FSPaths) CreateFileDirs(fileToken uint64) error {
	fileDir := p.FileDir(fileToken)
	chunksDir := p.ChunksDir(fileToken)
	for _, dir := range []string{fileDir, chunksDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("create dir %s: %w", dir, err)
		}
	}
	return nil
}

func copyTxtFiles(srcDir, dstDir string) error {
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		// If datasetDir doesn't exist, just no-op.
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if filepath.Ext(name) != ".txt" {
			continue
		}
		src := filepath.Join(srcDir, name)
		dst := filepath.Join(dstDir, name)
		if err := copyFile(src, dst); err != nil {
			return err
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst) // 0644
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	return err
}
