package storage

import (
	"io"
	"os"
	"path/filepath"
)

// DiskStorage implements core.Storage using the local filesystem.
type DiskStorage struct {
	BaseDir string
}

// NewDiskStorage creates a DiskStorage rooted at the given directory.
func NewDiskStorage(baseDir string) *DiskStorage {
	if baseDir == "" {
		baseDir = "."
	}
	_ = os.MkdirAll(baseDir, 0755)
	return &DiskStorage{BaseDir: baseDir}
}

// Read opens a file for reading.
func (d *DiskStorage) Read(path string) (io.ReadCloser, error) {
	fullPath := filepath.Join(d.BaseDir, path)
	return os.Open(fullPath)
}

// Write creates or overwrites the file.
func (d *DiskStorage) Write(path string, r io.Reader) error {
	fullPath := filepath.Join(d.BaseDir, path)
	_ = os.MkdirAll(filepath.Dir(fullPath), 0755)

	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, r)
	return err
}

// AtomicWrite writes to a temp file and atomically renames.
func (d *DiskStorage) AtomicWrite(path string, r io.Reader) error {
	fullPath := filepath.Join(d.BaseDir, path)
	dir := filepath.Dir(fullPath)
	_ = os.MkdirAll(dir, 0755)

	tmpFile, err := os.CreateTemp(dir, "mr-atomic-*")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
	}()

	if _, err := io.Copy(tmpFile, r); err != nil {
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}

	return os.Rename(tmpPath, fullPath)
}

// List returns all relative paths matching the given prefix glob.
func (d *DiskStorage) List(prefix string) ([]string, error) {
	pattern := filepath.Join(d.BaseDir, prefix+"*")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	var relPaths []string
	for _, m := range matches {
		rel, err := filepath.Rel(d.BaseDir, m)
		if err == nil {
			relPaths = append(relPaths, rel)
		}
	}
	return relPaths, nil
}

// Remove deletes the file, ignoring ErrNotExist.
func (d *DiskStorage) Remove(path string) error {
	fullPath := filepath.Join(d.BaseDir, path)
	err := os.Remove(fullPath)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}
