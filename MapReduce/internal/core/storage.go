package core

import "io"

// Storage defines the contract for reading and writing files.
// This is the dependency inversion boundary — coordinator and worker code
// depends on this interface, never on concrete implementations.
//
// Implementations:
//   - internal/storage/disk.go   → local filesystem (testing, single-node)
//   - internal/storage/s3.go     → S3/MinIO (distributed, multi-node)
type Storage interface {
	// Read returns a reader for the file at the given path.
	Read(path string) (io.ReadCloser, error)

	// Write creates or overwrites the file at path with data from the reader.
	Write(path string, r io.Reader) error

	// AtomicWrite writes data to a temp location and atomically renames to path.
	// This prevents readers from observing partially written files.
	AtomicWrite(path string, r io.Reader) error

	// List returns all file paths matching the given prefix/glob.
	List(prefix string) ([]string, error)

	// Remove deletes the file at path. No error if the file does not exist.
	Remove(path string) error
}
