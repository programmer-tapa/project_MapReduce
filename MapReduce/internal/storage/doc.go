// Package storage provides concrete implementations of the core.Storage interface.
//
// Implementations:
//   - DiskStorage: local filesystem (for testing, single-node, and intermediate files)
//   - S3Storage: S3-compatible object storage via MinIO/AWS SDK (for distributed deployments)
//
// The worker and coordinator never import this package directly — they depend
// on the core.Storage interface. Concrete storage is injected at startup via
// the cmd/ entrypoints.
package storage
