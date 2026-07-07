package storage

import (
	"context"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Storage implements core.Storage using an S3-compatible object store.
type S3Storage struct {
	client *minio.Client
	bucket string
}

// NewS3Storage creates an S3Storage client for the given bucket and endpoint.
func NewS3Storage(endpoint, bucket, accessKey, secretKey string) (*S3Storage, error) {
	// minio.New needs options
	useSSL := false // In typical internal cluster setups (e.g., local MinIO), TLS is off
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		err = client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
		if err != nil {
			return nil, err
		}
	}

	return &S3Storage{
		client: client,
		bucket: bucket,
	}, nil
}

// Read returns a reader for the file at the given path.
func (s *S3Storage) Read(path string) (io.ReadCloser, error) {
	return s.client.GetObject(context.Background(), s.bucket, path, minio.GetObjectOptions{})
}

// Write creates or overwrites the file at path with data from the reader.
func (s *S3Storage) Write(path string, r io.Reader) error {
	// If size is unknown (-1), minio PutObject requires a part size to be set or chunked upload.
	// We use -1 and a generic PutObjectOptions which minio-go handles.
	_, err := s.client.PutObject(context.Background(), s.bucket, path, r, -1, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	return err
}

// AtomicWrite writes data to S3. Since S3 writes are already atomic, it delegates to Write.
func (s *S3Storage) AtomicWrite(path string, r io.Reader) error {
	return s.Write(path, r)
}

// List returns all file paths matching the given prefix.
func (s *S3Storage) List(prefix string) ([]string, error) {
	var list []string
	opts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}

	for obj := range s.client.ListObjects(context.Background(), s.bucket, opts) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		list = append(list, obj.Key)
	}

	return list, nil
}

// Remove deletes the file at path.
func (s *S3Storage) Remove(path string) error {
	return s.client.RemoveObject(context.Background(), s.bucket, path, minio.RemoveObjectOptions{})
}
