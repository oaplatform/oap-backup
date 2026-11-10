package storage

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"oap-backup/internal/config"
)

// S3 stores backups in an S3-compatible bucket (AWS S3, MinIO, Ceph RGW,
// Hetzner/Wasabi/...). Objects are keyed <s3_prefix>/<dir>/<name>.
type S3 struct {
	client *minio.Client
	cfg    config.S3
}

// NewS3 returns an S3 storage for cfg. With no static keys configured,
// credentials come from the standard AWS environment variables or the
// instance/pod IAM role.
func NewS3(cfg config.S3) (*S3, error) {
	creds := credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, "")
	if cfg.AccessKey == "" && cfg.SecretKey == "" {
		creds = credentials.NewChainCredentials([]credentials.Provider{
			&credentials.EnvAWS{},
			&credentials.IAM{},
		})
	}
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  creds,
		Secure: !cfg.Insecure,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("creating s3 client for %s: %w", cfg.Endpoint, err)
	}
	return &S3{client: client, cfg: cfg}, nil
}

func (s *S3) key(dir, name string) string {
	return strings.TrimPrefix(path.Join(s.cfg.Prefix, dir, name), "/")
}

// Upload implements Storage.
func (s *S3) Upload(ctx context.Context, dir, name, localPath string) (string, error) {
	key := s.key(dir, name)
	_, err := s.client.FPutObject(ctx, s.cfg.Bucket, key, localPath, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	location := fmt.Sprintf("s3://%s/%s", s.cfg.Bucket, key)
	if err != nil {
		return location, fmt.Errorf("s3 put %s: %w", location, err)
	}
	return location, nil
}

// List implements Storage.
func (s *S3) List(ctx context.Context, dir string) ([]string, error) {
	prefix := s.key(dir, "") + "/"
	var names []string
	for obj := range s.client.ListObjects(ctx, s.cfg.Bucket, minio.ListObjectsOptions{Prefix: prefix}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("s3 list s3://%s/%s: %w", s.cfg.Bucket, prefix, obj.Err)
		}
		name := strings.TrimPrefix(obj.Key, prefix)
		// Non-recursive listing returns sub-"directories" as common prefixes.
		if name == "" || strings.Contains(name, "/") {
			continue
		}
		names = append(names, name)
	}
	return names, nil
}

// Delete implements Storage.
func (s *S3) Delete(ctx context.Context, dir, name string) error {
	key := s.key(dir, name)
	if err := s.client.RemoveObject(ctx, s.cfg.Bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("s3 delete s3://%s/%s: %w", s.cfg.Bucket, key, err)
	}
	return nil
}
