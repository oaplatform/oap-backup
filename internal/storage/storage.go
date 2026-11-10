// Package storage implements the remote backup stores (FTP and
// S3-compatible object storage).
package storage

import (
	"context"
	"fmt"

	"oap-backup/internal/config"
)

// Storage is a remote location that holds backup files, organised as
// <root>/<dir>/<name>. dir is a backup target's file folder (slash
// separated, without leading or trailing slashes).
type Storage interface {
	// Upload stores the local file localPath as <root>/<dir>/<name>,
	// creating dir if needed, and returns a printable remote location.
	Upload(ctx context.Context, dir, name, localPath string) (string, error)
	// List returns the base names of the files directly inside <root>/<dir>.
	// A missing or unreadable dir yields an empty list and no error.
	List(ctx context.Context, dir string) ([]string, error)
	// Delete removes <root>/<dir>/<name>.
	Delete(ctx context.Context, dir, name string) error
}

// New builds the Storage selected by cfg.Storage.
func New(cfg config.Config) (Storage, error) {
	switch cfg.Storage {
	case config.StorageFTP:
		return NewFTP(cfg.FTP), nil
	case config.StorageS3:
		return NewS3(cfg.S3)
	default:
		return nil, fmt.Errorf("unsupported storage %q", cfg.Storage)
	}
}
