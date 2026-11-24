// Package storage implements the remote backup stores (FTP and
// S3-compatible object storage).
package storage

import (
	"context"
	"fmt"
	"time"

	"oap-backup/internal/config"
)

// File describes one stored file.
type File struct {
	Name string
	Size int64
	// ModTime is when the store last wrote the file; zero if unknown.
	ModTime time.Time
}

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
	// ListFiles is List with the size and modification time of every file.
	ListFiles(ctx context.Context, dir string) ([]File, error)
	// Delete removes <root>/<dir>/<name>.
	Delete(ctx context.Context, dir, name string) error
	// ListDirs returns the base names of the folders directly inside
	// <root>/<dir>. A missing or unreadable dir yields an empty list and no
	// error.
	ListDirs(ctx context.Context, dir string) ([]string, error)
	// RemoveDir removes the empty folder <root>/<dir>. On stores without
	// real folders (S3) an empty folder does not exist and this is a no-op.
	RemoveDir(ctx context.Context, dir string) error
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
