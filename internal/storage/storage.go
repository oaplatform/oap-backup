// Package storage implements the remote backup stores (FTP and
// S3-compatible object storage).
package storage

import (
	"context"
	"fmt"
	"io"
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
	// progress, if not nil, is called while the file is being sent, each
	// time with the number of bytes sent since the previous call.
	//
	// Upload stores the local file localPath as <root>/<dir>/<name>,
	// creating dir if needed, and returns a printable remote location.
	Upload(ctx context.Context, dir, name, localPath string, progress func(n int64)) (string, error)
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

// progressReader reports what is read through it to fn. It serves both as a
// wrapper around the uploaded file (FTP) and as the bare progress hook of
// minio-go (S3), which calls Read with a buffer as long as the data it sent.
type progressReader struct {
	// r is the wrapped reader; nil for a bare hook.
	r  io.Reader
	fn func(n int64)
}

func (p *progressReader) Read(b []byte) (int, error) {
	n := len(b)
	var err error
	if p.r != nil {
		n, err = p.r.Read(b)
	}
	if n > 0 {
		p.fn(int64(n))
	}
	return n, err
}
