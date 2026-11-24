// Package progress tracks how much data a running backup has sent to the
// backup storage.
package progress

import (
	"context"
	"sync"
)

// Snapshot is the progress of a backup at one moment.
type Snapshot struct {
	// UploadedBytes is everything the current attempt has sent to the
	// storage: the finished files plus the sent part of the file that is
	// uploading now. It is the size of the stored data, after compression.
	UploadedBytes int64 `json:"uploaded_bytes"`
	// FilesUploaded counts the files the current attempt has finished.
	FilesUploaded int `json:"files_uploaded"`
}

// Tracker collects the progress of one backup run. It is safe for concurrent
// use. A nil *Tracker is valid: its methods do nothing, so code that reports
// progress does not need to know whether anyone listens.
type Tracker struct {
	mu sync.Mutex
	// done is the size of the files finished by the current attempt.
	done  int64
	files int
	// size and sent describe the file that is uploading now.
	size, sent int64
}

// New returns an empty Tracker.
func New() *Tracker {
	return &Tracker{}
}

// Reset starts a new attempt: all counters go back to zero.
func (t *Tracker) Reset() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.done, t.files, t.size, t.sent = 0, 0, 0, 0
}

// StartUpload begins the upload of a file of size bytes. What a previous,
// unfinished upload sent is dropped.
func (t *Tracker) StartUpload(size int64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.size, t.sent = size, 0
}

// AddUploaded records n more bytes of the current file sent to the storage.
// A store may resend data after an error, so the count is capped at the
// file's size.
func (t *Tracker) AddUploaded(n int64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sent = min(t.sent+n, t.size)
}

// FileDone marks the current file as completely uploaded.
func (t *Tracker) FileDone() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.done += t.size
	t.files++
	t.size, t.sent = 0, 0
}

// Snapshot returns the current progress. ok is false for a nil Tracker.
func (t *Tracker) Snapshot() (snap Snapshot, ok bool) {
	if t == nil {
		return Snapshot{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return Snapshot{UploadedBytes: t.done + t.sent, FilesUploaded: t.files}, true
}

type contextKey struct{}

// NewContext returns ctx carrying t, for the code that uploads.
func NewContext(ctx context.Context, t *Tracker) context.Context {
	return context.WithValue(ctx, contextKey{}, t)
}

// FromContext returns the Tracker in ctx, or nil (which is usable).
func FromContext(ctx context.Context) *Tracker {
	t, _ := ctx.Value(contextKey{}).(*Tracker)
	return t
}
