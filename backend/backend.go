// Package backend defines the filesystem abstraction used throughout
// jellyfin-cache and provides an rclone-backed implementation.
//
// The interface is intentionally narrow – it covers the operations that
// jellyfin-cache actually needs rather than the full POSIX surface.
package backend

import (
	"context"
	"io"
	"time"
)

// Info describes a single file or directory entry.
type Info struct {
	Name    string
	Path    string // full path relative to the remote root
	Size    int64
	ModTime time.Time
	IsDir   bool
	MimeType string
}

// Backend is the interface every remote source must satisfy.
type Backend interface {
	// Name returns the human-readable name from config.
	Name() string

	// List returns the immediate children of dir (empty string = root).
	List(ctx context.Context, dir string) ([]Info, error)

	// Stat returns metadata for a single path.
	Stat(ctx context.Context, path string) (*Info, error)

	// Open returns a ReadSeekCloser for the given file path.
	Open(ctx context.Context, path string) (io.ReadSeekCloser, error)

	// Put uploads r as a new or replaced file at path, with the given
	// modification time and size hint (-1 if unknown).
	Put(ctx context.Context, path string, r io.Reader, modTime time.Time, size int64) error

	// Remove deletes the file or empty directory at path.
	Remove(ctx context.Context, path string) error

	// Mkdir creates a directory (and any missing parents).
	Mkdir(ctx context.Context, path string) error

	// ReadOnly reports whether this backend rejects writes.
	ReadOnly() bool

	// Priority returns the union resolution priority (lower = higher priority).
	Priority() int

	// Passthrough reports whether this backend bypasses the cache: reads are
	// served directly from the remote and no data is ever stored locally.
	Passthrough() bool
}
