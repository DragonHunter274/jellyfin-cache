// Package testutil provides shared test helpers, including an in-memory
// backend that implements backend.Backend without any network or disk I/O.
package testutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"jellyfin-cache/backend"
)

// MemFile holds the contents of a single in-memory file.
type MemFile struct {
	Data    []byte
	ModTime time.Time
}

// MemBackend is a thread-safe in-memory Backend implementation for tests.
type MemBackend struct {
	mu       sync.RWMutex
	name     string
	files    map[string]*MemFile
	priority int
	readOnly bool
}

// NewMemBackend creates an empty MemBackend.
func NewMemBackend(name string, priority int, readOnly bool) *MemBackend {
	return &MemBackend{
		name:     name,
		files:    make(map[string]*MemFile),
		priority: priority,
		readOnly: readOnly,
	}
}

// AddFile inserts a file with the given content into the backend.
func (b *MemBackend) AddFile(path string, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.files[path] = &MemFile{Data: data, ModTime: time.Now()}
}

// GetFile returns the raw bytes stored for path (for assertions in tests).
func (b *MemBackend) GetFile(path string) ([]byte, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	f, ok := b.files[path]
	if !ok {
		return nil, false
	}
	cp := make([]byte, len(f.Data))
	copy(cp, f.Data)
	return cp, true
}

func (b *MemBackend) Name() string     { return b.name }
func (b *MemBackend) ReadOnly() bool   { return b.readOnly }
func (b *MemBackend) Priority() int    { return b.priority }

func (b *MemBackend) List(_ context.Context, dir string) ([]backend.Info, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var out []backend.Info
	seen := make(map[string]bool)
	for path, f := range b.files {
		if !strings.HasPrefix(path, dir) {
			continue
		}
		rel := strings.TrimPrefix(path, dir)
		rel = strings.TrimPrefix(rel, "/")
		if rel == "" {
			continue
		}
		// Immediate children only.
		parts := strings.SplitN(rel, "/", 2)
		entry := parts[0]
		isDir := len(parts) == 2
		fullPath := strings.TrimPrefix(dir+"/"+entry, "/")
		if seen[fullPath] {
			continue
		}
		seen[fullPath] = true
		info := backend.Info{
			Name:    entry,
			Path:    fullPath,
			IsDir:   isDir,
			ModTime: f.ModTime,
		}
		if !isDir {
			info.Size = int64(len(f.Data))
		}
		out = append(out, info)
	}
	return out, nil
}

func (b *MemBackend) Stat(_ context.Context, path string) (*backend.Info, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	f, ok := b.files[path]
	if !ok {
		return nil, fmt.Errorf("not found: %q", path)
	}
	return &backend.Info{
		Name:    lastName(path),
		Path:    path,
		Size:    int64(len(f.Data)),
		ModTime: f.ModTime,
	}, nil
}

func (b *MemBackend) Open(_ context.Context, path string) (io.ReadSeekCloser, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	f, ok := b.files[path]
	if !ok {
		return nil, fmt.Errorf("not found: %q", path)
	}
	// Copy so concurrent readers don't share state.
	cp := make([]byte, len(f.Data))
	copy(cp, f.Data)
	return &readSeekNopCloser{Reader: bytes.NewReader(cp)}, nil
}

type readSeekNopCloser struct{ *bytes.Reader }

func (r *readSeekNopCloser) Close() error { return nil }

func (b *MemBackend) Put(_ context.Context, path string, r io.Reader, modTime time.Time, _ int64) error {
	if b.readOnly {
		return fmt.Errorf("read-only")
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.files[path] = &MemFile{Data: data, ModTime: modTime}
	return nil
}

func (b *MemBackend) Remove(_ context.Context, path string) error {
	if b.readOnly {
		return fmt.Errorf("read-only")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.files, path)
	return nil
}

func (b *MemBackend) Mkdir(_ context.Context, _ string) error { return nil }

func lastName(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[i+1:]
		}
	}
	return path
}
