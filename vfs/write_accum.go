package vfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	billy "github.com/go-git/go-billy/v5"

	"jellyfin-cache/cache"
)

const (
	writeFlushIdle = 5 * time.Second
	writeFlushTick = 2 * time.Second
)

// writeAccumulator collects NFS WRITE RPCs for the same path into a single
// temp file and flushes to the backend after a period of write inactivity.
//
// go-nfs calls OpenFile → Write → Close for EVERY NFS WRITE RPC. Without
// this, each WRITE would create a new temp file and overwrite the destination
// with only that chunk; non-zero offsets also fail because writeFile.Seek
// returns ErrNotSupported.
type writeAccumulator struct {
	mu      sync.Mutex
	entries map[string]*accumEntry
}

type accumEntry struct {
	tmp       *os.File
	path      string
	lastWrite time.Time
	mgr       *cache.Manager
	ctx       context.Context
}

func newWriteAccumulator() *writeAccumulator {
	return &writeAccumulator{entries: make(map[string]*accumEntry)}
}

// open returns a write handle for path. When create is true, the accumulated
// data for that path is discarded and a fresh temp file is started (mirrors
// O_TRUNC / O_CREAT semantics from NFS CREATE).
func (a *writeAccumulator) open(ctx context.Context, mgr *cache.Manager, path string, create bool) (*accumFile, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	e, ok := a.entries[path]
	if !ok {
		tmp, err := os.CreateTemp("", "jellyfin-nfs-write-*")
		if err != nil {
			return nil, fmt.Errorf("write temp file for %q: %w", path, err)
		}
		e = &accumEntry{tmp: tmp, path: path, mgr: mgr, ctx: ctx}
		a.entries[path] = e
	} else if create {
		// Re-creation: reset the temp file instead of allocating a new one.
		if err := e.tmp.Truncate(0); err == nil {
			_, _ = e.tmp.Seek(0, io.SeekStart)
		}
	}
	e.lastWrite = time.Now()
	return &accumFile{entry: e, acc: a}, nil
}

// stat returns the current accumulated size for path, or -1 if no in-progress
// write exists.  Used by vfs.Stat so the WRITE handler's pre-op stat succeeds
// even before the file is flushed to the backend.
func (a *writeAccumulator) stat(path string) int64 {
	a.mu.Lock()
	e, ok := a.entries[path]
	a.mu.Unlock()
	if !ok {
		return -1
	}
	info, err := e.tmp.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

func (a *writeAccumulator) flushLoop(ctx context.Context) {
	t := time.NewTicker(writeFlushTick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			a.flushAll()
			return
		case <-t.C:
			a.flushIdle()
		}
	}
}

func (a *writeAccumulator) flushIdle() {
	a.mu.Lock()
	var ready []*accumEntry
	for path, e := range a.entries {
		if time.Since(e.lastWrite) >= writeFlushIdle {
			ready = append(ready, e)
			delete(a.entries, path)
		}
	}
	a.mu.Unlock()
	for _, e := range ready {
		flushEntry(e)
	}
}

func (a *writeAccumulator) flushAll() {
	a.mu.Lock()
	ready := make([]*accumEntry, 0, len(a.entries))
	for _, e := range a.entries {
		ready = append(ready, e)
	}
	a.entries = make(map[string]*accumEntry)
	a.mu.Unlock()
	for _, e := range ready {
		flushEntry(e)
	}
}

func flushEntry(e *accumEntry) {
	defer os.Remove(e.tmp.Name())
	defer e.tmp.Close()

	size, err := e.tmp.Seek(0, io.SeekEnd)
	if err != nil || size == 0 {
		return
	}
	if _, err := e.tmp.Seek(0, io.SeekStart); err != nil {
		return
	}
	_ = e.mgr.Put(e.ctx, e.path, e.tmp, time.Now(), size)
}

// accumFile is a write-only billy.File backed by a shared accumEntry temp file.
type accumFile struct {
	entry *accumEntry
	acc   *writeAccumulator
}

func (f *accumFile) Name() string                          { return f.entry.path }
func (f *accumFile) Read(_ []byte) (int, error)            { return 0, billy.ErrNotSupported }
func (f *accumFile) ReadAt(_ []byte, _ int64) (int, error) { return 0, billy.ErrNotSupported }
func (f *accumFile) Lock() error                           { return nil }
func (f *accumFile) Unlock() error                         { return nil }

func (f *accumFile) Write(p []byte) (int, error) {
	n, err := f.entry.tmp.Write(p)
	if err == nil {
		f.entry.lastWrite = time.Now()
	}
	return n, err
}

func (f *accumFile) Seek(offset int64, whence int) (int64, error) {
	return f.entry.tmp.Seek(offset, whence)
}

func (f *accumFile) Truncate(size int64) error {
	return f.entry.tmp.Truncate(size)
}

// Close is a no-op — the accumulator owns the temp file lifetime and flushes
// after write inactivity, not when individual RPC handles are released.
func (f *accumFile) Close() error { return nil }
