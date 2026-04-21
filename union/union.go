// Package union merges multiple Backend sources into a single filesystem
// view, similar to mergerfs or rclone's union remote.
//
// Resolution rules:
//   - For reads, the backend with the lowest Priority number that has the
//     file wins.  Ties are broken by backend order in the config.
//   - For writes, the first backend (lowest Priority) that is not read-only
//     receives the write.
//   - Directory listings are merged across all backends; duplicate paths are
//     deduplicated using the same priority ordering.
package union

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"jellyfin-cache/backend"
)

// Union presents multiple backends as one.
type Union struct {
	backends []backend.Backend // sorted by Priority ascending
}

// New creates a Union from the provided backends.
func New(backends []backend.Backend) *Union {
	sorted := make([]backend.Backend, len(backends))
	copy(sorted, backends)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Priority() < sorted[j].Priority()
	})
	return &Union{backends: sorted}
}

// Backends returns the sorted backend list (read-only).
func (u *Union) Backends() []backend.Backend { return u.backends }

// TaggedInfo is an Info entry annotated with its source backend.
type TaggedInfo struct {
	backend.Info
	BackendName     string
	BackendPriority int
	Passthrough     bool
}

// ListTagged merges directory listings from all backends and retains the
// source backend metadata for each entry so callers can record RemoteName
// without a follow-up Stat call.
// Files that appear on multiple backends are deduplicated: the entry from
// the highest-priority backend (lowest Priority number) wins.
func (u *Union) ListTagged(ctx context.Context, dir string) ([]TaggedInfo, error) {
	type result struct {
		b     backend.Backend
		infos []backend.Info
		err   error
	}
	results := make([]result, len(u.backends))
	var wg sync.WaitGroup
	for i, b := range u.backends {
		wg.Add(1)
		go func(idx int, b backend.Backend) {
			defer wg.Done()
			infos, err := b.List(ctx, dir)
			results[idx] = result{b: b, infos: infos, err: err}
		}(i, b)
	}
	wg.Wait()

	// Merge: highest-priority (index 0) wins on collision.
	type entry struct {
		info backend.Info
		b    backend.Backend
	}
	seen := make(map[string]entry)
	var lastErr error
	anyOK := false
	for _, r := range results {
		if r.err != nil {
			lastErr = r.err
			continue
		}
		anyOK = true
		for _, info := range r.infos {
			if _, exists := seen[info.Path]; !exists {
				seen[info.Path] = entry{info: info, b: r.b}
			}
		}
	}
	if !anyOK && lastErr != nil {
		return nil, lastErr
	}

	merged := make([]TaggedInfo, 0, len(seen))
	for _, e := range seen {
		merged = append(merged, TaggedInfo{
			Info:            e.info,
			BackendName:     e.b.Name(),
			BackendPriority: e.b.Priority(),
			Passthrough:     e.b.Passthrough(),
		})
	}
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Name < merged[j].Name
	})
	return merged, nil
}

// List merges directory listings from all backends.
// Files that appear on multiple backends are deduplicated: the entry from
// the highest-priority backend wins.
// Returns an error only when every backend fails; a partial result from at
// least one healthy backend is returned without error.
func (u *Union) List(ctx context.Context, dir string) ([]backend.Info, error) {
	tagged, err := u.ListTagged(ctx, dir)
	if err != nil {
		return nil, err
	}
	infos := make([]backend.Info, len(tagged))
	for i, t := range tagged {
		infos[i] = t.Info
	}
	return infos, nil
}

// Stat returns the Info for path from the highest-priority backend that has it.
func (u *Union) Stat(ctx context.Context, path string) (*backend.Info, backend.Backend, error) {
	for _, b := range u.backends {
		info, err := b.Stat(ctx, path)
		if err == nil {
			return info, b, nil
		}
	}
	return nil, nil, fmt.Errorf("path %q not found on any backend", path)
}

// Open opens path from the highest-priority backend that has it.
func (u *Union) Open(ctx context.Context, path string) (io.ReadSeekCloser, error) {
	for _, b := range u.backends {
		rc, err := b.Open(ctx, path)
		if err == nil {
			return rc, nil
		}
	}
	return nil, fmt.Errorf("path %q not found on any backend", path)
}

// WriteBackend returns the first writable backend (lowest priority number).
// Returns an error if all backends are read-only.
func (u *Union) WriteBackend() (backend.Backend, error) {
	for _, b := range u.backends {
		if !b.ReadOnly() {
			return b, nil
		}
	}
	return nil, fmt.Errorf("no writable backend available")
}

// Put writes data to the primary writable backend.
func (u *Union) Put(ctx context.Context, path string, r io.Reader, modTime time.Time, size int64) error {
	b, err := u.WriteBackend()
	if err != nil {
		return err
	}
	return b.Put(ctx, path, r, modTime, size)
}

// Remove removes path from all backends where it exists.
func (u *Union) Remove(ctx context.Context, path string) error {
	var lastErr error
	removed := 0
	for _, b := range u.backends {
		if b.ReadOnly() {
			continue
		}
		if err := b.Remove(ctx, path); err == nil {
			removed++
		} else {
			lastErr = err
		}
	}
	if removed == 0 && lastErr != nil {
		return fmt.Errorf("remove %q: %w", path, lastErr)
	}
	return nil
}

// Mkdir creates path on the primary writable backend.
func (u *Union) Mkdir(ctx context.Context, path string) error {
	b, err := u.WriteBackend()
	if err != nil {
		return err
	}
	return b.Mkdir(ctx, path)
}
