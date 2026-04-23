package cache

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"jellyfin-cache/backend"
	"jellyfin-cache/config"
	"jellyfin-cache/union"
)

// Manager orchestrates the smart cache.  It is the primary entry point used
// by the VFS layer.
type Manager struct {
	cfg         config.CacheConfig
	db          *DB
	store       *Store
	union       *union.Union
	log         *slog.Logger
	passthrough map[string]bool // backend name → passthrough flag

	// Per-file download locks: prevents concurrent full downloads of the same file.
	dlMu    sync.Mutex
	dlLocks map[string]*downloadLock

	// Channel for queuing prefix prefetch jobs.
	prefetchCh  chan string
	downloadCh  chan downloadJob

	// Parsed max cache size in bytes.
	maxBytes int64

	// Stop all background workers.
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type downloadLock struct {
	done chan struct{}
	err  error
}

type downloadJob struct {
	path string
	ttl  time.Duration
}

// NewManager creates and starts a Manager.
func NewManager(cfg config.CacheConfig, u *union.Union, logger *slog.Logger) (*Manager, error) {
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating cache dir: %w", err)
	}

	db, err := OpenDB(filepath.Join(cfg.Dir, "meta.db"))
	if err != nil {
		return nil, err
	}

	store, err := NewStore(cfg.Dir)
	if err != nil {
		db.Close()
		return nil, err
	}

	maxBytes, err := parseSize(cfg.MaxSize)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("invalid max_size %q: %w", cfg.MaxSize, err)
	}

	pt := make(map[string]bool)
	for _, b := range u.Backends() {
		if b.Passthrough() {
			pt[b.Name()] = true
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		cfg:         cfg,
		db:          db,
		store:       store,
		union:       u,
		log:         logger,
		passthrough: pt,
		dlLocks:     make(map[string]*downloadLock),
		prefetchCh:  make(chan string, 1024),
		downloadCh:  make(chan downloadJob, 256),
		maxBytes:    maxBytes,
		cancel:      cancel,
	}

	// Start background workers.
	for i := 0; i < cfg.PrefetchWorkers; i++ {
		m.wg.Add(1)
		go m.prefetchWorker(ctx)
	}
	for i := 0; i < cfg.DownloadWorkers; i++ {
		m.wg.Add(1)
		go m.downloadWorker(ctx)
	}
	m.wg.Add(1)
	go m.evictLoop(ctx)

	// Queue prefix prefetch for all known files that are uncached.
	m.wg.Add(1)
	go m.bootstrapPrefetch(ctx)

	// Walk the remote to discover and register files not yet in the DB.
	m.wg.Add(1)
	go m.walkAndScan(ctx)

	return m, nil
}

// Close stops all background workers and flushes the database.
func (m *Manager) Close() error {
	m.cancel()
	m.wg.Wait()
	return m.db.Close()
}

// ---- Public API used by VFS ------------------------------------------------

// Stat returns metadata for path, preferring cached info from the DB.
// If not yet tracked, it fetches from the union and creates a DB record.
func (m *Manager) Stat(ctx context.Context, path string) (*FileRecord, error) {
	rec, err := m.db.Get(path)
	if err != nil {
		return nil, err
	}
	if rec != nil {
		// Records created by the background scan (walkDir) may be missing
		// RemoteName.  Populate it lazily so passthrough detection is correct.
		if rec.RemoteName == "" {
			if _, b, err2 := m.union.Stat(ctx, path); err2 == nil {
				rec.RemoteName = b.Name()
				rec.RemotePriority = b.Priority()
				_ = m.db.Put(rec) // best-effort; ignore error
			}
		}
		return rec, nil
	}
	// Not in DB yet – fetch from union and register.
	info, b, err := m.union.Stat(ctx, path)
	if err != nil {
		return nil, err
	}
	rec = &FileRecord{
		Path:           path,
		Size:           info.Size,
		ModTime:        info.ModTime,
		State:          StateUncached,
		Kind:           KindRemote,
		LastAccess:     time.Now(),
		RemoteName:     b.Name(),
		RemotePriority: b.Priority(),
	}
	if err := m.db.Put(rec); err != nil {
		return nil, err
	}
	// Trigger prefix prefetch in background.
	select {
	case m.prefetchCh <- path:
	default:
	}
	return rec, nil
}

// Open returns a Reader for path backed by the local cache where possible.
//
// On the first read that crosses the playback trigger threshold the full file
// is scheduled for download.
//
// If the file's remote is marked passthrough, the read is served directly from
// the union without touching the local cache.
func (m *Manager) Open(ctx context.Context, path string) (io.ReadSeekCloser, error) {
	rec, err := m.Stat(ctx, path)
	if err != nil {
		return nil, err
	}

	if m.passthrough[rec.RemoteName] {
		return m.union.Open(ctx, path)
	}

	_ = m.db.TouchAccess(path)

	return &CacheReader{
		ctx:          ctx,
		path:         path,
		size:         rec.Size,
		manager:      m,
		earlyTrigger: rec.State == StatePrefix || rec.State == StateDownloading,
	}, nil
}

// RegisterUpload records a newly uploaded file, giving it the extended TTL.
func (m *Manager) RegisterUpload(path string, size int64, modTime time.Time) error {
	ttl := m.cfg.UploadTTL.Duration
	rec := &FileRecord{
		Path:        path,
		Size:        size,
		ModTime:     modTime,
		State:       StateFull,
		Kind:        KindUpload,
		CachedBytes: size,
		LastAccess:  time.Now(),
		FullAt:      time.Now(),
		ExpiresAt:   time.Now().Add(ttl),
	}
	return m.db.Put(rec)
}

// LocationInfo describes where a file lives across the cache and remote tiers.
type LocationInfo struct {
	Path           string  `json:"path"`
	CacheState     State   `json:"cache_state"`      // uncached/prefix/downloading/full
	CachedBytes    int64   `json:"cached_bytes"`     // bytes on local disk right now
	Size           int64   `json:"size"`
	CachePercent   float64 `json:"cache_percent"`    // 0–100; live during downloading
	RemoteName     string  `json:"remote_name"`      // backend name from config
	RemotePriority int     `json:"remote_priority"`  // lower = higher priority
}

// locationFromRecord builds a LocationInfo from a DB record, using the on-disk
// file size for live progress when a download is in progress.
func (m *Manager) locationFromRecord(rec *FileRecord) *LocationInfo {
	cachedBytes := rec.CachedBytes
	// For an active download the DB's CachedBytes is stale (updated only on
	// completion).  Read the actual on-disk file size for live progress.
	if rec.State == StateDownloading {
		if live := m.store.Size(rec.Path); live > cachedBytes {
			cachedBytes = live
		}
	}
	var pct float64
	if rec.Size > 0 {
		pct = float64(cachedBytes) / float64(rec.Size) * 100
		if pct > 100 {
			pct = 100
		}
	} else if rec.State == StateFull {
		pct = 100
	}
	return &LocationInfo{
		Path:           rec.Path,
		CacheState:     rec.State,
		CachedBytes:    cachedBytes,
		Size:           rec.Size,
		CachePercent:   pct,
		RemoteName:     rec.RemoteName,
		RemotePriority: rec.RemotePriority,
	}
}

// Location returns the LocationInfo for a single path.
// Returns (nil, nil) if the path is not tracked in the DB.
func (m *Manager) Location(path string) (*LocationInfo, error) {
	rec, err := m.db.Get(path)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, nil
	}
	return m.locationFromRecord(rec), nil
}

// ListLocations returns LocationInfo for every tracked file.
func (m *Manager) ListLocations() ([]*LocationInfo, error) {
	recs, err := m.db.AllRecords()
	if err != nil {
		return nil, err
	}
	out := make([]*LocationInfo, len(recs))
	for i, rec := range recs {
		out[i] = m.locationFromRecord(rec)
	}
	return out, nil
}

// CacheFile enqueues a full background download for path.
// Returns an error if the path is not tracked in the DB.
func (m *Manager) CacheFile(path string) error {
	rec, err := m.db.Get(path)
	if err != nil {
		return err
	}
	if rec == nil {
		return fmt.Errorf("path %q not tracked", path)
	}
	if rec.State == StateFull {
		return nil // already cached
	}
	m.TriggerFullDownload(path)
	return nil
}

// EvictToPrefix truncates the cached data for path back to the prefix size,
// reverting the file to StatePrefix.  If the file is already at prefix or
// smaller, nothing happens.  Returns an error if the path is not tracked.
func (m *Manager) EvictToPrefix(path string) error {
	rec, err := m.db.Get(path)
	if err != nil {
		return err
	}
	if rec == nil {
		return fmt.Errorf("path %q not tracked", path)
	}
	switch rec.State {
	case StateUncached, StatePrefix:
		return nil // nothing to evict
	}
	prefixSize := m.cfg.PrefixBytes
	if rec.Size <= prefixSize {
		return nil // file is smaller than the prefix; keep it
	}
	if err := m.store.Truncate(rec.Path, prefixSize); err != nil {
		return fmt.Errorf("truncating %q: %w", path, err)
	}
	if err := m.db.UpdateState(rec.Path, StatePrefix, prefixSize, time.Time{}); err != nil {
		return fmt.Errorf("updating db for %q: %w", path, err)
	}
	m.log.Info("evicted to prefix", "path", path)
	return nil
}

// EvictCompletely removes all cached data for path and reverts it to
// StateUncached.  If the file is already uncached, nothing happens.
// Returns an error if the path is not tracked.
func (m *Manager) EvictCompletely(path string) error {
	rec, err := m.db.Get(path)
	if err != nil {
		return err
	}
	if rec == nil {
		return fmt.Errorf("path %q not tracked", path)
	}
	if rec.State == StateUncached {
		return nil // already gone
	}
	if err := m.store.Delete(rec.Path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("deleting cache data for %q: %w", path, err)
	}
	if err := m.db.UpdateState(rec.Path, StateUncached, 0, time.Time{}); err != nil {
		return fmt.Errorf("updating db for %q: %w", path, err)
	}
	m.log.Info("evicted completely", "path", path)
	return nil
}

// TriggerFullDownload enqueues a full download for path.  Safe to call
// multiple times; concurrent calls coalesce on the same lock.
func (m *Manager) TriggerFullDownload(path string) {
	select {
	case m.downloadCh <- downloadJob{path: path, ttl: m.cfg.FullTTL.Duration}:
	default:
		// channel full; will be retried on next read
	}
}

// ---- Prefix prefetch -------------------------------------------------------

func (m *Manager) bootstrapPrefetch(ctx context.Context) {
	defer m.wg.Done()
	recs, err := m.db.AllRecords()
	if err != nil {
		m.log.Error("bootstrap prefetch: db error", "err", err)
		return
	}
	for _, rec := range recs {
		select {
		case <-ctx.Done():
			return
		default:
		}
		switch rec.State {
		case StateUncached:
			if !rec.PrefetchDone && !m.passthrough[rec.RemoteName] {
				select {
				case m.prefetchCh <- rec.Path:
				case <-ctx.Done():
					return
				}
			}
		case StateDownloading:
			// Daemon was killed mid-download. Resume from the furthest point
			// already on disk rather than restarting from the prefix boundary.
			resumeFrom := rec.CachedBytes
			if onDisk := m.store.Size(rec.Path); onDisk > resumeFrom {
				resumeFrom = onDisk
			}
			if err := m.db.UpdateState(rec.Path, StatePrefix, resumeFrom, time.Time{}); err != nil {
				m.log.Warn("bootstrap: failed to reset interrupted download", "path", rec.Path, "err", err)
				continue
			}
			m.log.Info("resuming interrupted download", "path", rec.Path, "resume_offset", resumeFrom)
			select {
			case m.downloadCh <- downloadJob{path: rec.Path, ttl: m.cfg.FullTTL.Duration}:
			case <-ctx.Done():
				return
			}
		}
	}
}

// walkAndScan recursively lists all remotes, registers new files, and updates
// records for files that have migrated between backends.  It runs once at
// startup and then repeats at cfg.ScanInterval so that moved files are picked
// up automatically without a daemon restart.
func (m *Manager) walkAndScan(ctx context.Context) {
	defer m.wg.Done()

	doScan := func() {
		m.log.Info("scanning remotes for new/migrated files")
		queued := 0
		if err := m.walkDir(ctx, "", &queued); err != nil && ctx.Err() == nil {
			m.log.Warn("remote scan failed", "err", err)
			return
		}
		m.log.Info("remote scan complete", "files_queued", queued)
	}

	doScan()

	interval := m.cfg.ScanInterval.Duration
	if interval == 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			doScan()
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) walkDir(ctx context.Context, dir string, queued *int) error {
	entries, err := m.union.ListTagged(ctx, dir)
	if err != nil {
		if dir == "" {
			m.log.Error("remote scan: cannot list root directory — check that your rclone_path ends with a colon (e.g. \"storagebox-media:\") and the remote is reachable", "err", err)
		} else {
			m.log.Warn("remote scan: cannot list directory", "dir", dir, "err", err)
		}
		return nil // don't abort the whole scan for one failed directory
	}
	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if entry.IsDir {
			if err := m.walkDir(ctx, entry.Path, queued); err != nil {
				m.log.Warn("scan: failed listing dir", "path", entry.Path, "err", err)
			}
			continue
		}
		// File: register in DB if not already tracked.
		rec, err := m.db.Get(entry.Path)
		if err != nil {
			m.log.Warn("scan: db get failed", "path", entry.Path, "err", err)
			continue
		}
		if rec != nil {
			// If the file has migrated to a different backend, update the record so
			// passthrough detection and prefetch routing stay correct.
			if rec.RemoteName != "" && rec.RemoteName != entry.BackendName {
				m.log.Info("scan: file migrated backend",
					"path", entry.Path,
					"old", rec.RemoteName,
					"new", entry.BackendName)
				rec.RemoteName = entry.BackendName
				rec.RemotePriority = entry.BackendPriority
				if err := m.db.Put(rec); err != nil {
					m.log.Warn("scan: db update failed for migrated file", "path", entry.Path, "err", err)
				} else if rec.State == StateUncached && !m.passthrough[entry.BackendName] {
					select {
					case m.prefetchCh <- entry.Path:
					default:
					}
					*queued++
				}
			}
			continue
		}
		rec = &FileRecord{
			Path:           entry.Path,
			Size:           entry.Size,
			ModTime:        entry.ModTime,
			State:          StateUncached,
			Kind:           KindRemote,
			LastAccess:     time.Now(),
			RemoteName:     entry.BackendName,
			RemotePriority: entry.BackendPriority,
		}
		if err := m.db.Put(rec); err != nil {
			m.log.Warn("scan: db put failed", "path", entry.Path, "err", err)
			continue
		}
		select {
		case m.prefetchCh <- entry.Path:
		default:
		}
		*queued++
	}
	return nil
}

func (m *Manager) prefetchWorker(ctx context.Context) {
	defer m.wg.Done()
	for {
		select {
		case path := <-m.prefetchCh:
			if err := m.doPrefetch(ctx, path); err != nil {
				m.log.Warn("prefetch failed", "path", path, "err", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) doPrefetch(ctx context.Context, path string) error {
	rec, err := m.db.Get(path)
	if err != nil {
		return err
	}
	if rec == nil {
		return nil // not tracked yet
	}
	if rec.State != StateUncached {
		return nil // already cached
	}
	// RemoteName is empty for records created by the background scan (walkDir
	// doesn't resolve individual backends).  Look it up now so the passthrough
	// check below is accurate, and persist it so this only happens once.
	if rec.RemoteName == "" {
		if _, b, err2 := m.union.Stat(ctx, path); err2 == nil {
			rec.RemoteName = b.Name()
			rec.RemotePriority = b.Priority()
			_ = m.db.Put(rec)
		}
	}
	if m.passthrough[rec.RemoteName] {
		return nil // passthrough remote: never cache
	}
	if !m.hasSpace(m.cfg.PrefixBytes) {
		m.evictForSpace(m.cfg.PrefixBytes)
	}

	m.log.Info("prefetching prefix", "path", path, "prefix_bytes", m.cfg.PrefixBytes)

	rc, err := m.union.Open(ctx, path)
	if err != nil {
		return fmt.Errorf("opening %q for prefetch: %w", path, err)
	}
	defer rc.Close()

	written, err := m.store.Write(path, rc, 0, m.cfg.PrefixBytes)
	if err != nil && err != io.EOF {
		return fmt.Errorf("writing prefix for %q: %w", path, err)
	}

	rec.State = StatePrefix
	rec.CachedBytes = written
	rec.PrefetchDone = true
	if err := m.db.Put(rec); err != nil {
		return err
	}
	m.log.Info("prefetch complete", "path", path, "bytes", written)
	return nil
}

// ---- Full download ----------------------------------------------------------

func (m *Manager) downloadWorker(ctx context.Context) {
	defer m.wg.Done()
	for {
		select {
		case job := <-m.downloadCh:
			if err := m.doFullDownload(ctx, job); err != nil {
				m.log.Warn("full download failed", "path", job.path, "err", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) acquireDownloadLock(path string) (*downloadLock, bool) {
	m.dlMu.Lock()
	defer m.dlMu.Unlock()
	if lk, ok := m.dlLocks[path]; ok {
		return lk, false // already in progress
	}
	lk := &downloadLock{done: make(chan struct{})}
	m.dlLocks[path] = lk
	return lk, true
}

func (m *Manager) releaseDownloadLock(path string, err error) {
	m.dlMu.Lock()
	lk := m.dlLocks[path]
	delete(m.dlLocks, path)
	m.dlMu.Unlock()
	if lk != nil {
		lk.err = err
		close(lk.done)
	}
}

func (m *Manager) doFullDownload(ctx context.Context, job downloadJob) error {
	lk, acquired := m.acquireDownloadLock(job.path)
	if !acquired {
		<-lk.done // wait for the in-progress download
		return lk.err
	}
	var downloadErr error
	defer func() { m.releaseDownloadLock(job.path, downloadErr) }()

	rec, err := m.db.Get(job.path)
	if err != nil {
		downloadErr = err
		return err
	}
	if rec == nil || rec.State == StateFull {
		return nil
	}

	// Check/claim space.
	needed := rec.Size - rec.CachedBytes
	if !m.hasSpace(needed) {
		m.evictForSpace(needed)
	}

	// Mark as downloading.
	if err := m.db.UpdateState(job.path, StateDownloading, -1, time.Time{}); err != nil {
		downloadErr = err
		return err
	}

	rc, err := m.union.Open(ctx, job.path)
	if err != nil {
		_ = m.db.UpdateState(job.path, StatePrefix, -1, time.Time{})
		downloadErr = err
		return err
	}
	defer rc.Close()

	// Start after the already-cached prefix.
	startOffset := rec.CachedBytes
	if startOffset > 0 {
		if _, err := rc.Seek(startOffset, io.SeekStart); err != nil {
			startOffset = 0 // fallback: re-download from start
			if _, err2 := rc.Seek(0, io.SeekStart); err2 != nil {
				downloadErr = err2
				return err2
			}
		}
	}

	written, err := m.store.Write(job.path, rc, startOffset, -1)
	if err != nil && err != io.EOF {
		_ = m.db.UpdateState(job.path, StatePrefix, rec.CachedBytes, time.Time{})
		downloadErr = err
		return err
	}

	totalCached := startOffset + written
	expiresAt := time.Now().Add(job.ttl)
	downloadErr = m.db.UpdateState(job.path, StateFull, totalCached, expiresAt)
	if downloadErr == nil {
		m.log.Info("full download complete", "path", job.path,
			"bytes", totalCached, "expires", expiresAt.Format(time.RFC3339))
	}
	return downloadErr
}

// ---- Eviction --------------------------------------------------------------

func (m *Manager) evictLoop(ctx context.Context) {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.EvictInterval.Duration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.runEviction()
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) runEviction() {
	// 1. Revert expired FULL files back to PREFIX.
	recs, _ := m.db.All(StateFull)
	now := time.Now()
	for _, rec := range recs {
		if rec.ExpiresAt.IsZero() || now.Before(rec.ExpiresAt) {
			continue
		}
		prefixSize := m.cfg.PrefixBytes
		if rec.Size <= prefixSize {
			continue // small file; keep as-is
		}
		if err := m.store.Truncate(rec.Path, prefixSize); err != nil {
			m.log.Warn("truncate failed", "path", rec.Path, "err", err)
			continue
		}
		if err := m.db.UpdateState(rec.Path, StatePrefix, prefixSize, time.Time{}); err != nil {
			m.log.Warn("db update failed on expire", "path", rec.Path, "err", err)
		} else {
			m.log.Info("expired full cache → prefix", "path", rec.Path)
		}
	}

	// 2. If still over quota, evict least-recently-accessed prefix caches.
	total, _ := m.db.TotalCachedBytes()
	if total <= m.maxBytes {
		return
	}
	m.evictForSpace(0)
}

func (m *Manager) hasSpace(needed int64) bool {
	total, _ := m.db.TotalCachedBytes()
	return total+needed <= m.maxBytes
}

func (m *Manager) evictForSpace(needed int64) {
	// Collect eviction candidates: PREFIX files sorted by oldest access first.
	recs, _ := m.db.All(StatePrefix)
	// Sort oldest last-access first.
	sortByLRU(recs)
	for _, rec := range recs {
		total, _ := m.db.TotalCachedBytes()
		if total+needed <= m.maxBytes {
			break
		}
		if err := m.store.Delete(rec.Path); err != nil {
			m.log.Warn("evict delete failed", "path", rec.Path, "err", err)
			continue
		}
		if err := m.db.UpdateState(rec.Path, StateUncached, 0, time.Time{}); err != nil {
			m.log.Warn("evict db update failed", "path", rec.Path, "err", err)
		} else {
			m.log.Info("evicted prefix cache", "path", rec.Path)
		}
	}
}

func sortByLRU(recs []*FileRecord) {
	// Insertion sort is fine for small slices.
	for i := 1; i < len(recs); i++ {
		for j := i; j > 0 && recs[j].LastAccess.Before(recs[j-1].LastAccess); j-- {
			recs[j], recs[j-1] = recs[j-1], recs[j]
		}
	}
}

// ---- Helpers ---------------------------------------------------------------

// parseSize parses a human-readable size string like "200GB" into bytes.
func parseSize(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty size string")
	}
	suffixes := []struct {
		suffix string
		mult   int64
	}{
		{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10},
		{"TB", 1e12}, {"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3},
		{"T", 1e12}, {"G", 1e9}, {"M", 1e6}, {"K", 1e3},
	}
	for _, sfx := range suffixes {
		if len(s) > len(sfx.suffix) && s[len(s)-len(sfx.suffix):] == sfx.suffix {
			n := int64(0)
			if _, err := fmt.Sscanf(s[:len(s)-len(sfx.suffix)], "%d", &n); err != nil {
				return 0, err
			}
			return n * sfx.mult, nil
		}
	}
	var n int64
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return 0, fmt.Errorf("cannot parse size %q", s)
	}
	return n, nil
}

// ---- NFS handle persistence (delegated to DB) ------------------------------

func (m *Manager) LoadNFSHandles() ([][16]byte, [][]string, error) {
	return m.db.LoadNFSHandles()
}
func (m *Manager) SaveNFSHandle(id [16]byte, path []string) error {
	return m.db.SaveNFSHandle(id, path)
}
func (m *Manager) DeleteNFSHandle(id [16]byte) error {
	return m.db.DeleteNFSHandle(id)
}

// ReadThrough opens the remote directly, bypassing the cache.
// Used as a fallback when the cache is unavailable.
func (m *Manager) ReadThrough(ctx context.Context, path string) (io.ReadSeekCloser, error) {
	return m.union.Open(ctx, path)
}

// List returns the merged directory listing (delegated to union).
func (m *Manager) List(ctx context.Context, dir string) ([]backend.Info, error) {
	return m.union.List(ctx, dir)
}

// Mkdir creates a directory on the primary writable backend.
func (m *Manager) Mkdir(ctx context.Context, path string) error {
	return m.union.Mkdir(ctx, path)
}

// Put writes a file through to the union and registers it as an upload.
func (m *Manager) Put(ctx context.Context, path string, r io.Reader, modTime time.Time, size int64) error {
	// Tee to both the store and the union.
	pr, pw := io.Pipe()
	var writeErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, writeErr = m.store.Write(path, pr, 0, -1)
	}()

	tr := io.TeeReader(r, pw)
	unionErr := m.union.Put(ctx, path, tr, modTime, size)
	pw.CloseWithError(unionErr)
	<-done

	if unionErr != nil {
		return unionErr
	}
	if writeErr != nil {
		m.log.Warn("failed to cache uploaded file locally", "path", path, "err", writeErr)
	}
	return m.RegisterUpload(path, size, modTime)
}
