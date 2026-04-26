package cache

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"jellyfin-cache/backend"
	"jellyfin-cache/config"
	"jellyfin-cache/union"
)

// opCounters tracks interesting operation counts for periodic diagnostics.
// All fields are accessed atomically.
type opCounters struct {
	statTotal   atomic.Int64 // total mgr.Stat calls
	statRemote  atomic.Int64 // subset that hit the remote (no DB record or empty RemoteName)
	openRemote  atomic.Int64 // CacheReader remote-fallback opens
	listMiss    atomic.Int64 // dir-cache misses that fetched from remote
}

// Manager orchestrates the smart cache.  It is the primary entry point used
// by the VFS layer.
type Manager struct {
	cfg         config.CacheConfig
	db          *DB
	store       *Store
	union       *union.Union
	log         *slog.Logger
	passthrough map[string]bool // backend name → passthrough flag

	// In-memory directory listing cache (stale-while-revalidate).
	dirMu         sync.RWMutex
	dirCache      map[string]dirCacheEntry
	dirRefreshing map[string]bool // dirs with a background refresh in flight
	knownDirs     map[string]bool // paths confirmed to be directories (never expires)

	// Negative cache: paths confirmed absent from all backends.
	//
	// Jellyfin probes for many sidecar files (*.nfo, poster.jpg, fanart.jpg,
	// season.nfo, tvshow.nfo, …) in every directory even when they don't exist
	// on the remote.  Without this cache, each probe triggers two remote
	// round-trips (union.Stat + mgr.List fallback) at ~200 ms each, turning a
	// 500-episode library scan into a 10-15 minute ordeal.
	negMu    sync.RWMutex
	negCache map[string]time.Time // path → expiry

	// Semaphore bounding concurrent remote directory listings (scan + eager warm).
	scanSem chan struct{}

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

	// Diagnostic counters — snapshot-logged periodically so the user can see
	// whether remote calls are happening during a library scan.
	ops opCounters
}

type dirCacheEntry struct {
	infos   []backend.Info
	expires time.Time
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
		dirCache:      make(map[string]dirCacheEntry),
		dirRefreshing: make(map[string]bool),
		knownDirs:     make(map[string]bool),
		negCache:      make(map[string]time.Time),
		scanSem:       make(chan struct{}, cfg.ScanWorkers),
		dlLocks:     make(map[string]*downloadLock),
		prefetchCh:  make(chan string, 1024),
		downloadCh:  make(chan downloadJob, 256),
		maxBytes:    maxBytes,
		cancel:      cancel,
	}

	// Pre-warm the in-memory dir cache from the previous run so the first
	// Jellyfin scan after a restart doesn't block on remote listings.
	m.loadPersistedDirCache()

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

	// Periodically log operation counters so users can diagnose whether
	// remote calls are happening during Jellyfin library scans.
	m.wg.Add(1)
	go m.opsLogLoop(ctx)

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
// Returns an error (without a remote call) for paths confirmed to be
// directories by any prior listing.
func (m *Manager) Stat(ctx context.Context, path string) (*FileRecord, error) {
	m.ops.statTotal.Add(1)

	// Fast path: already confirmed as a directory by a prior listing.
	// backend.Stat answers "is this a directory?" by listing its contents,
	// so we must short-circuit before reaching union.Stat.
	if m.IsKnownDir(path) {
		return nil, fmt.Errorf("%q is a directory", path)
	}
	rec, err := m.db.Get(path)
	if err != nil {
		return nil, err
	}
	if rec != nil {
		// Records created by the background scan (walkDir) may be missing
		// RemoteName.  Populate it lazily so passthrough detection is correct.
		if rec.RemoteName == "" {
			m.ops.statRemote.Add(1)
			m.log.Info("stat: remote lookup for missing RemoteName", "path", path)
			if _, b, err2 := m.union.Stat(ctx, path); err2 == nil {
				rec.RemoteName = b.Name()
				rec.RemotePriority = b.Priority()
				_ = m.db.Put(rec) // best-effort; ignore error
			}
		}
		return rec, nil
	}
	// Not in DB: check the negative cache before making a remote call.
	// Jellyfin probes for many sidecar files that don't exist; the neg cache
	// prevents a round-trip per probe after the first miss.
	if m.isNegCached(path) {
		return nil, os.ErrNotExist
	}
	// Not in DB, not neg-cached – fetch from union and register.
	m.ops.statRemote.Add(1)
	m.log.Info("stat: remote lookup for untracked path", "path", path)
	info, b, err := m.union.Stat(ctx, path)
	if err != nil {
		m.addNegCache(path)
		return nil, err
	}
	// backend.Stat confirms directories by listing them; don't store a FileRecord.
	if info.IsDir {
		m.dirMu.Lock()
		m.knownDirs[path] = true
		m.dirMu.Unlock()
		return nil, fmt.Errorf("%q is a directory", path)
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

	// Run asynchronously: TouchAccess does a write transaction (fsync) which
	// can stall for tens of milliseconds on network-backed PVCs.  The result
	// is best-effort LRU bookkeeping and does not need to block the caller.
	// db.Batch() inside TouchAccess coalesces concurrent calls into one fsync.
	go m.db.TouchAccess(path)

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
	m.invalidateNegCache(path)
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
		var queued int64
		if err := m.walkDir(ctx, "", &queued); err != nil && ctx.Err() == nil {
			m.log.Warn("remote scan failed", "err", err)
			return
		}
		m.log.Info("remote scan complete", "files_queued", atomic.LoadInt64(&queued))
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

func (m *Manager) walkDir(ctx context.Context, dir string, queued *int64) error {
	entries, err := m.union.ListTagged(ctx, dir)
	if err != nil {
		if dir == "" {
			m.log.Error("remote scan: cannot list root directory — check that your rclone_path ends with a colon (e.g. \"storagebox-media:\") and the remote is reachable", "err", err)
		} else {
			m.log.Warn("remote scan: cannot list directory", "dir", dir, "err", err)
		}
		return nil // don't abort the whole scan for one failed directory
	}
	if m.cfg.DirCacheTTL.Duration > 0 {
		infos := make([]backend.Info, len(entries))
		for i, e := range entries {
			infos[i] = e.Info
		}
		m.dirMu.Lock()
		m.storeDirCacheLocked(dir, infos)
		m.dirMu.Unlock()
	}
	var subWg sync.WaitGroup
	for _, entry := range entries {
		select {
		case <-ctx.Done():
			subWg.Wait()
			return ctx.Err()
		default:
		}
		if entry.IsDir {
			subdir := entry.Path
			select {
			case m.scanSem <- struct{}{}: // slot free: scan in parallel
				subWg.Add(1)
				go func() {
					defer subWg.Done()
					defer func() { <-m.scanSem }()
					if err := m.walkDir(ctx, subdir, queued); err != nil && ctx.Err() == nil {
						m.log.Warn("scan: failed listing dir", "path", subdir, "err", err)
					}
				}()
			default: // semaphore full: scan inline
				if err := m.walkDir(ctx, subdir, queued); err != nil && ctx.Err() == nil {
					m.log.Warn("scan: failed listing dir", "path", subdir, "err", err)
				}
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
			if rec.RemoteName == "" {
				// Old record missing RemoteName — backfill from listing info so
				// subsequent Stat calls don't fall through to a remote union.Stat.
				rec.RemoteName = entry.BackendName
				rec.RemotePriority = entry.BackendPriority
				if err := m.db.Put(rec); err != nil {
					m.log.Warn("scan: failed to backfill RemoteName", "path", entry.Path, "err", err)
				}
			} else if rec.RemoteName != entry.BackendName {
				// File migrated to a different backend — update so passthrough
				// detection and prefetch routing stay correct.
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
					atomic.AddInt64(queued, 1)
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
		m.invalidateNegCache(entry.Path)
		select {
		case m.prefetchCh <- entry.Path:
		default:
		}
		atomic.AddInt64(queued, 1)
	}
	subWg.Wait()
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

// ---- Diagnostic counters ---------------------------------------------------

// opsLogLoop logs a snapshot of operation counters every 30 s.  Non-zero
// remote call counts during a Jellyfin library scan indicate that files are
// being fetched from the remote rather than served from the local cache.
func (m *Manager) opsLogLoop(ctx context.Context) {
	defer m.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			st := m.ops.statTotal.Load()
			sr := m.ops.statRemote.Load()
			or := m.ops.openRemote.Load()
			lm := m.ops.listMiss.Load()
			if st == 0 && or == 0 && lm == 0 {
				continue // nothing interesting; skip log line
			}
			m.log.Info("cache ops snapshot",
				"stat_total", st,
				"stat_remote", sr,
				"open_remote", or,
				"list_miss", lm,
			)
		case <-ctx.Done():
			return
		}
	}
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

// List returns the merged directory listing.
//
// Stale-while-revalidate: if a cached entry exists but has expired, it is
// returned immediately and a background goroutine refreshes the cache, so
// callers never block after the first warm-up.
func (m *Manager) List(ctx context.Context, dir string) ([]backend.Info, error) {
	// Short-circuit for paths confirmed absent: vfs.Stat() falls back to
	// List() when mgr.Stat() returns an error, including for non-existent
	// sidecar files.  Without this check that fallback would make a second
	// remote round-trip for every missing .nfo / poster.jpg / etc.
	if m.isNegCached(dir) {
		return nil, os.ErrNotExist
	}
	if ttl := m.cfg.DirCacheTTL.Duration; ttl > 0 {
		m.dirMu.RLock()
		entry, ok := m.dirCache[dir]
		m.dirMu.RUnlock()
		if ok {
			if time.Now().Before(entry.expires) {
				return entry.infos, nil // fresh
			}
			// Stale: serve immediately, refresh in background.
			m.dirMu.Lock()
			if !m.dirRefreshing[dir] {
				m.dirRefreshing[dir] = true
				go func() {
					// Respect the scan semaphore: background refreshes compete for the
					// same rclone connections as reads. If the semaphore is full, skip
					// this cycle — the next access will retry when a slot is free.
					select {
					case m.scanSem <- struct{}{}:
						defer func() { <-m.scanSem }()
					default:
						m.dirMu.Lock()
						delete(m.dirRefreshing, dir)
						m.dirMu.Unlock()
						return
					}
					m.refreshDirCache(dir)
				}()
			}
			m.dirMu.Unlock()
			return entry.infos, nil
		}
	}
	// Cache miss: fetch synchronously, bounded by scanSem to prevent
	// concurrent Jellyfin library scans from flooding the rclone connection
	// pool and starving passthrough reads.
	m.ops.listMiss.Add(1)
	m.log.Debug("dir cache miss, fetching from remote", "dir", dir)
	select {
	case m.scanSem <- struct{}{}:
		defer func() { <-m.scanSem }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	tagged, err := m.union.ListTagged(ctx, dir)
	if err != nil {
		return nil, err
	}
	infos := taggedToInfos(tagged)
	if m.cfg.DirCacheTTL.Duration > 0 {
		m.dirMu.Lock()
		m.storeDirCacheLocked(dir, infos)
		m.dirMu.Unlock()
		// Eagerly warm uncached subdirectories so the next descent is fast.
		for _, info := range infos {
			if info.IsDir {
				m.scheduleSubdirWarm(info.Path)
			}
		}
	}
	// Pre-populate DB records for all file entries in this directory so
	// subsequent per-file Stat calls resolve from the DB instead of making
	// individual remote calls.  Done synchronously while we already hold
	// scanSem, so the records are in place before List() returns and Jellyfin
	// starts stat-ing individual files.
	m.populateDBFromListing(tagged)
	return infos, nil
}

func (m *Manager) refreshDirCache(dir string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tagged, err := m.union.ListTagged(ctx, dir)
	m.dirMu.Lock()
	delete(m.dirRefreshing, dir)
	if err != nil {
		m.dirMu.Unlock()
		return
	}
	infos := taggedToInfos(tagged)
	m.storeDirCacheLocked(dir, infos)
	m.dirMu.Unlock()
	m.populateDBFromListing(tagged)
}

// populateDBFromListing writes file entries from a directory listing into the
// DB in a single transaction.  This ensures that when Jellyfin stats individual
// files immediately after a ReadDir, the records are already present and no
// per-file remote call is needed.
func (m *Manager) populateDBFromListing(tagged []union.TaggedInfo) {
	recs := make([]*FileRecord, 0, len(tagged))
	for _, entry := range tagged {
		if entry.IsDir {
			continue
		}
		recs = append(recs, &FileRecord{
			Path:           entry.Path,
			Size:           entry.Size,
			ModTime:        entry.ModTime,
			State:          StateUncached,
			Kind:           KindRemote,
			LastAccess:     time.Now(),
			RemoteName:     entry.BackendName,
			RemotePriority: entry.BackendPriority,
		})
	}
	if len(recs) == 0 {
		return
	}
	if _, err := m.db.BatchUpsertFiles(recs); err != nil {
		m.log.Warn("populateDBFromListing: batch upsert failed", "err", err)
	}
}

func taggedToInfos(tagged []union.TaggedInfo) []backend.Info {
	infos := make([]backend.Info, len(tagged))
	for i, t := range tagged {
		infos[i] = t.Info
	}
	return infos
}

// scheduleSubdirWarm kicks off a background listing of dir if it is not
// already cached or being refreshed, bounded by scanSem.
func (m *Manager) scheduleSubdirWarm(dir string) {
	m.dirMu.Lock()
	_, inCache := m.dirCache[dir]
	refreshing := m.dirRefreshing[dir]
	if inCache || refreshing {
		m.dirMu.Unlock()
		return
	}
	m.dirRefreshing[dir] = true
	m.dirMu.Unlock()

	select {
	case m.scanSem <- struct{}{}: // slot free: warm in background
		go func() {
			defer func() { <-m.scanSem }()
			m.refreshDirCache(dir)
		}()
	default: // semaphore full: let it warm on first access
		m.dirMu.Lock()
		delete(m.dirRefreshing, dir)
		m.dirMu.Unlock()
	}
}

// storeDirCacheLocked stores infos under dir and marks all sub-directory
// entries as known dirs so Stat can answer immediately.  Caller must hold dirMu.
// It also persists the entry to BoltDB asynchronously so restarts are fast.
func (m *Manager) storeDirCacheLocked(dir string, infos []backend.Info) {
	entry := dirCacheEntry{infos: infos, expires: time.Now().Add(m.cfg.DirCacheTTL.Duration)}
	m.dirCache[dir] = entry
	for _, info := range infos {
		if info.IsDir {
			m.knownDirs[info.Path] = true
		}
	}
	// Persist asynchronously; db.Batch coalesces concurrent calls.
	infosCopy := make([]backend.Info, len(infos))
	copy(infosCopy, infos)
	expires := entry.expires
	go m.db.SaveDirCacheEntry(dir, infosCopy, expires)
}

// loadPersistedDirCache pre-warms the in-memory dir cache from BoltDB so
// that the first Jellyfin scan after a pod restart does not block on remote
// listings.  Expired entries are loaded as-is: the stale-while-revalidate
// path in List() will serve them instantly and trigger background refreshes.
func (m *Manager) loadPersistedDirCache() {
	entries := m.db.LoadDirCacheEntries()
	if len(entries) == 0 {
		return
	}
	m.dirMu.Lock()
	defer m.dirMu.Unlock()
	for dir, e := range entries {
		m.dirCache[dir] = dirCacheEntry{infos: e.Infos, expires: e.Expires}
		for _, info := range e.Infos {
			if info.IsDir {
				m.knownDirs[info.Path] = true
			}
		}
	}
	m.log.Info("pre-warmed dir cache from persistent store", "dirs", len(entries))
}

// IsKnownDir reports whether path has been observed as a directory in any
// cached listing.  Returns true instantly without a remote call.
func (m *Manager) IsKnownDir(path string) bool {
	m.dirMu.RLock()
	ok := m.knownDirs[path]
	m.dirMu.RUnlock()
	return ok
}

// negCacheTTL is how long a "not found" result is cached before the path is
// re-checked on the remote.  15 minutes is long enough to cover many Jellyfin
// scan cycles while still picking up genuinely new sidecar files promptly.
const negCacheTTL = 15 * time.Minute

// isNegCached reports whether path is in the negative cache (i.e. was
// confirmed absent from all backends within the last negCacheTTL).
func (m *Manager) isNegCached(path string) bool {
	m.negMu.RLock()
	exp, ok := m.negCache[path]
	m.negMu.RUnlock()
	return ok && time.Now().Before(exp)
}

// addNegCache records path as absent.  Called after union.Stat returns an
// error for a path not already in the DB.
func (m *Manager) addNegCache(path string) {
	m.negMu.Lock()
	m.negCache[path] = time.Now().Add(negCacheTTL)
	m.negMu.Unlock()
}

// invalidateNegCache removes path from the negative cache.  Called when a
// file is confirmed to exist (e.g. after an upload or walkDir discovery).
func (m *Manager) invalidateNegCache(path string) {
	m.negMu.Lock()
	delete(m.negCache, path)
	m.negMu.Unlock()
}

func (m *Manager) invalidateDirCache(path string) {
	dir := filepath.Dir(path)
	if dir == "." {
		dir = ""
	}
	m.dirMu.Lock()
	delete(m.dirCache, dir)
	m.dirMu.Unlock()
}

// Mkdir creates a directory on the primary writable backend.
func (m *Manager) Mkdir(ctx context.Context, path string) error {
	err := m.union.Mkdir(ctx, path)
	if err == nil {
		m.invalidateDirCache(path)
	}
	return err
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
	m.invalidateDirCache(path)
	return m.RegisterUpload(path, size, modTime)
}
