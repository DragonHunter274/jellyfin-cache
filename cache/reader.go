package cache

import (
	"context"
	"io"
	"os"
	"sync"
	"time"
)

// CacheReader is an io.ReadSeekCloser that serves bytes from the local cache
// when available, falling back to the remote for uncached regions.
//
// # Playback detection
//
// A full background download is triggered when either of these paths fires:
//
//  1. Sequential-read fast path (earlyTrigger): for files that are already in
//     StatePrefix or StateDownloading the download is triggered as soon as the
//     read pattern is confirmed sequential (no non-contiguous seeks) AND bytes
//     ≥ cfg.PlaybackTriggerBytes.  Sequential reads with no seeks reach the
//     threshold in ~1 s of real playback, giving the background download a
//     multi-second head start before the cached prefix is exhausted.
//
//  2. Duration path: the handle has been open and reading for at least
//     cfg.MinPlayDuration (default 10 s) AND bytes ≥ cfg.PlaybackTriggerBytes.
//     This is the catch-all for files not yet prefetched and for non-sequential
//     access patterns that don't qualify for the fast path.  It also filters
//     Jellyfin library scans: ffprobe opens every file briefly (1–3 s) and
//     closes it before the timer fires.
//
//  3. Cache-miss path: if a read lands beyond the cached boundary (prefix
//     exhausted before the timer fires) the download is triggered immediately
//     as long as the bytes threshold is already met.
//
// # Trickplay suppression
//
// Jellyfin's trickplay generator seeks to evenly-spaced positions throughout
// the entire file in rapid succession (~1 seek/s).  If cfg.TrickplaySeekThreshold
// seeks are observed within cfg.TrickplaySeekWindow the handle is permanently
// marked as trickplay: the download timer is cancelled and the earlyTrigger
// fast path is suppressed.
//
// New uploads and webhook-triggered downloads are unaffected by this logic.
type CacheReader struct {
	ctx     context.Context
	path    string
	size    int64
	manager *Manager

	// earlyTrigger enables the sequential-read fast path (path 1 above).
	// Set to true when the file is already in StatePrefix or StateDownloading,
	// i.e. it has been accessed before and is likely to be played rather than
	// just probed for metadata.
	earlyTrigger bool

	mu             sync.Mutex
	offset         int64
	cumulativeRead int64
	firstReadAt    time.Time
	triggered      bool
	isTrickplay    bool

	// Download timer armed after MinPlayDuration from the first read.
	timer *time.Timer

	// Sliding window of recent seek timestamps for trickplay detection.
	seekTimes []time.Time

	// Cached FileRecord, refreshed at most once per recTTL.
	//
	// readAt() queries the DB on every call to check how many bytes are locally
	// cached.  On network-backed PVCs each BoltDB read can cost 1–10 ms, and
	// go-nfs issues multiple reads per NFS READ RPC, so the overhead compounds
	// quickly during library scans.  Caching the record for a short window
	// reduces this to ≤1 DB hit per recTTL instead of one per RPC.
	recMu     sync.Mutex
	cachedRec *FileRecord
	recExpiry time.Time

	// Persistent local cache file kept open between consecutive readLocalAt calls.
	//
	// Opening the cache file on every NFS READ RPC (open + pread + close per
	// 128 KiB block) adds three syscalls per RPC.  On PVC-backed storage with
	// any per-operation latency this can dominate read throughput.  Keeping one
	// fd open for the lifetime of the CacheReader reduces this to one pread.
	localMu sync.Mutex
	localF  *os.File

	// Persistent remote reader kept open between consecutive readRemoteAt calls.
	//
	// Without this, every NFS READ RPC opens a brand-new remote connection,
	// fetches a 4 MiB read-ahead buffer, returns 128 KiB to the caller, then
	// discards the rest and closes.  For a SFTP/HTTPS remote each connection
	// costs 100–300 ms of RTT, making high-bitrate files completely unplayable
	// past the cached prefix.
	//
	// With the persistent reader, sequential NFS reads drain the same 4 MiB
	// buffer from memory (≈32 RPCs per network round-trip) and the connection
	// is only re-opened when the reader seeks to a non-contiguous position.
	remoteMu  sync.Mutex
	remoteRC  io.ReadSeekCloser
	remoteOff int64
}

const recTTL = 500 * time.Millisecond

// getRecord returns the cached FileRecord for this path, refreshing from the
// DB at most once per recTTL.  Returns nil if the path is not tracked.
func (r *CacheReader) getRecord() *FileRecord {
	r.recMu.Lock()
	if r.cachedRec != nil && time.Now().Before(r.recExpiry) {
		rec := r.cachedRec
		r.recMu.Unlock()
		return rec
	}
	r.recMu.Unlock()

	rec, err := r.manager.db.Get(r.path)
	if err != nil || rec == nil {
		return nil
	}

	r.recMu.Lock()
	r.cachedRec = rec
	r.recExpiry = time.Now().Add(recTTL)
	r.recMu.Unlock()
	return rec
}

func (r *CacheReader) Read(p []byte) (int, error) {
	n, err := r.readAt(p, r.offset)
	r.mu.Lock()
	r.offset += int64(n)
	r.cumulativeRead += int64(n)
	r.maybeArmTimer()
	r.mu.Unlock()
	return n, err
}

func (r *CacheReader) Seek(offset int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	prev := r.offset
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.offset + offset
	case io.SeekEnd:
		abs = r.size + offset
	}
	if abs < 0 {
		return 0, io.ErrUnexpectedEOF
	}
	r.offset = abs

	// Only count seeks that actually move the position.  The NFS layer calls
	// ReadAt(off, n) which issues Seek(off) before every read; for sequential
	// playback those seeks always land exactly on r.offset (where the last Read
	// left off), so abs == prev.  Counting them makes trickplay fire immediately
	// on any sequential stream.  True trickplay and user scrubbing both jump to
	// non-contiguous positions (abs != prev).
	if !r.triggered && !r.isTrickplay && abs != prev {
		r.recordSeek()
	}
	return abs, nil
}

func (r *CacheReader) Close() error {
	r.mu.Lock()
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}
	r.mu.Unlock()

	r.localMu.Lock()
	if r.localF != nil {
		r.localF.Close()
		r.localF = nil
	}
	r.localMu.Unlock()

	r.remoteMu.Lock()
	if r.remoteRC != nil {
		r.remoteRC.Close()
		r.remoteRC = nil
	}
	r.remoteMu.Unlock()
	return nil
}

// ---- timer management ------------------------------------------------------

// maybeArmTimer is called after every read (with r.mu held).  It fires the
// download trigger immediately for confirmed sequential readers (fast path) or
// arms the MinPlayDuration timer as a fallback.
func (r *CacheReader) maybeArmTimer() {
	if r.triggered || r.isTrickplay {
		return
	}
	if r.firstReadAt.IsZero() {
		r.firstReadAt = time.Now()
	}

	// Fast path: for files already in StatePrefix or StateDownloading, trigger
	// as soon as the bytes threshold is met AND no non-contiguous seeks have
	// been recorded.  Sequential NFS reads never produce non-contiguous seeks
	// (the Seek call in ReadAt always lands exactly at r.offset), so
	// len(r.seekTimes)==0 reliably distinguishes sequential playback from the
	// random-access patterns used by ffprobe metadata scans.
	if r.earlyTrigger && len(r.seekTimes) == 0 &&
		r.cumulativeRead >= r.manager.cfg.PlaybackTriggerBytes {
		if r.timer != nil {
			r.timer.Stop()
			r.timer = nil
		}
		r.triggered = true
		go r.manager.TriggerFullDownload(r.path)
		return
	}

	if r.timer != nil {
		return
	}
	r.timer = time.AfterFunc(r.manager.cfg.MinPlayDuration.Duration, r.onTimerFired)
}

// onTimerFired is called by time.AfterFunc after MinPlayDuration elapses.
func (r *CacheReader) onTimerFired() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.timer = nil
	if r.triggered || r.isTrickplay {
		return
	}
	if r.cumulativeRead < r.manager.cfg.PlaybackTriggerBytes {
		// Slow probe — not enough bytes yet; don't trigger.
		return
	}
	r.triggered = true
	go r.manager.TriggerFullDownload(r.path)
}

// ---- trickplay detection ----------------------------------------------------

// recordSeek appends the current time to the seek window and checks whether
// the rate exceeds the trickplay threshold.
// Must be called with r.mu held.
func (r *CacheReader) recordSeek() {
	now := time.Now()
	window := r.manager.cfg.TrickplaySeekWindow.Duration
	threshold := r.manager.cfg.TrickplaySeekThreshold

	// Evict seeks that are outside the rolling window.
	cutoff := now.Add(-window)
	valid := r.seekTimes[:0]
	for _, t := range r.seekTimes {
		if t.After(cutoff) {
			valid = append(valid, t)
		}
	}
	valid = append(valid, now)
	r.seekTimes = valid

	if len(r.seekTimes) >= threshold {
		r.manager.log.Info("trickplay detected – suppressing download trigger",
			"path", r.path,
			"seeks_in_window", len(r.seekTimes),
			"window", window)
		r.isTrickplay = true
		if r.timer != nil {
			r.timer.Stop()
			r.timer = nil
		}
	}
}

// ---- internal read helpers -------------------------------------------------

func (r *CacheReader) readAt(p []byte, offset int64) (int, error) {
	rec := r.getRecord()
	if rec == nil {
		return r.readRemoteAt(p, offset)
	}

	// During an active download the DB's CachedBytes is frozen at the prefix
	// size (UpdateState is called with -1 to leave it unchanged).  Consult the
	// actual on-disk size so that bytes already written by the download worker
	// are served locally instead of fetching them from the remote again.
	cachedBytes := rec.CachedBytes
	if rec.State == StateDownloading {
		if live := r.manager.store.Size(r.path); live > cachedBytes {
			cachedBytes = live
		}
	}

	end := offset + int64(len(p))
	if cachedBytes >= end {
		return r.readLocalAt(p, offset)
	}

	// This read requires data beyond the local cache boundary.  Trigger a full
	// background download immediately rather than waiting for the timer.
	//
	// The timer-based path (onTimerFired) only fires after MinPlayDuration, but
	// for high-bitrate files the cached prefix is exhausted in seconds: the
	// client stalls waiting on the remote and may close the handle before the
	// timer fires, which stops the timer via Close() and leaves the file
	// permanently uncached.
	r.manager.log.Debug("cache miss: read past cached boundary",
		"path", r.path,
		"offset", offset,
		"cached_bytes", cachedBytes,
		"file_size", r.size,
		"state", rec.State,
	)
	r.maybeDownloadOnCacheMiss()

	if offset < cachedBytes {
		// Split: serve the already-cached portion from disk, the rest from remote.
		cached := cachedBytes - offset
		n1, err := r.readLocalAt(p[:cached], offset)
		if err != nil {
			return n1, err
		}
		n2, err := r.readRemoteAt(p[cached:], cachedBytes)
		return n1 + n2, err
	}
	return r.readRemoteAt(p, offset)
}

// maybeDownloadOnCacheMiss triggers a full background download the first time
// a read requires data past the cached boundary.  It is the primary trigger
// for high-bitrate content where the timer-based path may never fire.
// Safe to call multiple times; only fires once per handle.
func (r *CacheReader) maybeDownloadOnCacheMiss() {
	r.mu.Lock()
	trigger := !r.triggered && !r.isTrickplay &&
		r.cumulativeRead >= r.manager.cfg.PlaybackTriggerBytes
	if trigger {
		r.triggered = true
	}
	r.mu.Unlock()
	if trigger {
		go r.manager.TriggerFullDownload(r.path)
	}
}

func (r *CacheReader) readLocalAt(p []byte, offset int64) (int, error) {
	r.localMu.Lock()
	if r.localF == nil {
		f, err := r.manager.store.OpenRead(r.path)
		if err != nil {
			r.localMu.Unlock()
			return r.readRemoteAt(p, offset)
		}
		r.localF = f
	}
	n, err := r.localF.ReadAt(p, offset)
	if err != nil && err != io.EOF {
		// Invalidate on unexpected errors so the next call retries open.
		r.localF.Close()
		r.localF = nil
	}
	r.localMu.Unlock()
	return n, err
}

func (r *CacheReader) readRemoteAt(p []byte, offset int64) (int, error) {
	r.remoteMu.Lock()
	defer r.remoteMu.Unlock()

	if r.remoteRC == nil {
		if err := r.openRemoteLocked(offset); err != nil {
			return 0, err
		}
	} else if r.remoteOff != offset {
		// Non-sequential access: try to seek within the existing connection
		// (cheap if within the rclone read-ahead buffer) and reopen if not.
		if _, err := r.remoteRC.Seek(offset, io.SeekStart); err != nil {
			r.remoteRC.Close()
			r.remoteRC = nil
			if err := r.openRemoteLocked(offset); err != nil {
				return 0, err
			}
		} else {
			r.remoteOff = offset
		}
	}

	n, err := io.ReadFull(r.remoteRC, p)
	r.remoteOff += int64(n)
	if err != nil && err != io.ErrUnexpectedEOF {
		// Broken stream — discard so the next call reopens cleanly.
		r.remoteRC.Close()
		r.remoteRC = nil
	}
	return n, err
}

// openRemoteLocked opens the union at offset and stores the reader in r.remoteRC.
// Must be called with r.remoteMu held.
func (r *CacheReader) openRemoteLocked(offset int64) error {
	r.manager.ops.openRemote.Add(1)
	r.manager.log.Info("remote fallback: opening remote connection",
		"path", r.path,
		"offset", offset,
		"file_size", r.size,
		"pct", func() float64 {
			if r.size <= 0 {
				return 0
			}
			return float64(offset) / float64(r.size) * 100
		}(),
	)

	rc, err := r.manager.union.Open(r.ctx, r.path)
	if err != nil {
		return err
	}
	if offset > 0 {
		if _, err := rc.Seek(offset, io.SeekStart); err != nil {
			rc.Close()
			return err
		}
	}
	r.remoteRC = rc
	r.remoteOff = offset
	return nil
}
