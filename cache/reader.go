package cache

import (
	"context"
	"io"
	"sync"
	"time"
)

// CacheReader is an io.ReadSeekCloser that serves bytes from the local cache
// when available, falling back to the remote for uncached regions.
//
// # Playback detection
//
// A full background download is triggered when ALL of these are true:
//
//  1. Bytes threshold: cumulative bytes read through this handle ≥
//     cfg.PlaybackTriggerBytes.  Filters tiny metadata probes.
//
//  2. Duration threshold: the handle has been open and reading for at least
//     cfg.MinPlayDuration.  Filters Jellyfin library scans (ffprobe opens
//     every file for a few seconds then closes).
//
//  3. Not classified as trickplay (see below).
//
// # Trickplay suppression
//
// Jellyfin's trickplay generator (preview thumbnail strips) works by having
// ffmpeg seek to evenly-spaced positions throughout the entire file in rapid
// succession — typically one seek per second or faster.  This would otherwise
// trigger full downloads of files that nobody is actually watching.
//
// If cfg.TrickplaySeekThreshold seeks are observed within
// cfg.TrickplaySeekWindow the handle is permanently marked as trickplay: the
// download timer is cancelled and no future trigger will fire on this handle.
//
// New uploads and webhook-triggered downloads are unaffected by this logic.
type CacheReader struct {
	ctx     context.Context
	path    string
	size    int64
	manager *Manager

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

	if !r.triggered && !r.isTrickplay {
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

	r.remoteMu.Lock()
	if r.remoteRC != nil {
		r.remoteRC.Close()
		r.remoteRC = nil
	}
	r.remoteMu.Unlock()
	return nil
}

// ---- timer management ------------------------------------------------------

// maybeArmTimer starts the playback-detection timer on the first read.
// Must be called with r.mu held.
func (r *CacheReader) maybeArmTimer() {
	if r.triggered || r.isTrickplay || r.timer != nil {
		return
	}
	if r.firstReadAt.IsZero() {
		r.firstReadAt = time.Now()
	}
	d := r.manager.cfg.MinPlayDuration.Duration
	r.timer = time.AfterFunc(d, r.onTimerFired)
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
	rec, err := r.manager.db.Get(r.path)
	if err != nil || rec == nil {
		return r.readRemoteAt(p, offset)
	}

	end := offset + int64(len(p))
	if rec.CachedBytes >= end {
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
	r.maybeDownloadOnCacheMiss()

	if offset < rec.CachedBytes {
		// Split: serve cached prefix from disk, uncached tail from remote.
		cached := rec.CachedBytes - offset
		n1, err := r.readLocalAt(p[:cached], offset)
		if err != nil {
			return n1, err
		}
		n2, err := r.readRemoteAt(p[cached:], rec.CachedBytes)
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
	f, err := r.manager.store.OpenRead(r.path)
	if err != nil {
		return r.readRemoteAt(p, offset)
	}
	defer f.Close()
	return f.ReadAt(p, offset)
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
