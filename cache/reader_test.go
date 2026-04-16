package cache

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"jellyfin-cache/backend"
	"jellyfin-cache/config"
	"jellyfin-cache/internal/testutil"
	"jellyfin-cache/union"
)

// ---- shared test helpers ---------------------------------------------------

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// buildTestManager creates a real Manager backed by an in-memory backend.
// fileData is pre-loaded into the backend.  overrides lets each test tweak
// config values on top of the fast/small test defaults.
func buildTestManager(t *testing.T, fileData map[string][]byte, overrides func(*config.CacheConfig)) *Manager {
	t.Helper()

	be := testutil.NewMemBackend("test", 0, false)
	for path, data := range fileData {
		be.AddFile(path, data)
	}
	u := union.New([]backend.Backend{be})

	cfg := config.CacheConfig{
		Dir:                    t.TempDir(),
		MaxSize:                "1GB",
		PrefixBytes:            100,
		PlaybackTriggerBytes:   500,
		FullTTL:                config.Duration{Duration: time.Hour},
		UploadTTL:              config.Duration{Duration: 24 * time.Hour},
		// Zero workers so background goroutines don't race the tests on the
		// download channel.  Reader tests only verify that the trigger fires,
		// not that the download completes.
		PrefetchWorkers:        0,
		DownloadWorkers:        0,
		EvictInterval:          config.Duration{Duration: time.Hour}, // don't auto-evict
		MinPlayDuration:        config.Duration{Duration: 50 * time.Millisecond},
		TrickplaySeekThreshold: 3,
		TrickplaySeekWindow:    config.Duration{Duration: 200 * time.Millisecond},
	}
	if overrides != nil {
		overrides(&cfg)
	}

	mgr, err := NewManager(cfg, u, newTestLogger())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { mgr.Close() })
	return mgr
}

// openReader creates a CacheReader for path against a pre-populated DB record.
func openReader(t *testing.T, mgr *Manager, path string, size int64) *CacheReader {
	t.Helper()
	_ = mgr.db.Put(&FileRecord{
		Path:  path,
		Size:  size,
		State: StateUncached,
	})
	return &CacheReader{
		ctx:     context.Background(),
		path:    path,
		size:    size,
		manager: mgr,
	}
}

// drainDownload waits up to timeout for a download job to appear on the
// manager's channel, returning the job path or "" if nothing arrived.
func drainDownload(mgr *Manager, timeout time.Duration) string {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case job := <-mgr.downloadCh:
		return job.path
	case <-timer.C:
		return ""
	}
}

// assertNoDownload checks that no download is queued within the window.
func assertNoDownload(t *testing.T, mgr *Manager, window time.Duration) {
	t.Helper()
	if path := drainDownload(mgr, window); path != "" {
		t.Errorf("unexpected download triggered for %q", path)
	}
}

// ---- Scan: short-lived handle must NOT trigger -----------------------------

func TestReader_ScanDoesNotTrigger(t *testing.T) {
	// Library scan: opens file, reads > trigger_bytes quickly, then closes.
	// The handle is never open long enough for min_play_duration to elapse.
	data := bytes.Repeat([]byte("S"), 10_000)
	mgr := buildTestManager(t, map[string][]byte{"ep1.mkv": data}, nil)
	// MinPlayDuration = 50ms in defaults.

	r := openReader(t, mgr, "ep1.mkv", int64(len(data)))
	defer r.Close()

	// Read well past PlaybackTriggerBytes (500).
	buf := make([]byte, 600)
	r.Read(buf)

	// Close BEFORE the 50ms timer fires.
	r.Close()

	// Wait longer than MinPlayDuration to confirm nothing was queued.
	assertNoDownload(t, mgr, 150*time.Millisecond)
}

// ---- Playback: sustained reading MUST trigger ------------------------------

func TestReader_PlaybackTriggers(t *testing.T) {
	data := bytes.Repeat([]byte("P"), 10_000)
	mgr := buildTestManager(t, map[string][]byte{"film.mkv": data}, nil)

	r := openReader(t, mgr, "film.mkv", int64(len(data)))
	defer r.Close()

	// Read past PlaybackTriggerBytes (500).
	r.Read(make([]byte, 600))

	// Keep handle open past MinPlayDuration (50ms).
	path := drainDownload(mgr, 500*time.Millisecond)
	if path == "" {
		t.Error("full download was not triggered after MinPlayDuration elapsed")
	} else if path != "film.mkv" {
		t.Errorf("wrong path triggered: %q", path)
	}
}

// ---- Timer does not fire when bytes threshold is not met ------------------

func TestReader_NotTriggeredWithoutEnoughBytes(t *testing.T) {
	data := bytes.Repeat([]byte("Q"), 10_000)
	mgr := buildTestManager(t, map[string][]byte{"f.mkv": data}, nil)

	r := openReader(t, mgr, "f.mkv", int64(len(data)))
	defer r.Close()

	// Read only 10 bytes — well under PlaybackTriggerBytes (500).
	r.Read(make([]byte, 10))

	// Wait well past MinPlayDuration.
	assertNoDownload(t, mgr, 200*time.Millisecond)
}

// ---- Trickplay: rapid seeks suppress the trigger --------------------------

func TestReader_TrickplayDetected(t *testing.T) {
	// Backend holds a small stub; the logical size is large for seek testing.
	fileData := map[string][]byte{"movie.mkv": bytes.Repeat([]byte("T"), 200)}
	mgr := buildTestManager(t, fileData, func(cfg *config.CacheConfig) {
		// Very low byte threshold so it would trigger without trickplay guard.
		cfg.PlaybackTriggerBytes = 50
		// MinPlayDuration long enough (80ms) that all 3 seeks (3×5ms=15ms)
		// complete and set isTrickplay BEFORE the timer fires.
		cfg.MinPlayDuration = config.Duration{Duration: 80 * time.Millisecond}
		cfg.TrickplaySeekThreshold = 3
		cfg.TrickplaySeekWindow = config.Duration{Duration: 300 * time.Millisecond}
	})

	const size = int64(100_000_000) // logical size (for seek positions)
	_ = mgr.db.Put(&FileRecord{Path: "movie.mkv", Size: size, State: StateUncached})
	r := &CacheReader{ctx: context.Background(), path: "movie.mkv", size: size, manager: mgr}
	defer r.Close()

	// Read enough bytes to arm the timer.
	r.Read(make([]byte, 100))

	// Three rapid seeks — 5ms apart, well within the 300ms window and all
	// completing before the 80ms MinPlayDuration timer fires.
	for _, pos := range []int64{10e6, 30e6, 60e6} {
		r.Seek(pos, io.SeekStart)
		time.Sleep(5 * time.Millisecond)
	}

	// Wait long enough for any timer that was not cancelled to fire.
	time.Sleep(200 * time.Millisecond)

	r.mu.Lock()
	isTrickplay := r.isTrickplay
	triggered := r.triggered
	r.mu.Unlock()

	if !isTrickplay {
		t.Error("expected handle to be classified as trickplay after rapid seeks")
	}
	if triggered {
		t.Error("triggered should remain false when trickplay is detected")
	}
	assertNoDownload(t, mgr, 50*time.Millisecond)
}

// ---- Trickplay: seeks spread out beyond the window are OK -----------------

func TestReader_SlowSeeksAreNotTrickplay(t *testing.T) {
	fileData := map[string][]byte{"slow.mkv": bytes.Repeat([]byte("s"), 200)}
	mgr := buildTestManager(t, fileData, func(cfg *config.CacheConfig) {
		cfg.PlaybackTriggerBytes = 50
		cfg.MinPlayDuration = config.Duration{Duration: 20 * time.Millisecond}
		cfg.TrickplaySeekThreshold = 3
		cfg.TrickplaySeekWindow = config.Duration{Duration: 60 * time.Millisecond}
	})

	const size = int64(100_000_000)
	_ = mgr.db.Put(&FileRecord{Path: "slow.mkv", Size: size, State: StateUncached})
	r := &CacheReader{ctx: context.Background(), path: "slow.mkv", size: size, manager: mgr}
	defer r.Close()

	r.Read(make([]byte, 100))
	r.Seek(1_000_000, io.SeekStart)
	time.Sleep(80 * time.Millisecond) // window expires between seeks
	r.Seek(2_000_000, io.SeekStart)   // only 1 seek visible in current window
	r.Seek(3_000_000, io.SeekStart)   // 2 seeks — still under threshold

	time.Sleep(150 * time.Millisecond)

	r.mu.Lock()
	isTrickplay := r.isTrickplay
	r.mu.Unlock()
	if isTrickplay {
		t.Error("slow seeks should not be classified as trickplay")
	}
}

// ---- Real playback with one user seek still triggers ----------------------

func TestReader_OneSeekStillTriggers(t *testing.T) {
	// A user skipping the intro → one seek, then sustained watching.
	fileData := map[string][]byte{"series.mkv": bytes.Repeat([]byte("v"), 200)}
	mgr := buildTestManager(t, fileData, func(cfg *config.CacheConfig) {
		cfg.PlaybackTriggerBytes = 50
		cfg.MinPlayDuration = config.Duration{Duration: 30 * time.Millisecond}
		cfg.TrickplaySeekThreshold = 3
		cfg.TrickplaySeekWindow = config.Duration{Duration: 200 * time.Millisecond}
	})

	const size = int64(100_000_000)
	_ = mgr.db.Put(&FileRecord{Path: "series.mkv", Size: size, State: StateUncached})
	r := &CacheReader{ctx: context.Background(), path: "series.mkv", size: size, manager: mgr}
	defer r.Close()

	r.Read(make([]byte, 100))
	time.Sleep(50 * time.Millisecond) // space out the single seek
	r.Seek(5_000_000, io.SeekStart)

	path := drainDownload(mgr, 500*time.Millisecond)
	if path == "" {
		t.Error("playback with a single seek should still trigger a full download")
	}
}

// ---- Triggered only once even after multiple reads/seeks ------------------

func TestReader_TriggerOnlyOnce(t *testing.T) {
	fileData := map[string][]byte{"once.mkv": bytes.Repeat([]byte("o"), 200)}
	mgr := buildTestManager(t, fileData, func(cfg *config.CacheConfig) {
		cfg.PlaybackTriggerBytes = 50
		cfg.MinPlayDuration = config.Duration{Duration: 20 * time.Millisecond}
	})

	_ = mgr.db.Put(&FileRecord{Path: "once.mkv", Size: 1_000_000, State: StateUncached})
	r := &CacheReader{ctx: context.Background(), path: "once.mkv", size: 1_000_000, manager: mgr}
	defer r.Close()

	r.Read(make([]byte, 100))
	// Drain the first (and only expected) trigger.
	if path := drainDownload(mgr, 300*time.Millisecond); path == "" {
		t.Fatal("first trigger did not arrive")
	}

	// More reads must not re-arm the timer.
	r.Read(make([]byte, 100))
	time.Sleep(100 * time.Millisecond)

	assertNoDownload(t, mgr, 50*time.Millisecond)
}

// ---- isTrickplay blocks the timer even if it somehow fires -----------------

func TestReader_TrickplayFlagBlocksTimer(t *testing.T) {
	fileData := map[string][]byte{"tp.mkv": bytes.Repeat([]byte("t"), 200)}
	mgr := buildTestManager(t, fileData, func(cfg *config.CacheConfig) {
		cfg.PlaybackTriggerBytes = 10
		cfg.MinPlayDuration = config.Duration{Duration: 10 * time.Millisecond}
	})

	_ = mgr.db.Put(&FileRecord{Path: "tp.mkv", Size: 1_000_000, State: StateUncached})
	r := &CacheReader{ctx: context.Background(), path: "tp.mkv", size: 1_000_000, manager: mgr}
	defer r.Close()

	r.Read(make([]byte, 50))

	// Manually set trickplay before timer fires.
	r.mu.Lock()
	r.isTrickplay = true
	r.mu.Unlock()

	time.Sleep(100 * time.Millisecond)
	assertNoDownload(t, mgr, 50*time.Millisecond)
}
