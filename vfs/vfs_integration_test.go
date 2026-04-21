//go:build integration

package vfs_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/all"
	"github.com/rclone/rclone/fs/config/configfile"

	"jellyfin-cache/backend"
	"jellyfin-cache/cache"
	"jellyfin-cache/config"
	"jellyfin-cache/union"
	"jellyfin-cache/vfs"
)

const (
	testRemote    = "storagebox-media"
	testFile      = "TV/The Rookie/Season 8/The Rookie - S08E10 - His Name Was Martin WEBRip-1080p.mkv"
	avatarFile    = "Movies/Avatar Fire and Ash (2025)/Avatar Fire and Ash (2025) WEBDL-2160p.mkv"
	nfsBlockSize  = 128 * 1024 // 128 KiB — go-nfs default read size
	nfsTestBlocks = 16         // 2 MiB total
)

func TestMain(m *testing.M) {
	configfile.Install()
	m.Run()
}

func newTestFS(t *testing.T) *vfs.FS {
	t.Helper()
	ctx := context.Background()

	b, err := backend.NewRclone(ctx, config.RemoteConfig{
		Name:        "test",
		RclonePath:  testRemote + ":",
		ReadOnly:    true,
		Passthrough: true,
	})
	if err != nil {
		t.Fatalf("NewRclone: %v", err)
	}

	u := union.New([]backend.Backend{b})
	mgr, err := cache.NewManager(config.CacheConfig{
		Dir:             t.TempDir(),
		MaxSize:         "1GB",
		PrefixBytes:     0,
		EvictInterval:   config.Duration{Duration: time.Minute},
		FullTTL:         config.Duration{Duration: 24 * time.Hour},
		UploadTTL:       config.Duration{Duration: 24 * time.Hour},
		MinPlayDuration: config.Duration{Duration: 0},
	}, u, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { mgr.Close() })

	return vfs.New(ctx, mgr)
}

// TestVFS_NFSReadPattern_Pool simulates go-nfs onRead: Open → ReadAt → Close
// per 128 KiB block.  The handle pool in vfs.FS should reuse the open remote
// connection across iterations, giving sequential-stream throughput rather than
// one HTTP range-request per block.
func TestVFS_NFSReadPattern_Pool(t *testing.T) {
	total := nfsTestBlocks * nfsBlockSize
	fs := newTestFS(t)

	// Reference: single open, sequential read.
	ref := make([]byte, total)
	{
		f, err := fs.Open(testFile)
		if err != nil {
			t.Fatalf("Open (ref): %v", err)
		}
		if _, err := io.ReadFull(f, ref); err != nil {
			t.Fatalf("ReadFull (ref): %v", err)
		}
		f.Close()
	}

	got := make([]byte, total)
	start := time.Now()
	for i := 0; i < nfsTestBlocks; i++ {
		off := int64(i * nfsBlockSize)

		// go-nfs calls Open on the billy.Filesystem for each READ RPC.
		f, err := fs.Open(testFile)
		if err != nil {
			t.Fatalf("block %d Open: %v", i, err)
		}
		n, err := f.ReadAt(got[off:off+int64(nfsBlockSize)], off)
		f.Close()
		if err != nil && err != io.EOF {
			t.Fatalf("block %d ReadAt: %v", i, err)
		}
		if n != nfsBlockSize {
			t.Fatalf("block %d: read %d bytes, want %d", i, n, nfsBlockSize)
		}
	}

	elapsed := time.Since(start)
	mbps := float64(total) / elapsed.Seconds() / 1024 / 1024
	t.Logf("pooled NFS pattern: %d open+ReadAt+close in %s (%.1f MB/s)", nfsTestBlocks, elapsed, mbps)
	if mbps < 1.0 {
		t.Errorf("throughput %.1f MB/s is suspiciously low — pool may not be working", mbps)
	}

	if !bytes.Equal(ref, got) {
		t.Error("pooled NFS pattern: data does not match reference")
	}
}

// TestVFS_Avatar simulates exactly what go-nfs does when Jellyfin opens
// a large 4K file: Stat → Open → ReadAt(header) → Seek(EOF) → ReadAt(tail) → Close.
func TestVFS_Avatar(t *testing.T) {
	fs := newTestFS(t)

	// 1. Stat — go-nfs calls this before and after every READ RPC.
	info, err := fs.Stat(avatarFile)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	t.Logf("size=%d (%d GiB)", info.Size(), info.Size()>>30)
	if info.Size() < 1<<30 {
		t.Errorf("suspiciously small size %d", info.Size())
	}

	// 2. Read the first 512 KiB (container header).
	header := make([]byte, 512*1024)
	{
		f, err := fs.Open(avatarFile)
		if err != nil {
			t.Fatalf("Open (header): %v", err)
		}
		n, err := f.ReadAt(header, 0)
		f.Close()
		if err != nil && err != io.EOF {
			t.Fatalf("ReadAt header: %v (read %d bytes)", err, n)
		}
		t.Logf("header read: %d bytes", n)
	}

	// 3. Seek near EOF and read a chunk — ffprobe locates the moov/cues atom.
	tail := make([]byte, 512*1024)
	tailOff := info.Size() - int64(len(tail))
	{
		f, err := fs.Open(avatarFile)
		if err != nil {
			t.Fatalf("Open (tail): %v", err)
		}
		n, err := f.ReadAt(tail, tailOff)
		f.Close()
		if err != nil && err != io.EOF {
			t.Fatalf("ReadAt tail at %d: %v (read %d bytes)", tailOff, err, n)
		}
		t.Logf("tail read: %d bytes at offset %d", n, tailOff)
	}

	// 4. Sequential NFS pattern on the first 4 MiB — playback start.
	const playBlocks = 32
	start := time.Now()
	for i := 0; i < playBlocks; i++ {
		off := int64(i * nfsBlockSize)
		f, err := fs.Open(avatarFile)
		if err != nil {
			t.Fatalf("block %d Open: %v", i, err)
		}
		buf := make([]byte, nfsBlockSize)
		n, err := f.ReadAt(buf, off)
		f.Close()
		if err != nil && err != io.EOF {
			t.Fatalf("block %d ReadAt at %d: %v (read %d)", i, off, err, n)
		}
	}
	elapsed := time.Since(start)
	mbps := float64(playBlocks*nfsBlockSize) / elapsed.Seconds() / 1024 / 1024
	t.Logf("playback start: %d blocks in %s (%.1f MB/s)", playBlocks, elapsed, mbps)
}
