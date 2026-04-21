//go:build integration

package backend_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/all"
	"github.com/rclone/rclone/fs/config/configfile"

	"jellyfin-cache/backend"
	"jellyfin-cache/config"
)

const (
	testRemote = "storagebox-media"
	testPath   = "TV/The Rookie/Season 8/The Rookie - S08E10 - His Name Was Martin WEBRip-1080p.mkv"
	blockSize  = 128 * 1024 // 128 KiB — matches go-nfs READDIRPLUS read size
)

func TestMain(m *testing.M) {
	configfile.Install()
	m.Run()
}

func openBackend(t *testing.T) backend.Backend {
	t.Helper()
	b, err := backend.NewRclone(context.Background(), config.RemoteConfig{
		Name:        "test",
		RclonePath:  testRemote + ":",
		ReadOnly:    true,
		Passthrough: true,
	})
	if err != nil {
		t.Fatalf("NewRclone: %v", err)
	}
	return b
}

// TestRclone_Stat verifies the file is reachable and has a sensible size.
func TestRclone_Stat(t *testing.T) {
	b := openBackend(t)
	info, err := b.Stat(context.Background(), testPath)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	t.Logf("size=%d modTime=%s", info.Size, info.ModTime.Format(time.RFC3339))
	if info.Size < 1<<20 {
		t.Errorf("suspiciously small size %d", info.Size)
	}
	if info.IsDir {
		t.Error("expected file, got directory")
	}
}

// TestRclone_SequentialRead reads the first 4 MB in 128 KiB blocks via ReadAt
// (the NFS pattern) and compares the result against a fresh sequential stream.
// This is the primary regression test for the Seek optimisation: sequential
// ReadAt calls must not reopen the remote connection for every block.
func TestRclone_SequentialRead(t *testing.T) {
	const total = 4 * 1024 * 1024

	ctx := context.Background()
	b := openBackend(t)

	// Reference: single contiguous read.
	ref := func() []byte {
		rc, err := b.Open(ctx, testPath)
		if err != nil {
			t.Fatalf("Open (ref): %v", err)
		}
		defer rc.Close()
		buf := make([]byte, total)
		if _, err := io.ReadFull(rc, buf); err != nil {
			t.Fatalf("ReadFull (ref): %v", err)
		}
		return buf
	}()

	// Subject: sequential seek+read calls (simulates vfs.readFile.ReadAt which
	// does Seek(off, SeekStart) then Read — the pattern go-nfs uses).
	got := func() []byte {
		rc, err := b.Open(ctx, testPath)
		if err != nil {
			t.Fatalf("Open (subject): %v", err)
		}
		defer rc.Close()

		buf := make([]byte, total)
		start := time.Now()
		for off := 0; off < total; off += blockSize {
			end := off + blockSize
			if end > total {
				end = total
			}
			if _, err := rc.Seek(int64(off), io.SeekStart); err != nil {
				t.Fatalf("Seek offset=%d: %v", off, err)
			}
			if _, err := io.ReadFull(rc, buf[off:end]); err != nil && err != io.ErrUnexpectedEOF {
				t.Fatalf("Read offset=%d: %v", off, err)
			}
		}
		t.Logf("sequential seek+read of %d bytes in %d-byte blocks took %s", total, blockSize, time.Since(start))
		return buf
	}()

	if !bytes.Equal(ref, got) {
		t.Error("sequential ReadAt data does not match reference stream")
	}
}

// TestRclone_SeekNoOp verifies that seeking to the current position returns
// immediately without a remote round-trip (no data divergence, no error).
func TestRclone_SeekNoOp(t *testing.T) {
	ctx := context.Background()
	b := openBackend(t)
	rc, err := b.Open(ctx, testPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()
	rs := rc.(io.ReadSeeker)

	first := make([]byte, blockSize)
	if _, err := io.ReadFull(rs, first); err != nil {
		t.Fatalf("initial read: %v", err)
	}

	// Seek to current position — should be a no-op.
	pos, err := rs.Seek(blockSize, io.SeekStart)
	if err != nil {
		t.Fatalf("no-op seek: %v", err)
	}
	if pos != int64(blockSize) {
		t.Fatalf("seek returned %d, want %d", pos, blockSize)
	}

	// Continue reading — must get the next block, not a repeat.
	second := make([]byte, blockSize)
	if _, err := io.ReadFull(rs, second); err != nil {
		t.Fatalf("read after no-op seek: %v", err)
	}
	if bytes.Equal(first, second) {
		t.Error("first and second blocks are identical — seek may have rewound")
	}
}

// TestRclone_SmallForwardSeek verifies that a forward seek within the discard
// threshold reads the correct data at the new position.
func TestRclone_SmallForwardSeek(t *testing.T) {
	const skip = 256 * 1024 // 256 KiB — within 512 KiB discard threshold
	ctx := context.Background()
	b := openBackend(t)

	// Reference: open fresh and skip via Read.
	ref := func() []byte {
		rc, err := b.Open(ctx, testPath)
		if err != nil {
			t.Fatalf("Open (ref): %v", err)
		}
		defer rc.Close()
		if _, err := io.CopyN(io.Discard, rc, skip); err != nil {
			t.Fatalf("discard (ref): %v", err)
		}
		buf := make([]byte, blockSize)
		if _, err := io.ReadFull(rc, buf); err != nil {
			t.Fatalf("read (ref): %v", err)
		}
		return buf
	}()

	// Subject: seek forward via Seek.
	got := func() []byte {
		rc, err := b.Open(ctx, testPath)
		if err != nil {
			t.Fatalf("Open (subject): %v", err)
		}
		defer rc.Close()
		rs := rc.(io.ReadSeeker)
		if _, err := rs.Seek(skip, io.SeekStart); err != nil {
			t.Fatalf("Seek: %v", err)
		}
		buf := make([]byte, blockSize)
		if _, err := io.ReadFull(rs, buf); err != nil {
			t.Fatalf("read (subject): %v", err)
		}
		return buf
	}()

	if !bytes.Equal(ref, got) {
		t.Error("small forward seek: data mismatch")
	}
}

// TestRclone_LargeForwardSeek verifies that a forward seek beyond the discard
// threshold (forces a reopen) returns the correct data.
func TestRclone_LargeForwardSeek(t *testing.T) {
	const skip = 2 * 1024 * 1024 // 2 MiB — beyond 512 KiB discard threshold
	ctx := context.Background()
	b := openBackend(t)

	ref := func() []byte {
		rc, err := b.Open(ctx, testPath)
		if err != nil {
			t.Fatalf("Open (ref): %v", err)
		}
		defer rc.Close()
		if _, err := io.CopyN(io.Discard, rc, skip); err != nil {
			t.Fatalf("discard (ref): %v", err)
		}
		buf := make([]byte, blockSize)
		if _, err := io.ReadFull(rc, buf); err != nil {
			t.Fatalf("read (ref): %v", err)
		}
		return buf
	}()

	got := func() []byte {
		rc, err := b.Open(ctx, testPath)
		if err != nil {
			t.Fatalf("Open (subject): %v", err)
		}
		defer rc.Close()
		rs := rc.(io.ReadSeeker)
		if _, err := rs.Seek(skip, io.SeekStart); err != nil {
			t.Fatalf("Seek: %v", err)
		}
		buf := make([]byte, blockSize)
		if _, err := io.ReadFull(rs, buf); err != nil {
			t.Fatalf("read (subject): %v", err)
		}
		return buf
	}()

	if !bytes.Equal(ref, got) {
		t.Error("large forward seek: data mismatch")
	}
}

// TestRclone_NFSReadPattern simulates exactly what go-nfs onRead does for each
// NFS READ RPC: Open → Seek(offset) → Read(128KiB) → Close.
//
// With lazy open, each iteration issues one range-request starting at offset.
// Without lazy open it would issue two (offset 0, then reopen at offset).
func TestRclone_NFSReadPattern(t *testing.T) {
	const (
		total  = 2 * 1024 * 1024 // 2 MiB — enough to show the pattern
		nBlocks = total / blockSize
	)
	ctx := context.Background()
	b := openBackend(t)

	// Reference: single stream.
	ref := make([]byte, total)
	{
		rc, err := b.Open(ctx, testPath)
		if err != nil {
			t.Fatalf("Open (ref): %v", err)
		}
		if _, err := io.ReadFull(rc, ref); err != nil {
			t.Fatalf("ReadFull (ref): %v", err)
		}
		rc.Close()
	}

	got := make([]byte, total)
	start := time.Now()
	for i := 0; i < nBlocks; i++ {
		off := int64(i * blockSize)
		rc, err := b.Open(ctx, testPath)
		if err != nil {
			t.Fatalf("block %d Open: %v", i, err)
		}
		if _, err := rc.Seek(off, io.SeekStart); err != nil {
			rc.Close()
			t.Fatalf("block %d Seek: %v", i, err)
		}
		if _, err := io.ReadFull(rc, got[off:off+blockSize]); err != nil {
			rc.Close()
			t.Fatalf("block %d Read: %v", i, err)
		}
		rc.Close()
	}
	elapsed := time.Since(start)
	t.Logf("NFS pattern: %d open+seek+read+close in %s (%.1f blocks/s, ~%.1f MB/s)",
		nBlocks, elapsed,
		float64(nBlocks)/elapsed.Seconds(),
		float64(total)/elapsed.Seconds()/1024/1024)

	if !bytes.Equal(ref, got) {
		t.Error("NFS pattern data does not match reference stream")
	}
}

// TestRclone_BackwardSeek verifies that seeking backward (always reopens) gives
// the same data as a fresh read from that position.
func TestRclone_BackwardSeek(t *testing.T) {
	const first = 512 * 1024  // read 512 KiB, then seek back to 128 KiB
	const seekTo = 128 * 1024
	ctx := context.Background()
	b := openBackend(t)

	ref := func() []byte {
		rc, err := b.Open(ctx, testPath)
		if err != nil {
			t.Fatalf("Open (ref): %v", err)
		}
		defer rc.Close()
		if _, err := io.CopyN(io.Discard, rc, seekTo); err != nil {
			t.Fatalf("discard (ref): %v", err)
		}
		buf := make([]byte, blockSize)
		if _, err := io.ReadFull(rc, buf); err != nil {
			t.Fatalf("read (ref): %v", err)
		}
		return buf
	}()

	got := func() []byte {
		rc, err := b.Open(ctx, testPath)
		if err != nil {
			t.Fatalf("Open (subject): %v", err)
		}
		defer rc.Close()
		rs := rc.(io.ReadSeeker)

		// Advance past seekTo.
		if _, err := io.CopyN(io.Discard, rs, first); err != nil {
			t.Fatalf("initial read: %v", err)
		}
		// Seek backward.
		if _, err := rs.Seek(seekTo, io.SeekStart); err != nil {
			t.Fatalf("backward seek: %v", err)
		}
		buf := make([]byte, blockSize)
		if _, err := io.ReadFull(rs, buf); err != nil {
			t.Fatalf("read after backward seek: %v", err)
		}
		return buf
	}()

	if !bytes.Equal(ref, got) {
		t.Error("backward seek: data mismatch")
	}
}
