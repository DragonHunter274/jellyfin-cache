package backend

import (
	"context"
	"fmt"
	"io"
	"time"

	// rclone filesystem abstraction
	rfs "github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/walk"

	"jellyfin-cache/config"
)

// RcloneBackend wraps an rclone fs.Fs as a Backend.
type RcloneBackend struct {
	name        string
	fs          rfs.Fs
	readOnly    bool
	priority    int
	passthrough bool
}

// NewRclone creates a Backend backed by the given rclone remote path.
// rclone must already be configured (config file set, backends registered).
func NewRclone(ctx context.Context, cfg config.RemoteConfig) (*RcloneBackend, error) {
	f, err := rfs.NewFs(ctx, cfg.RclonePath)
	if err != nil {
		return nil, fmt.Errorf("opening rclone remote %q: %w", cfg.RclonePath, err)
	}
	return &RcloneBackend{
		name:        cfg.Name,
		fs:          f,
		readOnly:    cfg.ReadOnly,
		priority:    cfg.Priority,
		passthrough: cfg.Passthrough,
	}, nil
}

func (b *RcloneBackend) Name() string        { return b.name }
func (b *RcloneBackend) ReadOnly() bool      { return b.readOnly }
func (b *RcloneBackend) Priority() int       { return b.priority }
func (b *RcloneBackend) Passthrough() bool   { return b.passthrough }

func (b *RcloneBackend) List(ctx context.Context, dir string) ([]Info, error) {
	var infos []Info
	err := walk.ListR(ctx, b.fs, dir, true, 1, walk.ListAll, func(entries rfs.DirEntries) error {
		for _, e := range entries {
			info := entryToInfo(e)
			infos = append(infos, info)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("listing %q on %q: %w", dir, b.name, err)
	}
	return infos, nil
}

func (b *RcloneBackend) Stat(ctx context.Context, path string) (*Info, error) {
	obj, err := b.fs.NewObject(ctx, path)
	if err == rfs.ErrorObjectNotFound {
		// Check if it is a directory
		entries, err2 := b.fs.List(ctx, path)
		if err2 != nil {
			return nil, fmt.Errorf("%q not found on %q", path, b.name)
		}
		_ = entries
		return &Info{
			Name:  lastName(path),
			Path:  path,
			IsDir: true,
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("stat %q on %q: %w", path, b.name, err)
	}
	info := entryToInfo(obj)
	return &info, nil
}

func (b *RcloneBackend) Open(ctx context.Context, path string) (io.ReadSeekCloser, error) {
	obj, err := b.fs.NewObject(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("opening %q on %q: %w", path, b.name, err)
	}
	// Connection is opened lazily on first Read so that a preceding Seek
	// (e.g. from go-nfs onRead → Open → ReadAt) sets the start offset for
	// free, avoiding an extra range-request from position 0.
	return &rcloneReadSeeker{obj: obj, ctx: ctx}, nil
}

func (b *RcloneBackend) Put(ctx context.Context, path string, r io.Reader, modTime time.Time, size int64) error {
	if b.readOnly {
		return fmt.Errorf("remote %q is read-only", b.name)
	}
	info := object.NewStaticObjectInfo(path, modTime, size, true, nil, b.fs)
	_, err := b.fs.Put(ctx, r, info)
	if err != nil {
		return fmt.Errorf("writing %q to %q: %w", path, b.name, err)
	}
	return nil
}

func (b *RcloneBackend) Remove(ctx context.Context, path string) error {
	if b.readOnly {
		return fmt.Errorf("remote %q is read-only", b.name)
	}
	obj, err := b.fs.NewObject(ctx, path)
	if err != nil {
		return fmt.Errorf("remove %q on %q: %w", path, b.name, err)
	}
	return obj.Remove(ctx)
}

func (b *RcloneBackend) Mkdir(ctx context.Context, path string) error {
	if b.readOnly {
		return fmt.Errorf("remote %q is read-only", b.name)
	}
	return b.fs.Mkdir(ctx, path)
}

// ---- helpers ---------------------------------------------------------------

func entryToInfo(e rfs.DirEntry) Info {
	info := Info{
		Name:    lastName(e.Remote()),
		Path:    e.Remote(),
		ModTime: e.ModTime(context.Background()),
	}
	switch v := e.(type) {
	case rfs.Object:
		info.Size = v.Size()
		info.MimeType = rfs.MimeType(context.Background(), v)
	case rfs.Directory:
		info.IsDir = true
	}
	return info
}

func lastName(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[i+1:]
		}
	}
	return path
}

// readAheadSize is the number of bytes fetched per remote read. A single large
// read lets rclone pipeline SFTP/HTTP requests internally, which is far more
// efficient than many small reads each paying full per-request RTT overhead.
const readAheadSize = 4 * 1024 * 1024 // 4 MiB

// rcloneReadSeeker adapts rclone's io.ReadCloser into io.ReadSeekCloser.
//
// Connection is opened lazily on first Read (avoiding a redundant open-at-0
// when callers seek before reading, as go-nfs does for each READ RPC).
//
// A read-ahead buffer absorbs the per-request overhead of protocols like SFTP
// that pipeline concurrent sub-requests over a single stream: the first Read
// at any offset fetches readAheadSize bytes in one shot; sequential Reads are
// then served from memory. A seek within the already-buffered window is free.
type rcloneReadSeeker struct {
	rc     io.ReadCloser // nil until first Read
	obj    rfs.Object
	ctx    context.Context
	offset int64

	ra    []byte // read-ahead buffer
	raOff int64  // file offset of ra[0]; -1 = invalid
	raLen int    // valid bytes in ra
}

// inBuffer reports whether offset abs is within the valid read-ahead window.
func (r *rcloneReadSeeker) inBuffer(abs int64) bool {
	return r.raLen > 0 && abs >= r.raOff && abs < r.raOff+int64(r.raLen)
}

func (r *rcloneReadSeeker) openAt(abs int64) error {
	if r.rc != nil {
		_ = r.rc.Close()
		r.rc = nil
	}
	var (
		rc  io.ReadCloser
		err error
	)
	if abs == 0 {
		rc, err = r.obj.Open(r.ctx)
	} else {
		rc, err = r.obj.Open(r.ctx, &rfs.RangeOption{Start: abs, End: -1})
	}
	if err != nil {
		return fmt.Errorf("open at %d: %w", abs, err)
	}
	r.rc = rc
	r.offset = abs
	return nil
}

// fillBuffer opens the remote at r.offset (if needed) and reads readAheadSize
// bytes into the buffer. It must be called when the buffer doesn't cover r.offset.
func (r *rcloneReadSeeker) fillBuffer() error {
	if r.rc == nil {
		if err := r.openAt(r.offset); err != nil {
			return err
		}
	}
	if cap(r.ra) < readAheadSize {
		r.ra = make([]byte, readAheadSize)
	}
	buf := r.ra[:readAheadSize]
	n, err := io.ReadFull(r.rc, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		r.raLen = 0
		r.raOff = -1
		return err
	}
	r.raOff = r.offset
	r.raLen = n
	return nil
}

func (r *rcloneReadSeeker) Read(p []byte) (int, error) {
	if !r.inBuffer(r.offset) {
		if err := r.fillBuffer(); err != nil {
			return 0, err
		}
		// fillBuffer may have read 0 bytes at EOF.
		if r.raLen == 0 {
			return 0, io.EOF
		}
	}
	bufStart := int(r.offset - r.raOff)
	avail := r.ra[bufStart:r.raLen]
	n := copy(p, avail)
	r.offset += int64(n)
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

func (r *rcloneReadSeeker) Seek(offset int64, whence int) (int64, error) {
	size := r.obj.Size()
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.offset + offset
	case io.SeekEnd:
		abs = size + offset
	default:
		return 0, fmt.Errorf("invalid whence %d", whence)
	}
	if abs < 0 {
		return 0, fmt.Errorf("negative seek position")
	}
	if abs == r.offset {
		return abs, nil
	}
	// If the target is within the buffer we can just move the cursor —
	// no network I/O required.
	if r.inBuffer(abs) {
		r.offset = abs
		return abs, nil
	}
	// Outside the buffer: invalidate and reopen lazily on next Read.
	r.raLen = 0
	r.raOff = -1
	if r.rc != nil {
		_ = r.rc.Close()
		r.rc = nil
	}
	r.offset = abs
	return abs, nil
}

func (r *rcloneReadSeeker) Close() error {
	r.raLen = 0
	if r.rc == nil {
		return nil
	}
	err := r.rc.Close()
	r.rc = nil
	return err
}
