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

// rcloneReadSeeker adapts rclone's io.ReadCloser into io.ReadSeekCloser.
//
// The connection is opened lazily on the first Read so that callers that seek
// before reading (e.g. go-nfs onRead: Open → ReadAt(buf, offset) → Close) get
// a single range-request starting at the correct offset instead of two requests
// (one from position 0, one reopened at the seek target).
type rcloneReadSeeker struct {
	rc     io.ReadCloser // nil until first Read
	obj    rfs.Object
	ctx    context.Context
	offset int64
}

func (r *rcloneReadSeeker) ensureOpen() error {
	if r.rc != nil {
		return nil
	}
	var (
		rc  io.ReadCloser
		err error
	)
	if r.offset == 0 {
		rc, err = r.obj.Open(r.ctx)
	} else {
		rc, err = r.obj.Open(r.ctx, &rfs.RangeOption{Start: r.offset, End: -1})
	}
	if err != nil {
		return fmt.Errorf("open at %d: %w", r.offset, err)
	}
	r.rc = rc
	return nil
}

func (r *rcloneReadSeeker) Read(p []byte) (int, error) {
	if err := r.ensureOpen(); err != nil {
		return 0, err
	}
	n, err := r.rc.Read(p)
	r.offset += int64(n)
	return n, err
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

	// No-op seek — covers the sequential ReadAt pattern after connection opens.
	if abs == r.offset {
		return abs, nil
	}

	// Not yet connected: just update the start offset for free.
	if r.rc == nil {
		r.offset = abs
		return abs, nil
	}

	// Small forward seek on an open connection: discard rather than reopen.
	const discardThreshold = 512 * 1024
	if abs > r.offset && abs-r.offset <= discardThreshold {
		if _, err := io.CopyN(io.Discard, r.rc, abs-r.offset); err == nil {
			r.offset = abs
			return abs, nil
		}
		// discard failed — fall through to close+lazy-reopen
	}

	// Backward seek or large forward seek: close and let ensureOpen reopen at
	// the new position on the next Read.
	_ = r.rc.Close()
	r.rc = nil
	r.offset = abs
	return abs, nil
}

func (r *rcloneReadSeeker) Close() error {
	if r.rc == nil {
		return nil
	}
	return r.rc.Close()
}
