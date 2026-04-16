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
	name     string
	fs       rfs.Fs
	readOnly bool
	priority int
}

// NewRclone creates a Backend backed by the given rclone remote path.
// rclone must already be configured (config file set, backends registered).
func NewRclone(ctx context.Context, cfg config.RemoteConfig) (*RcloneBackend, error) {
	f, err := rfs.NewFs(ctx, cfg.RclonePath)
	if err != nil {
		return nil, fmt.Errorf("opening rclone remote %q: %w", cfg.RclonePath, err)
	}
	return &RcloneBackend{
		name:     cfg.Name,
		fs:       f,
		readOnly: cfg.ReadOnly,
		priority: cfg.Priority,
	}, nil
}

func (b *RcloneBackend) Name() string     { return b.name }
func (b *RcloneBackend) ReadOnly() bool   { return b.readOnly }
func (b *RcloneBackend) Priority() int    { return b.priority }

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
	rc, err := obj.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading %q on %q: %w", path, b.name, err)
	}
	return &rcloneReadSeeker{rc: rc, obj: obj, ctx: ctx}, nil
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

// rcloneReadSeeker adapts rclone's io.ReadCloser into io.ReadSeekCloser by
// re-opening the object at the requested offset on Seek.
type rcloneReadSeeker struct {
	rc     io.ReadCloser
	obj    rfs.Object
	ctx    context.Context
	offset int64
}

func (r *rcloneReadSeeker) Read(p []byte) (int, error) {
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

	// Re-open with Range header equivalent (rclone Options).
	_ = r.rc.Close()
	rc, err := r.obj.Open(r.ctx, &rfs.RangeOption{Start: abs, End: -1})
	if err != nil {
		return 0, fmt.Errorf("seek reopen at %d: %w", abs, err)
	}
	r.rc = rc
	r.offset = abs
	return abs, nil
}

func (r *rcloneReadSeeker) Close() error {
	return r.rc.Close()
}
