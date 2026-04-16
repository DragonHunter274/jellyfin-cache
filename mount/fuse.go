// Package mount provides FUSE and NFS mounting for jellyfin-cache.
package mount

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"jellyfin-cache/cache"
	"jellyfin-cache/config"
)

// MountFUSE mounts the cache manager at cfg.Mount.Path using FUSE.
// It blocks until the context is cancelled, then unmounts cleanly.
func MountFUSE(ctx context.Context, mgr *cache.Manager, cfg config.MountConfig, log *slog.Logger) error {
	root := &fuseRoot{mgr: mgr, ctx: ctx}

	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName:     "jellyfin-cache",
			Name:       "jellyfin-cache",
			AllowOther: cfg.AllowOther,
			Debug:      false,
			Options:    []string{"ro"},
		},
	}

	server, err := fs.Mount(cfg.Path, root, opts)
	if err != nil {
		return fmt.Errorf("FUSE mount at %q: %w", cfg.Path, err)
	}
	log.Info("FUSE mounted", "path", cfg.Path)

	// Wait for context cancellation, then unmount.
	go func() {
		<-ctx.Done()
		if err := server.Unmount(); err != nil {
			log.Warn("FUSE unmount error", "err", err)
		}
	}()
	server.Wait()
	return nil
}

// ---- FUSE node types -------------------------------------------------------

// fuseRoot is the root inode.
type fuseRoot struct {
	fs.Inode
	mgr *cache.Manager
	ctx context.Context
}

var _ = (fs.NodeOnAdder)((*fuseRoot)(nil))
var _ = (fs.NodeLookuper)((*fuseRoot)(nil))
var _ = (fs.NodeReaddirer)((*fuseRoot)(nil))
var _ = (fs.NodeGetattrer)((*fuseRoot)(nil))

func (r *fuseRoot) OnAdd(ctx context.Context) {}

func (r *fuseRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return r.lookupPath(ctx, name, out)
}

func (r *fuseRoot) lookupPath(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	rec, err := r.mgr.Stat(ctx, name)
	if err != nil {
		return nil, syscall.ENOENT
	}
	fillAttr(rec, &out.Attr)
	out.SetAttrTimeout(30 * time.Second)
	out.SetEntryTimeout(30 * time.Second)

	child := r.NewInode(ctx, &fuseFile{
		mgr:  r.mgr,
		path: name,
		size: uint64(rec.Size),
	}, fs.StableAttr{Mode: syscall.S_IFREG | 0o444})
	return child, 0
}

func (r *fuseRoot) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return r.readdirPath(ctx, "")
}

func (r *fuseRoot) readdirPath(ctx context.Context, dir string) (fs.DirStream, syscall.Errno) {
	infos, err := r.mgr.List(ctx, dir)
	if err != nil {
		return nil, syscall.EIO
	}
	entries := make([]fuse.DirEntry, len(infos))
	for i, info := range infos {
		mode := uint32(syscall.S_IFREG | 0o444)
		if info.IsDir {
			mode = uint32(syscall.S_IFDIR | 0o555)
		}
		entries[i] = fuse.DirEntry{
			Name: info.Name,
			Mode: mode,
		}
	}
	return fs.NewListDirStream(entries), 0
}

func (r *fuseRoot) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Attr.Mode = syscall.S_IFDIR | 0o555
	out.Attr.Nlink = 2
	return 0
}

// fuseDir handles sub-directories.
type fuseDir struct {
	fs.Inode
	mgr  *cache.Manager
	path string
	root *fuseRoot
}

var _ = (fs.NodeLookuper)((*fuseDir)(nil))
var _ = (fs.NodeReaddirer)((*fuseDir)(nil))
var _ = (fs.NodeGetattrer)((*fuseDir)(nil))

func (d *fuseDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	full := path.Join(d.path, name)
	rec, err := d.mgr.Stat(ctx, full)
	if err != nil {
		return nil, syscall.ENOENT
	}
	if rec == nil {
		return nil, syscall.ENOENT
	}
	fillAttr(rec, &out.Attr)
	child := d.NewInode(ctx, &fuseFile{
		mgr:  d.mgr,
		path: full,
		size: uint64(rec.Size),
	}, fs.StableAttr{Mode: syscall.S_IFREG | 0o444})
	return child, 0
}

func (d *fuseDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return d.root.readdirPath(ctx, d.path)
}

func (d *fuseDir) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Attr.Mode = syscall.S_IFDIR | 0o555
	out.Attr.Nlink = 2
	return 0
}

// fuseFile implements a file node.
type fuseFile struct {
	fs.Inode
	mgr  *cache.Manager
	path string
	size uint64
}

var _ = (fs.NodeOpener)((*fuseFile)(nil))
var _ = (fs.NodeGetattrer)((*fuseFile)(nil))

func (f *fuseFile) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Attr.Size = f.size
	out.Attr.Mode = syscall.S_IFREG | 0o444
	out.Attr.Nlink = 1
	return 0
}

func (f *fuseFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	rc, err := f.mgr.Open(ctx, f.path)
	if err != nil {
		return nil, 0, syscall.EIO
	}
	return &fuseHandle{rc: rc}, fuse.FOPEN_KEEP_CACHE, 0
}

// fuseHandle is an open file handle.
type fuseHandle struct {
	rc io.ReadSeekCloser
}

var _ = (fs.FileReader)((*fuseHandle)(nil))
var _ = (fs.FileReleaser)((*fuseHandle)(nil))

func (h *fuseHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if _, err := h.rc.Seek(off, io.SeekStart); err != nil {
		return nil, syscall.EIO
	}
	n, err := h.rc.Read(dest)
	if err != nil && err != io.EOF {
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), 0
}

func (h *fuseHandle) Release(ctx context.Context) syscall.Errno {
	h.rc.Close()
	return 0
}

// ---- helpers ---------------------------------------------------------------

func fillAttr(rec *cache.FileRecord, attr *fuse.Attr) {
	attr.Size = uint64(rec.Size)
	attr.Mode = syscall.S_IFREG | 0o444
	attr.Nlink = 1
	t := rec.ModTime
	attr.Mtime = uint64(t.Unix())
	attr.Mtimensec = uint32(t.Nanosecond())
	attr.Ctime = attr.Mtime
	attr.Ctimensec = attr.Mtimensec
}

// ensureMountDir creates the mount point directory if it doesn't exist.
func ensureMountDir(p string) error {
	return os.MkdirAll(p, 0o755)
}
