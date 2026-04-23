// Package vfs exposes the cache.Manager as both a billy.Filesystem (for NFS)
// and as a FUSE node tree (for FUSE mounts).
package vfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	billy "github.com/go-git/go-billy/v5"

	"jellyfin-cache/backend"
	"jellyfin-cache/cache"
)

// FS implements billy.Filesystem on top of the cache.Manager.
// go-nfs uses billy.Filesystem; the FUSE layer calls the same methods.
type FS struct {
	mgr  *cache.Manager
	ctx  context.Context
	pool *handlePool
}

// New creates an FS backed by the given cache.Manager.
func New(ctx context.Context, mgr *cache.Manager) *FS {
	fs := &FS{mgr: mgr, ctx: ctx, pool: newHandlePool()}
	go fs.pool.evictLoop(ctx)
	return fs
}

// ---- per-file handle pool --------------------------------------------------
//
// go-nfs calls Open → ReadAt(buf, offset) → Close for every NFS READ RPC.
// Without pooling each call opens a new remote connection (one HTTP range
// request per 128 KiB block).  The pool lets sequential reads reuse the same
// open stream: the ReadSeekCloser is returned to the pool on Close() instead
// of being discarded, and the next Open() for the same path grabs it.

const (
	poolMaxPerFile = 4              // max idle handles per path
	poolIdleTTL    = 60 * time.Second
	poolEvictEvery = 30 * time.Second
)

type poolEntry struct {
	rc   io.ReadSeekCloser
	idle time.Time
}

type handlePool struct {
	mu      sync.Mutex
	entries map[string][]poolEntry
}

func newHandlePool() *handlePool {
	return &handlePool{entries: make(map[string][]poolEntry)}
}

func (p *handlePool) get(path string) io.ReadSeekCloser {
	p.mu.Lock()
	defer p.mu.Unlock()
	list := p.entries[path]
	if len(list) == 0 {
		return nil
	}
	e := list[len(list)-1]
	p.entries[path] = list[:len(list)-1]
	return e.rc
}

func (p *handlePool) put(path string, rc io.ReadSeekCloser) {
	p.mu.Lock()
	defer p.mu.Unlock()
	list := p.entries[path]
	if len(list) >= poolMaxPerFile {
		// Pool full — close the oldest entry and discard.
		list[0].rc.Close()
		list = list[1:]
	}
	p.entries[path] = append(list, poolEntry{rc: rc, idle: time.Now()})
}

func (p *handlePool) evictLoop(ctx context.Context) {
	t := time.NewTicker(poolEvictEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			p.closeAll()
			return
		case <-t.C:
			p.evict()
		}
	}
}

func (p *handlePool) evict() {
	p.mu.Lock()
	defer p.mu.Unlock()
	cutoff := time.Now().Add(-poolIdleTTL)
	for path, list := range p.entries {
		kept := list[:0]
		for _, e := range list {
			if e.idle.After(cutoff) {
				kept = append(kept, e)
			} else {
				e.rc.Close()
			}
		}
		if len(kept) == 0 {
			delete(p.entries, path)
		} else {
			p.entries[path] = kept
		}
	}
}

func (p *handlePool) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, list := range p.entries {
		for _, e := range list {
			e.rc.Close()
		}
	}
	p.entries = make(map[string][]poolEntry)
}

// pooledFile wraps a readFile and returns its ReadSeekCloser to the pool on Close.
type pooledFile struct {
	readFile
	pool *handlePool
	path string
}

func (f *pooledFile) Close() error {
	f.pool.put(f.path, f.rc)
	return nil
}

// ---- billy.Filesystem implementation ---------------------------------------

func (fs *FS) Create(filename string) (billy.File, error) {
	return fs.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
}

func (fs *FS) Open(filename string) (billy.File, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0o644)
}

func (fs *FS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	filename = clean(filename)
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE) != 0 {
		return newWriteFile(fs.ctx, fs.mgr, filename, perm)
	}

	// Try the pool first — avoids opening a new remote connection for every
	// NFS READ RPC (go-nfs calls Open → ReadAt → Close per request).
	if rc := fs.pool.get(filename); rc != nil {
		info, err := fs.mgr.Stat(fs.ctx, filename)
		if err != nil {
			rc.Close()
			return nil, err
		}
		return &pooledFile{
			readFile: readFile{name: filename, rc: rc, size: info.Size},
			pool:     fs.pool,
			path:     filename,
		}, nil
	}

	rc, err := fs.mgr.Open(fs.ctx, filename)
	if err != nil {
		return nil, err
	}
	info, err := fs.mgr.Stat(fs.ctx, filename)
	if err != nil {
		rc.Close()
		return nil, err
	}
	return &pooledFile{
		readFile: readFile{name: filename, rc: rc, size: info.Size},
		pool:     fs.pool,
		path:     filename,
	}, nil
}

func (fs *FS) Stat(filename string) (os.FileInfo, error) {
	filename = clean(filename)
	if filename == "" {
		return &fileInfo{name: ".", modTime: time.Now(), isDir: true}, nil
	}
	// Check knownDirs first: backend.Stat answers "is this a dir?" by listing
	// its contents, so we must avoid manager.Stat entirely for known directories.
	if fs.mgr.IsKnownDir(filename) {
		return &fileInfo{name: path.Base(filename), modTime: time.Now(), isDir: true}, nil
	}
	rec, err := fs.mgr.Stat(fs.ctx, filename)
	if err == nil {
		return infoToFileInfo(rec), nil
	}
	// Not a known file — check whether it is a directory (populates knownDirs).
	if _, listErr := fs.mgr.List(fs.ctx, filename); listErr == nil {
		return &fileInfo{name: path.Base(filename), modTime: time.Now(), isDir: true}, nil
	}
	return nil, err
}

func (fs *FS) Rename(oldpath, newpath string) error {
	return billy.ErrNotSupported
}

func (fs *FS) Remove(filename string) error {
	// TODO: propagate to union
	return billy.ErrNotSupported
}

func (fs *FS) Join(elem ...string) string {
	return path.Join(elem...)
}

func (fs *FS) TempFile(dir, prefix string) (billy.File, error) {
	return nil, billy.ErrNotSupported
}

func (fs *FS) ReadDir(p string) ([]os.FileInfo, error) {
	p = clean(p)
	infos, err := fs.mgr.List(fs.ctx, p)
	if err != nil {
		return nil, err
	}
	out := make([]os.FileInfo, len(infos))
	for i, info := range infos {
		out[i] = backendInfoToFileInfo(info)
	}
	return out, nil
}

func (fs *FS) MkdirAll(filename string, perm os.FileMode) error {
	filename = clean(filename)
	if filename == "" {
		return nil // root always exists
	}
	return fs.mgr.Mkdir(fs.ctx, filename)
}

func (fs *FS) Lstat(filename string) (os.FileInfo, error) {
	return fs.Stat(filename)
}

func (fs *FS) Symlink(target, link string) error {
	return billy.ErrNotSupported
}

func (fs *FS) Readlink(link string) (string, error) {
	return "", billy.ErrNotSupported
}

func (fs *FS) Chroot(p string) (billy.Filesystem, error) {
	return &chrootFS{FS: fs, base: p}, nil
}

func (fs *FS) Root() string { return "/" }

// ---- helpers ---------------------------------------------------------------

func clean(p string) string {
	c := path.Clean("/" + p)
	if c == "/" {
		return ""
	}
	return c[1:] // strip leading /
}

func infoToFileInfo(rec *cache.FileRecord) os.FileInfo {
	return &fileInfo{
		name:    path.Base(rec.Path),
		size:    rec.Size,
		modTime: rec.ModTime,
		isDir:   false,
	}
}

func backendInfoToFileInfo(info backend.Info) os.FileInfo {
	return &fileInfo{
		name:    info.Name,
		size:    info.Size,
		modTime: info.ModTime,
		isDir:   info.IsDir,
	}
}

// fileInfo implements os.FileInfo.
type fileInfo struct {
	name    string
	size    int64
	modTime time.Time
	isDir   bool
}

func (fi *fileInfo) Name() string      { return fi.name }
func (fi *fileInfo) Size() int64       { return fi.size }
func (fi *fileInfo) Mode() os.FileMode {
	if fi.isDir {
		return os.ModeDir | 0o755
	}
	return 0o644
}
func (fi *fileInfo) ModTime() time.Time { return fi.modTime }
func (fi *fileInfo) IsDir() bool        { return fi.isDir }
func (fi *fileInfo) Sys() interface{}   { return nil }

// ---- readFile: wraps cache.CacheReader as billy.File -----------------------

type readFile struct {
	name string
	rc   io.ReadSeekCloser
	size int64
}

func (f *readFile) Name() string                               { return f.name }
func (f *readFile) Read(p []byte) (int, error)                 { return f.rc.Read(p) }
func (f *readFile) ReadAt(p []byte, off int64) (int, error) {
	if _, err := f.rc.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}
	// Loop until the buffer is full or the stream ends — a single Read may
	// return fewer bytes than requested even without error (short read).
	var total int
	for total < len(p) {
		n, err := f.rc.Read(p[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
func (f *readFile) Seek(offset int64, whence int) (int64, error) { return f.rc.Seek(offset, whence) }
func (f *readFile) Close() error                                 { return f.rc.Close() }
func (f *readFile) Write(p []byte) (int, error)                  { return 0, billy.ErrReadOnly }
func (f *readFile) Truncate(size int64) error                    { return billy.ErrReadOnly }
func (f *readFile) Lock() error                                  { return nil }
func (f *readFile) Unlock() error                                { return nil }

// ---- writeFile: pipes writes through the cache.Manager via a temp file ----
//
// Using a temp file instead of an in-memory buffer keeps heap usage flat for
// large uploads (e.g. multi-GB media files).

type writeFile struct {
	ctx     context.Context
	mgr     *cache.Manager
	name    string
	tmp     *os.File
	modTime time.Time
}

func newWriteFile(ctx context.Context, mgr *cache.Manager, name string, _ os.FileMode) (*writeFile, error) {
	tmp, err := os.CreateTemp("", "jellyfin-cache-upload-*")
	if err != nil {
		return nil, fmt.Errorf("creating temp file for upload of %q: %w", name, err)
	}
	return &writeFile{ctx: ctx, mgr: mgr, name: name, tmp: tmp, modTime: time.Now()}, nil
}

func (f *writeFile) Name() string                          { return f.name }
func (f *writeFile) Read(p []byte) (int, error)            { return 0, billy.ErrNotSupported }
func (f *writeFile) ReadAt(p []byte, _ int64) (int, error) { return 0, billy.ErrNotSupported }
func (f *writeFile) Seek(_ int64, _ int) (int64, error)    { return 0, billy.ErrNotSupported }
func (f *writeFile) Lock() error                           { return nil }
func (f *writeFile) Unlock() error                         { return nil }

func (f *writeFile) Truncate(size int64) error {
	return f.tmp.Truncate(size)
}

func (f *writeFile) Write(p []byte) (int, error) {
	return f.tmp.Write(p)
}

func (f *writeFile) Close() error {
	defer os.Remove(f.tmp.Name())
	defer f.tmp.Close()
	if _, err := f.tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}
	stat, err := f.tmp.Stat()
	if err != nil {
		return err
	}
	return f.mgr.Put(f.ctx, f.name, f.tmp, f.modTime, stat.Size())
}

// ---- chrootFS --------------------------------------------------------------

type chrootFS struct {
	*FS
	base string
}

func (c *chrootFS) Root() string { return c.base }
