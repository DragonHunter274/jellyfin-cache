package mount

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"strings"
	"sync"

	billy "github.com/go-git/go-billy/v5"
	nfs "github.com/willscott/go-nfs"
	nfshelper "github.com/willscott/go-nfs/helpers"

	"jellyfin-cache/cache"
	"jellyfin-cache/config"
	"jellyfin-cache/vfs"
)

// ServeNFS starts an NFS server at cfg.Mount.Listen that exposes the cache
// manager via a billy.Filesystem adapter.
//
// It blocks until the context is cancelled.
func ServeNFS(ctx context.Context, mgr *cache.Manager, cfg config.MountConfig, log *slog.Logger) error {
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("NFS listen on %q: %w", cfg.Listen, err)
	}
	log.Info("NFS listening", "addr", cfg.Listen)

	billyFS := vfs.New(ctx, mgr)
	handler, err := newPersistentHandler(billyFS, mgr)
	if err != nil {
		return fmt.Errorf("building NFS handler: %w", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- nfs.Serve(listener, handler)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		listener.Close()
		return nil
	}
}

// persistentHandler implements nfs.Handler with handle↔path mappings stored in
// BoltDB.  On startup it reloads all previously issued handles so the kernel
// NFS client never sees NFS3ERR_STALE after a daemon restart.
type persistentHandler struct {
	inner nfs.Handler      // NullAuthHandler — provides Mount/Change/FSStat
	fs    billy.Filesystem // the single filesystem we serve
	mgr   *cache.Manager   // for handle persistence

	mu     sync.RWMutex
	toPath map[[16]byte][]string // handle → path segments
	toID   map[string][16]byte   // joined path → handle

	// Verifier cache for READDIRPLUS (in-memory only; not critical).
	vMu       sync.Mutex
	verifiers map[uint64]verifierEntry
}

type verifierEntry struct {
	path     string
	contents []fs.FileInfo
}

func newPersistentHandler(billyFS billy.Filesystem, mgr *cache.Manager) (*persistentHandler, error) {
	h := &persistentHandler{
		inner:     nfshelper.NewNullAuthHandler(billyFS),
		fs:        billyFS,
		mgr:       mgr,
		toPath:    make(map[[16]byte][]string),
		toID:      make(map[string][16]byte),
		verifiers: make(map[uint64]verifierEntry),
	}

	// Restore persisted handles so existing kernel mounts survive the restart.
	ids, paths, err := mgr.LoadNFSHandles()
	if err != nil {
		return nil, fmt.Errorf("loading persisted NFS handles: %w", err)
	}
	for i, id := range ids {
		h.toPath[id] = paths[i]
		h.toID[strings.Join(paths[i], "/")] = id
	}
	return h, nil
}

// ---- nfs.Handler implementation --------------------------------------------

func (h *persistentHandler) Mount(ctx context.Context, conn net.Conn, req nfs.MountRequest) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	return h.inner.Mount(ctx, conn, req)
}

func (h *persistentHandler) Change(f billy.Filesystem) billy.Change {
	return h.inner.Change(f)
}

func (h *persistentHandler) FSStat(ctx context.Context, f billy.Filesystem, s *nfs.FSStat) error {
	return h.inner.FSStat(ctx, f, s)
}

func (h *persistentHandler) HandleLimit() int {
	// No cap — every handle lives in the DB, not an LRU.
	return 1 << 20
}

func (h *persistentHandler) ToHandle(_ billy.Filesystem, path []string) []byte {
	key := strings.Join(path, "/")

	h.mu.RLock()
	if id, ok := h.toID[key]; ok {
		h.mu.RUnlock()
		return id[:]
	}
	h.mu.RUnlock()

	// Generate a new random 16-byte handle.
	var id [16]byte
	if _, err := io.ReadFull(rand.Reader, id[:]); err != nil {
		panic("crypto/rand: " + err.Error())
	}

	h.mu.Lock()
	// Re-check after acquiring the write lock (another goroutine may have
	// created the handle while we were waiting).
	if existing, ok := h.toID[key]; ok {
		h.mu.Unlock()
		return existing[:]
	}
	pathCopy := make([]string, len(path))
	copy(pathCopy, path)
	h.toID[key] = id
	h.toPath[id] = pathCopy
	h.mu.Unlock()

	// Persist asynchronously so we don't delay the NFS response.
	go func() { _ = h.mgr.SaveNFSHandle(id, pathCopy) }()

	return id[:]
}

func (h *persistentHandler) FromHandle(fh []byte) (billy.Filesystem, []string, error) {
	if len(fh) != 16 {
		return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
	}
	var id [16]byte
	copy(id[:], fh)

	h.mu.RLock()
	path, ok := h.toPath[id]
	h.mu.RUnlock()
	if !ok {
		return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
	}

	out := make([]string, len(path))
	copy(out, path)
	return h.fs, out, nil
}

func (h *persistentHandler) InvalidateHandle(_ billy.Filesystem, fh []byte) error {
	if len(fh) != 16 {
		return nil
	}
	var id [16]byte
	copy(id[:], fh)

	h.mu.Lock()
	if path, ok := h.toPath[id]; ok {
		delete(h.toID, strings.Join(path, "/"))
		delete(h.toPath, id)
	}
	h.mu.Unlock()

	go func() { _ = h.mgr.DeleteNFSHandle(id) }()
	return nil
}

// ---- nfs.CachingHandler implementation (directory listing verifiers) -------

func (h *persistentHandler) VerifierFor(path string, contents []fs.FileInfo) uint64 {
	sum := sha256.New()
	sum.Write([]byte(path))
	for _, fi := range contents {
		sum.Write([]byte(fi.Name()))
	}
	v := binary.BigEndian.Uint64(sum.Sum(nil)[:8])

	h.vMu.Lock()
	h.verifiers[v] = verifierEntry{path, contents}
	// Rough cap: drop the map when it grows too large.
	if len(h.verifiers) > 4096 {
		h.verifiers = make(map[uint64]verifierEntry)
	}
	h.vMu.Unlock()
	return v
}

func (h *persistentHandler) DataForVerifier(path string, v uint64) []fs.FileInfo {
	h.vMu.Lock()
	e, ok := h.verifiers[v]
	h.vMu.Unlock()
	if !ok || e.path != path {
		return nil
	}
	return e.contents
}
