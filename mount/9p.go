package mount

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net"
	"path"
	"syscall"
	"time"

	"github.com/hugelgupf/p9/fsimpl/templatefs"
	"github.com/hugelgupf/p9/p9"

	"jellyfin-cache/cache"
	"jellyfin-cache/config"
)

// Serve9P starts a 9P2000.L server at cfg.Listen that exposes the cache
// manager. It blocks until the context is cancelled.
func Serve9P(ctx context.Context, mgr *cache.Manager, cfg config.MountConfig, log *slog.Logger) error {
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("9P listen on %q: %w", cfg.Listen, err)
	}
	log.Info("9P listening", "addr", cfg.Listen)

	srv := p9.NewServer(&p9Attacher{mgr: mgr, ctx: ctx})

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ServeContext(ctx, listener)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		listener.Close()
		return nil
	}
}

type p9Attacher struct {
	mgr *cache.Manager
	ctx context.Context
}

func (a *p9Attacher) Attach() (p9.File, error) {
	return &p9Dir{mgr: a.mgr, ctx: a.ctx, logPath: ""}, nil
}

func pathQID(logPath string, isDir bool) p9.QID {
	h := fnv.New64a()
	h.Write([]byte(logPath))
	t := p9.TypeRegular
	if isDir {
		t = p9.TypeDir
	}
	return p9.QID{Type: t, Path: h.Sum64()}
}

// p9Dir is a directory fid backed by cache.Manager.
type p9Dir struct {
	templatefs.ReadOnlyDir
	mgr     *cache.Manager
	ctx     context.Context
	logPath string
}

func (d *p9Dir) Walk(names []string) ([]p9.QID, p9.File, error) {
	if len(names) == 0 {
		return nil, &p9Dir{mgr: d.mgr, ctx: d.ctx, logPath: d.logPath}, nil
	}
	qids := make([]p9.QID, 0, len(names))
	cur := d.logPath
	var last p9.File
	for i, name := range names {
		child := path.Join(cur, name)
		if rec, err := d.mgr.Stat(d.ctx, child); err == nil {
			qids = append(qids, pathQID(child, false))
			last = &p9File{mgr: d.mgr, ctx: d.ctx, logPath: child, size: rec.Size, modTime: rec.ModTime}
			cur = child
			continue
		}
		if _, err := d.mgr.List(d.ctx, child); err == nil {
			qids = append(qids, pathQID(child, true))
			last = &p9Dir{mgr: d.mgr, ctx: d.ctx, logPath: child}
			cur = child
			continue
		}
		if i == 0 {
			return nil, nil, syscall.ENOENT
		}
		return qids, last, nil
	}
	return qids, last, nil
}

func (d *p9Dir) WalkGetAttr(names []string) ([]p9.QID, p9.File, p9.AttrMask, p9.Attr, error) {
	qids, f, err := d.Walk(names)
	if err != nil {
		return nil, nil, p9.AttrMask{}, p9.Attr{}, err
	}
	mask := p9.AttrMask{Mode: true, Size: true, MTime: true, NLink: true}
	_, filled, attr, err := f.GetAttr(mask)
	if err != nil {
		f.Close()
		return nil, nil, p9.AttrMask{}, p9.Attr{}, err
	}
	return qids, f, filled, attr, nil
}

func (d *p9Dir) Open(_ p9.OpenFlags) (p9.QID, uint32, error) {
	return pathQID(d.logPath, true), 0, nil
}

func (d *p9Dir) Readdir(offset uint64, count uint32) (p9.Dirents, error) {
	infos, err := d.mgr.List(d.ctx, d.logPath)
	if err != nil {
		return nil, syscall.EIO
	}
	var out p9.Dirents
	for i, info := range infos {
		if uint64(i) < offset {
			continue
		}
		if uint32(len(out)) >= count {
			break
		}
		t := p9.TypeRegular
		if info.IsDir {
			t = p9.TypeDir
		}
		out = append(out, p9.Dirent{
			QID:    pathQID(path.Join(d.logPath, info.Name), info.IsDir),
			Offset: uint64(i + 1),
			Type:   t,
			Name:   info.Name,
		})
	}
	return out, nil
}

func (d *p9Dir) GetAttr(req p9.AttrMask) (p9.QID, p9.AttrMask, p9.Attr, error) {
	attr := p9.Attr{
		Mode:  p9.ModeDirectory | 0o555,
		NLink: p9.NLink(2),
		UID:   p9.NoUID,
		GID:   p9.NoGID,
	}
	return pathQID(d.logPath, true), req, attr, nil
}

func (d *p9Dir) StatFS() (p9.FSStat, error) {
	return p9.FSStat{Type: 0x01021997, BlockSize: 4096, NameLength: 255}, nil
}

func (d *p9Dir) Close() error                        { return nil }
func (d *p9Dir) Renamed(_ p9.File, _ string)         {}

// p9File is a regular file fid backed by cache.Manager.
type p9File struct {
	templatefs.ReadOnlyFile
	mgr     *cache.Manager
	ctx     context.Context
	logPath string
	size    int64
	modTime time.Time
	rc      io.ReadSeekCloser
}

func (f *p9File) Walk(names []string) ([]p9.QID, p9.File, error) {
	if len(names) != 0 {
		return nil, nil, syscall.ENOTDIR
	}
	return nil, &p9File{mgr: f.mgr, ctx: f.ctx, logPath: f.logPath, size: f.size, modTime: f.modTime}, nil
}

func (f *p9File) WalkGetAttr(names []string) ([]p9.QID, p9.File, p9.AttrMask, p9.Attr, error) {
	qids, file, err := f.Walk(names)
	if err != nil {
		return nil, nil, p9.AttrMask{}, p9.Attr{}, err
	}
	mask := p9.AttrMask{Mode: true, Size: true, MTime: true, NLink: true}
	_, filled, attr, err := file.GetAttr(mask)
	if err != nil {
		file.Close()
		return nil, nil, p9.AttrMask{}, p9.Attr{}, err
	}
	return qids, file, filled, attr, nil
}

func (f *p9File) Open(_ p9.OpenFlags) (p9.QID, uint32, error) {
	rc, err := f.mgr.Open(f.ctx, f.logPath)
	if err != nil {
		return p9.QID{}, 0, syscall.EIO
	}
	f.rc = rc
	return pathQID(f.logPath, false), 0, nil
}

func (f *p9File) ReadAt(p []byte, offset int64) (int, error) {
	if f.rc == nil {
		return 0, syscall.EBADF
	}
	if _, err := f.rc.Seek(offset, io.SeekStart); err != nil {
		return 0, syscall.EIO
	}
	n, err := f.rc.Read(p)
	if err == io.EOF {
		return n, nil
	}
	return n, err
}

func (f *p9File) GetAttr(req p9.AttrMask) (p9.QID, p9.AttrMask, p9.Attr, error) {
	attr := p9.Attr{
		Mode:             p9.ModeRegular | 0o444,
		Size:             uint64(f.size),
		NLink:            p9.NLink(1),
		UID:              p9.NoUID,
		GID:              p9.NoGID,
		MTimeSeconds:     uint64(f.modTime.Unix()),
		MTimeNanoSeconds: uint64(f.modTime.Nanosecond()),
	}
	return pathQID(f.logPath, false), req, attr, nil
}

func (f *p9File) StatFS() (p9.FSStat, error) {
	return p9.FSStat{Type: 0x01021997, BlockSize: 4096, NameLength: 255}, nil
}

func (f *p9File) Close() error {
	if f.rc != nil {
		err := f.rc.Close()
		f.rc = nil
		return err
	}
	return nil
}

func (f *p9File) Renamed(_ p9.File, _ string) {}
