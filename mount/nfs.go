package mount

import (
	"context"
	"fmt"
	"log/slog"
	"net"

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
	handler := nfshelper.NewNullAuthHandler(billyFS)
	// 1024 is far too small: NFS3 READDIRPLUS creates a handle for every file
	// in every directory during a library scan, evicting video handles before
	// playback starts and causing NFS3ERR_STALE → EIO on the client.
	cacheHelper := nfshelper.NewCachingHandler(handler, 1<<17) // 131072

	errCh := make(chan error, 1)
	go func() {
		errCh <- nfs.Serve(listener, cacheHelper)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		listener.Close()
		return nil
	}
}
