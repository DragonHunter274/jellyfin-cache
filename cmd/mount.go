package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	// Register all rclone backends.
	_ "github.com/rclone/rclone/backend/all"
	rclonecfg "github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configfile"

	"jellyfin-cache/api"
	"jellyfin-cache/backend"
	"jellyfin-cache/cache"
	"jellyfin-cache/config"
	"jellyfin-cache/mount"
	"jellyfin-cache/union"
	"jellyfin-cache/webhook"
)

var mountCmd = &cobra.Command{
	Use:   "mount",
	Short: "Mount the cache filesystem (FUSE or NFS)",
	RunE:  runMount,
}

func init() {
	rootCmd.AddCommand(mountCmd)
}

func runMount(cmd *cobra.Command, args []string) error {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	log := buildLogger(cfg.Log)

	// Point rclone at the configured config file, then install the file-based
	// config loader so rclone reads remotes from rclone.conf.
	if cfg.RcloneConfig != "" {
		rclonecfg.SetConfigPath(cfg.RcloneConfig)
	}
	configfile.Install()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Build backends.
	backends := make([]backend.Backend, 0, len(cfg.Remotes))
	for _, rc := range cfg.Remotes {
		log.Info("opening remote", "name", rc.Name, "path", rc.RclonePath)
		b, err := backend.NewRclone(ctx, rc)
		if err != nil {
			return fmt.Errorf("backend %q: %w", rc.Name, err)
		}
		backends = append(backends, b)
	}

	u := union.New(backends)

	// Start cache manager.
	mgr, err := cache.NewManager(cfg.Cache, u, log)
	if err != nil {
		return fmt.Errorf("cache manager: %w", err)
	}
	defer func() {
		if err := mgr.Close(); err != nil {
			log.Error("closing cache manager", "err", err)
		}
	}()

	// Handle OS signals for clean shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Info("shutting down")
		cancel()
	}()

	// Start optional Jellyfin webhook listener.
	if cfg.Webhook.Enabled {
		wh := webhook.New(cfg.Webhook, mgr, log)
		go func() {
			if err := wh.Run(ctx); err != nil {
				log.Error("webhook server stopped", "err", err)
			}
		}()
	}

	// Start optional storage-location API server.
	if cfg.API.Enabled {
		srv := api.New(cfg.API, mgr, log)
		go func() {
			if err := srv.Run(ctx); err != nil {
				log.Error("api server stopped", "err", err)
			}
		}()
	}

	switch cfg.Mount.Type {
	case "fuse":
		if err := os.MkdirAll(cfg.Mount.Path, 0o755); err != nil {
			return fmt.Errorf("creating mount dir %q: %w", cfg.Mount.Path, err)
		}
		log.Info("mounting via FUSE", "path", cfg.Mount.Path)
		return mount.MountFUSE(ctx, mgr, cfg.Mount, log)
	case "nfs":
		log.Info("serving via NFS", "addr", cfg.Mount.Listen)
		return mount.ServeNFS(ctx, mgr, cfg.Mount, log)
	default:
		return fmt.Errorf("unknown mount type %q", cfg.Mount.Type)
	}
}

func buildLogger(cfg config.LogConfig) *slog.Logger {
	level := slog.LevelInfo
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	var out *os.File
	if cfg.File != "" {
		f, err := os.OpenFile(cfg.File, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err == nil {
			out = f
		}
	}
	if out == nil {
		out = os.Stderr
	}
	return slog.New(slog.NewTextHandler(out, &slog.HandlerOptions{Level: level}))
}
