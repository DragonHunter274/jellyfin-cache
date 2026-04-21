package cmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"jellyfin-cache/config"
	csisrv "jellyfin-cache/csi"
)

var (
	csiNodeID   string
	csiEndpoint string
	csiLogLevel string
)

var csiCmd = &cobra.Command{
	Use:   "csi",
	Short: "Run the CSI node plugin (9P2000.L)",
	RunE:  runCSI,
}

func init() {
	rootCmd.AddCommand(csiCmd)
	csiCmd.Flags().StringVar(&csiNodeID, "node-id", "", "Kubernetes node name (required; set via spec.nodeName downward API)")
	csiCmd.Flags().StringVar(&csiEndpoint, "endpoint", "unix:///csi/csi.sock", "CSI gRPC endpoint")
	csiCmd.Flags().StringVar(&csiLogLevel, "log-level", "info", "Log level: debug|info|warn|error")
	_ = csiCmd.MarkFlagRequired("node-id")
}

func runCSI(_ *cobra.Command, _ []string) error {
	log := buildLogger(config.LogConfig{Level: csiLogLevel})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Info("CSI node plugin shutting down")
		cancel()
	}()

	srv := csisrv.NewServer(csiNodeID, csiEndpoint, "1.0.0", log)
	return srv.Run(ctx)
}
