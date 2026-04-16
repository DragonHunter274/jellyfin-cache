// Package cmd defines the CLI surface of jellyfin-cache using cobra.
package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "jellyfin-cache",
	Short: "Smart read-ahead cache for Jellyfin media libraries",
	Long: `jellyfin-cache merges multiple remote filesystems (rclone remotes)
into a single mount point and keeps the first few minutes of every media file
pre-cached on disk.  When playback starts the full file is downloaded in the
background; the cache expires back to prefix-only after a configurable TTL.`,
}

// Execute runs the root command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c",
		"/etc/jellyfin-cache/config.yaml", "path to config file")
}
