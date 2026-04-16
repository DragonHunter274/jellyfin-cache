// Package config holds all configuration types and loading logic.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the root configuration structure, loaded from a YAML file.
type Config struct {
	Cache   CacheConfig    `yaml:"cache"`
	Remotes []RemoteConfig `yaml:"remotes"`
	Mount   MountConfig    `yaml:"mount"`
	Webhook WebhookConfig  `yaml:"webhook"`
	API     APIConfig      `yaml:"api"`
	Log     LogConfig      `yaml:"log"`

	// Path to the rclone config file used for remote credentials.
	RcloneConfig string `yaml:"rclone_config"`
}

// CacheConfig controls caching behaviour.
type CacheConfig struct {
	// Directory where cached data is stored on disk.
	Dir string `yaml:"dir"`

	// Maximum total size of the cache (e.g. "200GB", "500GiB").
	MaxSize string `yaml:"max_size"`

	// Number of bytes to pre-cache at the start of every media file.
	// Default: 52428800 (50 MiB).  Translates to roughly the first 3–5
	// minutes for typical streaming bitrates.
	PrefixBytes int64 `yaml:"prefix_bytes"`

	// How many bytes read through a single file handle trigger a full
	// background download of that file.  Default: 5242880 (5 MiB) –
	// ignores metadata probes by Jellyfin but catches real playback.
	PlaybackTriggerBytes int64 `yaml:"playback_trigger_bytes"`

	// How long a fully-downloaded file stays in "FULL" state before
	// expiring back to PREFIX-only.  Default: 72h.
	FullTTL Duration `yaml:"full_ttl"`

	// How long a file that was *uploaded through* the cache stays in
	// "FULL" state.  Default: 336h (2 weeks).
	UploadTTL Duration `yaml:"upload_ttl"`

	// Minimum time a file handle must be continuously reading before a full
	// download is triggered.  This prevents Jellyfin library scans (which
	// open every file briefly for ffprobe metadata extraction) from
	// triggering full downloads of your entire library.
	//
	// Both MinPlayDuration AND PlaybackTriggerBytes must be satisfied before
	// a full download starts.
	// Default: 10s.
	MinPlayDuration Duration `yaml:"min_play_duration"`

	// Trickplay detection: Jellyfin's trickplay (preview thumbnail) generator
	// seeks to many positions throughout a file in rapid succession.  If at
	// least TrickplaySeekThreshold seeks occur within TrickplaySeekWindow the
	// handle is classified as trickplay and a full download is NOT triggered.
	//
	// New uploads are unaffected (they are already StateFull).
	// Webhook-triggered downloads are also unaffected.
	// Default: 3 seeks within 10s.
	TrickplaySeekThreshold int      `yaml:"trickplay_seek_threshold"`
	TrickplaySeekWindow    Duration `yaml:"trickplay_seek_window"`

	// Number of parallel background prefetch workers.  Default: 4.
	PrefetchWorkers int `yaml:"prefetch_workers"`

	// Number of parallel full-download workers.  Default: 2.
	DownloadWorkers int `yaml:"download_workers"`

	// Cache eviction check interval.  Default: 5m.
	EvictInterval Duration `yaml:"evict_interval"`
}

// RemoteConfig describes one backend source.
type RemoteConfig struct {
	// Human-readable name used in logs.
	Name string `yaml:"name"`

	// rclone remote path, e.g. "gdrive:Media" or "s3:my-bucket/videos".
	RclonePath string `yaml:"rclone_path"`

	// Priority for union resolution: lower value = higher priority.
	// When the same path exists on multiple remotes the highest-priority
	// one wins for reads; writes always go to the first writable remote
	// with Priority 0 (or the lowest numbered one).
	Priority int `yaml:"priority"`

	// ReadOnly prevents writes to this remote.
	ReadOnly bool `yaml:"read_only"`

	// Passthrough disables caching for this remote entirely.  Files are always
	// read directly from the source; no prefix bytes are pre-fetched and no
	// full downloads are triggered.  Files still appear in directory listings
	// and the location API (with state "uncached").  Useful for fast local or
	// LAN remotes where caching adds no value.
	Passthrough bool `yaml:"passthrough"`
}

// WebhookConfig enables an optional HTTP endpoint that receives Jellyfin
// playback events and immediately triggers full downloads, bypassing the
// timer/bytes heuristic entirely.
//
// Setup in Jellyfin:
//  1. Install the "Webhook" plugin (Catalog → Plugins).
//  2. Add a webhook with your server URL, e.g. http://localhost:8089/webhook
//  3. Tick "Playback" → "Playback Start" (and optionally Resume).
//  4. Under "Template" use the built-in Generic template or paste a custom
//     one that emits a "Path" field (see path_field below).
type WebhookConfig struct {
	// Enabled controls whether the webhook server starts at all.
	// Default: false (uses the timer/bytes heuristic).
	Enabled bool `yaml:"enabled"`

	// Listen is the address:port for the HTTP server.
	// Default: "127.0.0.1:8089".
	Listen string `yaml:"listen"`

	// HTTPPath is the URL path the server listens on.
	// Default: "/webhook".
	HTTPPath string `yaml:"http_path"`

	// Secret is an optional shared secret for request validation.
	// If non-empty the server accepts requests that either carry:
	//   Authorization: Bearer <secret>
	// or a query parameter ?secret=<secret>.
	// Requests with a wrong or missing secret are rejected with 401.
	Secret string `yaml:"secret"`

	// PathField is a dot-notation JSON key path used to extract the media
	// file path from the webhook payload.
	//
	// Examples:
	//   "Path"                      – top-level field (Jellyfin default)
	//   "data.path"                 – nested under "data"
	//   "MediaSources.0.Path"       – first element of a JSON array
	//
	// Default: "Path".
	PathField string `yaml:"path_field"`

	// StripPrefix is removed from the start of the path received in the
	// webhook before looking it up in the VFS.
	//
	// Example: Jellyfin stores files as "/mnt/nas/Movies/Inception.mkv"
	// but the VFS root exposes "Movies/Inception.mkv".
	// Set strip_prefix to "/mnt/nas/" to translate correctly.
	StripPrefix string `yaml:"strip_prefix"`

	// Events is the list of Jellyfin notification types that trigger a
	// download.  Leave empty to react to every incoming POST.
	// Common values: "PlaybackStart", "PlaybackUnpause".
	// Default: ["PlaybackStart"].
	Events []string `yaml:"events"`
}

// APIConfig enables an optional HTTP API for querying file storage locations.
// This is useful for adding cache-state indicators to the Jellyfin interface.
type APIConfig struct {
	// Enabled controls whether the API server starts.  Default: false.
	Enabled bool `yaml:"enabled"`

	// Listen is the address:port for the HTTP server.
	// Default: "127.0.0.1:8090".
	Listen string `yaml:"listen"`

	// Secret guards the read-only query endpoints (GET /api/location,
	// GET /api/files).  If non-empty, requests must carry:
	//   Authorization: Bearer <secret>
	// or the query parameter ?secret=<secret>.
	// Leave empty to allow unauthenticated reads.
	Secret string `yaml:"secret"`

	// AdminSecret guards the mutating endpoints (POST /api/cache,
	// POST /api/evict).  If empty, those endpoints are disabled entirely
	// and return 403.  Must be different from Secret.
	AdminSecret string `yaml:"admin_secret"`
}

// MountConfig controls how the virtual filesystem is exposed.
type MountConfig struct {
	// Type is "fuse" or "nfs".
	Type string `yaml:"type"`

	// Path is the local directory for FUSE mounts.
	Path string `yaml:"path"`

	// Listen is the address:port for NFS mounts, e.g. "0.0.0.0:2049".
	Listen string `yaml:"listen"`

	// AllowOther passes allow_other to FUSE (needed for Jellyfin running
	// as a different user).  Default: true.
	AllowOther bool `yaml:"allow_other"`
}

// LogConfig controls logging.
type LogConfig struct {
	Level string `yaml:"level"` // "debug", "info", "warn", "error"
	File  string `yaml:"file"`  // empty = stderr
}

// Duration is a time.Duration that unmarshals from a human string ("72h").
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	d.Duration = parsed
	return nil
}

func (d Duration) MarshalYAML() (interface{}, error) {
	return d.String(), nil
}

// Defaults fills in zero values with sensible defaults.
func (c *Config) Defaults() {
	cc := &c.Cache
	if cc.Dir == "" {
		cc.Dir = "/var/cache/jellyfin-cache"
	}
	if cc.MaxSize == "" {
		cc.MaxSize = "200GB"
	}
	if cc.PrefixBytes == 0 {
		cc.PrefixBytes = 50 * 1024 * 1024 // 50 MiB
	}
	if cc.PlaybackTriggerBytes == 0 {
		cc.PlaybackTriggerBytes = 5 * 1024 * 1024 // 5 MiB
	}
	if cc.FullTTL.Duration == 0 {
		cc.FullTTL.Duration = 72 * time.Hour
	}
	if cc.UploadTTL.Duration == 0 {
		cc.UploadTTL.Duration = 336 * time.Hour // 2 weeks
	}
	if cc.PrefetchWorkers == 0 {
		cc.PrefetchWorkers = 4
	}
	if cc.DownloadWorkers == 0 {
		cc.DownloadWorkers = 2
	}
	if cc.EvictInterval.Duration == 0 {
		cc.EvictInterval.Duration = 5 * time.Minute
	}
	if cc.MinPlayDuration.Duration == 0 {
		cc.MinPlayDuration.Duration = 10 * time.Second
	}
	if cc.TrickplaySeekThreshold == 0 {
		cc.TrickplaySeekThreshold = 3
	}
	if cc.TrickplaySeekWindow.Duration == 0 {
		cc.TrickplaySeekWindow.Duration = 10 * time.Second
	}

	mc := &c.Mount
	if mc.Type == "" {
		mc.Type = "fuse"
	}
	if mc.Listen == "" {
		mc.Listen = "127.0.0.1:2049"
	}
	if !mc.AllowOther {
		mc.AllowOther = true
	}

	if c.Log.Level == "" {
		c.Log.Level = "info"
	}

	if c.API.Listen == "" {
		c.API.Listen = "127.0.0.1:8090"
	}

	wc := &c.Webhook
	if wc.Listen == "" {
		wc.Listen = "127.0.0.1:8089"
	}
	if wc.HTTPPath == "" {
		wc.HTTPPath = "/webhook"
	}
	if wc.PathField == "" {
		wc.PathField = "Path"
	}
	if len(wc.Events) == 0 {
		wc.Events = []string{"PlaybackStart"}
	}
}

// Validate returns an error if required fields are missing or invalid.
func (c *Config) Validate() error {
	if len(c.Remotes) == 0 {
		return fmt.Errorf("at least one remote must be configured")
	}
	for i, r := range c.Remotes {
		if r.RclonePath == "" {
			return fmt.Errorf("remote[%d] %q: rclone_path is required", i, r.Name)
		}
	}
	switch c.Mount.Type {
	case "fuse", "nfs":
	default:
		return fmt.Errorf("mount.type must be 'fuse' or 'nfs', got %q", c.Mount.Type)
	}
	if c.Mount.Type == "fuse" && c.Mount.Path == "" {
		return fmt.Errorf("mount.path is required for FUSE mounts")
	}
	return nil
}

// Load reads and parses a YAML config file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config %q: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config %q: %w", path, err)
	}
	cfg.Defaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &cfg, nil
}
