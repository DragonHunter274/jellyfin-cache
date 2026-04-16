# jellyfin-cache

A smart read-ahead cache daemon for [Jellyfin](https://jellyfin.org/) that keeps the beginning of every media file warm on local disk and automatically downloads the full file the moment playback starts.

It merges multiple [rclone](https://rclone.org/) remotes into a single mount point (like mergerfs) and exposes the result via **FUSE** or **NFS** — Jellyfin just points at the mount and never knows the difference.

---

## How it works

Every media file passes through four cache states:

```
UNCACHED ──► PREFIX ──► DOWNLOADING ──► FULL
                 ▲                         │
                 └─────── TTL expires ─────┘
```

| State | What is on disk |
|---|---|
| `UNCACHED` | Nothing yet |
| `PREFIX` | First `prefix_bytes` (default 50 MiB ≈ 3–5 min) |
| `DOWNLOADING` | Full download in progress |
| `FULL` | Entire file |

**On startup** — background workers pre-cache the prefix of every known file so the first few minutes are always available instantly.

**On playback** — once enough bytes have been read and the handle has been open long enough (both thresholds must pass, see [Playback detection](#playback-detection)), the full file is downloaded in the background.

**On expiry** — after `full_ttl` (default 72 h) the tail is truncated and the file reverts to `PREFIX`. The beginning remains hot for the next viewer.

**On upload** — files written *through* the cache stay in `FULL` state for `upload_ttl` (default 2 weeks).

---

## Features

- **Prefix pre-caching** — configurable first-N-bytes always kept warm
- **Automatic full pull on playback** — transparent to Jellyfin
- **TTL-based expiry** — reverts to prefix after configurable time, not full eviction
- **LRU eviction** — prefix caches evicted oldest-first when the disk quota is reached
- **Multi-remote union** — merge any number of rclone remotes with priority ordering (reads from highest priority, writes to first writable)
- **FUSE or NFS mount** — works with local Jellyfin installs and Docker/containers
- **Jellyfin webhook integration** — optional; replaces heuristics with an exact playback event
- **Scan / trickplay protection** — library scans and thumbnail generation never trigger a full download
- **Storage-location HTTP API** — query cache state and storage tier per file; manually cache or evict files; live download progress for progress bars

---

## Requirements

- **Go 1.23+** to build
- **rclone** configured with your remotes (`~/.config/rclone/rclone.conf` or a custom path)
- **FUSE** — `libfuse` / `fuse3` installed; `user_allow_other` in `/etc/fuse.conf` if Jellyfin runs as a different user
- **NFS** — no extra dependencies; useful for Docker/container deployments

---

## Installation

```bash
git clone https://github.com/yourname/jellyfin-cache
cd jellyfin-cache
go build -o jellyfin-cache .
sudo cp jellyfin-cache /usr/local/bin/
```

---

## Quick start

**1. Copy and edit the example config:**

```bash
sudo mkdir -p /etc/jellyfin-cache
sudo cp config.example.yaml /etc/jellyfin-cache/config.yaml
$EDITOR /etc/jellyfin-cache/config.yaml
```

Minimum required fields:

```yaml
remotes:
  - name: my-drive
    rclone_path: "gdrive:Media"   # any rclone remote path
    priority: 0

mount:
  type: fuse
  path: /mnt/jellyfin
```

**2. Mount:**

```bash
jellyfin-cache mount -c /etc/jellyfin-cache/config.yaml
```

**3. Point Jellyfin at the mount point** (`/mnt/jellyfin` or your NFS address) and add it as a library.

---

## Playback detection

Without a webhook, jellyfin-cache uses two heuristics to distinguish real playback from Jellyfin's background activity. **Both** must be satisfied before a full download is queued:

| Guard | Default | Purpose |
|---|---|---|
| `playback_trigger_bytes` | 5 MiB | Filters metadata probes — ffprobe reads a few KB/MB per file |
| `min_play_duration` | 10 s | Filters library scans — ffprobe opens a file for 1–3 s then closes it |

A **seek past the prefix** (e.g. a user jumping to a timestamp) bypasses both guards and triggers immediately — seeks never happen during scans.

### Trickplay suppression

Jellyfin's trickplay generator extracts thumbnail frames by seeking to evenly-spaced positions throughout the file, roughly once per second. Without protection this would trigger full downloads of files nobody is watching.

Detection: if `trickplay_seek_threshold` (default 3) seeks occur within `trickplay_seek_window` (default 10 s), the file handle is permanently classified as trickplay and download is suppressed.

New uploads are unaffected (already `FULL`). Webhook-triggered downloads are also unaffected.

---

## Jellyfin webhook (recommended)

The webhook replaces all heuristics with an exact signal from Jellyfin.

**Setup:**

1. In Jellyfin: **Dashboard → Plugins → Catalog** → search *Webhook* → install.
2. **Plugins → Webhook → Add destination:**
   - URL: `http://localhost:8089/webhook?secret=CHANGE_ME`
   - Notification type: **Playback Start** (optionally also *Playback Unpause*)
   - Template: leave as **Generic**
3. Restart Jellyfin.

**Config:**

```yaml
webhook:
  enabled: true
  listen: "127.0.0.1:8089"
  secret: "CHANGE_ME"
  strip_prefix: "/mnt/nas/"   # strip Jellyfin's absolute path prefix
```

The `strip_prefix` translates Jellyfin's absolute path (e.g. `/mnt/nas/Movies/film.mkv`) into the VFS-relative path (`Movies/film.mkv`) used internally. Set it to whatever prefix Jellyfin sees that isn't part of your rclone remote path.

Authentication accepts a `Bearer` token in the `Authorization` header **or** a `?secret=` query parameter — the latter is easier to paste into Jellyfin's webhook URL field.

---

## Union / multi-remote

Any number of rclone remotes can be listed under `remotes`. The union layer merges them:

- **Reads** — served from the highest-priority backend (lowest `priority` number) that has the file. If it fails, the next backend is tried.
- **Writes** — go to the first non-`read_only` backend.
- **Directory listings** — merged across all backends; duplicates resolved by priority.

```yaml
remotes:
  - name: fast-ssd
    rclone_path: "/mnt/local-ssd/media"
    priority: 0          # checked first for reads

  - name: cloud
    rclone_path: "gdrive:Media"
    priority: 1
    read_only: true      # never written to

  - name: cold-backup
    rclone_path: "s3:archive-bucket/media"
    priority: 2
    read_only: true
```

---

## Storage-location API

An optional HTTP API lets you query which storage tier each file is on and manually control caching.  Useful for building cache-state badges in the Jellyfin interface (browser extension, plugin, etc.).

Enable it in config:

```yaml
api:
  enabled: true
  listen: "127.0.0.1:8090"
  secret: "read-secret"        # guards GET endpoints; empty = open
  admin_secret: "write-secret" # guards POST endpoints; empty = disabled
```

### Read endpoints

Both accept the secret via `Authorization: Bearer <secret>` or `?secret=<secret>`.

---

#### `GET /api/location?path=<vfs-path>`

Returns the storage state for a single file. `path` is relative to the VFS root (no leading slash).

```bash
curl "http://localhost:8090/api/location?path=Movies/Inception.mkv&secret=read-secret"
```

```json
{
  "path":             "Movies/Inception.mkv",
  "cache_state":      "downloading",
  "cached_bytes":     1800000000,
  "size":             4000000000,
  "cache_percent":    45.0,
  "remote_name":      "cold-backup",
  "remote_priority":  2
}
```

| Field | Description |
|---|---|
| `cache_state` | `uncached` / `prefix` / `downloading` / `full` |
| `cached_bytes` | Bytes currently on local disk (live during `downloading`) |
| `cache_percent` | 0–100; real-time during `downloading` — use for a progress bar |
| `remote_name` | Name of the backend this file lives on (from your config) |
| `remote_priority` | `0` = highest priority (fastest); higher = slower / cold storage |

Returns **404** if the file is not yet tracked (hasn't been scanned or accessed).

---

#### `GET /api/files`

Same fields as above, for every tracked file, as a JSON array.

```bash
curl "http://localhost:8090/api/files?secret=read-secret"
```

---

### Admin endpoints

Require `admin_secret`.  Return **403** if `admin_secret` is not configured.

---

#### `POST /api/cache?path=<vfs-path>`

Enqueues a full background download for the file.  Returns `202 Accepted` immediately; poll `GET /api/location` for progress.  No-op if the file is already fully cached.

```bash
curl -X POST "http://localhost:8090/api/cache?path=Movies/Inception.mkv&secret=write-secret"
```

---

#### `POST /api/evict?path=<vfs-path>`

Truncates cached data back to the prefix only (`StatePrefix`).  The first `prefix_bytes` remain on disk so playback can start immediately; the tail is freed.

```bash
curl -X POST "http://localhost:8090/api/evict?path=Movies/Inception.mkv&secret=write-secret"
```

#### `POST /api/evict?path=<vfs-path>&full=true`

Deletes **all** cached data for the file (`StateUncached`).  The next playback will stream entirely from the remote.

```bash
curl -X POST "http://localhost:8090/api/evict?path=Movies/Inception.mkv&full=true&secret=write-secret"
```

---

## NFS mount (Docker / containers)

For Jellyfin running in Docker, NFS is often easier than FUSE:

```yaml
mount:
  type: nfs
  listen: "0.0.0.0:2049"   # or bind to a specific interface
```

Then mount inside the container:

```bash
mount -t nfs host-ip:/ /media
```

Or in `docker-compose.yml`:

```yaml
volumes:
  - type: volume
    driver: local
    driver_opts:
      type: nfs
      o: addr=host-ip,nfsvers=3
      device: "/"
```

---

## Configuration reference

| Key | Default | Description |
|---|---|---|
| `rclone_config` | `""` | Path to rclone.conf; empty = rclone default |
| **cache** | | |
| `cache.dir` | `/var/cache/jellyfin-cache` | Cache data and metadata DB location |
| `cache.max_size` | `200GB` | Maximum total disk usage |
| `cache.prefix_bytes` | `52428800` (50 MiB) | Bytes pre-cached at start of every file |
| `cache.playback_trigger_bytes` | `5242880` (5 MiB) | Min bytes read before full download |
| `cache.min_play_duration` | `10s` | Min handle lifetime before full download |
| `cache.full_ttl` | `72h` | Time before full cache reverts to prefix |
| `cache.upload_ttl` | `336h` (2 weeks) | TTL for files uploaded through the cache |
| `cache.trickplay_seek_threshold` | `3` | Seeks within window → classify as trickplay |
| `cache.trickplay_seek_window` | `10s` | Rolling window for trickplay seek counting |
| `cache.prefetch_workers` | `4` | Background prefix prefetch parallelism |
| `cache.download_workers` | `2` | Background full-download parallelism |
| `cache.evict_interval` | `5m` | How often the eviction loop runs |
| **remotes** | | List of rclone remote sources (see above) |
| **mount** | | |
| `mount.type` | `fuse` | `fuse` or `nfs` |
| `mount.path` | | Local directory for FUSE mount |
| `mount.listen` | `127.0.0.1:2049` | Address for NFS server |
| `mount.allow_other` | `true` | FUSE `allow_other` (needed for multi-user) |
| **webhook** | | |
| `webhook.enabled` | `false` | Enable Jellyfin webhook listener |
| `webhook.listen` | `127.0.0.1:8089` | Webhook HTTP listen address |
| `webhook.http_path` | `/webhook` | URL path |
| `webhook.secret` | `""` | Shared secret (Bearer or `?secret=`) |
| `webhook.path_field` | `Path` | Dot-notation JSON field for the file path |
| `webhook.strip_prefix` | `""` | Filesystem prefix to strip from received paths |
| `webhook.events` | `[PlaybackStart]` | Notification types that trigger a download |
| **api** | | |
| `api.enabled` | `false` | Enable storage-location API server |
| `api.listen` | `127.0.0.1:8090` | API HTTP listen address |
| `api.secret` | `""` | Secret for read endpoints; empty = open |
| `api.admin_secret` | `""` | Secret for mutating endpoints; empty = disabled |
| **log** | | |
| `log.level` | `info` | `debug`, `info`, `warn`, `error` |
| `log.file` | `""` | Log file path; empty = stderr |

---

## Systemd unit

```ini
[Unit]
Description=jellyfin-cache smart media cache
After=network.target

[Service]
ExecStart=/usr/local/bin/jellyfin-cache mount -c /etc/jellyfin-cache/config.yaml
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now jellyfin-cache
```

---

## Project layout

```
jellyfin-cache/
├── backend/        Backend interface + rclone adapter
├── cache/          Cache state machine, BoltDB metadata, disk store, manager, reader
├── cmd/            cobra CLI (mount subcommand)
├── config/         YAML config types and defaults
├── internal/
│   └── testutil/   In-memory backend for tests
├── api/            Storage-location HTTP API (query, cache, evict)
├── mount/          FUSE (hanwen/go-fuse) and NFS (willscott/go-nfs) servers
├── union/          Priority-ordered multi-remote union
├── vfs/            billy.Filesystem adapter (used by NFS)
└── webhook/        Optional Jellyfin playback event HTTP listener
```

---

## Running the tests

```bash
go test ./...
```

The test suite covers the cache state machine, playback/scan/trickplay detection, the union merge layer, and the webhook HTTP handler. All tests are self-contained (in-memory backends, temp directories) and require no network access.

---

## Known limitations / TODO

- Sub-directory FUSE lookup is implemented but the tree is currently flat (one level deep)
- No Prometheus metrics endpoint yet
- No `status` command to inspect cache state from the CLI
