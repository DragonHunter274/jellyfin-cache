// Package api provides an HTTP API for querying and managing file cache state.
//
// This is useful for adding cache-state indicators to the Jellyfin interface.
// For example, a browser extension or Jellyfin plugin can call this API to
// show whether a media file is fully cached locally, downloading, prefetched,
// or still on cold remote storage — and to manually warm or evict files.
//
// # Read-only endpoints (guarded by api.secret)
//
//	GET /api/location?path=<vfs-path>
//	    Returns the location info for a single file.
//	    Returns 404 if the path is not tracked.
//
//	GET /api/files
//	    Returns location info for every tracked file.
//
// # Mutating endpoints (guarded by api.admin_secret; disabled if unset)
//
//	POST /api/cache?path=<vfs-path>
//	    Enqueues a full background download for the file.
//	    Idempotent: no-op if the file is already fully cached.
//
//	POST /api/evict?path=<vfs-path>
//	    Truncates the file back to prefix-only (StatePrefix).
//	    The first N bytes remain cached for fast playback starts.
//
//	POST /api/evict?path=<vfs-path>&full=true
//	    Deletes all cached data for the file (StateUncached).
//	    The next playback will stream entirely from the remote.
//
// # Authentication
//
//	Both endpoints accept the secret via:
//	  Authorization: Bearer <secret>
//	  ?secret=<secret>  (query parameter)
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"jellyfin-cache/cache"
	"jellyfin-cache/config"
)

// Querier is the interface the Server needs for read-only operations.
type Querier interface {
	Location(path string) (*cache.LocationInfo, error)
	ListLocations() ([]*cache.LocationInfo, error)
}

// Manager extends Querier with mutating cache operations.
type Manager interface {
	Querier
	CacheFile(path string) error
	EvictToPrefix(path string) error
	EvictCompletely(path string) error
}

// Server is the HTTP API server.
type Server struct {
	cfg    config.APIConfig
	mgr    Manager
	log    *slog.Logger
	server *http.Server
}

// New creates a Server.  Call Run to start it.
func New(cfg config.APIConfig, mgr Manager, log *slog.Logger) *Server {
	s := &Server{cfg: cfg, mgr: mgr, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/location", s.handleLocation)
	mux.HandleFunc("/api/files", s.handleFiles)
	mux.HandleFunc("/api/cache", s.handleCache)
	mux.HandleFunc("/api/evict", s.handleEvict)
	s.server = &http.Server{
		Addr:        cfg.Listen,
		Handler:     mux,
		ReadTimeout: 10 * time.Second,
	}
	return s
}

// Run starts the HTTP server and blocks until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() { errCh <- s.server.ListenAndServe() }()

	adminStatus := "disabled"
	if s.cfg.AdminSecret != "" {
		adminStatus = "enabled"
	}
	s.log.Info("api server listening", "addr", s.cfg.Listen, "admin_endpoints", adminStatus)

	select {
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}
		return fmt.Errorf("api server: %w", err)
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.server.Shutdown(shutCtx)
	}
}

// ---- HTTP handlers ---------------------------------------------------------

// handleLocation handles GET /api/location?path=<vfs-path>
//
// Response (200):
//
//	{
//	  "path":            "Movies/Inception.mkv",
//	  "cache_state":     "downloading",
//	  "cached_bytes":    1800000000,
//	  "size":            4000000000,
//	  "cache_percent":   45.0,
//	  "remote_name":     "gdrive",
//	  "remote_priority": 1
//	}
//
// cache_state values:
//   - "uncached"    – no local data; streams from remote
//   - "prefix"      – first N bytes cached; rest streams from remote
//   - "downloading" – full background download in progress
//   - "full"        – entire file is cached locally
//
// cache_percent is 0–100 and is updated in real time during "downloading".
// remote_priority is 0 for the highest-priority (fastest) backend.
func (s *Server) handleLocation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.authenticateRead(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	path := strings.TrimPrefix(r.URL.Query().Get("path"), "/")
	if path == "" {
		http.Error(w, "missing path query parameter", http.StatusBadRequest)
		return
	}

	loc, err := s.mgr.Location(path)
	if err != nil {
		s.log.Error("api: location query failed", "path", path, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if loc == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	writeJSON(w, loc)
}

// handleFiles handles GET /api/files
//
// Returns a JSON array of LocationInfo for every file tracked in the DB.
func (s *Server) handleFiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.authenticateRead(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	locs, err := s.mgr.ListLocations()
	if err != nil {
		s.log.Error("api: list locations failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if locs == nil {
		locs = []*cache.LocationInfo{} // return [] not null
	}

	writeJSON(w, locs)
}

// handleCache handles POST /api/cache?path=<vfs-path>
//
// Enqueues a full background download for the file.
// Returns 202 Accepted immediately; the download happens asynchronously.
// Poll GET /api/location to track progress via cache_state and cache_percent.
func (s *Server) handleCache(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.authenticateAdmin(w, r) {
		return
	}

	path := strings.TrimPrefix(r.URL.Query().Get("path"), "/")
	if path == "" {
		http.Error(w, "missing path query parameter", http.StatusBadRequest)
		return
	}

	if err := s.mgr.CacheFile(path); err != nil {
		s.log.Error("api: cache file failed", "path", path, "err", err)
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	s.log.Info("api: cache enqueued", "path", path)
	w.WriteHeader(http.StatusAccepted)
}

// handleEvict handles POST /api/evict?path=<vfs-path>[&full=true]
//
// Default (full omitted or false): truncates to prefix only (StatePrefix).
// The first N bytes stay cached so playback can start immediately.
//
// With ?full=true: deletes all cached data (StateUncached).
// The next playback will stream entirely from the remote.
func (s *Server) handleEvict(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.authenticateAdmin(w, r) {
		return
	}

	path := strings.TrimPrefix(r.URL.Query().Get("path"), "/")
	if path == "" {
		http.Error(w, "missing path query parameter", http.StatusBadRequest)
		return
	}

	fullDelete := r.URL.Query().Get("full") == "true"

	var err error
	if fullDelete {
		err = s.mgr.EvictCompletely(path)
	} else {
		err = s.mgr.EvictToPrefix(path)
	}
	if err != nil {
		s.log.Error("api: evict failed", "path", path, "full", fullDelete, "err", err)
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	s.log.Info("api: evicted", "path", path, "full", fullDelete)
	w.WriteHeader(http.StatusNoContent)
}

// ---- helpers ---------------------------------------------------------------

// authenticateRead checks the read-only secret.
// If no secret is configured, all requests are accepted.
func (s *Server) authenticateRead(r *http.Request) bool {
	if s.cfg.Secret == "" {
		return true
	}
	return s.checkSecret(r, s.cfg.Secret)
}

// authenticateAdmin checks the admin secret and writes the appropriate error
// response if auth fails.  Returns true only when the request is authorised.
// If AdminSecret is empty, admin endpoints are disabled and 403 is returned.
func (s *Server) authenticateAdmin(w http.ResponseWriter, r *http.Request) bool {
	if s.cfg.AdminSecret == "" {
		http.Error(w, "admin endpoints are disabled (admin_secret not configured)", http.StatusForbidden)
		return false
	}
	if !s.checkSecret(r, s.cfg.AdminSecret) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

func (s *Server) checkSecret(r *http.Request, secret string) bool {
	if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
		if strings.TrimPrefix(auth, "Bearer ") == secret {
			return true
		}
	}
	if r.URL.Query().Get("secret") == secret {
		return true
	}
	return false
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Headers already sent; nothing more we can do.
		return
	}
}
