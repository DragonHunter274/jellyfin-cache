// Package webhook provides an optional HTTP listener that receives Jellyfin
// playback events and immediately triggers full background downloads for the
// affected file, bypassing the timer/bytes heuristic in cache.CacheReader.
//
// Jellyfin setup
//
//  1. Install the "Webhook" plugin via the Plugin Catalog.
//  2. Add a new webhook destination pointing at this server, e.g.
//       http://localhost:8089/webhook
//  3. Enable at least "Playback Start" notifications.
//  4. The default Generic template already includes a top-level "Path" field;
//     if you use a custom template make sure it emits the field named in
//     webhook.path_field (default: "Path").
package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"jellyfin-cache/config"
)

// Triggerer is the narrow interface the Server needs from cache.Manager.
type Triggerer interface {
	TriggerFullDownload(path string)
}

// Server is an HTTP server that listens for Jellyfin webhook payloads.
type Server struct {
	cfg    config.WebhookConfig
	mgr    Triggerer
	log    *slog.Logger
	server *http.Server
}

// New creates a Server.  Call Run to start it.
func New(cfg config.WebhookConfig, mgr Triggerer, log *slog.Logger) *Server {
	s := &Server{cfg: cfg, mgr: mgr, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc(cfg.HTTPPath, s.handle)
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
	s.log.Info("webhook server listening", "addr", s.cfg.Listen, "path", s.cfg.HTTPPath)

	select {
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}
		return fmt.Errorf("webhook server: %w", err)
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.server.Shutdown(shutCtx)
	}
}

// ---- HTTP handler ----------------------------------------------------------

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !s.authenticate(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1 MiB cap
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(body, &payload); err != nil {
		s.log.Warn("webhook: invalid JSON", "err", err)
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	// Check notification type if the filter list is non-empty.
	if len(s.cfg.Events) > 0 {
		eventType, _ := extractField(payload, "NotificationType")
		if !s.eventAllowed(eventType) {
			s.log.Debug("webhook: ignoring event", "type", eventType)
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}

	// Extract file path.
	rawPath, ok := extractField(payload, s.cfg.PathField)
	if !ok || rawPath == "" {
		s.log.Warn("webhook: path field not found or empty",
			"field", s.cfg.PathField, "payload_keys", payloadKeys(payload))
		http.Error(w, "path field missing", http.StatusBadRequest)
		return
	}

	vfsPath := s.toVFSPath(rawPath)
	s.log.Info("webhook: triggering full download", "raw_path", rawPath, "vfs_path", vfsPath)
	s.mgr.TriggerFullDownload(vfsPath)
	w.WriteHeader(http.StatusNoContent)
}

// ---- helpers ---------------------------------------------------------------

// authenticate validates the request against the configured secret.
// If no secret is configured every request is accepted.
func (s *Server) authenticate(r *http.Request) bool {
	if s.cfg.Secret == "" {
		return true
	}
	// Bearer token in Authorization header.
	if auth := r.Header.Get("Authorization"); auth != "" {
		if strings.HasPrefix(auth, "Bearer ") && strings.TrimPrefix(auth, "Bearer ") == s.cfg.Secret {
			return true
		}
	}
	// ?secret=... query parameter (simpler to configure in Jellyfin URL).
	if r.URL.Query().Get("secret") == s.cfg.Secret {
		return true
	}
	return false
}

// eventAllowed returns true if eventType is in the configured allow-list.
func (s *Server) eventAllowed(eventType string) bool {
	for _, e := range s.cfg.Events {
		if strings.EqualFold(e, eventType) {
			return true
		}
	}
	return false
}

// toVFSPath strips the configured prefix and cleans the path so it matches
// the keys stored in the cache DB (relative, no leading slash).
func (s *Server) toVFSPath(p string) string {
	if s.cfg.StripPrefix != "" {
		p = strings.TrimPrefix(p, s.cfg.StripPrefix)
	}
	p = strings.TrimPrefix(p, "/")
	return p
}

// extractField retrieves a value from a nested JSON map using dot notation.
//
//	"Path"               → payload["Path"]
//	"data.path"          → payload["data"]["path"]
//	"MediaSources.0.Path"→ payload["MediaSources"][0]["Path"]
func extractField(m map[string]interface{}, dotPath string) (string, bool) {
	parts := strings.Split(dotPath, ".")
	var cur interface{} = m
	for _, part := range parts {
		switch v := cur.(type) {
		case map[string]interface{}:
			cur = v[part]
		case []interface{}:
			idx, err := strconv.Atoi(part)
			if err != nil || idx < 0 || idx >= len(v) {
				return "", false
			}
			cur = v[idx]
		default:
			return "", false
		}
		if cur == nil {
			return "", false
		}
	}
	s, ok := cur.(string)
	return s, ok
}

// payloadKeys returns the top-level keys of a map for debug logging.
func payloadKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
