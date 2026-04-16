package webhook

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"jellyfin-cache/config"
)

// mockTriggerer counts TriggerFullDownload calls and records the paths.
type mockTriggerer struct {
	count atomic.Int32
	paths chan string
}

func newMockTriggerer() *mockTriggerer {
	return &mockTriggerer{paths: make(chan string, 16)}
}

func (m *mockTriggerer) TriggerFullDownload(path string) {
	m.count.Add(1)
	m.paths <- path
}

func (m *mockTriggerer) lastPath(t *testing.T, timeout time.Duration) string {
	t.Helper()
	select {
	case p := <-m.paths:
		return p
	case <-time.After(timeout):
		t.Helper()
		return ""
	}
}

// buildServer creates a Server with the given config overrides.
func buildServer(t *testing.T, overrides func(*config.WebhookConfig)) (*Server, *mockTriggerer) {
	t.Helper()
	cfg := config.WebhookConfig{
		Listen:    "127.0.0.1:0",
		HTTPPath:  "/webhook",
		PathField: "Path",
		Events:    []string{"PlaybackStart"},
	}
	if overrides != nil {
		overrides(&cfg)
	}
	mgr := newMockTriggerer()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(cfg, mgr, log), mgr
}

// post sends a POST request to the server's handler and returns the response.
func post(t *testing.T, srv *Server, url string, body interface{}) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, url, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(w, req)
	return w.Result()
}

// ---- extractField unit tests -----------------------------------------------

func TestExtractField_TopLevel(t *testing.T) {
	m := map[string]interface{}{"Path": "/media/film.mkv"}
	v, ok := extractField(m, "Path")
	if !ok || v != "/media/film.mkv" {
		t.Errorf("got (%q, %v), want (%q, true)", v, ok, "/media/film.mkv")
	}
}

func TestExtractField_Nested(t *testing.T) {
	m := map[string]interface{}{
		"data": map[string]interface{}{"path": "/nested/file.mkv"},
	}
	v, ok := extractField(m, "data.path")
	if !ok || v != "/nested/file.mkv" {
		t.Errorf("got (%q, %v)", v, ok)
	}
}

func TestExtractField_ArrayIndex(t *testing.T) {
	m := map[string]interface{}{
		"MediaSources": []interface{}{
			map[string]interface{}{"Path": "/source/0.mkv"},
			map[string]interface{}{"Path": "/source/1.mkv"},
		},
	}
	v, ok := extractField(m, "MediaSources.0.Path")
	if !ok || v != "/source/0.mkv" {
		t.Errorf("got (%q, %v)", v, ok)
	}
}

func TestExtractField_MissingKey(t *testing.T) {
	m := map[string]interface{}{"Other": "value"}
	_, ok := extractField(m, "Path")
	if ok {
		t.Error("expected ok=false for missing key")
	}
}

func TestExtractField_OutOfBoundsIndex(t *testing.T) {
	m := map[string]interface{}{
		"Items": []interface{}{"only one"},
	}
	_, ok := extractField(m, "Items.5")
	if ok {
		t.Error("expected ok=false for out-of-bounds index")
	}
}

func TestExtractField_NonStringLeaf(t *testing.T) {
	m := map[string]interface{}{"Count": float64(42)}
	_, ok := extractField(m, "Count")
	if ok {
		t.Error("expected ok=false for non-string leaf")
	}
}

// ---- Handler: valid playback start triggers download -----------------------

func TestWebhook_PlaybackStartTriggers(t *testing.T) {
	srv, mgr := buildServer(t, nil)

	resp := post(t, srv, "/webhook", map[string]interface{}{
		"NotificationType": "PlaybackStart",
		"Path":             "Movies/Inception.mkv",
	})
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("status: got %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	path := mgr.lastPath(t, 100*time.Millisecond)
	if path == "" {
		t.Fatal("TriggerFullDownload was not called")
	}
	if path != "Movies/Inception.mkv" {
		t.Errorf("wrong path: got %q, want %q", path, "Movies/Inception.mkv")
	}
}

// ---- Handler: strip prefix applied before lookup --------------------------

func TestWebhook_StripPrefix(t *testing.T) {
	srv, mgr := buildServer(t, func(cfg *config.WebhookConfig) {
		cfg.StripPrefix = "/mnt/nas/"
	})

	post(t, srv, "/webhook", map[string]interface{}{
		"NotificationType": "PlaybackStart",
		"Path":             "/mnt/nas/Movies/Dune.mkv",
	})

	path := mgr.lastPath(t, 100*time.Millisecond)
	if path != "Movies/Dune.mkv" {
		t.Errorf("path after strip: got %q, want %q", path, "Movies/Dune.mkv")
	}
}

// ---- Handler: event filter -------------------------------------------------

func TestWebhook_IgnoredEventType(t *testing.T) {
	srv, mgr := buildServer(t, func(cfg *config.WebhookConfig) {
		cfg.Events = []string{"PlaybackStart"} // only this event
	})

	resp := post(t, srv, "/webhook", map[string]interface{}{
		"NotificationType": "PlaybackStop", // different event
		"Path":             "f.mkv",
	})
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("status: got %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
	if mgr.count.Load() != 0 {
		t.Error("TriggerFullDownload should not be called for filtered events")
	}
}

func TestWebhook_EmptyEventListAcceptsAll(t *testing.T) {
	srv, mgr := buildServer(t, func(cfg *config.WebhookConfig) {
		cfg.Events = nil // accept everything
	})

	post(t, srv, "/webhook", map[string]interface{}{
		"NotificationType": "AnyEventAtAll",
		"Path":             "f.mkv",
	})

	if mgr.lastPath(t, 100*time.Millisecond) == "" {
		t.Error("empty events list should accept all notification types")
	}
}

// ---- Handler: authentication -----------------------------------------------

func TestWebhook_Auth_BearerToken(t *testing.T) {
	srv, mgr := buildServer(t, func(cfg *config.WebhookConfig) {
		cfg.Secret = "secret123"
	})

	// Wrong token → rejected.
	body := map[string]interface{}{"NotificationType": "PlaybackStart", "Path": "f.mkv"}
	data, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer wrong")
	w := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("wrong token: got %d, want 401", w.Code)
	}
	if mgr.count.Load() != 0 {
		t.Error("should not trigger with wrong token")
	}

	// Correct Bearer token → accepted.
	data, _ = json.Marshal(body)
	req = httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer secret123")
	w = httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Errorf("correct token: got %d, want 204", w.Code)
	}
}

func TestWebhook_Auth_QueryParam(t *testing.T) {
	srv, mgr := buildServer(t, func(cfg *config.WebhookConfig) {
		cfg.Secret = "mytoken"
	})

	body := map[string]interface{}{"NotificationType": "PlaybackStart", "Path": "f.mkv"}
	resp := post(t, srv, "/webhook?secret=mytoken", body)
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("query param auth: got %d, want 204", resp.StatusCode)
	}
	if mgr.lastPath(t, 100*time.Millisecond) == "" {
		t.Error("trigger not called with correct query param secret")
	}
}

func TestWebhook_Auth_NoSecretConfigured(t *testing.T) {
	srv, mgr := buildServer(t, func(cfg *config.WebhookConfig) {
		cfg.Secret = "" // no auth required
	})

	post(t, srv, "/webhook", map[string]interface{}{
		"NotificationType": "PlaybackStart",
		"Path":             "f.mkv",
	})
	if mgr.lastPath(t, 100*time.Millisecond) == "" {
		t.Error("trigger not called when no secret configured")
	}
}

// ---- Handler: bad requests -------------------------------------------------

func TestWebhook_MissingPathField(t *testing.T) {
	srv, _ := buildServer(t, nil)

	resp := post(t, srv, "/webhook", map[string]interface{}{
		"NotificationType": "PlaybackStart",
		// "Path" deliberately omitted
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("missing path: got %d, want 400", resp.StatusCode)
	}
}

func TestWebhook_InvalidJSON(t *testing.T) {
	srv, _ := buildServer(t, nil)
	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader([]byte("not json{")))
	w := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("invalid JSON: got %d, want 400", w.Code)
	}
}

func TestWebhook_MethodNotAllowed(t *testing.T) {
	srv, _ := buildServer(t, nil)
	req := httptest.NewRequest(http.MethodGet, "/webhook", nil)
	w := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET: got %d, want 405", w.Code)
	}
}

// ---- Custom path_field -----------------------------------------------------

func TestWebhook_CustomPathField(t *testing.T) {
	srv, mgr := buildServer(t, func(cfg *config.WebhookConfig) {
		cfg.PathField = "MediaSources.0.Path"
	})

	post(t, srv, "/webhook", map[string]interface{}{
		"NotificationType": "PlaybackStart",
		"MediaSources": []interface{}{
			map[string]interface{}{"Path": "TV/Show/S01E01.mkv"},
		},
	})

	path := mgr.lastPath(t, 100*time.Millisecond)
	if path != "TV/Show/S01E01.mkv" {
		t.Errorf("custom path field: got %q, want %q", path, "TV/Show/S01E01.mkv")
	}
}
