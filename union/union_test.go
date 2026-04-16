package union

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"jellyfin-cache/backend"
	"jellyfin-cache/internal/testutil"
)

func ctx() context.Context { return context.Background() }

// ---- List ------------------------------------------------------------------

func TestUnion_ListMergesBackends(t *testing.T) {
	a := testutil.NewMemBackend("a", 0, false)
	b := testutil.NewMemBackend("b", 1, false)
	a.AddFile("Movies/Dune.mkv", []byte("dune"))
	b.AddFile("Movies/Arrival.mkv", []byte("arrival"))

	u := New([]backend.Backend{a, b})
	infos, err := u.List(ctx(), "Movies")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	names := make(map[string]bool)
	for _, info := range infos {
		names[info.Name] = true
	}
	if !names["Dune.mkv"] {
		t.Error("expected Dune.mkv in merged listing")
	}
	if !names["Arrival.mkv"] {
		t.Error("expected Arrival.mkv in merged listing")
	}
}

func TestUnion_ListDeduplicatesOnPriority(t *testing.T) {
	// Same path on two backends → high-priority (lower number) entry wins.
	primary := testutil.NewMemBackend("primary", 0, false)
	fallback := testutil.NewMemBackend("fallback", 1, false)
	primary.AddFile("Movies/film.mkv", []byte("primary-content"))
	fallback.AddFile("Movies/film.mkv", []byte("fallback-content"))

	u := New([]backend.Backend{primary, fallback})
	infos, err := u.List(ctx(), "Movies")
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	var count int
	for _, info := range infos {
		if info.Name == "film.mkv" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected 1 entry for film.mkv, got %d", count)
	}
}

func TestUnion_ListBackendErrorSkipped(t *testing.T) {
	good := testutil.NewMemBackend("good", 0, false)
	good.AddFile("TV/ep.mkv", []byte("data"))

	// errBackend always errors on List.
	bad := &errBackend{name: "bad", priority: 1}

	u := New([]backend.Backend{good, bad})
	infos, err := u.List(ctx(), "TV")
	if err != nil {
		t.Fatalf("List should not fail when one backend errors: %v", err)
	}
	if len(infos) == 0 {
		t.Error("expected results from the good backend")
	}
}

// ---- Stat ------------------------------------------------------------------

func TestUnion_StatPriorityOrdering(t *testing.T) {
	primary := testutil.NewMemBackend("primary", 0, false)
	fallback := testutil.NewMemBackend("fallback", 1, false)
	primary.AddFile("f.mkv", []byte("primary"))
	fallback.AddFile("f.mkv", []byte("fallback"))

	u := New([]backend.Backend{fallback, primary}) // note: inserted in wrong order
	info, be, err := u.Stat(ctx(), "f.mkv")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if be.Name() != "primary" {
		t.Errorf("expected primary backend, got %q", be.Name())
	}
	if info.Size != int64(len("primary")) {
		t.Errorf("Size: got %d, want %d", info.Size, len("primary"))
	}
}

func TestUnion_StatFallsThrough(t *testing.T) {
	primary := testutil.NewMemBackend("primary", 0, false)
	fallback := testutil.NewMemBackend("fallback", 1, false)
	// Only fallback has the file.
	fallback.AddFile("rare.mkv", []byte("rare"))

	u := New([]backend.Backend{primary, fallback})
	info, be, err := u.Stat(ctx(), "rare.mkv")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if be.Name() != "fallback" {
		t.Errorf("expected fallback backend, got %q", be.Name())
	}
	_ = info
}

func TestUnion_StatNotFound(t *testing.T) {
	a := testutil.NewMemBackend("a", 0, false)
	u := New([]backend.Backend{a})
	_, _, err := u.Stat(ctx(), "nowhere.mkv")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

// ---- Open ------------------------------------------------------------------

func TestUnion_OpenReadsPrimaryContent(t *testing.T) {
	primary := testutil.NewMemBackend("primary", 0, false)
	fallback := testutil.NewMemBackend("fallback", 1, false)
	primary.AddFile("f.mkv", []byte("primary-data"))
	fallback.AddFile("f.mkv", []byte("fallback-data"))

	u := New([]backend.Backend{primary, fallback})
	rc, err := u.Open(ctx(), "f.mkv")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	data, _ := io.ReadAll(rc)
	if string(data) != "primary-data" {
		t.Errorf("got %q, want %q", data, "primary-data")
	}
}

// ---- Put -------------------------------------------------------------------

func TestUnion_PutGoesToFirstWritable(t *testing.T) {
	ro := testutil.NewMemBackend("ro", 0, true) // read-only, higher priority
	rw := testutil.NewMemBackend("rw", 1, false)

	u := New([]backend.Backend{ro, rw})
	content := []byte("uploaded")
	err := u.Put(ctx(), "uploads/new.mkv", bytes.NewReader(content), time.Now(), int64(len(content)))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	data, ok := rw.GetFile("uploads/new.mkv")
	if !ok {
		t.Fatal("file not found in writable backend")
	}
	if !bytes.Equal(data, content) {
		t.Errorf("content mismatch: got %q, want %q", data, content)
	}

	// Must NOT have written to the read-only backend.
	if _, ok := ro.GetFile("uploads/new.mkv"); ok {
		t.Error("file should not appear in read-only backend")
	}
}

func TestUnion_PutAllReadOnlyErrors(t *testing.T) {
	ro := testutil.NewMemBackend("ro", 0, true)
	u := New([]backend.Backend{ro})
	err := u.Put(ctx(), "f.mkv", bytes.NewReader(nil), time.Now(), 0)
	if err == nil {
		t.Error("expected error when all backends are read-only")
	}
}

// ---- Remove ----------------------------------------------------------------

func TestUnion_RemoveDeletesFromAllWritable(t *testing.T) {
	a := testutil.NewMemBackend("a", 0, false)
	b := testutil.NewMemBackend("b", 1, false)
	a.AddFile("del.mkv", []byte("a"))
	b.AddFile("del.mkv", []byte("b"))

	u := New([]backend.Backend{a, b})
	if err := u.Remove(ctx(), "del.mkv"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, ok := a.GetFile("del.mkv"); ok {
		t.Error("file should be deleted from backend a")
	}
	if _, ok := b.GetFile("del.mkv"); ok {
		t.Error("file should be deleted from backend b")
	}
}

// ---- Priority sorting ------------------------------------------------------

func TestUnion_BackendsSortedByPriority(t *testing.T) {
	high := testutil.NewMemBackend("high", 0, false)
	low := testutil.NewMemBackend("low", 2, false)
	mid := testutil.NewMemBackend("mid", 1, false)

	u := New([]backend.Backend{low, high, mid})
	backends := u.Backends()
	if backends[0].Name() != "high" {
		t.Errorf("first backend should be highest priority, got %q", backends[0].Name())
	}
	if backends[1].Name() != "mid" {
		t.Errorf("second backend should be mid priority, got %q", backends[1].Name())
	}
	if backends[2].Name() != "low" {
		t.Errorf("third backend should be lowest priority, got %q", backends[2].Name())
	}
}

// ---- errBackend: always fails List (for error-path testing) ----------------

type errBackend struct {
	name     string
	priority int
}

func (e *errBackend) Name() string     { return e.name }
func (e *errBackend) ReadOnly() bool   { return false }
func (e *errBackend) Priority() int    { return e.priority }
func (e *errBackend) List(_ context.Context, _ string) ([]backend.Info, error) {
	return nil, context.DeadlineExceeded
}
func (e *errBackend) Stat(_ context.Context, _ string) (*backend.Info, error) {
	return nil, context.DeadlineExceeded
}
func (e *errBackend) Open(_ context.Context, _ string) (io.ReadSeekCloser, error) {
	return nil, context.DeadlineExceeded
}
func (e *errBackend) Put(_ context.Context, _ string, _ io.Reader, _ time.Time, _ int64) error {
	return context.DeadlineExceeded
}
func (e *errBackend) Remove(_ context.Context, _ string) error  { return context.DeadlineExceeded }
func (e *errBackend) Mkdir(_ context.Context, _ string) error   { return context.DeadlineExceeded }
