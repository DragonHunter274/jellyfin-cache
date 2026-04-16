package cache

import (
	"path/filepath"
	"testing"
	"time"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := OpenDB(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestDB_GetMissing(t *testing.T) {
	db := openTestDB(t)
	rec, err := db.Get("does/not/exist.mkv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec != nil {
		t.Fatalf("expected nil for missing key, got %+v", rec)
	}
}

func TestDB_PutAndGet(t *testing.T) {
	db := openTestDB(t)
	now := time.Now().Truncate(time.Millisecond)

	want := &FileRecord{
		Path:        "Movies/Inception.mkv",
		Size:        1_000_000,
		ModTime:     now,
		State:       StatePrefix,
		Kind:        KindRemote,
		CachedBytes: 50_000,
		LastAccess:  now,
	}
	if err := db.Put(want); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := db.Get(want.Path)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("expected record, got nil")
	}
	if got.Path != want.Path {
		t.Errorf("Path: got %q, want %q", got.Path, want.Path)
	}
	if got.Size != want.Size {
		t.Errorf("Size: got %d, want %d", got.Size, want.Size)
	}
	if got.State != StatePrefix {
		t.Errorf("State: got %v, want %v", got.State, StatePrefix)
	}
	if got.CachedBytes != want.CachedBytes {
		t.Errorf("CachedBytes: got %d, want %d", got.CachedBytes, want.CachedBytes)
	}
}

func TestDB_Delete(t *testing.T) {
	db := openTestDB(t)

	rec := &FileRecord{Path: "TV/show.mkv", State: StateUncached}
	if err := db.Put(rec); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db.Delete(rec.Path); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, err := db.Get(rec.Path)
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil after delete, got %+v", got)
	}
}

func TestDB_DeleteMissing(t *testing.T) {
	db := openTestDB(t)
	// Deleting a non-existent key must not error.
	if err := db.Delete("ghost.mkv"); err != nil {
		t.Fatalf("Delete of missing key should not error: %v", err)
	}
}

func TestDB_UpdateState(t *testing.T) {
	db := openTestDB(t)

	rec := &FileRecord{
		Path:        "Movies/Dune.mkv",
		Size:        2_000_000,
		State:       StatePrefix,
		CachedBytes: 50_000,
	}
	if err := db.Put(rec); err != nil {
		t.Fatalf("Put: %v", err)
	}

	expiry := time.Now().Add(72 * time.Hour).Truncate(time.Second)
	if err := db.UpdateState(rec.Path, StateFull, 2_000_000, expiry); err != nil {
		t.Fatalf("UpdateState: %v", err)
	}

	got, _ := db.Get(rec.Path)
	if got.State != StateFull {
		t.Errorf("State: got %v, want %v", got.State, StateFull)
	}
	if got.CachedBytes != 2_000_000 {
		t.Errorf("CachedBytes: got %d, want 2000000", got.CachedBytes)
	}
	if got.ExpiresAt.IsZero() {
		t.Error("ExpiresAt should be set")
	}
}

func TestDB_UpdateState_MissingRecord(t *testing.T) {
	db := openTestDB(t)
	err := db.UpdateState("ghost.mkv", StateFull, 0, time.Time{})
	if err == nil {
		t.Error("expected error updating missing record")
	}
}

func TestDB_TouchAccess(t *testing.T) {
	db := openTestDB(t)
	before := time.Now().Add(-time.Minute)

	rec := &FileRecord{
		Path:       "TV/ep1.mkv",
		State:      StatePrefix,
		LastAccess: before,
	}
	if err := db.Put(rec); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := db.TouchAccess(rec.Path); err != nil {
		t.Fatalf("TouchAccess: %v", err)
	}

	got, _ := db.Get(rec.Path)
	if !got.LastAccess.After(before) {
		t.Errorf("LastAccess was not updated: got %v, want after %v", got.LastAccess, before)
	}
}

func TestDB_TouchAccess_Missing(t *testing.T) {
	db := openTestDB(t)
	// TouchAccess on a missing record must silently succeed.
	if err := db.TouchAccess("nowhere.mkv"); err != nil {
		t.Fatalf("TouchAccess on missing: %v", err)
	}
}

func TestDB_TotalCachedBytes(t *testing.T) {
	db := openTestDB(t)

	records := []*FileRecord{
		{Path: "a.mkv", State: StatePrefix, CachedBytes: 100},
		{Path: "b.mkv", State: StateFull, CachedBytes: 200},
		{Path: "c.mkv", State: StateUncached, CachedBytes: 0},
	}
	for _, r := range records {
		if err := db.Put(r); err != nil {
			t.Fatalf("Put %q: %v", r.Path, err)
		}
	}

	total, err := db.TotalCachedBytes()
	if err != nil {
		t.Fatalf("TotalCachedBytes: %v", err)
	}
	if total != 300 {
		t.Errorf("TotalCachedBytes: got %d, want 300", total)
	}
}

func TestDB_AllRecords(t *testing.T) {
	db := openTestDB(t)

	paths := []string{"a.mkv", "b.mkv", "c.mkv"}
	for _, p := range paths {
		if err := db.Put(&FileRecord{Path: p, State: StateUncached}); err != nil {
			t.Fatalf("Put %q: %v", p, err)
		}
	}

	recs, err := db.AllRecords()
	if err != nil {
		t.Fatalf("AllRecords: %v", err)
	}
	if len(recs) != len(paths) {
		t.Errorf("AllRecords: got %d, want %d", len(recs), len(paths))
	}
}

func TestDB_All_FilterByState(t *testing.T) {
	db := openTestDB(t)

	records := []*FileRecord{
		{Path: "a.mkv", State: StatePrefix},
		{Path: "b.mkv", State: StateFull},
		{Path: "c.mkv", State: StatePrefix},
		{Path: "d.mkv", State: StateUncached},
	}
	for _, r := range records {
		if err := db.Put(r); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	prefix, err := db.All(StatePrefix)
	if err != nil {
		t.Fatalf("All(StatePrefix): %v", err)
	}
	if len(prefix) != 2 {
		t.Errorf("All(StatePrefix): got %d records, want 2", len(prefix))
	}

	full, _ := db.All(StateFull)
	if len(full) != 1 {
		t.Errorf("All(StateFull): got %d records, want 1", len(full))
	}
}
