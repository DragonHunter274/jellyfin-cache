package cache

import (
	"bytes"
	"testing"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s
}

func TestStore_WriteAndRead(t *testing.T) {
	s := openTestStore(t)

	data := bytes.Repeat([]byte("hello"), 1000) // 5 000 bytes
	written, err := s.Write("Movies/film.mkv", bytes.NewReader(data), 0, -1)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if written != int64(len(data)) {
		t.Fatalf("Write returned %d, want %d", written, len(data))
	}

	f, err := s.OpenRead("Movies/film.mkv")
	if err != nil {
		t.Fatalf("OpenRead: %v", err)
	}
	defer f.Close()

	buf := make([]byte, len(data))
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Error("read data does not match written data")
	}
}

func TestStore_WritePrefix_LimitRespected(t *testing.T) {
	s := openTestStore(t)

	data := bytes.Repeat([]byte("x"), 10_000)
	limit := int64(4_000)

	written, err := s.Write("tv/ep.mkv", bytes.NewReader(data), 0, limit)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if written != limit {
		t.Errorf("written %d bytes, want %d", written, limit)
	}
	if s.Size("tv/ep.mkv") != limit {
		t.Errorf("on-disk size %d, want %d", s.Size("tv/ep.mkv"), limit)
	}
}

func TestStore_WriteAtOffset(t *testing.T) {
	s := openTestStore(t)

	// Write prefix first.
	prefix := bytes.Repeat([]byte("A"), 100)
	if _, err := s.Write("f.mkv", bytes.NewReader(prefix), 0, -1); err != nil {
		t.Fatalf("Write prefix: %v", err)
	}

	// Write continuation starting at offset 100.
	tail := bytes.Repeat([]byte("B"), 100)
	if _, err := s.Write("f.mkv", bytes.NewReader(tail), 100, -1); err != nil {
		t.Fatalf("Write tail: %v", err)
	}

	f, _ := s.OpenRead("f.mkv")
	defer f.Close()

	all := make([]byte, 200)
	f.ReadAt(all, 0)
	for i, b := range all[:100] {
		if b != 'A' {
			t.Errorf("byte[%d] = %q, want 'A'", i, b)
			break
		}
	}
	for i, b := range all[100:] {
		if b != 'B' {
			t.Errorf("byte[%d+100] = %q, want 'B'", i, b)
			break
		}
	}
}

func TestStore_Has(t *testing.T) {
	s := openTestStore(t)

	if s.Has("missing.mkv") {
		t.Error("Has should return false for missing file")
	}

	s.Write("present.mkv", bytes.NewReader([]byte("data")), 0, -1)
	if !s.Has("present.mkv") {
		t.Error("Has should return true after write")
	}
}

func TestStore_Size(t *testing.T) {
	s := openTestStore(t)

	if s.Size("missing.mkv") != 0 {
		t.Error("Size of missing file should be 0")
	}

	data := make([]byte, 42)
	s.Write("f.mkv", bytes.NewReader(data), 0, -1)
	if s.Size("f.mkv") != 42 {
		t.Errorf("Size: got %d, want 42", s.Size("f.mkv"))
	}
}

func TestStore_Truncate(t *testing.T) {
	s := openTestStore(t)

	data := bytes.Repeat([]byte("Z"), 1000)
	s.Write("big.mkv", bytes.NewReader(data), 0, -1)

	if err := s.Truncate("big.mkv", 200); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if s.Size("big.mkv") != 200 {
		t.Errorf("Size after truncate: got %d, want 200", s.Size("big.mkv"))
	}
}

func TestStore_Delete(t *testing.T) {
	s := openTestStore(t)

	s.Write("del.mkv", bytes.NewReader([]byte("data")), 0, -1)
	if !s.Has("del.mkv") {
		t.Fatal("file should exist before delete")
	}

	if err := s.Delete("del.mkv"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if s.Has("del.mkv") {
		t.Error("file should not exist after delete")
	}
}

func TestStore_OpenRead_Missing(t *testing.T) {
	s := openTestStore(t)
	_, err := s.OpenRead("ghost.mkv")
	if err == nil {
		t.Error("OpenRead of missing file should return an error")
	}
}

func TestStore_PathEscape(t *testing.T) {
	// Paths with slashes and special chars must not collide or cause errors.
	s := openTestStore(t)

	paths := []string{
		"Movies/2001: A Space Odyssey.mkv",
		"TV/Show (2024)/S01E01.mkv",
		"Music/50% Off.mp3",
	}
	for _, p := range paths {
		data := []byte(p) // use path as content for easy verification
		if _, err := s.Write(p, bytes.NewReader(data), 0, -1); err != nil {
			t.Errorf("Write %q: %v", p, err)
		}
	}

	// Each path must be independently readable.
	for _, p := range paths {
		f, err := s.OpenRead(p)
		if err != nil {
			t.Errorf("OpenRead %q: %v", p, err)
			continue
		}
		buf := make([]byte, len(p))
		f.ReadAt(buf, 0)
		f.Close()
		if string(buf) != p {
			t.Errorf("path %q: read back %q", p, buf)
		}
	}
}
