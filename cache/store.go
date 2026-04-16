package cache

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Store manages the on-disk layout of cached file data.
//
// Layout:
//
//	<cacheDir>/data/<sha256-escaped-path>   – cached bytes
//
// We hash the logical path to avoid filesystem path length / special
// character issues.  The DB holds the canonical path→hash mapping.
type Store struct {
	dir string
}

// NewStore creates a Store rooted at dir.
func NewStore(dir string) (*Store, error) {
	dataDir := filepath.Join(dir, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating cache data dir %q: %w", dataDir, err)
	}
	return &Store{dir: dir}, nil
}

// filePath returns the local path for a cached file identified by its logical
// path.  We use a simple hex-escaped version of the path for readability.
func (s *Store) filePath(logicalPath string) string {
	safe := pathEscape(logicalPath)
	return filepath.Join(s.dir, "data", safe)
}

// Has reports whether any cached data exists for path.
func (s *Store) Has(path string) bool {
	_, err := os.Lstat(s.filePath(path))
	return err == nil
}

// Size returns the number of bytes currently on disk for path.
func (s *Store) Size(path string) int64 {
	fi, err := os.Lstat(s.filePath(path))
	if err != nil {
		return 0
	}
	return fi.Size()
}

// OpenRead opens the cached file for reading.
func (s *Store) OpenRead(path string) (*os.File, error) {
	f, err := os.Open(s.filePath(path))
	if err != nil {
		return nil, fmt.Errorf("cache read %q: %w", path, err)
	}
	return f, nil
}

// Write writes all data from r into the cache file for path, starting at
// startOffset.  This is used for both prefix writes (startOffset=0,
// limit=prefixBytes) and full-file writes.
//
// If the file already exists the new data is merged: bytes before
// startOffset are preserved, new bytes are written from startOffset onward.
func (s *Store) Write(path string, r io.Reader, startOffset int64, limit int64) (int64, error) {
	dst := s.filePath(path)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return 0, err
	}

	flags := os.O_CREATE | os.O_WRONLY
	f, err := os.OpenFile(dst, flags, 0o644)
	if err != nil {
		return 0, fmt.Errorf("cache write open %q: %w", path, err)
	}
	defer f.Close()

	if startOffset > 0 {
		if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
			return 0, err
		}
	}

	var written int64
	buf := make([]byte, 256*1024)
	for {
		if limit >= 0 && written >= limit {
			break
		}
		toRead := int64(len(buf))
		if limit >= 0 && written+toRead > limit {
			toRead = limit - written
		}
		n, readErr := r.Read(buf[:toRead])
		if n > 0 {
			if _, err := f.Write(buf[:n]); err != nil {
				return written, err
			}
			written += int64(n)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return written, readErr
		}
	}
	return written, nil
}

// Truncate shrinks the cached file for path to size bytes.
// Used when reverting from StateFull back to StatePrefix.
func (s *Store) Truncate(path string, size int64) error {
	dst := s.filePath(path)
	return os.Truncate(dst, size)
}

// Delete removes all cached data for path.
func (s *Store) Delete(path string) error {
	return os.Remove(s.filePath(path))
}

// pathEscape converts a logical path into a safe filename.
// Replaces '/' with '_SL_' and '%' with '_PC_'.
func pathEscape(p string) string {
	out := make([]byte, 0, len(p))
	for i := 0; i < len(p); i++ {
		switch p[i] {
		case '/':
			out = append(out, '_', 'S', 'L', '_')
		case '%':
			out = append(out, '_', 'P', 'C', '_')
		default:
			out = append(out, p[i])
		}
	}
	return string(out)
}
