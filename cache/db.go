package cache

import (
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	bucketFiles = []byte("files")
)

// FileRecord is the persistent metadata stored in BoltDB for every tracked
// file.  Fields are exported so they encode cleanly with encoding/json.
type FileRecord struct {
	Path         string     `json:"path"`
	Size         int64      `json:"size"`
	ModTime      time.Time  `json:"mod_time"`
	State        State      `json:"state"`
	Kind         UploadKind `json:"kind"`
	CachedBytes  int64      `json:"cached_bytes"`   // how many bytes are on disk
	LastAccess   time.Time  `json:"last_access"`
	FullAt       time.Time  `json:"full_at"`        // when the file became StateFull
	ExpiresAt    time.Time  `json:"expires_at"`     // when to revert from Full → Prefix
	PrefetchDone bool       `json:"prefetch_done"`  // prefix prefetch finished
	RemoteName   string     `json:"remote_name"`    // name of the backend this file lives on
	RemotePriority int      `json:"remote_priority"` // priority of that backend (lower = higher priority)
}

// DB wraps a BoltDB instance with typed helpers for file records.
type DB struct {
	db *bolt.DB
}

// OpenDB opens (or creates) the metadata database at path.
func OpenDB(path string) (*DB, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("opening metadata db %q: %w", path, err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketFiles)
		return err
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialising metadata db: %w", err)
	}
	return &DB{db: db}, nil
}

// Close closes the underlying database.
func (d *DB) Close() error { return d.db.Close() }

// Get returns the FileRecord for path, or (nil, nil) if not found.
func (d *DB) Get(path string) (*FileRecord, error) {
	var rec *FileRecord
	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFiles)
		v := b.Get([]byte(path))
		if v == nil {
			return nil
		}
		rec = new(FileRecord)
		return json.Unmarshal(v, rec)
	})
	return rec, err
}

// Put creates or replaces the FileRecord for path.
func (d *DB) Put(rec *FileRecord) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFiles)
		data, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		return b.Put([]byte(rec.Path), data)
	})
}

// Delete removes the record for path (if present).
func (d *DB) Delete(path string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFiles).Delete([]byte(path))
	})
}

// All returns all FileRecords, optionally filtered by state.
// Pass -1 as state to return all.
func (d *DB) All(state State) ([]*FileRecord, error) {
	var out []*FileRecord
	err := d.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFiles).ForEach(func(_, v []byte) error {
			var rec FileRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return err
			}
			if state == 255 || rec.State == state {
				out = append(out, &rec)
			}
			return nil
		})
	})
	return out, err
}

// AllRecords returns every FileRecord in the database.
func (d *DB) AllRecords() ([]*FileRecord, error) {
	return d.All(255)
}

// UpdateState atomically changes the state and optionally sets timestamps.
func (d *DB) UpdateState(path string, state State, cachedBytes int64, expiresAt time.Time) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFiles)
		v := b.Get([]byte(path))
		if v == nil {
			return fmt.Errorf("record not found: %q", path)
		}
		var rec FileRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			return err
		}
		rec.State = state
		if cachedBytes >= 0 {
			rec.CachedBytes = cachedBytes
		}
		if !expiresAt.IsZero() {
			rec.ExpiresAt = expiresAt
			rec.FullAt = time.Now()
		}
		data, err := json.Marshal(&rec)
		if err != nil {
			return err
		}
		return b.Put([]byte(path), data)
	})
}

// TouchAccess updates LastAccess to now.
func (d *DB) TouchAccess(path string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFiles)
		v := b.Get([]byte(path))
		if v == nil {
			return nil // not tracked yet, fine
		}
		var rec FileRecord
		if err := json.Unmarshal(v, &rec); err != nil {
			return err
		}
		rec.LastAccess = time.Now()
		data, err := json.Marshal(&rec)
		if err != nil {
			return err
		}
		return b.Put([]byte(path), data)
	})
}

// TotalCachedBytes sums CachedBytes across all records.
func (d *DB) TotalCachedBytes() (int64, error) {
	var total int64
	err := d.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketFiles).ForEach(func(_, v []byte) error {
			var rec FileRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return err
			}
			total += rec.CachedBytes
			return nil
		})
	})
	return total, err
}
