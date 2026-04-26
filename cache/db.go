package cache

import (
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"jellyfin-cache/backend"
)

var (
	bucketFiles      = []byte("files")
	bucketNFSHandles = []byte("nfs_handles")
	bucketDirCache   = []byte("dircache")
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
		for _, name := range [][]byte{bucketFiles, bucketNFSHandles, bucketDirCache} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		return nil
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
// Uses db.Batch so that concurrent calls from many NFS open operations are
// coalesced into a single transaction and fsync, avoiding per-open disk stalls
// on network-backed PVCs.
func (d *DB) TouchAccess(path string) error {
	return d.db.Batch(func(tx *bolt.Tx) error {
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

// ---- NFS handle persistence ------------------------------------------------
//
// NFS file handles are opaque 16-byte IDs that the kernel NFS client caches
// and reuses across mounts.  If the server restarts with a fresh in-memory
// map (as the go-nfs CachingHandler uses) every handle the client holds
// becomes NFS3ERR_STALE, requiring a restart of every pod that has the volume
// mounted.  Storing the handle→path mapping here means the same IDs are served
// after a daemon restart, so kernel clients reconnect seamlessly.

// LoadNFSHandles returns all persisted handle→path mappings.
func (d *DB) LoadNFSHandles() ([][16]byte, [][]string, error) {
	var ids [][16]byte
	var paths [][]string
	err := d.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketNFSHandles).ForEach(func(k, v []byte) error {
			if len(k) != 16 {
				return nil // skip any corrupt entry
			}
			var path []string
			if err := json.Unmarshal(v, &path); err != nil {
				return nil
			}
			var id [16]byte
			copy(id[:], k)
			ids = append(ids, id)
			paths = append(paths, path)
			return nil
		})
	})
	return ids, paths, err
}

// SaveNFSHandle persists a handle→path mapping.
func (d *DB) SaveNFSHandle(id [16]byte, path []string) error {
	v, err := json.Marshal(path)
	if err != nil {
		return err
	}
	return d.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketNFSHandles).Put(id[:], v)
	})
}

// DeleteNFSHandle removes a persisted handle (called when a file is deleted).
func (d *DB) DeleteNFSHandle(id [16]byte) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketNFSHandles).Delete(id[:])
	})
}

// BatchUpsertFiles inserts or updates a set of file records in a single
// transaction, minimising fsync overhead when pre-populating from a directory
// listing.  For each record:
//   - Not in DB → inserted as-is; its path is appended to created.
//   - In DB with empty RemoteName → RemoteName/RemotePriority updated only.
//   - In DB with RemoteName already set → skipped (no change).
func (d *DB) BatchUpsertFiles(recs []*FileRecord) (created []string, err error) {
	err = d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketFiles)
		for _, rec := range recs {
			existing := b.Get([]byte(rec.Path))
			if existing == nil {
				data, e := json.Marshal(rec)
				if e != nil {
					continue
				}
				if e := b.Put([]byte(rec.Path), data); e != nil {
					return e
				}
				created = append(created, rec.Path)
			} else {
				var cur FileRecord
				if e := json.Unmarshal(existing, &cur); e != nil {
					continue
				}
				if cur.RemoteName != "" {
					continue // already populated; leave it alone
				}
				cur.RemoteName = rec.RemoteName
				cur.RemotePriority = rec.RemotePriority
				data, e := json.Marshal(&cur)
				if e != nil {
					continue
				}
				if e := b.Put([]byte(cur.Path), data); e != nil {
					return e
				}
			}
		}
		return nil
	})
	return created, err
}

// ---- Directory listing cache persistence ------------------------------------
//
// Persisting the in-memory dir cache to BoltDB lets the daemon pre-warm the
// cache on restart.  Without this, every directory listing is a synchronous
// remote call on the first Jellyfin scan after each pod restart, which
// serialises through the scanSem and can take 10+ minutes for large libraries.
//
// Entries are stored with their original expires timestamp.  Loaded entries
// that are already expired are served via the stale-while-revalidate path
// (instant return + background refresh) so the startup behaviour is correct.

type dirCachePersistedEntry struct {
	Infos   []backend.Info `json:"infos"`
	Expires time.Time      `json:"expires"`
}

// SaveDirCacheEntry persists one directory listing to the dircache bucket.
// It uses db.Batch so concurrent calls from many refreshDirCache goroutines
// are coalesced into a small number of BoltDB transactions.
func (d *DB) SaveDirCacheEntry(dir string, infos []backend.Info, expires time.Time) {
	data, err := json.Marshal(dirCachePersistedEntry{Infos: infos, Expires: expires})
	if err != nil {
		return
	}
	_ = d.db.Batch(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketDirCache).Put([]byte(dir), data)
	})
}

// LoadDirCacheEntries returns every persisted dir cache entry.
func (d *DB) LoadDirCacheEntries() map[string]dirCachePersistedEntry {
	out := make(map[string]dirCachePersistedEntry)
	_ = d.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketDirCache).ForEach(func(k, v []byte) error {
			var e dirCachePersistedEntry
			if err := json.Unmarshal(v, &e); err != nil {
				return nil // skip corrupt entries
			}
			out[string(k)] = e
			return nil
		})
	})
	return out
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
