// Package cache implements the smart caching layer for jellyfin-cache.
package cache

import "fmt"

// State represents how much of a remote file is currently cached locally.
type State uint8

const (
	// StateUncached: no local data.
	StateUncached State = iota
	// StatePrefix: only the first N bytes are cached.
	StatePrefix
	// StateDownloading: a full background download is in progress.
	StateDownloading
	// StateFull: the entire file is cached locally.
	StateFull
)

func (s State) String() string {
	switch s {
	case StateUncached:
		return "uncached"
	case StatePrefix:
		return "prefix"
	case StateDownloading:
		return "downloading"
	case StateFull:
		return "full"
	default:
		return fmt.Sprintf("state(%d)", s)
	}
}

// UploadKind differentiates regular cached content from user-uploaded files,
// which receive an extended TTL.
type UploadKind uint8

const (
	KindRemote UploadKind = iota // originally from a remote
	KindUpload                    // uploaded through the cache
)
