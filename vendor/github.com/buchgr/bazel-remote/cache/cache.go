package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"path/filepath"
)

// EntryKind describes the kind of cache entry
type EntryKind int

const (
	// AC stands for Action Cache.
	AC EntryKind = iota

	// CAS stands for Content Addressable Storage.
	CAS

	// RAW cache items are not validated. Not exposed externally, only
	// used for HTTP when running with the --disable_http_ac_validation
	// commandline flag.
	RAW
)

func (e EntryKind) String() string {
	if e == AC {
		return "ac"
	}
	if e == CAS {
		return "cas"
	}
	return "raw"
}

// Logger is designed to be satisfied by log.Logger.
type Logger interface {
	Printf(format string, v ...interface{})
}

// Error is used by Cache implementations to return a structured error.
type Error struct {
	// Corresponds to a http.Status* code
	Code int
	// A human-readable string describing the error
	Text string
}

func (e *Error) Error() string {
	return e.Text
}

// Proxy is the interface that (optional) proxy backends must implement.
// Implementations are expected to be safe for concurrent use.
type Proxy interface {
	// Put should make a reasonable effort to proxy this data to the backend.
	// This is allowed to fail silently (eg when under heavy load).
	Put(kind EntryKind, hash string, size int64, rdr io.ReadCloser)

	// Get should return the cache item identified by `hash`, or an error
	// if something went wrong. If the item was not found, the io.ReadCloser
	// will be nil.
	Get(kind EntryKind, hash string) (io.ReadCloser, int64, error)

	// Contains returns whether or not the cache item exists on the
	// remote end, and the size if it exists (and -1 if the size is
	// unknown).
	Contains(kind EntryKind, hash string) (bool, int64)
}

// TransformActionCacheKey takes an ActionCache key and an instance name
// and returns a new ActionCache key to use instead. If the instance name
// is empty, then the original key is returned unchanged.
func TransformActionCacheKey(key, instance string, logger Logger) string {
	if instance == "" {
		return key
	}

	h := sha256.New()
	h.Write([]byte(key))
	h.Write([]byte(instance))
	b := h.Sum(nil)
	newKey := hex.EncodeToString(b[:])

	logger.Printf("REMAP AC HASH %s : %s => %s", key, instance, newKey)

	return newKey
}

// Key returns the proper cache key for an entry kind and hash.
func Key(kind EntryKind, hash string) string {
	return filepath.Join(kind.String(), hash[:2], hash)
}
