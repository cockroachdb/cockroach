// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// +build short

package storage

import (
	"context"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// RocksDB is a type alias to Pebble. This should not be used at all in short
// builds.
type RocksDB = Pebble

// RocksDBCache is a wrapper around C.DBCache
type RocksDBCache struct{}

// NewRocksDBCache creates a new cache of the specified size. Note that the
// cache is refcounted internally and starts out with a refcount of one (i.e.
// Release() should be called after having used the cache).
func NewRocksDBCache(cacheSize int64) RocksDBCache {
	return RocksDBCache{}
}

// Release releases the cache. Note that the cache will continue to be used
// until all of the RocksDB engines it was attached to have been closed, and
// that RocksDB engines which use it auto-release when they close.
func (c RocksDBCache) Release() {
}

// RocksDBConfig holds all configuration parameters and knobs used in setting
// up a new RocksDB instance.
type RocksDBConfig struct {
	// StorageConfig contains storage configs for all storage engines.
	base.StorageConfig
	// ReadOnly will open the database in read only mode if set to true.
	ReadOnly bool
	// MaxOpenFiles controls the maximum number of file descriptors RocksDB
	// creates. If MaxOpenFiles is zero, this is set to DefaultMaxOpenFiles.
	MaxOpenFiles uint64
	// WarnLargeBatchThreshold controls if a log message is printed when a
	// WriteBatch takes longer than WarnLargeBatchThreshold. If it is set to
	// zero, no log messages are ever printed.
	WarnLargeBatchThreshold time.Duration
	// RocksDBOptions contains RocksDB specific options using a semicolon
	// separated key-value syntax ("key1=value1; key2=value2").
	RocksDBOptions string
}

// NewRocksDB allocates and returns a new RocksDB stub object.
func NewRocksDB(cfg RocksDBConfig, cache RocksDBCache) (*RocksDB, error) {
	panic("rocksdb used in short build")
}

func newRocksDBInMem(attrs roachpb.Attributes, cacheSize int64) *RocksDB {
	panic("rocksdb used in short build")
}

func newMemRocksDB(attrs roachpb.Attributes, cache RocksDBCache, maxSize int64) (*RocksDB, error) {
	panic("rocksdb used in short build")
}

type lockStruct struct{}

// lockFile sets a lock on the specified file using RocksDB's file locking interface.
func lockFile(filename string) (lockStruct, error) {
	return lockStruct{}, nil
}

// unlockFile unlocks the file asscoiated with the specified lock and GCs any allocated memory for the lock.
func unlockFile(lock lockStruct) error {
	return nil
}

// IsValidSplitKey returns whether the key is a valid split key. Passthrough
// to the Go version of isValidSplitKey.
//
// TODO(bilal): Replace the exported version of IsValidSplitKey with this one
// on all builds, short or otherwise.
func IsValidSplitKey(key roachpb.Key) bool {
	return isValidSplitKey(key, keys.NoSplitSpans)
}

// InitRocksDBLogger initializes the logger to use for RocksDB log messages. If
// not called, WARNING, ERROR, and FATAL logs will be output to the normal
// CockroachDB log. The caller is responsible for ensuring the
// Close() method is eventually called on the new logger.
func InitRocksDBLogger(ctx context.Context) *log.SecondaryLogger {
	return nil
}

// SetRocksDBOpenHook sets the DBOpenHook function that will be called during
// RocksDB initialization. It is intended to be called by CCL code.
func SetRocksDBOpenHook(fn unsafe.Pointer) {
	// No-op.
}

// ThreadStacks returns the stacks for all threads. The stacks are raw
// addresses, and do not contain symbols. Use addr2line (or atos on Darwin) to
// symbolize.
func ThreadStacks() string {
	return ""
}

// RunLDB runs RocksDB's ldb command-line tool. The passed
// command-line arguments should not include argv[0].
func RunLDB(args []string) {
	panic("rocksdb used in short build")

}

// RunSSTDump runs RocksDB's sst_dump command-line tool. The passed
// command-line arguments should not include argv[0].
func RunSSTDump(args []string) {
	panic("rocksdb used in short build")
}
