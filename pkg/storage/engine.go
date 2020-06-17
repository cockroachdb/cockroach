// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// DefaultStorageEngine represents the default storage engine to use.
var DefaultStorageEngine enginepb.EngineType

func init() {
	_ = DefaultStorageEngine.Set(envutil.EnvOrDefaultString("COCKROACH_STORAGE_ENGINE", "default"))
}

// SimpleIterator is an interface for iterating over key/value pairs in an
// engine. SimpleIterator implementations are thread safe unless otherwise
// noted. SimpleIterator is a subset of the functionality offered by Iterator.
type SimpleIterator interface {
	// Close frees up resources held by the iterator.
	Close()
	// SeekGE advances the iterator to the first key in the engine which
	// is >= the provided key.
	SeekGE(key MVCCKey)
	// Valid must be called after any call to Seek(), Next(), Prev(), or
	// similar methods. It returns (true, nil) if the iterator points to
	// a valid key (it is undefined to call Key(), Value(), or similar
	// methods unless Valid() has returned (true, nil)). It returns
	// (false, nil) if the iterator has moved past the end of the valid
	// range, or (false, err) if an error has occurred. Valid() will
	// never return true with a non-nil error.
	Valid() (bool, error)
	// Next advances the iterator to the next key/value in the
	// iteration. After this call, Valid() will be true if the
	// iterator was not positioned at the last key.
	Next()
	// NextKey advances the iterator to the next MVCC key. This operation is
	// distinct from Next which advances to the next version of the current key
	// or the next key if the iterator is currently located at the last version
	// for a key.
	NextKey()
	// UnsafeKey returns the same value as Key, but the memory is invalidated on
	// the next call to {Next,Prev,Seek,SeekReverse,Close}.
	UnsafeKey() MVCCKey
	// UnsafeValue returns the same value as Value, but the memory is
	// invalidated on the next call to {Next,Prev,Seek,SeekReverse,Close}.
	UnsafeValue() []byte
}

// IteratorStats is returned from (Iterator).Stats.
type IteratorStats struct {
	InternalDeleteSkippedCount int
	TimeBoundNumSSTs           int
}

// Iterator is an interface for iterating over key/value pairs in an
// engine. Iterator implementations are thread safe unless otherwise
// noted.
type Iterator interface {
	SimpleIterator

	// SeekLT advances the iterator to the first key in the engine which
	// is < the provided key.
	SeekLT(key MVCCKey)
	// Prev moves the iterator backward to the previous key/value
	// in the iteration. After this call, Valid() will be true if the
	// iterator was not positioned at the first key.
	Prev()
	// Key returns the current key.
	Key() MVCCKey
	// Value returns the current value as a byte slice.
	Value() []byte
	// ValueProto unmarshals the value the iterator is currently
	// pointing to using a protobuf decoder.
	ValueProto(msg protoutil.Message) error
	// ComputeStats scans the underlying engine from start to end keys and
	// computes stats counters based on the values. This method is used after a
	// range is split to recompute stats for each subrange. The start key is
	// always adjusted to avoid counting local keys in the event stats are being
	// recomputed for the first range (i.e. the one with start key == KeyMin).
	// The nowNanos arg specifies the wall time in nanoseconds since the
	// epoch and is used to compute the total age of all intents.
	ComputeStats(start, end roachpb.Key, nowNanos int64) (enginepb.MVCCStats, error)
	// FindSplitKey finds a key from the given span such that the left side of
	// the split is roughly targetSize bytes. The returned key will never be
	// chosen from the key ranges listed in keys.NoSplitSpans and will always
	// sort equal to or after minSplitKey.
	//
	// DO NOT CALL directly (except in wrapper Iterator implementations). Use the
	// package-level MVCCFindSplitKey instead. For correct operation, the caller
	// must set the upper bound on the iterator before calling this method.
	FindSplitKey(start, end, minSplitKey roachpb.Key, targetSize int64) (MVCCKey, error)
	// CheckForKeyCollisions checks whether any keys collide between the iterator
	// and the encoded SST data specified, within the provided key range. Returns
	// stats on skipped KVs, or an error if a collision is found.
	CheckForKeyCollisions(sstData []byte, start, end roachpb.Key) (enginepb.MVCCStats, error)
	// SetUpperBound installs a new upper bound for this iterator.
	SetUpperBound(roachpb.Key)
	// Stats returns statistics about the iterator.
	Stats() IteratorStats
}

// MVCCIterator is an interface that extends Iterator and provides concrete
// implementations for MVCCGet and MVCCScan operations. It is used by instances
// of the interface backed by RocksDB iterators to avoid cgo hops.
type MVCCIterator interface {
	Iterator
	// MVCCOpsSpecialized returns whether the iterator has a specialized
	// implementation of MVCCGet and MVCCScan. This is exposed as a method
	// so that wrapper types can defer to their wrapped iterators.
	MVCCOpsSpecialized() bool
	// MVCCGet is the internal implementation of the family of package-level
	// MVCCGet functions.
	MVCCGet(
		key roachpb.Key, timestamp hlc.Timestamp, opts MVCCGetOptions,
	) (*roachpb.Value, *roachpb.Intent, error)
	// MVCCScan is the internal implementation of the family of package-level
	// MVCCScan functions. The notable difference is that key/value pairs are
	// returned raw, as a series of buffers of length-prefixed slices,
	// alternating from key to value, where numKVs specifies the number of pairs
	// in the buffer.
	MVCCScan(
		start, end roachpb.Key, timestamp hlc.Timestamp, opts MVCCScanOptions,
	) (MVCCScanResult, error)
}

// IterOptions contains options used to create an Iterator.
//
// For performance, every Iterator must specify either Prefix or UpperBound.
type IterOptions struct {
	// If Prefix is true, Seek will use the user-key prefix of
	// the supplied MVCC key to restrict which sstables are searched,
	// but iteration (using Next) over keys without the same user-key
	// prefix will not work correctly (keys may be skipped).
	Prefix bool
	// LowerBound gives this iterator an inclusive lower bound. Attempts to
	// SeekReverse or Prev to a key that is strictly less than the bound will
	// invalidate the iterator.
	LowerBound roachpb.Key
	// UpperBound gives this iterator an exclusive upper bound. Attempts to Seek
	// or Next to a key that is greater than or equal to the bound will invalidate
	// the iterator. UpperBound must be provided unless Prefix is true, in which
	// case the end of the prefix will be used as the upper bound.
	UpperBound roachpb.Key
	// If WithStats is true, the iterator accumulates RocksDB performance
	// counters over its lifetime which can be queried via `Stats()`.
	WithStats bool
	// MinTimestampHint and MaxTimestampHint, if set, indicate that keys outside
	// of the time range formed by [MinTimestampHint, MaxTimestampHint] do not
	// need to be presented by the iterator. The underlying iterator may be able
	// to efficiently skip over keys outside of the hinted time range, e.g., when
	// an SST indicates that it contains no keys within the time range.
	//
	// Note that time bound hints are strictly a performance optimization, and
	// iterators with time bounds hints will frequently return keys outside of the
	// [start, end] time range. If you must guarantee that you never see a key
	// outside of the time bounds, perform your own filtering.
	MinTimestampHint, MaxTimestampHint hlc.Timestamp
}

// Reader is the read interface to an engine's data.
type Reader interface {
	// Close closes the reader, freeing up any outstanding resources. Note that
	// various implementations have slightly different behaviors. In particular,
	// Distinct() batches release their parent batch for future use while
	// Engines, Snapshots and Batches free the associated C++ resources.
	Close()
	// Closed returns true if the reader has been closed or is not usable.
	// Objects backed by this reader (e.g. Iterators) can check this to ensure
	// that they are not using a closed engine. Intended for use within package
	// engine; exported to enable wrappers to exist in other packages.
	Closed() bool
	// ExportToSst exports changes to the keyrange [startKey, endKey) over the
	// interval (startTS, endTS]. Passing exportAllRevisions exports
	// every revision of a key for the interval, otherwise only the latest value
	// within the interval is exported. Deletions are included if all revisions are
	// requested or if the start.Timestamp is non-zero. Returns the bytes of an
	// SSTable containing the exported keys, the size of exported data, or an error.
	//
	// If targetSize is positive, it indicates that the export should produce SSTs
	// which are roughly target size. Specifically, it will return an SST such that
	// the last key is responsible for meeting or exceeding the targetSize. If the
	// resumeKey is non-nil then the data size of the returned sst will be greater
	// than or equal to the targetSize.
	//
	// If maxSize is positive, it is an absolute maximum on byte size for the
	// returned sst. If it is the case that the versions of the last key will lead
	// to an SST that exceeds maxSize, an error will be returned. This parameter
	// exists to prevent creating SSTs which are too large to be used.
	ExportToSst(
		startKey, endKey roachpb.Key, startTS, endTS hlc.Timestamp,
		exportAllRevisions bool, targetSize uint64, maxSize uint64,
		io IterOptions,
	) (sst []byte, _ roachpb.BulkOpSummary, resumeKey roachpb.Key, _ error)
	// Get returns the value for the given key, nil otherwise.
	//
	// Deprecated: use MVCCGet instead.
	Get(key MVCCKey) ([]byte, error)
	// GetProto fetches the value at the specified key and unmarshals it
	// using a protobuf decoder. Returns true on success or false if the
	// key was not found. On success, returns the length in bytes of the
	// key and the value.
	//
	// Deprecated: use Iterator.ValueProto instead.
	GetProto(key MVCCKey, msg protoutil.Message) (ok bool, keyBytes, valBytes int64, err error)
	// Iterate scans from the start key to the end key (exclusive), invoking the
	// function f on each key value pair. If f returns an error or if the scan
	// itself encounters an error, the iteration will stop and return the error.
	// If the first result of f is true, the iteration stops and returns a nil
	// error. Note that this method is not expected take into account the
	// timestamp of the end key; all MVCCKeys at end.Key are considered excluded
	// in the iteration.
	Iterate(start, end roachpb.Key, f func(MVCCKeyValue) (stop bool, err error)) error
	// NewIterator returns a new instance of an Iterator over this
	// engine. The caller must invoke Iterator.Close() when finished
	// with the iterator to free resources.
	NewIterator(opts IterOptions) Iterator
}

// Writer is the write interface to an engine's data.
type Writer interface {
	// ApplyBatchRepr atomically applies a set of batched updates. Created by
	// calling Repr() on a batch. Using this method is equivalent to constructing
	// and committing a batch whose Repr() equals repr. If sync is true, the
	// batch is synchronously written to disk. It is an error to specify
	// sync=true if the Writer is a Batch.
	//
	// It is safe to modify the contents of the arguments after ApplyBatchRepr
	// returns.
	ApplyBatchRepr(repr []byte, sync bool) error
	// Clear removes the item from the db with the given key. Note that clear
	// actually removes entries from the storage engine, rather than inserting
	// tombstones.
	//
	// It is safe to modify the contents of the arguments after Clear returns.
	Clear(key MVCCKey) error
	// SingleClear removes the most recent write to the item from the db with
	// the given key. Whether older version of the item will come back to life
	// if not also removed with SingleClear is undefined. See the following:
	//   https://github.com/facebook/rocksdb/wiki/Single-Delete
	// for details on the SingleDelete operation that this method invokes. Note
	// that clear actually removes entries from the storage engine, rather than
	// inserting tombstones.
	//
	// It is safe to modify the contents of the arguments after SingleClear
	// returns.
	SingleClear(key MVCCKey) error
	// ClearRange removes a set of entries, from start (inclusive) to end
	// (exclusive). Similar to Clear, this method actually removes entries from
	// the storage engine.
	//
	// Note that when used on batches, subsequent reads may not reflect the result
	// of the ClearRange.
	//
	// It is safe to modify the contents of the arguments after ClearRange
	// returns.
	//
	// TODO(peter): Most callers want to pass roachpb.Key, except for
	// MVCCClearTimeRange. That function actually does what to clear records
	// between specific versions.
	ClearRange(start, end MVCCKey) error
	// ClearIterRange removes a set of entries, from start (inclusive) to end
	// (exclusive). Similar to Clear and ClearRange, this method actually removes
	// entries from the storage engine. Unlike ClearRange, the entries to remove
	// are determined by iterating over iter and per-key tombstones are
	// generated.
	//
	// It is safe to modify the contents of the arguments after ClearIterRange
	// returns.
	ClearIterRange(iter Iterator, start, end roachpb.Key) error
	// Merge is a high-performance write operation used for values which are
	// accumulated over several writes. Multiple values can be merged
	// sequentially into a single key; a subsequent read will return a "merged"
	// value which is computed from the original merged values.
	//
	// Merge currently provides specialized behavior for three data types:
	// integers, byte slices, and time series observations. Merged integers are
	// summed, acting as a high-performance accumulator.  Byte slices are simply
	// concatenated in the order they are merged. Time series observations
	// (stored as byte slices with a special tag on the roachpb.Value) are
	// combined with specialized logic beyond that of simple byte slices.
	//
	// The logic for merges is written in db.cc in order to be compatible with
	// RocksDB.
	//
	// It is safe to modify the contents of the arguments after Merge returns.
	Merge(key MVCCKey, value []byte) error
	// Put sets the given key to the value provided.
	//
	// It is safe to modify the contents of the arguments after Put returns.
	Put(key MVCCKey, value []byte) error
	// LogData adds the specified data to the RocksDB WAL. The data is
	// uninterpreted by RocksDB (i.e. not added to the memtable or sstables).
	//
	// It is safe to modify the contents of the arguments after LogData returns.
	LogData(data []byte) error
	// LogLogicalOp logs the specified logical mvcc operation with the provided
	// details to the writer, if it has logical op logging enabled. For most
	// Writer implementations, this is a no-op.
	LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails)
}

// ReadWriter is the read/write interface to an engine's data.
type ReadWriter interface {
	Reader
	Writer
}

// Engine is the interface that wraps the core operations of a key/value store.
type Engine interface {
	ReadWriter
	// Attrs returns the engine/store attributes.
	Attrs() roachpb.Attributes
	// Capacity returns capacity details for the engine's available storage.
	Capacity() (roachpb.StoreCapacity, error)
	// Compact forces compaction over the entire database.
	Compact() error
	// Flush causes the engine to write all in-memory data to disk
	// immediately.
	Flush() error
	// GetSSTables retrieves metadata about this engine's live sstables.
	GetSSTables() SSTableInfos
	// GetCompactionStats returns the internal RocksDB compaction stats. See
	// https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#rocksdb-statistics.
	GetCompactionStats() string
	// GetStats retrieves stats from the engine.
	GetStats() (*Stats, error)
	// GetEncryptionRegistries returns the file and key registries when encryption is enabled
	// on the store.
	GetEncryptionRegistries() (*EncryptionRegistries, error)
	// GetEnvStats retrieves stats about the engine's environment
	// For RocksDB, this includes details of at-rest encryption.
	GetEnvStats() (*EnvStats, error)
	// GetAuxiliaryDir returns a path under which files can be stored
	// persistently, and from which data can be ingested by the engine.
	//
	// Not thread safe.
	GetAuxiliaryDir() string
	// NewBatch returns a new instance of a batched engine which wraps
	// this engine. Batched engines accumulate all mutations and apply
	// them atomically on a call to Commit().
	NewBatch() Batch
	// NewReadOnly returns a new instance of a ReadWriter that wraps this
	// engine. This wrapper panics when unexpected operations (e.g., write
	// operations) are executed on it and caches iterators to avoid the overhead
	// of creating multiple iterators for batched reads.
	//
	// All iterators created from a read-only engine with the same "Prefix"
	// option are guaranteed to provide a consistent snapshot of the underlying
	// engine. For instance, two prefix iterators created from a read-only
	// engine will provide a consistent snapshot. Similarly, two non-prefix
	// iterators created from a read-only engine will provide a consistent
	// snapshot. However, a prefix iterator and a non-prefix iterator created
	// from a read-only engine are not guaranteed to provide a consistent view
	// of the underlying engine.
	//
	// TODO(nvanbenschoten): remove this complexity when we're fully on Pebble
	// and can guarantee that all iterators created from a read-only engine are
	// consistent. To do this, we will want to add an Iterator.Clone method.
	NewReadOnly() ReadWriter
	// NewWriteOnlyBatch returns a new instance of a batched engine which wraps
	// this engine. A write-only batch accumulates all mutations and applies them
	// atomically on a call to Commit(). Read operations return an error.
	//
	// Note that a distinct write-only batch allows reads. Distinct batches are a
	// means of indicating that the user does not need to read its own writes.
	//
	// TODO(peter): This should return a WriteBatch interface, but there are mild
	// complications in both defining that interface and implementing it. In
	// particular, Batch.Close would no longer come from Reader and we'd need to
	// refactor a bunch of code in rocksDBBatch.
	NewWriteOnlyBatch() Batch
	// NewSnapshot returns a new instance of a read-only snapshot
	// engine. Snapshots are instantaneous and, as long as they're
	// released relatively quickly, inexpensive. Snapshots are released
	// by invoking Close(). Note that snapshots must not be used after the
	// original engine has been stopped.
	NewSnapshot() Reader
	// Type returns engine type.
	Type() enginepb.EngineType
	// IngestExternalFiles atomically links a slice of files into the RocksDB
	// log-structured merge-tree.
	IngestExternalFiles(ctx context.Context, paths []string) error
	// PreIngestDelay offers an engine the chance to backpressure ingestions.
	// When called, it may choose to block if the engine determines that it is in
	// or approaching a state where further ingestions may risk its health.
	PreIngestDelay(ctx context.Context)
	// ApproximateDiskBytes returns an approximation of the on-disk size for the given key span.
	ApproximateDiskBytes(from, to roachpb.Key) (uint64, error)
	// CompactRange ensures that the specified range of key value pairs is
	// optimized for space efficiency. The forceBottommost parameter ensures
	// that the key range is compacted all the way to the bottommost level of
	// SSTables, which is necessary to pick up changes to bloom filters.
	CompactRange(start, end roachpb.Key, forceBottommost bool) error
	// InMem returns true if the receiver is an in-memory engine and false
	// otherwise.
	//
	// TODO(peter): This is a bit of a wart in the interface. It is used by
	// addSSTablePreApply to select alternate code paths, but really there should
	// be a unified code path there.
	InMem() bool

	// Filesystem functionality.
	fs.FS
	// ReadFile reads the content from the file with the given filename int this RocksDB's env.
	ReadFile(filename string) ([]byte, error)
	// WriteFile writes data to a file in this RocksDB's env.
	WriteFile(filename string, data []byte) error
	// CreateCheckpoint creates a checkpoint of the engine in the given directory,
	// which must not exist. The directory should be on the same file system so
	// that hard links can be used.
	CreateCheckpoint(dir string) error
}

// Batch is the interface for batch specific operations.
type Batch interface {
	ReadWriter
	// Commit atomically applies any batched updates to the underlying
	// engine. This is a noop unless the batch was created via NewBatch(). If
	// sync is true, the batch is synchronously committed to disk.
	Commit(sync bool) error
	// Distinct returns a view of the existing batch which only sees writes that
	// were performed before the Distinct batch was created. That is, the
	// returned batch will not read its own writes, but it will read writes to
	// the parent batch performed before the call to Distinct(), except if the
	// parent batch is a WriteOnlyBatch, in which case the Distinct() batch will
	// read from the underlying engine.
	//
	// The returned
	// batch needs to be closed before using the parent batch again. This is used
	// as an optimization to avoid flushing mutations buffered by the batch in
	// situations where we know all of the batched operations are for distinct
	// keys.
	//
	// TODO(tbg): it seems insane that you cannot read from a WriteOnlyBatch but
	// you can read from a Distinct on top of a WriteOnlyBatch but randomly don't
	// see the batch at all. I was personally just bitten by this.
	//
	// TODO(itsbilal): Improve comments around how/why distinct batches are an
	// optimization in the rocksdb write path.
	Distinct() ReadWriter
	// Empty returns whether the batch has been written to or not.
	Empty() bool
	// Len returns the size of the underlying representation of the batch.
	// Because of the batch header, the size of the batch is never 0 and should
	// not be used interchangeably with Empty. The method avoids the memory copy
	// that Repr imposes, but it still may require flushing the batch's mutations.
	Len() int
	// Repr returns the underlying representation of the batch and can be used to
	// reconstitute the batch on a remote node using Writer.ApplyBatchRepr().
	Repr() []byte
}

// Stats is a set of Engine stats. Most are described in RocksDB.
// Some stats (eg, `IngestedBytes`) are only exposed by Pebble.
//
// Currently, we collect stats from the following sources:
// 1. RocksDB's internal "tickers" (i.e. counters). They're defined in
//    rocksdb/statistics.h
// 2. DBEventListener, which implements RocksDB's EventListener interface.
// 3. rocksdb::DB::GetProperty().
//
// This is a good resource describing RocksDB's memory-related stats:
// https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB
type Stats struct {
	BlockCacheHits                 int64
	BlockCacheMisses               int64
	BlockCacheUsage                int64
	BlockCachePinnedUsage          int64
	BloomFilterPrefixChecked       int64
	BloomFilterPrefixUseful        int64
	MemtableTotalSize              int64
	Flushes                        int64
	FlushedBytes                   int64
	Compactions                    int64
	IngestedBytes                  int64 // Pebble only
	CompactedBytesRead             int64
	CompactedBytesWritten          int64
	TableReadersMemEstimate        int64
	PendingCompactionBytesEstimate int64
	L0FileCount                    int64
	L0SublevelCount                int64
}

// EnvStats is a set of RocksDB env stats, including encryption status.
type EnvStats struct {
	// TotalFiles is the total number of files reported by rocksdb.
	TotalFiles uint64
	// TotalBytes is the total size of files reported by rocksdb.
	TotalBytes uint64
	// ActiveKeyFiles is the number of files using the active data key.
	ActiveKeyFiles uint64
	// ActiveKeyBytes is the size of files using the active data key.
	ActiveKeyBytes uint64
	// EncryptionType is an enum describing the active encryption algorithm.
	// See: ccl/storageccl/engineccl/enginepbccl/key_registry.proto
	EncryptionType int32
	// EncryptionStatus is a serialized enginepbccl/stats.proto::EncryptionStatus protobuf.
	EncryptionStatus []byte
}

// EncryptionRegistries contains the encryption-related registries:
// Both are serialized protobufs.
type EncryptionRegistries struct {
	// FileRegistry is the list of files with encryption status.
	// serialized storage/engine/enginepb/file_registry.proto::FileRegistry
	FileRegistry []byte
	// KeyRegistry is the list of keys, scrubbed of actual key data.
	// serialized ccl/storageccl/engineccl/enginepbccl/key_registry.proto::DataKeysRegistry
	KeyRegistry []byte
}

// NewEngine creates a new storage engine.
func NewEngine(
	engine enginepb.EngineType, cacheSize int64, storageConfig base.StorageConfig,
) (Engine, error) {
	switch engine {
	case enginepb.EngineTypeTeePebbleRocksDB:
		pebbleConfig := PebbleConfig{
			StorageConfig: storageConfig,
			Opts:          DefaultPebbleOptions(),
		}
		pebbleConfig.Opts.Cache = pebble.NewCache(cacheSize)
		defer pebbleConfig.Opts.Cache.Unref()

		pebbleConfig.Dir = filepath.Join(pebbleConfig.Dir, "pebble")
		cache := NewRocksDBCache(cacheSize)
		defer cache.Release()

		ctx := context.Background()
		pebbleDB, err := NewPebble(ctx, pebbleConfig)
		if err != nil {
			return nil, err
		}

		rocksDBConfig := RocksDBConfig{StorageConfig: storageConfig}
		rocksDBConfig.Dir = filepath.Join(rocksDBConfig.Dir, "rocksdb")
		rocksDB, err := NewRocksDB(rocksDBConfig, cache)
		if err != nil {
			return nil, err
		}

		return NewTee(ctx, rocksDB, pebbleDB), nil
	case enginepb.EngineTypeDefault, enginepb.EngineTypePebble:
		pebbleConfig := PebbleConfig{
			StorageConfig: storageConfig,
			Opts:          DefaultPebbleOptions(),
		}
		pebbleConfig.Opts.Cache = pebble.NewCache(cacheSize)
		defer pebbleConfig.Opts.Cache.Unref()

		return NewPebble(context.Background(), pebbleConfig)
	case enginepb.EngineTypeRocksDB:
		cache := NewRocksDBCache(cacheSize)
		defer cache.Release()

		return NewRocksDB(
			RocksDBConfig{StorageConfig: storageConfig},
			cache)
	}
	panic(fmt.Sprintf("unknown engine type: %d", engine))
}

// NewDefaultEngine allocates and returns a new, opened engine with the default configuration.
// The caller must call the engine's Close method when the engine is no longer needed.
func NewDefaultEngine(cacheSize int64, storageConfig base.StorageConfig) (Engine, error) {
	return NewEngine(DefaultStorageEngine, cacheSize, storageConfig)
}

// PutProto sets the given key to the protobuf-serialized byte string
// of msg and the provided timestamp. Returns the length in bytes of
// key and the value.
//
// Deprecated: use MVCCPutProto instead.
func PutProto(
	writer Writer, key MVCCKey, msg protoutil.Message,
) (keyBytes, valBytes int64, err error) {
	bytes, err := protoutil.Marshal(msg)
	if err != nil {
		return 0, 0, err
	}

	if err := writer.Put(key, bytes); err != nil {
		return 0, 0, err
	}

	return int64(key.EncodedSize()), int64(len(bytes)), nil
}

// Scan returns up to max key/value objects starting from
// start (inclusive) and ending at end (non-inclusive).
// Specify max=0 for unbounded scans.
func Scan(reader Reader, start, end roachpb.Key, max int64) ([]MVCCKeyValue, error) {
	var kvs []MVCCKeyValue
	err := reader.Iterate(start, end, func(kv MVCCKeyValue) (bool, error) {
		if max != 0 && int64(len(kvs)) >= max {
			return true, nil
		}
		kvs = append(kvs, kv)
		return false, nil
	})
	return kvs, err
}

// WriteSyncNoop carries out a synchronous no-op write to the engine.
func WriteSyncNoop(ctx context.Context, eng Engine) error {
	batch := eng.NewBatch()
	defer batch.Close()

	if err := batch.LogData(nil); err != nil {
		return err
	}

	if err := batch.Commit(true /* sync */); err != nil {
		return err
	}
	return nil
}

// ClearRangeWithHeuristic clears the keys from start (inclusive) to end
// (exclusive). Depending on the number of keys, it will either use ClearRange
// or ClearIterRange.
func ClearRangeWithHeuristic(reader Reader, writer Writer, start, end roachpb.Key) error {
	iter := reader.NewIterator(IterOptions{UpperBound: end})
	defer iter.Close()

	// It is expensive for there to be many range deletion tombstones in the same
	// sstable because all of the tombstones in an sstable are loaded whenever the
	// sstable is accessed. So we avoid using range deletion unless there is some
	// minimum number of keys. The value here was pulled out of thin air. It might
	// be better to make this dependent on the size of the data being deleted. Or
	// perhaps we should fix RocksDB to handle large numbers of tombstones in an
	// sstable better.
	const clearRangeMinKeys = 64
	// Peek into the range to see whether it's large enough to justify
	// ClearRange. Note that the work done here is bounded by
	// clearRangeMinKeys, so it will be fairly cheap even for large
	// ranges.
	//
	// TODO(bdarnell): Move this into ClearIterRange so we don't have
	// to do this scan twice.
	count := 0
	iter.SeekGE(MakeMVCCMetadataKey(start))
	for {
		valid, err := iter.Valid()
		if err != nil {
			return err
		}
		if !valid {
			break
		}
		count++
		if count > clearRangeMinKeys {
			break
		}
		iter.Next()
	}
	var err error
	if count > clearRangeMinKeys {
		err = writer.ClearRange(MakeMVCCMetadataKey(start), MakeMVCCMetadataKey(end))
	} else {
		err = writer.ClearIterRange(iter, start, end)
	}
	if err != nil {
		return err
	}
	return nil
}

var ingestDelayL0Threshold = settings.RegisterIntSetting(
	"rocksdb.ingest_backpressure.l0_file_count_threshold",
	"number of L0 files after which to backpressure SST ingestions",
	20,
)

var ingestDelayTime = settings.RegisterDurationSetting(
	"rocksdb.ingest_backpressure.max_delay",
	"maximum amount of time to backpressure a single SST ingestion",
	time.Second*5,
)

// PreIngestDelay may choose to block for some duration if L0 has an excessive
// number of files in it or if PendingCompactionBytesEstimate is elevated. This
// it is intended to be called before ingesting a new SST, since we'd rather
// backpressure the bulk operation adding SSTs than slow down the whole RocksDB
// instance and impact all forground traffic by adding too many files to it.
// After the number of L0 files exceeds the configured limit, it gradually
// begins delaying more for each additional file in L0 over the limit until
// hitting its configured (via settings) maximum delay. If the pending
// compaction limit is exceeded, it waits for the maximum delay.
func preIngestDelay(ctx context.Context, eng Engine, settings *cluster.Settings) {
	if settings == nil {
		return
	}
	stats, err := eng.GetStats()
	if err != nil {
		log.Warningf(ctx, "failed to read stats: %+v", err)
		return
	}
	targetDelay := calculatePreIngestDelay(settings, stats)

	if targetDelay == 0 {
		return
	}
	log.VEventf(ctx, 2, "delaying SST ingestion %s. %d L0 files, %d L0 Sublevels", targetDelay, stats.L0FileCount, stats.L0SublevelCount)

	select {
	case <-time.After(targetDelay):
	case <-ctx.Done():
	}
}

func calculatePreIngestDelay(settings *cluster.Settings, stats *Stats) time.Duration {
	maxDelay := ingestDelayTime.Get(&settings.SV)
	l0ReadAmpLimit := ingestDelayL0Threshold.Get(&settings.SV)

	const ramp = 10
	l0ReadAmp := stats.L0FileCount
	if stats.L0SublevelCount >= 0 {
		l0ReadAmp = stats.L0SublevelCount
	}
	if l0ReadAmp > l0ReadAmpLimit {
		delayPerFile := maxDelay / time.Duration(ramp)
		targetDelay := time.Duration(l0ReadAmp-l0ReadAmpLimit) * delayPerFile
		if targetDelay > maxDelay {
			return maxDelay
		}
		return targetDelay
	}
	return 0
}

// Helper function to implement Reader.Iterate().
func iterateOnReader(
	reader Reader, start, end roachpb.Key, f func(MVCCKeyValue) (stop bool, err error),
) error {
	if reader.Closed() {
		return errors.New("cannot call Iterate on a closed batch")
	}
	if start.Compare(end) >= 0 {
		return nil
	}

	it := reader.NewIterator(IterOptions{UpperBound: end})
	defer it.Close()

	it.SeekGE(MakeMVCCMetadataKey(start))
	for ; ; it.Next() {
		ok, err := it.Valid()
		if err != nil {
			return err
		} else if !ok {
			break
		}
		if done, err := f(MVCCKeyValue{Key: it.Key(), Value: it.Value()}); done || err != nil {
			return err
		}
	}
	return nil
}
