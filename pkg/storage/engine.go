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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// DefaultStorageEngine represents the default storage engine to use.
var DefaultStorageEngine enginepb.EngineType

func init() {
	_ = DefaultStorageEngine.Set(envutil.EnvOrDefaultString("COCKROACH_STORAGE_ENGINE", "pebble"))
}

// SimpleMVCCIterator is an interface for iterating over key/value pairs in an
// engine. SimpleMVCCIterator implementations are thread safe unless otherwise
// noted. SimpleMVCCIterator is a subset of the functionality offered by MVCCIterator.
type SimpleMVCCIterator interface {
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
	// the next call to {Next,NextKey,Prev,SeekGE,SeekLT,Close}.
	UnsafeKey() MVCCKey
	// UnsafeValue returns the same value as Value, but the memory is
	// invalidated on the next call to {Next,NextKey,Prev,SeekGE,SeekLT,Close}.
	UnsafeValue() []byte
}

// IteratorStats is returned from (MVCCIterator).Stats.
type IteratorStats struct {
	InternalDeleteSkippedCount int
	TimeBoundNumSSTs           int
}

// MVCCIterator is an interface for iterating over key/value pairs in an
// engine. It is used for iterating over the key space that can have multiple
// versions, and if often also used (due to historical reasons) for iterating
// over the key space that never has multiple versions (i.e.,
// MVCCKey.Timestamp == hlc.Timestamp{}).
//
// MVCCIterator implementations are thread safe unless otherwise noted.
type MVCCIterator interface {
	SimpleMVCCIterator

	// SeekLT advances the iterator to the first key in the engine which
	// is < the provided key.
	SeekLT(key MVCCKey)
	// Prev moves the iterator backward to the previous key/value
	// in the iteration. After this call, Valid() will be true if the
	// iterator was not positioned at the first key.
	Prev()
	// Key returns the current key.
	Key() MVCCKey
	// UnsafeRawKey returns the current raw key (i.e. the encoded MVCC key).
	// TODO(sumeer): this is a dangerous method since it may expose the
	// raw key of a separated intent. Audit all callers and fix.
	UnsafeRawKey() []byte
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
	// DO NOT CALL directly (except in wrapper MVCCIterator implementations). Use the
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
	// SupportsPrev returns true if MVCCIterator implementation supports reverse
	// iteration with Prev() or SeekLT().
	SupportsPrev() bool
}

// EngineIterator is an iterator over key-value pairs where the key is
// an EngineKey.
//lint:ignore U1001 unused
type EngineIterator interface {
	// Close frees up resources held by the iterator.
	Close()
	// SeekGE advances the iterator to the first key in the engine which
	// is >= the provided key.
	SeekGE(key EngineKey) (valid bool, err error)
	// SeekLT advances the iterator to the first key in the engine which
	// is < the provided key.
	SeekLT(key EngineKey) (valid bool, err error)
	// Next advances the iterator to the next key/value in the
	// iteration. After this call, valid will be true if the
	// iterator was not originally positioned at the last key.
	Next() (valid bool, err error)
	// Prev moves the iterator backward to the previous key/value
	// in the iteration. After this call, valid will be true if the
	// iterator was not originally positioned at the first key.
	Prev() (valid bool, err error)
	// UnsafeKey returns the same value as Key, but the memory is invalidated on
	// the next call to {Next,NextKey,Prev,SeekGE,SeekLT,Close}.
	// REQUIRES: latest positioning function returned valid=true.
	UnsafeKey() EngineKey
	// UnsafeValue returns the same value as Value, but the memory is
	// invalidated on the next call to {Next,NextKey,Prev,SeekGE,SeekLT,Close}.
	// REQUIRES: latest positioning function returned valid=true.
	UnsafeValue() []byte
	// Key returns the current key.
	// REQUIRES: latest positioning function returned valid=true.
	Key() EngineKey
	// Value returns the current value as a byte slice.
	// REQUIRES: latest positioning function returned valid=true.
	Value() []byte
	// SetUpperBound installs a new upper bound for this iterator.
	SetUpperBound(roachpb.Key)
}

// IterOptions contains options used to create an {MVCC,Engine}Iterator.
//
// For performance, every {MVCC,Engine}Iterator must specify either Prefix or
// UpperBound.
type IterOptions struct {
	// If Prefix is true, Seek will use the user-key prefix of the supplied
	// {MVCC,Engine}Key (the Key field) to restrict which sstables are searched,
	// but iteration (using Next) over keys without the same user-key prefix
	// will not work correctly (keys may be skipped).
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
	// If WithStats is true, the iterator accumulates performance
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
	//
	// These fields are only relevant for MVCCIterators.
	MinTimestampHint, MaxTimestampHint hlc.Timestamp
}

// MVCCIterKind is used to inform Reader about the kind of iteration desired
// by the caller.
type MVCCIterKind int

const (
	// MVCCKeyAndIntentsIterKind specifies that intents must be seen, and appear
	// interleaved with keys, even if they are in a separated lock table.
	MVCCKeyAndIntentsIterKind MVCCIterKind = iota
	// MVCCKeyIterKind specifies that the caller does not need to see intents.
	// Any interleaved intents may be seen, but no correctness properties are
	// derivable from such partial knowledge of intents. NB: this is a performance
	// optimization when iterating over (a) MVCC keys where the caller does
	// not need to see intents, (b) a key space that is known to not have multiple
	// versions (and therefore will never have intents), like the raft log.
	MVCCKeyIterKind
)

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
	// ExportMVCCToSst exports changes to the keyrange [startKey, endKey) over the
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
	//
	// This function looks at MVCC versions and intents, and returns an error if an
	// intent is found.
	ExportMVCCToSst(
		startKey, endKey roachpb.Key, startTS, endTS hlc.Timestamp,
		exportAllRevisions bool, targetSize uint64, maxSize uint64,
		io IterOptions,
	) (sst []byte, _ roachpb.BulkOpSummary, resumeKey roachpb.Key, _ error)
	// Get returns the value for the given key, nil otherwise. Semantically, it
	// behaves as if an iterator with MVCCKeyAndIntentsIterKind was used.
	//
	// Deprecated: use storage.MVCCGet instead.
	MVCCGet(key MVCCKey) ([]byte, error)
	// MVCCGetProto fetches the value at the specified key and unmarshals it
	// using a protobuf decoder. Returns true on success or false if the
	// key was not found. On success, returns the length in bytes of the
	// key and the value. Semantically, it behaves as if an iterator with
	// MVCCKeyAndIntentsIterKind was used.
	//
	// Deprecated: use MVCCIterator.ValueProto instead.
	MVCCGetProto(key MVCCKey, msg protoutil.Message) (ok bool, keyBytes, valBytes int64, err error)
	// MVCCIterate scans from the start key to the end key (exclusive), invoking the
	// function f on each key value pair. If f returns an error or if the scan
	// itself encounters an error, the iteration will stop and return the error.
	// If the first result of f is true, the iteration stops and returns a nil
	// error. Note that this method is not expected take into account the
	// timestamp of the end key; all MVCCKeys at end.Key are considered excluded
	// in the iteration.
	MVCCIterate(start, end roachpb.Key, iterKind MVCCIterKind, f func(MVCCKeyValue) error) error
	// NewMVCCIterator returns a new instance of an MVCCIterator over this
	// engine. The caller must invoke MVCCIterator.Close() when finished
	// with the iterator to free resources.
	NewMVCCIterator(iterKind MVCCIterKind, opts IterOptions) MVCCIterator
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

	// ClearMVCC removes the item from the db with the given MVCCKey. It
	// requires that the timestamp is non-empty (see
	// {ClearUnversioned,ClearIntent} if the timestamp is empty). Note that
	// clear actually removes entries from the storage engine, rather than
	// inserting MVCC tombstones.
	//
	// It is safe to modify the contents of the arguments after it returns.
	ClearMVCC(key MVCCKey) error
	// ClearUnversioned removes an unversioned item from the db. It is for use
	// with inline metadata (not intents) and other unversioned keys (like
	// Range-ID local keys).
	//
	// It is safe to modify the contents of the arguments after it returns.
	ClearUnversioned(key roachpb.Key) error
	// ClearIntent removes an intent from the db. Unlike
	// {ClearMVCC,ClearUnversioned} this is a higher-level method that may make
	// changes in parts of the key space that are not only a function of the
	// input, and may choose to use a single-clear under the covers.
	// TODO(sumeer): add additional parameters. This initial placeholder is so
	// that we can identify all call-sites.
	//
	// It is safe to modify the contents of the arguments after it returns.
	ClearIntent(key roachpb.Key) error

	// ClearRawRange removes a set of entries, from start (inclusive) to end
	// (exclusive). It can be applied to a range consisting of MVCCKeys or the
	// more general EngineKeys -- it simply uses the roachpb.Key parameters as
	// the Key field of an EngineKey. Similar to the other Clear* methods,
	// this method actually removes entries from the storage engine.
	//
	// Note that when used on batches, subsequent reads may not reflect the result
	// of the ClearRawRange.
	//
	// It is safe to modify the contents of the arguments after it returns.
	ClearRawRange(start, end roachpb.Key) error
	// ClearMVCCRangeAndIntents removes MVCC keys and intents from start (inclusive)
	// to end (exclusive). This is a higher-level method that handles both
	// interleaved and separated intents. Similar to the other Clear* methods,
	// this method actually removes entries from the storage engine.
	//
	// Note that when used on batches, subsequent reads may not reflect the result
	// of the ClearMVCCRangeAndIntents.
	//
	// It is safe to modify the contents of the arguments after it returns.
	ClearMVCCRangeAndIntents(start, end roachpb.Key) error
	// ClearMVCCRange removes MVCC keys from start (inclusive) to end
	// (exclusive). It should not be expected to clear intents, though may clear
	// interleaved intents that it encounters. It is meant for efficiently
	// clearing a subset of versions of a key, since the parameters are MVCCKeys
	// and not roachpb.Keys. Similar to the other Clear* methods, this method
	// actually removes entries from the storage engine.
	//
	// Note that when used on batches, subsequent reads may not reflect the result
	// of the ClearMVCCRange.
	//
	// It is safe to modify the contents of the arguments after it returns.
	ClearMVCCRange(start, end MVCCKey) error

	// ClearIterRange removes a set of entries, from start (inclusive) to end
	// (exclusive). Similar to Clear and ClearRange, this method actually
	// removes entries from the storage engine. Unlike ClearRange, the entries
	// to remove are determined by iterating over iter and per-key storage
	// tombstones (not MVCC tombstones) are generated. If the MVCCIterator was
	// constructed using MVCCKeyAndIntentsIterKind, any separated intents/locks
	// will also be cleared.
	//
	// It is safe to modify the contents of the arguments after ClearIterRange
	// returns.
	ClearIterRange(iter MVCCIterator, start, end roachpb.Key) error

	// Merge is a high-performance write operation used for values which are
	// accumulated over several writes. Multiple values can be merged
	// sequentially into a single key; a subsequent read will return a "merged"
	// value which is computed from the original merged values. We only
	// support Merge for keys with no version.
	//
	// Merge currently provides specialized behavior for three data types:
	// integers, byte slices, and time series observations. Merged integers are
	// summed, acting as a high-performance accumulator.  Byte slices are simply
	// concatenated in the order they are merged. Time series observations
	// (stored as byte slices with a special tag on the roachpb.Value) are
	// combined with specialized logic beyond that of simple byte slices.
	//
	//
	// It is safe to modify the contents of the arguments after Merge returns.
	Merge(key MVCCKey, value []byte) error

	// PutMVCC sets the given key to the value provided. It requires that the
	// timestamp is non-empty (see {PutUnversioned,PutIntent} if the timestamp
	// is empty).
	//
	// It is safe to modify the contents of the arguments after Put returns.
	PutMVCC(key MVCCKey, value []byte) error
	// PutUnversioned sets the given key to the value provided. It is for use
	// with inline metadata (not intents) and other unversioned keys (like
	// Range-ID local keys).
	//
	// It is safe to modify the contents of the arguments after Put returns.
	PutUnversioned(key roachpb.Key, value []byte) error
	// PutIntent puts an intent at the given key to the value provided.
	// This is a higher-level method that may make changes in parts of the key
	// space that are not only a function of the input key, and may explicitly
	// clear the preceding intent.
	// TODO(sumeer): add additional parameters. This initial placeholder is so
	// that we can identify all call-sites.
	//
	// It is safe to modify the contents of the arguments after Put returns.
	PutIntent(key roachpb.Key, value []byte) error

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
	// GetCompactionStats returns the internal RocksDB compaction stats. See
	// https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#rocksdb-statistics.
	GetCompactionStats() string
	// GetMetrics retrieves metrics from the engine.
	GetMetrics() (*Metrics, error)
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
	// consistent. To do this, we will want to add an MVCCIterator.Clone method.
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
	// SingleClearEngine removes the most recent write to the item from the db
	// with the given key. Whether older writes of the item will come back
	// to life if not also removed with SingleClear is undefined. See the
	// following:
	//   https://github.com/facebook/rocksdb/wiki/Single-Delete
	// for details on the SingleDelete operation that this method invokes. Note
	// that clear actually removes entries from the storage engine, rather than
	// inserting MVCC tombstones. This is a low-level interface that must not be
	// called from outside the storage package. It is part of the interface because
	// there are structs that wrap Batch and implement the Batch interface, that are
	// not part of the storage package.
	// TODO(sumeer): try to remove it from this exported interface.
	//
	// It is safe to modify the contents of the arguments after it returns.
	SingleClearEngine(key EngineKey) error
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

// Metrics is a set of Engine metrics. Most are described in RocksDB.
// Some metrics (eg, `IngestedBytes`) are only exposed by Pebble.
//
// Currently, we collect stats from the following sources:
// 1. RocksDB's internal "tickers" (i.e. counters). They're defined in
//    rocksdb/statistics.h
// 2. DBEventListener, which implements RocksDB's EventListener interface.
// 3. rocksdb::DB::GetProperty().
//
// This is a good resource describing RocksDB's memory-related stats:
// https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB
//
// TODO(jackson): Refactor to mirror or even expose pebble.Metrics when
// RocksDB is removed.
type Metrics struct {
	BlockCacheHits                 int64
	BlockCacheMisses               int64
	BlockCacheUsage                int64
	BlockCachePinnedUsage          int64
	BloomFilterPrefixChecked       int64
	BloomFilterPrefixUseful        int64
	DiskSlowCount                  int64
	DiskStallCount                 int64
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
	ReadAmplification              int64
	NumSSTables                    int64
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
func NewEngine(cacheSize int64, storageConfig base.StorageConfig) (Engine, error) {
	pebbleConfig := PebbleConfig{
		StorageConfig: storageConfig,
		Opts:          DefaultPebbleOptions(),
	}
	pebbleConfig.Opts.Cache = pebble.NewCache(cacheSize)
	defer pebbleConfig.Opts.Cache.Unref()

	return NewPebble(context.Background(), pebbleConfig)
}

// NewDefaultEngine allocates and returns a new, opened engine with the default configuration.
// The caller must call the engine's Close method when the engine is no longer needed.
func NewDefaultEngine(cacheSize int64, storageConfig base.StorageConfig) (Engine, error) {
	return NewEngine(cacheSize, storageConfig)
}

// PutProto sets the given key to the protobuf-serialized byte string
// of msg. Returns the length in bytes of key and the value.
//
// Deprecated: use MVCCPutProto instead.
func PutProto(
	writer Writer, key roachpb.Key, msg protoutil.Message,
) (keyBytes, valBytes int64, err error) {
	bytes, err := protoutil.Marshal(msg)
	if err != nil {
		return 0, 0, err
	}

	if err := writer.PutUnversioned(key, bytes); err != nil {
		return 0, 0, err
	}

	return int64(MVCCKey{Key: key}.EncodedSize()), int64(len(bytes)), nil
}

// Scan returns up to max key/value objects starting from
// start (inclusive) and ending at end (non-inclusive).
// Specify max=0 for unbounded scans.
func Scan(reader Reader, start, end roachpb.Key, max int64) ([]MVCCKeyValue, error) {
	var kvs []MVCCKeyValue
	err := reader.MVCCIterate(start, end, MVCCKeyAndIntentsIterKind, func(kv MVCCKeyValue) error {
		if max != 0 && int64(len(kvs)) >= max {
			return iterutil.StopIteration()
		}
		kvs = append(kvs, kv)
		return nil
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
	iter := reader.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{UpperBound: end})
	defer iter.Close()

	// It is expensive for there to be many range deletion tombstones in the same
	// sstable because all of the tombstones in an sstable are loaded whenever the
	// sstable is accessed. So we avoid using range deletion unless there is some
	// minimum number of keys. The value here was pulled out of thin air. It might
	// be better to make this dependent on the size of the data being deleted. Or
	// perhaps we should fix RocksDB to handle large numbers of tombstones in an
	// sstable better. Note that we are referring to storage-level tombstones here,
	// and not MVCC tombstones.
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
		err = writer.ClearRawRange(start, end)
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
	metrics, err := eng.GetMetrics()
	if err != nil {
		log.Warningf(ctx, "failed to read metrics: %+v", err)
		return
	}
	targetDelay := calculatePreIngestDelay(settings, metrics)

	if targetDelay == 0 {
		return
	}
	log.VEventf(ctx, 2, "delaying SST ingestion %s. %d L0 files, %d L0 Sublevels", targetDelay, metrics.L0FileCount, metrics.L0SublevelCount)

	select {
	case <-time.After(targetDelay):
	case <-ctx.Done():
	}
}

func calculatePreIngestDelay(settings *cluster.Settings, metrics *Metrics) time.Duration {
	maxDelay := ingestDelayTime.Get(&settings.SV)
	l0ReadAmpLimit := ingestDelayL0Threshold.Get(&settings.SV)

	const ramp = 10
	l0ReadAmp := metrics.L0FileCount
	if metrics.L0SublevelCount >= 0 {
		l0ReadAmp = metrics.L0SublevelCount
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

// Helper function to implement Reader.MVCCIterate().
func iterateOnReader(
	reader Reader, start, end roachpb.Key, iterKind MVCCIterKind, f func(MVCCKeyValue) error,
) error {
	if reader.Closed() {
		return errors.New("cannot call MVCCIterate on a closed batch")
	}
	if start.Compare(end) >= 0 {
		return nil
	}

	it := reader.NewMVCCIterator(iterKind, IterOptions{UpperBound: end})
	defer it.Close()

	it.SeekGE(MakeMVCCMetadataKey(start))
	for ; ; it.Next() {
		ok, err := it.Valid()
		if err != nil {
			return err
		} else if !ok {
			break
		}
		if err := f(MVCCKeyValue{Key: it.Key(), Value: it.Value()}); err != nil {
			if iterutil.Done(err) {
				return nil
			}
			return err
		}
	}
	return nil
}
