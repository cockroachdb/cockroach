// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Andrew Bonventre (andybons@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Jiang-Ming Yang (jiangming.yang@gmail.com)

package engine

import (
	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/util/protoutil"
)

// Iterator is an interface for iterating over key/value pairs in an
// engine. Iterator implementations are thread safe unless otherwise
// noted.
type Iterator interface {
	// Close frees up resources held by the iterator.
	Close()
	// Seek advances the iterator to the first key in the engine which
	// is >= the provided key.
	Seek(key MVCCKey)
	// SeekReverse advances the iterator to the first key in the engine which
	// is <= the provided key.
	SeekReverse(key MVCCKey)
	// Valid returns true if the iterator is currently valid. An
	// iterator which hasn't been seeked or has gone past the end of the
	// key range is invalid.
	Valid() bool
	// Next advances the iterator to the next key/value in the
	// iteration. After this call, Valid() will be true if the
	// iterator was not positioned at the last key.
	Next()
	// Prev moves the iterator backward to the previous key/value
	// in the iteration. After this call, Valid() will be true if the
	// iterator was not positioned at the first key.
	Prev()
	// NextKey advances the iterator to the next MVCC key. This operation is
	// distinct from Next which advances to the next version of the current key
	// or the next key if the iterator is currently located at the last version
	// for a key.
	NextKey()
	// PrevKey moves the iterator backward to the previous MVCC key. This
	// operation is distinct from Prev which moves the iterator backward to the
	// prev version of the current key or the prev key if the iterator is
	// currently located at the first version for a key.
	PrevKey()
	// Key returns the current key.
	Key() MVCCKey
	// Value returns the current value as a byte slice.
	Value() []byte
	// ValueProto unmarshals the value the iterator is currently
	// pointing to using a protobuf decoder.
	ValueProto(msg proto.Message) error
	// unsafeKey returns the same value as Key, but the memory is invalidated on
	// the next call to {Next,Prev,Seek,SeekReverse,Close}.
	unsafeKey() MVCCKey
	// unsafeKey returns the same value as Value, but the memory is invalidated
	// on the next call to {Next,Prev,Seek,SeekReverse,Close}.
	unsafeValue() []byte
	// Less returns true if the key the iterator is currently positioned at is
	// less than the specified key.
	Less(key MVCCKey) bool
	// Error returns the error, if any, which the iterator encountered.
	Error() error
	// ComputeStats scans the underlying engine from start to end keys and
	// computes stats counters based on the values. This method is used after a
	// range is split to recompute stats for each subrange. The start key is
	// always adjusted to avoid counting local keys in the event stats are being
	// recomputed for the first range (i.e. the one with start key == KeyMin).
	// The nowNanos arg specifies the wall time in nanoseconds since the
	// epoch and is used to compute the total age of all intents.
	ComputeStats(start, end MVCCKey, nowNanos int64) (enginepb.MVCCStats, error)
}

// Reader is the read interface to an engine's data.
type Reader interface {
	// Close closes the reader, freeing up any outstanding resources. Note that
	// various implementations have slightly different behaviors. In particular,
	// Distinct() batches release their parent batch for future use while
	// Engines, Snapshots and Batches free the associated C++ resources.
	Close()
	// closed returns true if the reader has been closed or is not usable.
	// Objects backed by this reader (e.g. Iterators) can check this to ensure
	// that they are not using a closed engine.
	closed() bool
	// Get returns the value for the given key, nil otherwise.
	Get(key MVCCKey) ([]byte, error)
	// GetProto fetches the value at the specified key and unmarshals it
	// using a protobuf decoder. Returns true on success or false if the
	// key was not found. On success, returns the length in bytes of the
	// key and the value.
	GetProto(key MVCCKey, msg proto.Message) (ok bool, keyBytes, valBytes int64, err error)
	// Iterate scans from start to end keys, visiting at most max
	// key/value pairs. On each key value pair, the function f is
	// invoked. If f returns an error or if the scan itself encounters
	// an error, the iteration will stop and return the error.
	// If the first result of f is true, the iteration stops.
	Iterate(start, end MVCCKey, f func(MVCCKeyValue) (bool, error)) error
	// NewIterator returns a new instance of an Iterator over this engine. When
	// prefix is true, Seek will use the user-key prefix of the supplied MVCC key
	// to restrict which sstables are searched, but iteration (using Next) over
	// keys without the same user-key prefix will not work correctly (keys may be
	// skipped). The caller must invoke Iterator.Close() when finished with the
	// iterator to free resources.
	NewIterator(prefix bool) Iterator
}

// Writer is the write interface to an engine's data.
type Writer interface {
	// ApplyBatchRepr atomically applies a set of batched updates. Created by
	// calling Repr() on a batch. Using this method is equivalent to constructing
	// and committing a batch whose Repr() equals repr.
	ApplyBatchRepr(repr []byte) error
	// Clear removes the item from the db with the given key.
	// Note that clear actually removes entries from the storage
	// engine, rather than inserting tombstones.
	Clear(key MVCCKey) error
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
	// The logic for merges is written in db.cc in order to be compatible with RocksDB.
	Merge(key MVCCKey, value []byte) error
	// Put sets the given key to the value provided.
	Put(key MVCCKey, value []byte) error
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
	// Checkpoint creates a point-in-time snapshot of the on-disk state of the
	// key/value store, hard-linking and copying files into the specified
	// directory.
	Checkpoint(dir string) error
	// Capacity returns capacity details for the engine's available storage.
	Capacity() (roachpb.StoreCapacity, error)
	// Flush causes the engine to write all in-memory data to disk
	// immediately.
	Flush() error
	// GetStats retrieves stats from the engine.
	GetStats() (*Stats, error)
	// NewBatch returns a new instance of a batched engine which wraps
	// this engine. Batched engines accumulate all mutations and apply
	// them atomically on a call to Commit().
	NewBatch() Batch
	// NewSnapshot returns a new instance of a read-only snapshot
	// engine. Snapshots are instantaneous and, as long as they're
	// released relatively quickly, inexpensive. Snapshots are released
	// by invoking Close(). Note that snapshots must not be used after the
	// original engine has been stopped.
	NewSnapshot() Reader
	// Open initializes the engine.
	Open() error
}

// Batch is the interface for batch specific operations.
type Batch interface {
	ReadWriter
	// Commit atomically applies any batched updates to the underlying
	// engine. This is a noop unless the engine was created via NewBatch().
	Commit() error
	// Distinct returns a view of the existing batch which only sees writes that
	// were performed before the Distinct batch was created. That is, the
	// returned batch will not read its own writes, but it will read writes to
	// the parent batch performed before the call to Distinct(). The returned
	// batch needs to be closed before using the parent batch again. This is used
	// as an optimization to avoid flushing mutations buffered by the batch in
	// situations where we know all of the batched operations are for distinct
	// keys.
	Distinct() ReadWriter
	// Repr returns the underlying representation of the batch and can be used to
	// reconstitute the batch on a remote node using Writer.ApplyBatchRepr().
	Repr() []byte
}

// Stats is a set of RocksDB stats. These are all described in RocksDB
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
	BlockCacheHits           int64
	BlockCacheMisses         int64
	BlockCacheUsage          int64
	BlockCachePinnedUsage    int64
	BloomFilterPrefixChecked int64
	BloomFilterPrefixUseful  int64
	MemtableHits             int64
	MemtableMisses           int64
	MemtableTotalSize        int64
	Flushes                  int64
	Compactions              int64
	TableReadersMemEstimate  int64
}

// PutProto sets the given key to the protobuf-serialized byte string
// of msg and the provided timestamp. Returns the length in bytes of
// key and the value.
func PutProto(engine Writer, key MVCCKey, msg proto.Message) (keyBytes, valBytes int64, err error) {
	bytes, err := protoutil.Marshal(msg)
	if err != nil {
		return 0, 0, err
	}

	if err := engine.Put(key, bytes); err != nil {
		return 0, 0, err
	}

	return int64(key.EncodedSize()), int64(len(bytes)), nil
}

// Scan returns up to max key/value objects starting from
// start (inclusive) and ending at end (non-inclusive).
// Specify max=0 for unbounded scans.
func Scan(engine Reader, start, end MVCCKey, max int64) ([]MVCCKeyValue, error) {
	var kvs []MVCCKeyValue
	err := engine.Iterate(start, end, func(kv MVCCKeyValue) (bool, error) {
		if max != 0 && int64(len(kvs)) >= max {
			return true, nil
		}
		kvs = append(kvs, kv)
		return false, nil
	})
	return kvs, err
}

// ClearRange removes a set of entries, from start (inclusive) to end
// (exclusive). This function returns the number of entries
// removed. Either all entries within the range will be deleted, or
// none, and an error will be returned. Note that this function
// actually removes entries from the storage engine, rather than
// inserting tombstones, as with deletion through the MVCC.
func ClearRange(engine ReadWriter, start, end MVCCKey) (int, error) {
	count := 0
	if err := engine.Iterate(start, end, func(kv MVCCKeyValue) (bool, error) {
		if err := engine.Clear(kv.Key); err != nil {
			return false, err
		}
		count++
		return false, nil
	}); err != nil {
		return 0, err
	}
	return count, nil
}
