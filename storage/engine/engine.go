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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Andrew Bonventre (andybons@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Jiang-Ming Yang (jiangming.yang@gmail.com)

package engine

import (
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	gogoproto "github.com/gogo/protobuf/proto"
)

// StoreCapacity contains capacity information for a storage device.
type StoreCapacity struct {
	Capacity  int64
	Available int64
}

// PercentAvail computes the percentage of disk space that is available.
func (sc StoreCapacity) PercentAvail() float64 {
	return float64(sc.Available) / float64(sc.Capacity)
}

// Iterator is an interface for iterating over key/value pairs in an
// engine. Iterator implementation are thread safe unless otherwise
// noted.
type Iterator interface {
	// Close frees up resources held by the iterator.
	Close()
	// Seek advances the iterator to the first key in the engine which
	// is >= the provided key.
	Seek(key []byte)
	// Valid returns true if the iterator is currently valid. An
	// iterator which hasn't been seeked or has gone past the end of the
	// key range is invalid.
	Valid() bool
	// Advances the iterator to the next key/value in the
	// iteration. After this call, the Valid() will be true if the
	// iterator was not positioned at the last key.
	Next()
	// Key returns the current key as a byte slice.
	Key() proto.EncodedKey
	// Value returns the current value as a byte slice.
	Value() []byte
	// Error returns the error, if any, which the iterator encountered.
	Error() error
}

// Engine is the interface that wraps the core operations of a
// key/value store.
type Engine interface {
	// Start initializes and starts the engine.
	Start() error
	// Stop closes the engine, freeing up any outstanding resources.
	Stop()
	// Attrs returns the engine/store attributes.
	Attrs() proto.Attributes
	// Put sets the given key to the value provided.
	Put(key proto.EncodedKey, value []byte) error
	// Get returns the value for the given key, nil otherwise.
	Get(key proto.EncodedKey) ([]byte, error)
	// Iterate scans from start to end keys, visiting at most max
	// key/value pairs. On each key value pair, the function f is
	// invoked. If f returns an error or if the scan itself encounters
	// an error, the iteration will stop and return the error.
	// If the first result of f is true, the iteration stops.
	Iterate(start, end proto.EncodedKey, f func(proto.RawKeyValue) (bool, error)) error
	// Clear removes the item from the db with the given key.
	// Note that clear actually removes entries from the storage
	// engine, rather than inserting tombstones.
	Clear(key proto.EncodedKey) error
	// WriteBatch atomically applies the specified writes, deletions and
	// merges. The list passed to WriteBatch must only contain elements
	// of type Batch{Put,Merge,Delete}.
	WriteBatch([]interface{}) error
	// Merge is a high-performance write operation used for values which are
	// accumulated over several writes. Multiple values can be merged
	// sequentially into a single key; a subsequent read will return a "merged"
	// value which is computed from the original merged values.
	//
	// Merge currently provides specialized behavior for three data types:
	// integers, byte slices, and time series observations. Merged integers are
	// summed, acting as a high-performance accumulator.  Byte slices are simply
	// concatenated in the order they are merged. Time series observations
	// (stored as byte slices with a special tag on the proto.Value) are
	// combined with specialized logic beyond that of simple byte slices.
	//
	// The logic for merges is written in db.cc in order to be compatible with RocksDB.
	Merge(key proto.EncodedKey, value []byte) error
	// Capacity returns capacity details for the engine's available storage.
	Capacity() (StoreCapacity, error)
	// SetGCTimeouts sets a function which yields timeout values for GC
	// compaction of transaction and response cache entries. The return
	// values are in unix nanoseconds for the minimum transaction row
	// timestamp and the minimum response cache row timestamp respectively.
	// Rows with timestamps less than the associated value will be GC'd
	// during compaction.
	SetGCTimeouts(minTxnTS, minRCacheTS int64)
	// ApproximateSize returns the approximate number of bytes the engine is
	// using to store data for the given range of keys.
	ApproximateSize(start, end proto.EncodedKey) (uint64, error)
	// NewIterator returns a new instance of an Iterator over this
	// engine. The caller must invoke Iterator.Close() when finished with
	// the iterator to free resources.
	NewIterator() Iterator
	// NewSnapshot returns a new instance of a read-only snapshot
	// engine. Snapshots are instantaneous and, as long as they're
	// released relatively quickly, inexpensive. Snapshots are released
	// by invoking Stop(). Note that snapshots must not be used after the
	// original engine has been stopped.
	NewSnapshot() Engine
	// NewBatch returns a new instance of a batched engine which wraps
	// this engine. Batched engines accumulate all mutations and apply
	// them atomically on a call to Commit().
	NewBatch() Engine
	// Commit atomically applies any batched updates to the underlying
	// engine. This is a noop unless the engine was created via NewBatch().
	Commit() error
}

// A BatchDelete is a delete operation executed as part of an atomic batch.
type BatchDelete struct {
	proto.RawKeyValue
}

// A BatchPut is a put operation executed as part of an atomic batch.
type BatchPut struct {
	proto.RawKeyValue
}

// A BatchMerge is a merge operation executed as part of an atomic batch.
type BatchMerge struct {
	proto.RawKeyValue
}

// PutProto sets the given key to the protobuf-serialized byte string
// of msg and the provided timestamp. Returns the length in bytes of
// key and the value.
func PutProto(engine Engine, key proto.EncodedKey, msg gogoproto.Message) (keyBytes, valBytes int64, err error) {
	var data []byte
	if data, err = gogoproto.Marshal(msg); err != nil {
		return
	}
	if err = engine.Put(key, data); err != nil {
		return
	}
	keyBytes = int64(len(key))
	valBytes = int64(len(data))
	return
}

// GetProto fetches the value at the specified key and unmarshals it
// using a protobuf decoder. Returns true on success or false if the
// key was not found. On success, returns the length in bytes of the
// key and the value.
func GetProto(engine Engine, key proto.EncodedKey, msg gogoproto.Message) (ok bool, keyBytes, valBytes int64, err error) {
	var data []byte
	if data, err = engine.Get(key); err != nil {
		return
	}
	if data == nil {
		return
	}
	ok = true
	if msg != nil {
		if err = gogoproto.Unmarshal(data, msg); err != nil {
			return
		}
	}
	keyBytes = int64(len(key))
	valBytes = int64(len(data))
	return
}

// Increment fetches the varint encoded int64 value specified by key
// and adds "inc" to it then re-encodes as varint. The newly incremented
// value is returned.
func Increment(engine Engine, key proto.EncodedKey, inc int64) (int64, error) {
	// First retrieve existing value.
	val, err := engine.Get(key)
	if err != nil {
		return 0, err
	}
	var int64Val int64
	// If the value exists, attempt to decode it as a varint.
	if len(val) != 0 {
		decoded, err := encoding.Decode(key, val)
		if err != nil {
			return 0, err
		}
		if _, ok := decoded.(int64); !ok {
			return 0, util.Errorf("received value of wrong type %T", decoded)
		}
		int64Val = decoded.(int64)
	}

	// Check for overflow and underflow.
	if encoding.WillOverflow(int64Val, inc) {
		return 0, util.Errorf("key %q with value %d incremented by %d results in overflow", key, int64Val, inc)
	}

	if inc == 0 {
		return int64Val, nil
	}

	r := int64Val + inc
	encoded, err := encoding.Encode(key, r)
	if err != nil {
		return 0, util.Errorf("error encoding %d", r)
	}
	if err = engine.Put(key, encoded); err != nil {
		return 0, err
	}
	return r, nil
}

// Scan returns up to max key/value objects starting from
// start (inclusive) and ending at end (non-inclusive).
// Specify max=0 for unbounded scans.
func Scan(engine Engine, start, end proto.EncodedKey, max int64) ([]proto.RawKeyValue, error) {
	var kvs []proto.RawKeyValue
	err := engine.Iterate(start, end, func(kv proto.RawKeyValue) (bool, error) {
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
func ClearRange(engine Engine, start, end proto.EncodedKey) (int, error) {
	var deletes []interface{}
	if err := engine.Iterate(start, end, func(kv proto.RawKeyValue) (bool, error) {
		deletes = append(deletes, BatchDelete{proto.RawKeyValue{Key: kv.Key}})
		return false, nil
	}); err != nil {
		return 0, err
	}
	return len(deletes), engine.WriteBatch(deletes)
}
