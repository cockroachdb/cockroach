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
	"bytes"
	"encoding/gob"
	"reflect"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
)

// RawKeyValue contains the raw bytes of the value for a key.
type RawKeyValue struct {
	Key   Key
	Value []byte
}

// StoreCapacity contains capacity information for a storage device.
type StoreCapacity struct {
	Capacity  int64
	Available int64
}

// PercentAvail computes the percentage of disk space that is available.
func (sc StoreCapacity) PercentAvail() float64 {
	return float64(sc.Available) / float64(sc.Capacity)
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
	Put(key Key, value []byte) error
	// Get returns the value for the given key, nil otherwise.
	Get(key Key) ([]byte, error)
	// Scan returns up to max key/value objects starting from
	// start (inclusive) and ending at end (non-inclusive).
	// Specify max=0 for unbounded scans.
	Scan(start, end Key, max int64) ([]RawKeyValue, error)
	// Clear removes the item from the db with the given key.
	// Note that clear actually removes entries from the storage
	// engine, rather than inserting tombstones.
	Clear(key Key) error
	// WriteBatch atomically applies the specified writes, deletions and
	// merges. The list passed to WriteBatch must only contain elements
	// of type Batch{Put,Merge,Delete}.
	WriteBatch([]interface{}) error
	// Merge implements a merge operation with counter semantics.
	// See the docs for goMergeInit and goMerge for details.
	Merge(key Key, value []byte) error
	// Capacity returns capacity details for the engine's available storage.
	Capacity() (StoreCapacity, error)
	// SetGCTimeouts sets a function which yields timeout values for GC
	// compaction of transaction and response cache entries. The return
	// values are in unix nanoseconds for the minimum transaction row
	// timestamp and the minimum response cache row timestamp respectively.
	// Rows with timestamps less than the associated value will be GC'd
	// during compaction.
	SetGCTimeouts(gcTimeouts func() (minTxnTS, minRCacheTS int64))
	// CreateSnapshot creates a snapshot handle from engine and returns
	// a snapshotID of the created snapshot.
	CreateSnapshot() (string, error)
	// ReleaseSnapshot releases the existing snapshot handle for the
	// given snapshotID.
	ReleaseSnapshot(snapshotID string) error
	// GetSnapshot returns the value for the given key from the given
	// snapshotID, nil otherwise.
	GetSnapshot(key Key, snapshotID string) ([]byte, error)
	// ScanSnapshot returns up to max key/value objects starting from
	// start (inclusive) and ending at end (non-inclusive) from the
	// given snapshotID.
	// Specify max=0 for unbounded scans.
	ScanSnapshot(start, end Key, max int64, snapshotID string) ([]RawKeyValue, error)
}

// A BatchDelete is a delete operation executed as part of an atomic batch.
type BatchDelete Key

// A BatchPut is a put operation executed as part of an atomic batch.
type BatchPut RawKeyValue

// A BatchMerge is a merge operation executed as part of an atomic batch.
type BatchMerge RawKeyValue

// PutI sets the given key to the gob-serialized byte string of the
// value provided. Used internally.
func PutI(engine Engine, key Key, value interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}
	return engine.Put(key, buf.Bytes())
}

// PutProto sets the given key to the protobuf-serialized byte string
// of msg and the provided timestamp.
func PutProto(engine Engine, key Key, msg gogoproto.Message) error {
	data, err := gogoproto.Marshal(msg)
	if err != nil {
		return err
	}
	return engine.Put(key, data)
}

// GetI fetches the specified key and gob-deserializes it into
// "value". Returns true on success or false if the key was not
// found.
func GetI(engine Engine, key Key, value interface{}) (bool, error) {
	val, err := engine.Get(key)
	if err != nil {
		return false, err
	}
	if len(val) == 0 {
		return false, nil
	}
	if value != nil {
		if err = gob.NewDecoder(bytes.NewBuffer(val)).Decode(value); err != nil {
			return true, err
		}
	}
	return true, nil
}

// GetProto fetches the value at the specified key and unmarshals it
// using a protobuf decoder. Returns true on success or false if the
// key was not found.
func GetProto(engine Engine, key Key, msg gogoproto.Message) (bool, error) {
	val, err := engine.Get(key)
	if err != nil {
		return false, err
	}
	if val == nil {
		return false, nil
	}
	if msg != nil {
		if err := gogoproto.Unmarshal(val, msg); err != nil {
			return true, err
		}
	}
	return true, nil
}

// Increment fetches the varint encoded int64 value specified by key
// and adds "inc" to it then re-encodes as varint. The newly incremented
// value is returned.
func Increment(engine Engine, key Key, inc int64) (int64, error) {
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
			return 0, util.Errorf("received value of wrong type %v", reflect.TypeOf(decoded))
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

// ClearRange removes a set of entries, from start (inclusive)
// to end (exclusive), up to max entries.  If max is 0, all
// entries between start and end are deleted.  This function
// returns the number of entries removed.  Either all entries
// within the range, up to max, will be deleted, or none, and
// an error will be returned.  Note that this function actually
// removes entries from the storage engine, rather than inserting
// tombstones.
func ClearRange(engine Engine, start, end Key, max int64) (int, error) {
	scanned, err := engine.Scan(start, end, max)

	if err != nil {
		return 0, err
	}

	var numElements = len(scanned)
	var deletes = make([]interface{}, numElements, numElements)
	// Loop over the scanned entries and add to a delete batch
	for idx, kv := range scanned {
		deletes[idx] = BatchDelete(kv.Key)
	}

	err = engine.WriteBatch(deletes)
	if err != nil {
		return 0, err
	}
	return numElements, nil
}
