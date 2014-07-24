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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Andrew Bonventre (andybons@gmail.com)

package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"time"

	"github.com/cockroachdb/cockroach/util"
)

// Engine is the interface that wraps the core operations of a
// key/value store.
type Engine interface {
	// The engine/store attributes.
	Attrs() Attributes
	// put sets the given key to the value provided.
	put(key Key, value Value) error
	// get returns the value for the given key, nil otherwise.
	get(key Key) (Value, error)
	// scan returns up to max key/value objects starting from
	// start (inclusive) and ending at end (non-inclusive).
	// Specify max=0 for unbounded scans.
	scan(start, end Key, max int64) ([]KeyValue, error)
	// delete removes the item from the db with the given key.
	del(key Key) error
	// writeBatch atomically applies the specified writes, deletions and
	// merges. The list passed to writeBatch must only contain elements
	// of type Batch{Put,Merge,Delete}.
	writeBatch([]interface{}) error
	// capacity returns capacity details for the engine's available storage.
	capacity() (StoreCapacity, error)
}

// A BatchDelete is a delete operation executed as part of
// an atomic batch.
type BatchDelete Key

// A BatchPut is a put operation executed as part of an atomic batch.
type BatchPut KeyValue

// A BatchMerge is a merge operation executed as part of an atomic batch.
type BatchMerge KeyValue

// putI sets the given key to the gob-serialized byte string of the
// value provided. Used internally. Uses current time and default
// expiration.
func putI(engine Engine, key Key, value interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}
	return engine.put(key, Value{
		Bytes:     buf.Bytes(),
		Timestamp: time.Now().UnixNano(),
	})
}

// getI fetches the specified key and gob-deserializes it into
// "value". Returns true on success or false if the key was not
// found. The timestamp of the write is returned as the second return
// value.
func getI(engine Engine, key Key, value interface{}) (bool, int64, error) {
	val, err := engine.get(key)
	if err != nil {
		return false, 0, err
	}
	if len(val.Bytes) == 0 {
		return false, 0, nil
	}
	if value != nil {
		if err = gob.NewDecoder(bytes.NewBuffer(val.Bytes)).Decode(value); err != nil {
			return true, val.Timestamp, err
		}
	}
	return true, val.Timestamp, nil
}

// increment fetches the varint encoded int64 value specified by key
// and adds "inc" to it then re-encodes as varint and puts the new
// value to key using the timestamp "ts". The newly incremented value
// is returned.
func increment(engine Engine, key Key, inc int64, ts int64) (int64, error) {
	// First retrieve existing value.
	val, err := engine.get(key)
	if err != nil {
		return 0, err
	}
	var int64Val int64
	// If the value exists, attempt to decode it as a varint.
	if len(val.Bytes) != 0 {
		var numBytes int
		int64Val, numBytes = binary.Varint(val.Bytes)
		if numBytes == 0 {
			return 0, util.Errorf("key %q cannot be incremented; not varint-encoded", key)
		} else if numBytes < 0 {
			return 0, util.Errorf("key %q cannot be incremented; integer overflow", key)
		}
	}

	// Check for overflow and underflow.
	r := int64Val + inc
	if (r < int64Val) != (inc < 0) {
		return 0, util.Errorf("key %q with value %d incremented by %d results in overflow", key, int64Val, inc)
	}

	encoded := make([]byte, binary.MaxVarintLen64)
	numBytes := binary.PutVarint(encoded, r)
	encoded = encoded[:numBytes]
	if err = engine.put(key, Value{Bytes: encoded, Timestamp: ts}); err != nil {
		return 0, err
	}
	return r, nil
}
