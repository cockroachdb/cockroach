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
	"encoding/gob"
	"reflect"

	"github.com/cockroachdb/cockroach/encoding"
	"github.com/cockroachdb/cockroach/util"
)

type rawKeyValue struct {
	key   Key
	value []byte
}

// Engine is the interface that wraps the core operations of a
// key/value store.
type Engine interface {
	// The engine/store attributes.
	Attrs() Attributes
	// put sets the given key to the value provided.
	put(key Key, value []byte) error
	// get returns the value for the given key, nil otherwise.
	get(key Key) ([]byte, error)
	// scan returns up to max key/value objects starting from
	// start (inclusive) and ending at end (non-inclusive).
	// Specify max=0 for unbounded scans.
	scan(start, end Key, max int64) ([]rawKeyValue, error)
	// clear removes the item from the db with the given key.
	// Note that clear actually removes entries from the storage
	// engine, rather than inserting tombstones.
	clear(key Key) error
	// writeBatch atomically applies the specified writes, deletions and
	// merges. The list passed to writeBatch must only contain elements
	// of type Batch{Put,Merge,Delete}.
	writeBatch([]interface{}) error
	// merge implements a merge operation with counter semantics.
	// See the docs for goMergeInit and goMerge for details.
	merge(key Key, value []byte) error
	// capacity returns capacity details for the engine's available storage.
	capacity() (StoreCapacity, error)
}

// A BatchDelete is a delete operation executed as part of an atomic batch.
type BatchDelete Key

// A BatchPut is a put operation executed as part of an atomic batch.
type BatchPut rawKeyValue

// A BatchMerge is a merge operation executed as part of an atomic batch.
type BatchMerge rawKeyValue

// putI sets the given key to the gob-serialized byte string of the
// value provided. Used internally. Uses current time and default
// expiration.
func putI(engine Engine, key Key, value interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}
	return engine.put(key, buf.Bytes())
}

// getI fetches the specified key and gob-deserializes it into
// "value". Returns true on success or false if the key was not
// found.
func getI(engine Engine, key Key, value interface{}) (bool, error) {
	val, err := engine.get(key)
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

// increment fetches the varint encoded int64 value specified by key
// and adds "inc" to it then re-encodes as varint. The newly incremented
// value is returned.
func increment(engine Engine, key Key, inc int64) (int64, error) {
	// First retrieve existing value.
	val, err := engine.get(key)
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
	if err = engine.put(key, encoded); err != nil {
		return 0, err
	}
	return r, nil
}

// clearRange removes a set of entries, from start (inclusive)
// to end (exclusive), up to max entries.  If max is 0, all
// entries between start and end are deleted.  This function
// returns the number of entries removed.  Either all entries
// within the range, up to max, will be deleted, or none, and
// an error will be returned.  Note that this function actually
// removes entries from the storage engine, rather than inserting
// tombstones.
func clearRange(engine Engine, start, end Key, max int64) (int, error) {
	scanned, err := engine.scan(start, end, max)

	if err != nil {
		return 0, err
	}

	var numElements = len(scanned)
	var deletes = make([]interface{}, numElements, numElements)
	// Loop over the scanned entries and add to a delete batch
	for idx, kv := range scanned {
		deletes[idx] = BatchDelete(kv.key)
	}

	err = engine.writeBatch(deletes)
	if err != nil {
		return 0, err
	}
	return numElements, nil
}
