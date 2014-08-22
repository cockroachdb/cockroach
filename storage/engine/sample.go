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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package engine

import (
	"bytes"
	"encoding/binary"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
)

var (
	// The variables below are used to build the keys used for storage
	// of state and data of an SampleStorage, where they are
	// appended to a per-sample unique prefix.

	// keyDataPrefix prefixes the keys holding the data sampled.
	keyDataPrefix = Key("data")
	// keySize holds the (maximal) size of the storage.
	keySize = Key("size")
	// keySeen holds the number of elements observed during sampling.
	keySeen = Key("seen")
)

// SampleStorage implements the util.SampleStorage interface.
// It uses an Engine to persistently store the sampled data and the
// sampling state.
type SampleStorage struct {
	prefix     Key
	engine     Engine
	stateCache map[string]int64
}

// NewSampleStorage creates a new SampleStorage which uses
// the keyspace prefixed by "prefix" on the given engine and stores
// up to "size" values.
// Care must be taken that the prefix does not collide with any existing
// data and that a stored state will never be reinstantiated with another
// size (or the state will be reset).
func NewSampleStorage(engine Engine, prefix Key, size int) util.SampleStorage {
	if size < 1 {
		panic("sample storage size must be at least 1")
	}
	es := &SampleStorage{
		prefix:     prefix,
		engine:     engine,
		stateCache: make(map[string]int64),
	}
	// Set size correctly. If the storage has already
	// been setup for this prefix and the size does not
	// match, reset (=delete) the stored values.
	existingSize := es.incVar(keySize, 0)
	size64 := int64(size)
	if existingSize != size64 {
		if existingSize > 0 {
			// We're trying to use old data with a new size,
			// better to start from scratch.
			es.Reset()
			log.Warningf("existing sample of size %d re-initialized with size %d",
				existingSize, size)
		}
		es.incVar(keySize, size64)
	}
	return es
}

// Size returns the number of elements which can be stored.
func (es *SampleStorage) Size() int {
	return int(es.incVar(keySize, 0))
}

// Seen returns the number of elements that have been observed
// so far. An element is observed each time Discard() or Put()
// are invoked.
func (es *SampleStorage) Seen() int64 {
	return es.incVar(keySeen, 0)
}

// Discard increases Seen() by one. It corresponds to observing
// an event in a stream that is being sampled, but not putting it
// into the sample.
func (es *SampleStorage) Discard() {
	es.incVar(keySeen, 1)
}

// Get returns the data stored at index i. Valid indexes are
// between 0 and the size of the storage minus one, inclusively.
func (es *SampleStorage) Get(i int) interface{} {
	var ret interface{}
	key := es.indexToKey(i)
	ok, err := GetI(es.engine, key, &ret)
	if !ok {
		log.Warningf("key %v not found", string(key))
	} else if err != nil {
		log.Warning(err)
	} else {
		return ret
	}
	return nil
}

// Put adds an element to the storage at the index specified.
func (es *SampleStorage) Put(i int, v interface{}) {
	if i < 0 || i >= es.Size() {
		return
	}
	err := PutI(es.engine, es.indexToKey(i), &v)
	if err != nil {
		log.Warning(err)
	} else {
		es.incVar(keySeen, 1)
	}
}

// Slice returns the data stored in the underlying storage.
func (es *SampleStorage) Slice() []interface{} {
	startKey := MakeKey(es.prefix, keyDataPrefix)
	res, err := es.engine.Scan(
		startKey, PrefixEndKey(startKey), es.Seen())
	if err != nil {
		log.Warning(err)
		return nil
	}

	sl := make([]interface{}, len(res))
	for i, kv := range res {
		if dv, err := encoding.GobDecode(kv.Value); err == nil {
			sl[i] = dv
		} else {
			log.Warning(err)
		}
	}
	return sl
}

// Reset clears all data related to this SampleStorage, allowing it
// to be reused. This should always be called before abandoning a
// sample as it removes all data from the underlying engine.
// TODO(Tobias): It can't do that yet. See comment inside.
func (es *SampleStorage) Reset() {
	// TODO(Tobias) clearRange(es.prefix, prefixEndKey(es.prefix)) once D104 has landed.
	// Until then, workaround: simply mess with the prefix so that we won't
	// see any of the previous data. That way we can already have the tests in place.
	es.prefix = MakeKey(es.prefix, Key("\x55"))
	es.stateCache = make(map[string]int64)
}

// indexToKey translates slots into key names.
func (es *SampleStorage) indexToKey(i int) Key {
	// TODO(Tobias): Simply use the sqlite4 encoding once it is available.
	// The only requirement for the created key is that it respects the
	// integer sorting.
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, int64(i))
	return MakeKey(es.prefix, keyDataPrefix, Key(buf.Bytes()))
}

// incVar increments the state variable given by k and updates the
// internal cache. In case of errors, the last value from the cache
// is returned.
func (es *SampleStorage) incVar(k Key, inc int64) int64 {
	// Get the last seen value from the cache.
	ks := string(k)
	s, ok := es.stateCache[ks]
	if ok && inc == 0 {
		return s
	}
	r, err := Increment(es.engine, MakeKey(es.prefix, k), inc)
	if err != nil {
		log.Warning(err)
		// If the increment failed, stick with the old value if possible.
		// This will also make sure that Seen() will get stuck at MaxInt64.
		if ok {
			return s
		}
		return 0
	}
	es.stateCache[ks] = r
	return r
}
