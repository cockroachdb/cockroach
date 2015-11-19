// Copyright 2015 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
)

// keyRange is a helper struct for the rangeDataIterator.
type keyRange struct {
	start, end engine.MVCCKey
}

// replicaDataIterator provides a complete iteration over all key / value
// rows in a range, including all system-local metadata and user data.
// The ranges keyRange slice specifies the key ranges which comprise
// all of the range's data.
//
// A replicaDataIterator provides the same API as an Engine iterator
// with the exception of the Seek() method.
type replicaDataIterator struct {
	curIndex int
	ranges   []keyRange
	engine.Iterator
}

func makeReplicaKeyRanges(d *roachpb.RangeDescriptor) []keyRange {
	// The first range in the keyspace starts at KeyMin, which includes the
	// node-local space. We need the original StartKey to find the range
	// metadata, but the actual data starts at LocalMax.
	dataStartKey := d.StartKey.AsRawKey()
	if d.StartKey.Equal(roachpb.RKeyMin) {
		dataStartKey = keys.LocalMax
	}
	return []keyRange{
		{
			start: engine.MVCCEncodeKey(keys.MakeRangeIDPrefix(d.RangeID)),
			end:   engine.MVCCEncodeKey(keys.MakeRangeIDPrefix(d.RangeID + 1)),
		},
		{
			start: engine.MVCCEncodeKey(keys.MakeRangeKeyPrefix(d.StartKey)),
			end:   engine.MVCCEncodeKey(keys.MakeRangeKeyPrefix(d.EndKey)),
		},
		{
			start: engine.MVCCEncodeKey(dataStartKey),
			end:   engine.MVCCEncodeKey(d.EndKey.AsRawKey()),
		},
	}
}

func newReplicaDataIterator(d *roachpb.RangeDescriptor, e engine.Engine) *replicaDataIterator {
	ri := &replicaDataIterator{
		ranges:   makeReplicaKeyRanges(d),
		Iterator: e.NewIterator(),
	}
	ri.Seek(ri.ranges[ri.curIndex].start)
	return ri
}

// Close closes the underlying iterator.
func (ri *replicaDataIterator) Close() {
	ri.curIndex = len(ri.ranges)
	ri.Iterator.Close()
}

// Seek seeks to the specified key.
func (ri *replicaDataIterator) Seek(key []byte) {
	ri.Iterator.Seek(key)
	ri.advance()
}

// Next returns the next raw key value in the iteration, or nil if
// iteration is done.
func (ri *replicaDataIterator) Next() {
	ri.Iterator.Next()
	ri.advance()
}

// advance moves the iterator forward through the ranges until a valid
// key is found or the iteration is done and the iterator becomes
// invalid.
func (ri *replicaDataIterator) advance() {
	for {
		if !ri.Valid() || ri.Key().Less(ri.ranges[ri.curIndex].end) {
			return
		}
		ri.curIndex++
		if ri.curIndex < len(ri.ranges) {
			ri.Iterator.Seek(ri.ranges[ri.curIndex].start)
		} else {
			// Otherwise, seek to end to make iterator invalid.
			ri.Iterator.Seek(engine.MVCCKeyMax)
			return
		}
	}
}

func (ri *replicaDataIterator) SeekReverse(key []byte) {
	panic("cannot reverse scan replicaDataIterator")
}

func (ri *replicaDataIterator) Prev() {
	panic("cannot reverse scan replicaDataIterator")
}
