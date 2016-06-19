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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/bufalloc"
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
// A replicaDataIterator provides a subset of the engine.Iterator interface.
type replicaDataIterator struct {
	curIndex int
	ranges   []keyRange
	iterator engine.Iterator
}

// makeAllKeyRanges returns all key ranges for the given Range.
func makeAllKeyRanges(d *roachpb.RangeDescriptor) []keyRange {
	return makeReplicaKeyRanges(d, keys.MakeRangeIDPrefix)
}

// makeReplicatedKeyRanges returns all key ranges that are fully Raft replicated
// for the given Range.
func makeReplicatedKeyRanges(d *roachpb.RangeDescriptor) []keyRange {
	return makeReplicaKeyRanges(d, keys.MakeRangeIDReplicatedPrefix)
}

func makeReplicaKeyRanges(d *roachpb.RangeDescriptor, metaFunc func(roachpb.RangeID) roachpb.Key) []keyRange {
	// The first range in the keyspace starts at KeyMin, which includes the
	// node-local space. We need the original StartKey to find the range
	// metadata, but the actual data starts at LocalMax.
	dataStartKey := d.StartKey.AsRawKey()
	if d.StartKey.Equal(roachpb.RKeyMin) {
		dataStartKey = keys.LocalMax
	}
	sysRangeIDKey := metaFunc(d.RangeID)
	return []keyRange{
		{
			start: engine.MakeMVCCMetadataKey(sysRangeIDKey),
			end:   engine.MakeMVCCMetadataKey(sysRangeIDKey.PrefixEnd()),
		},
		{
			start: engine.MakeMVCCMetadataKey(keys.MakeRangeKeyPrefix(d.StartKey)),
			end:   engine.MakeMVCCMetadataKey(keys.MakeRangeKeyPrefix(d.EndKey)),
		},
		{
			start: engine.MakeMVCCMetadataKey(dataStartKey),
			end:   engine.MakeMVCCMetadataKey(d.EndKey.AsRawKey()),
		},
	}
}

func newReplicaDataIterator(
	d *roachpb.RangeDescriptor, e engine.Reader, replicatedOnly bool,
) *replicaDataIterator {
	rangeFunc := makeAllKeyRanges
	if replicatedOnly {
		rangeFunc = makeReplicatedKeyRanges
	}
	ri := &replicaDataIterator{
		ranges:   rangeFunc(d),
		iterator: e.NewIterator(false),
	}
	ri.iterator.Seek(ri.ranges[ri.curIndex].start)
	ri.advance()
	return ri
}

// Close the underlying iterator.
func (ri *replicaDataIterator) Close() {
	ri.curIndex = len(ri.ranges)
	ri.iterator.Close()
}

// Next advances to the next key in the iteration.
func (ri *replicaDataIterator) Next() {
	ri.iterator.Next()
	ri.advance()
}

// advance moves the iterator forward through the ranges until a valid
// key is found or the iteration is done and the iterator becomes
// invalid.
func (ri *replicaDataIterator) advance() {
	for {
		if !ri.Valid() || ri.iterator.Less(ri.ranges[ri.curIndex].end) {
			return
		}
		ri.curIndex++
		if ri.curIndex < len(ri.ranges) {
			ri.iterator.Seek(ri.ranges[ri.curIndex].start)
		} else {
			// Otherwise, seek to end to make iterator invalid.
			ri.iterator.Seek(engine.MVCCKeyMax)
			return
		}
	}
}

// Valid returns true if the iterator currently points to a valid value.
func (ri *replicaDataIterator) Valid() bool {
	return ri.iterator.Valid()
}

// Key returns the current key.
func (ri *replicaDataIterator) Key() engine.MVCCKey {
	return ri.iterator.Key()
}

// Value returns the current value.
func (ri *replicaDataIterator) Value() []byte {
	return ri.iterator.Value()
}

// Error returns the error, if any, which the iterator encountered.
func (ri *replicaDataIterator) Error() error {
	return ri.iterator.Error()
}

// AllocIterKeyValue returns ri.Key() and ri.Value() with the underlying
// storage allocated from the passed ByteAllocator.
func (ri *replicaDataIterator) AllocIterKeyValue(
	a bufalloc.ByteAllocator,
) (bufalloc.ByteAllocator, engine.MVCCKey, []byte) {
	return engine.AllocIterKeyValue(a, ri.iterator)
}
