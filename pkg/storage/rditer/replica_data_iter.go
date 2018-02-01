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

package rditer

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
)

// KeyRange is a helper struct for the ReplicaDataIterator.
type KeyRange struct {
	Start, End engine.MVCCKey
}

// ReplicaDataIterator provides a complete iteration over all key / value
// rows in a range, including all system-local metadata and user data.
// The ranges keyRange slice specifies the key ranges which comprise
// all of the range's data.
//
// A ReplicaDataIterator provides a subset of the engine.Iterator interface.
type ReplicaDataIterator struct {
	curIndex int
	ranges   []KeyRange
	it       engine.SimpleIterator
	a        bufalloc.ByteAllocator
}

// MakeAllKeyRanges returns all key ranges for the given Range.
func MakeAllKeyRanges(d *roachpb.RangeDescriptor) []KeyRange {
	return makeReplicaKeyRanges(d, keys.MakeRangeIDPrefix)
}

// MakeReplicatedKeyRanges returns all key ranges that are fully Raft replicated
// for the given Range.
func MakeReplicatedKeyRanges(d *roachpb.RangeDescriptor) []KeyRange {
	return makeReplicaKeyRanges(d, keys.MakeRangeIDReplicatedPrefix)
}

// makeReplicaKeyRanges returns a slice of 3 key ranges. The last key range in
// the returned slice corresponds to the actual range data (i.e. not the range
// metadata).
func makeReplicaKeyRanges(
	d *roachpb.RangeDescriptor, metaFunc func(roachpb.RangeID) roachpb.Key,
) []KeyRange {
	// The first range in the keyspace starts at KeyMin, which includes the
	// node-local space. We need the original StartKey to find the range
	// metadata, but the actual data starts at LocalMax.
	dataStartKey := d.StartKey.AsRawKey()
	if d.StartKey.Equal(roachpb.RKeyMin) {
		dataStartKey = keys.LocalMax
	}
	sysRangeIDKey := metaFunc(d.RangeID)
	return []KeyRange{
		{
			Start: engine.MakeMVCCMetadataKey(sysRangeIDKey),
			End:   engine.MakeMVCCMetadataKey(sysRangeIDKey.PrefixEnd()),
		},
		{
			Start: engine.MakeMVCCMetadataKey(keys.MakeRangeKeyPrefix(d.StartKey)),
			End:   engine.MakeMVCCMetadataKey(keys.MakeRangeKeyPrefix(d.EndKey)),
		},
		{
			Start: engine.MakeMVCCMetadataKey(dataStartKey),
			End:   engine.MakeMVCCMetadataKey(d.EndKey.AsRawKey()),
		},
	}
}

// NewReplicaDataIterator creates a ReplicaDataIterator for the given replica.
func NewReplicaDataIterator(
	d *roachpb.RangeDescriptor, e engine.Reader, replicatedOnly bool,
) *ReplicaDataIterator {
	return WrapWithReplicaDataIterator(d, e.NewIterator(false), replicatedOnly)
}

// WrapWithReplicaDataIterator creates a ReplicaDataIterator for the given
// replica that wraps the provided iterator.
func WrapWithReplicaDataIterator(
	d *roachpb.RangeDescriptor, it engine.SimpleIterator, replicatedOnly bool,
) *ReplicaDataIterator {
	rangeFunc := MakeAllKeyRanges
	if replicatedOnly {
		rangeFunc = MakeReplicatedKeyRanges
	}
	ri := &ReplicaDataIterator{
		ranges: rangeFunc(d),
		it:     it,
	}
	ri.it.Seek(ri.ranges[ri.curIndex].Start)
	ri.advance()
	return ri
}

// Close the underlying iterator.
func (ri *ReplicaDataIterator) Close() {
	ri.curIndex = len(ri.ranges)
	ri.it.Close()
}

// Next advances to the next key in the iteration.
func (ri *ReplicaDataIterator) Next() {
	ri.it.Next()
	ri.advance()
}

// advance moves the iterator forward through the ranges until a valid
// key is found or the iteration is done and the iterator becomes
// invalid.
func (ri *ReplicaDataIterator) advance() {
	for {
		if ok, _ := ri.Valid(); !ok || ri.it.UnsafeKey().Less(ri.ranges[ri.curIndex].End) {
			return
		}
		ri.curIndex++
		if ri.curIndex < len(ri.ranges) {
			ri.it.Seek(ri.ranges[ri.curIndex].Start)
		} else {
			// Otherwise, seek to end to make iterator invalid.
			ri.it.Seek(engine.MVCCKeyMax)
			return
		}
	}
}

// Valid returns true if the iterator currently points to a valid value.
func (ri *ReplicaDataIterator) Valid() (bool, error) {
	return ri.it.Valid()
}

// Key returns the current key.
func (ri *ReplicaDataIterator) Key() engine.MVCCKey {
	key := ri.it.UnsafeKey()
	ri.a, key.Key = ri.a.Copy(key.Key, 0)
	return key
}

// Value returns the current value.
func (ri *ReplicaDataIterator) Value() []byte {
	value := ri.it.UnsafeValue()
	ri.a, value = ri.a.Copy(value, 0)
	return value
}

// ResetAllocator resets the ReplicaDataIterator's internal byte allocator.
func (ri *ReplicaDataIterator) ResetAllocator() {
	ri.a = nil
}
