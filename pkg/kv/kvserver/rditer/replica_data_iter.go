// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rditer

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
)

// KeyRange is a helper struct for the ReplicaDataIterator.
type KeyRange struct {
	Start, End storage.MVCCKey
}

// ReplicaDataIterator provides a complete iteration over all key / value
// rows in a range, including all system-local metadata and user data.
// The ranges keyRange slice specifies the key ranges which comprise
// all of the range's data.
//
// A ReplicaDataIterator provides a subset of the engine.Iterator interface.
//
// TODO(tschottdorf): the API is awkward. By default, ReplicaDataIterator uses
// a byte allocator which needs to be reset manually using `ResetAllocator`.
// This is problematic as it requires of every user careful tracking of when
// to call that method; some just never call it and pull the whole replica
// into memory. Use of an allocator should be opt-in.
type ReplicaDataIterator struct {
	curIndex int
	ranges   []KeyRange
	it       storage.Iterator
	a        bufalloc.ByteAllocator
}

// MakeAllKeyRanges returns all key ranges for the given Range.
func MakeAllKeyRanges(d *roachpb.RangeDescriptor) []KeyRange {
	return []KeyRange{
		MakeRangeIDLocalKeyRange(d.RangeID, false /* replicatedOnly */),
		MakeRangeLocalKeyRange(d),
		MakeUserKeyRange(d),
	}
}

// MakeReplicatedKeyRanges returns all key ranges that are fully Raft
// replicated for the given Range.
//
// NOTE: The logic for receiving snapshot relies on this function returning the
// ranges in the following sorted order:
//
// 1. Replicated range-id local key range
// 2. Range-local key range
// 3. User key range
func MakeReplicatedKeyRanges(d *roachpb.RangeDescriptor) []KeyRange {
	return []KeyRange{
		MakeRangeIDLocalKeyRange(d.RangeID, true /* replicatedOnly */),
		MakeRangeLocalKeyRange(d),
		MakeUserKeyRange(d),
	}
}

// MakeRangeIDLocalKeyRange returns the range-id local key range. If
// replicatedOnly is true, then it returns only the replicated keys, otherwise,
// it only returns both the replicated and unreplicated keys.
func MakeRangeIDLocalKeyRange(rangeID roachpb.RangeID, replicatedOnly bool) KeyRange {
	var prefixFn func(roachpb.RangeID) roachpb.Key
	if replicatedOnly {
		prefixFn = keys.MakeRangeIDReplicatedPrefix
	} else {
		prefixFn = keys.MakeRangeIDPrefix
	}
	sysRangeIDKey := prefixFn(rangeID)
	return KeyRange{
		Start: storage.MakeMVCCMetadataKey(sysRangeIDKey),
		End:   storage.MakeMVCCMetadataKey(sysRangeIDKey.PrefixEnd()),
	}
}

// MakeRangeLocalKeyRange returns the range local key range. Range-local keys
// are replicated keys that do not belong to the range they would naturally
// sort into. For example, /Local/Range/Table/1 would sort into [/Min,
// /System), but it actually belongs to [/Table/1, /Table/2).
func MakeRangeLocalKeyRange(d *roachpb.RangeDescriptor) KeyRange {
	return KeyRange{
		Start: storage.MakeMVCCMetadataKey(keys.MakeRangeKeyPrefix(d.StartKey)),
		End:   storage.MakeMVCCMetadataKey(keys.MakeRangeKeyPrefix(d.EndKey)),
	}
}

// MakeUserKeyRange returns the user key range.
func MakeUserKeyRange(d *roachpb.RangeDescriptor) KeyRange {
	// The first range in the keyspace starts at KeyMin, which includes the
	// node-local space. We need the original StartKey to find the range
	// metadata, but the actual data starts at LocalMax.
	dataStartKey := d.StartKey.AsRawKey()
	if d.StartKey.Equal(roachpb.RKeyMin) {
		dataStartKey = keys.LocalMax
	}
	return KeyRange{
		Start: storage.MakeMVCCMetadataKey(dataStartKey),
		End:   storage.MakeMVCCMetadataKey(d.EndKey.AsRawKey()),
	}
}

// NewReplicaDataIterator creates a ReplicaDataIterator for the given replica.
func NewReplicaDataIterator(
	d *roachpb.RangeDescriptor, reader storage.Reader, replicatedOnly bool, seekEnd bool,
) *ReplicaDataIterator {
	it := reader.NewIterator(storage.IterOptions{UpperBound: d.EndKey.AsRawKey()})

	rangeFunc := MakeAllKeyRanges
	if replicatedOnly {
		rangeFunc = MakeReplicatedKeyRanges
	}
	ri := &ReplicaDataIterator{
		ranges: rangeFunc(d),
		it:     it,
	}
	if seekEnd {
		ri.seekEnd()
	} else {
		ri.seekStart()
	}
	return ri
}

// seekStart seeks the iterator to the start of its data range.
func (ri *ReplicaDataIterator) seekStart() {
	ri.curIndex = 0
	ri.it.SeekGE(ri.ranges[ri.curIndex].Start)
	ri.advance()
}

// seekEnd seeks the iterator to the end of its data range.
func (ri *ReplicaDataIterator) seekEnd() {
	ri.curIndex = len(ri.ranges) - 1
	ri.it.SeekLT(ri.ranges[ri.curIndex].End)
	ri.retreat()
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
		if ok, _ := ri.Valid(); ok && ri.it.UnsafeKey().Less(ri.ranges[ri.curIndex].End) {
			return
		}
		ri.curIndex++
		if ri.curIndex < len(ri.ranges) {
			ri.it.SeekGE(ri.ranges[ri.curIndex].Start)
		} else {
			return
		}
	}
}

// Prev advances the iterator one key backwards.
func (ri *ReplicaDataIterator) Prev() {
	ri.it.Prev()
	ri.retreat()
}

// retreat is the opposite of advance.
func (ri *ReplicaDataIterator) retreat() {
	for {
		if ok, _ := ri.Valid(); ok && ri.ranges[ri.curIndex].Start.Less(ri.it.UnsafeKey()) {
			return
		}
		ri.curIndex--
		if ri.curIndex >= 0 {
			ri.it.SeekLT(ri.ranges[ri.curIndex].End)
		} else {
			return
		}
	}
}

// Valid returns true if the iterator currently points to a valid value.
func (ri *ReplicaDataIterator) Valid() (bool, error) {
	ok, err := ri.it.Valid()
	ok = ok && ri.curIndex >= 0 && ri.curIndex < len(ri.ranges)
	return ok, err
}

// Key returns the current key.
func (ri *ReplicaDataIterator) Key() storage.MVCCKey {
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

// UnsafeKey returns the same value as Key, but the memory is invalidated on
// the next call to {Next,Prev,Close}.
func (ri *ReplicaDataIterator) UnsafeKey() storage.MVCCKey {
	return ri.it.UnsafeKey()
}

// UnsafeValue returns the same value as Value, but the memory is invalidated on
// the next call to {Next,Prev,Close}.
func (ri *ReplicaDataIterator) UnsafeValue() []byte {
	return ri.it.UnsafeValue()
}

// ResetAllocator resets the ReplicaDataIterator's internal byte allocator.
func (ri *ReplicaDataIterator) ResetAllocator() {
	ri.a = nil
}
