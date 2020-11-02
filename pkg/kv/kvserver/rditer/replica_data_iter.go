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

// KeyRange is a helper struct for the ReplicaMVCCDataIterator and
// ReplicaEngineDataIterator.
// TODO(sumeer): change these to roachpb.Key since the timestamp is
// always empty and the code below assumes that fact.
type KeyRange struct {
	Start, End storage.MVCCKey
}

// ReplicaMVCCDataIterator provides a complete iteration over MVCC or unversioned
// (which can be made to look like an MVCCKey) key / value
// rows in a range, including system-local metadata and user data.
// The ranges keyRange slice specifies the key ranges which comprise
// the range's data. This cannot be used to iterate over keys that are not
// representable as MVCCKeys, except when such non-MVCCKeys are limited to
// intents, which can be made to look like interleaved MVCCKeys. Most callers
// want the real keys, and should use ReplicaEngineDataIterator.
//
// A ReplicaMVCCDataIterator provides a subset of the engine.MVCCIterator interface.
//
// TODO(tschottdorf): the API is awkward. By default, ReplicaMVCCDataIterator uses
// a byte allocator which needs to be reset manually using `ResetAllocator`.
// This is problematic as it requires of every user careful tracking of when
// to call that method; some just never call it and pull the whole replica
// into memory. Use of an allocator should be opt-in.
//
// TODO(sumeer): merge with ReplicaEngineDataIterator. We can use an EngineIterator
// for MVCC key ranges and convert from EngineKey to MVCCKey.
type ReplicaMVCCDataIterator struct {
	curIndex int
	ranges   []KeyRange
	it       storage.MVCCIterator
	a        bufalloc.ByteAllocator
}

// ReplicaEngineDataIterator is like ReplicaMVCCDataIterator, but iterates
// using the general EngineKeys. It provides a subset of the engine.EngineIterator
// interface.
//
// TODO(tschottdorf): the API is awkward. By default, ReplicaMVCCDataIterator uses
// a byte allocator which needs to be reset manually using `ResetAllocator`.
// This is problematic as it requires of every user careful tracking of when
// to call that method; some just never call it and pull the whole replica
// into memory. Use of an allocator should be opt-in.
type ReplicaEngineDataIterator struct {
	curIndex int
	ranges   []KeyRange
	it       storage.EngineIterator
	valid    bool
	err      error
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

// NewReplicaMVCCDataIterator creates a ReplicaMVCCDataIterator for the given replica.
// TODO(sumeer): narrow this interface since only gcIterator uses
// ReplicaMVCCDataIterator.
func NewReplicaMVCCDataIterator(
	d *roachpb.RangeDescriptor, reader storage.Reader, replicatedOnly bool, seekEnd bool,
) *ReplicaMVCCDataIterator {
	it := reader.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{UpperBound: d.EndKey.AsRawKey()})

	rangeFunc := MakeAllKeyRanges
	if replicatedOnly {
		rangeFunc = MakeReplicatedKeyRanges
	}
	ri := &ReplicaMVCCDataIterator{
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
func (ri *ReplicaMVCCDataIterator) seekStart() {
	ri.curIndex = 0
	ri.it.SeekGE(ri.ranges[ri.curIndex].Start)
	ri.advance()
}

// seekEnd seeks the iterator to the end of its data range.
func (ri *ReplicaMVCCDataIterator) seekEnd() {
	ri.curIndex = len(ri.ranges) - 1
	ri.it.SeekLT(ri.ranges[ri.curIndex].End)
	ri.retreat()
}

// Close the underlying iterator.
func (ri *ReplicaMVCCDataIterator) Close() {
	ri.curIndex = len(ri.ranges)
	ri.it.Close()
}

// Next advances to the next key in the iteration.
func (ri *ReplicaMVCCDataIterator) Next() {
	ri.it.Next()
	ri.advance()
}

// advance moves the iterator forward through the ranges until a valid
// key is found or the iteration is done and the iterator becomes
// invalid.
func (ri *ReplicaMVCCDataIterator) advance() {
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
func (ri *ReplicaMVCCDataIterator) Prev() {
	ri.it.Prev()
	ri.retreat()
}

// retreat is the opposite of advance.
func (ri *ReplicaMVCCDataIterator) retreat() {
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
func (ri *ReplicaMVCCDataIterator) Valid() (bool, error) {
	ok, err := ri.it.Valid()
	ok = ok && ri.curIndex >= 0 && ri.curIndex < len(ri.ranges)
	return ok, err
}

// Key returns the current key.
func (ri *ReplicaMVCCDataIterator) Key() storage.MVCCKey {
	key := ri.it.UnsafeKey()
	ri.a, key.Key = ri.a.Copy(key.Key, 0)
	return key
}

// Value returns the current value.
func (ri *ReplicaMVCCDataIterator) Value() []byte {
	value := ri.it.UnsafeValue()
	ri.a, value = ri.a.Copy(value, 0)
	return value
}

// UnsafeKey returns the same value as Key, but the memory is invalidated on
// the next call to {Next,Prev,Close}.
func (ri *ReplicaMVCCDataIterator) UnsafeKey() storage.MVCCKey {
	return ri.it.UnsafeKey()
}

// UnsafeValue returns the same value as Value, but the memory is invalidated on
// the next call to {Next,Prev,Close}.
func (ri *ReplicaMVCCDataIterator) UnsafeValue() []byte {
	return ri.it.UnsafeValue()
}

// ResetAllocator resets the ReplicaMVCCDataIterator's internal byte allocator.
func (ri *ReplicaMVCCDataIterator) ResetAllocator() {
	ri.a = nil
}

// NewReplicaEngineDataIterator creates a ReplicaEngineDataIterator for the given replica.
func NewReplicaEngineDataIterator(
	d *roachpb.RangeDescriptor, reader storage.Reader, replicatedOnly bool,
) *ReplicaEngineDataIterator {
	it := reader.NewEngineIterator(storage.IterOptions{UpperBound: d.EndKey.AsRawKey()})

	rangeFunc := MakeAllKeyRanges
	if replicatedOnly {
		rangeFunc = MakeReplicatedKeyRanges
	}
	ri := &ReplicaEngineDataIterator{
		ranges: rangeFunc(d),
		it:     it,
	}
	ri.seekStart()
	return ri
}

// seekStart seeks the iterator to the start of its data range.
func (ri *ReplicaEngineDataIterator) seekStart() {
	ri.curIndex = 0
	ri.valid, ri.err = ri.it.SeekEngineKeyGE(storage.EngineKey{Key: ri.ranges[ri.curIndex].Start.Key})
	ri.advance()
}

// Close the underlying iterator.
func (ri *ReplicaEngineDataIterator) Close() {
	ri.valid = false
	ri.it.Close()
}

// Next advances to the next key in the iteration.
func (ri *ReplicaEngineDataIterator) Next() {
	ri.valid, ri.err = ri.it.NextEngineKey()
	ri.advance()
}

// advance moves the iterator forward through the ranges until a valid
// key is found or the iteration is done and the iterator becomes
// invalid.
func (ri *ReplicaEngineDataIterator) advance() {
	for ri.valid {
		var k storage.EngineKey
		k, ri.err = ri.it.UnsafeEngineKey()
		if ri.err != nil {
			ri.valid = false
			return
		}
		if k.Key.Compare(ri.ranges[ri.curIndex].End.Key) < 0 {
			return
		}
		ri.curIndex++
		if ri.curIndex < len(ri.ranges) {
			ri.valid, ri.err = ri.it.SeekEngineKeyGE(
				storage.EngineKey{Key: ri.ranges[ri.curIndex].Start.Key})
		} else {
			ri.valid = false
			return
		}
	}
}

// Valid returns true if the iterator currently points to a valid value.
func (ri *ReplicaEngineDataIterator) Valid() (bool, error) {
	return ri.valid, ri.err
}

// Key returns the current key.
// TODO(sumeer): only used in tests. Remove method and allocator.
func (ri *ReplicaEngineDataIterator) Key() storage.EngineKey {
	key := ri.UnsafeKey()
	key, ri.a = key.CopyUsingAlloc(ri.a)
	return key
}

// Value returns the current value.
// TODO(sumeer): only used in tests. Remove method and allocator.
func (ri *ReplicaEngineDataIterator) Value() []byte {
	value := ri.it.UnsafeValue()
	ri.a, value = ri.a.Copy(value, 0)
	return value
}

// UnsafeKey returns the same value as Key, but the memory is invalidated on
// the next call to {Next,Close}.
func (ri *ReplicaEngineDataIterator) UnsafeKey() storage.EngineKey {
	key, err := ri.it.UnsafeEngineKey()
	if err != nil {
		// If Valid(), we've already extracted an EngineKey earlier,
		// when doing the key comparison, so this will not happen.
		panic("method called on an invalid iter")
	}
	return key
}

// UnsafeValue returns the same value as Value, but the memory is invalidated on
// the next call to {Next,Close}.
func (ri *ReplicaEngineDataIterator) UnsafeValue() []byte {
	return ri.it.UnsafeValue()
}
