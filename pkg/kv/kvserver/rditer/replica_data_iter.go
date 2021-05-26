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
// TODO(sumeer): merge with ReplicaEngineDataIterator. We can use an EngineIterator
// for MVCC key ranges and convert from EngineKey to MVCCKey.
type ReplicaMVCCDataIterator struct {
	reader   storage.Reader
	curIndex int
	ranges   []KeyRange
	// When it is non-nil, it represents the iterator for curIndex.
	// A non-nil it is valid, else it is either done, or err != nil.
	it      storage.MVCCIterator
	err     error
	reverse bool
}

// ReplicaEngineDataIterator is like ReplicaMVCCDataIterator, but iterates
// using the general EngineKeys. It provides a subset of the engine.EngineIterator
// interface.
type ReplicaEngineDataIterator struct {
	curIndex int
	ranges   []KeyRange
	it       storage.EngineIterator
	valid    bool
	err      error
}

// MakeAllKeyRanges returns all key ranges for the given Range, in
// sorted order.
func MakeAllKeyRanges(d *roachpb.RangeDescriptor) []KeyRange {
	return makeRangeKeyRanges(d, false /* replicatedOnly */)
}

// MakeReplicatedKeyRanges returns all key ranges that are fully Raft
// replicated for the given Range.
//
// NOTE: The logic for receiving snapshot relies on this function returning the
// ranges in the following sorted order:
//
// 1. Replicated range-id local key range
// 2. Range-local key range
// 3. Lock-table key ranges
// 4. User key range
func MakeReplicatedKeyRanges(d *roachpb.RangeDescriptor) []KeyRange {
	return makeRangeKeyRanges(d, true /* replicatedOnly */)
}

func makeRangeKeyRanges(d *roachpb.RangeDescriptor, replicatedOnly bool) []KeyRange {
	rangeIDLocal := MakeRangeIDLocalKeyRange(d.RangeID, replicatedOnly)
	rangeLocal := makeRangeLocalKeyRange(d)
	rangeLockTable := makeRangeLockTableKeyRanges(d)
	user := MakeUserKeyRange(d)
	ranges := make([]KeyRange, 5)
	ranges[0] = rangeIDLocal
	ranges[1] = rangeLocal
	if len(rangeLockTable) != 2 {
		panic("unexpected number of lock table ranges")
	}
	ranges[2] = rangeLockTable[0]
	ranges[3] = rangeLockTable[1]
	ranges[4] = user
	return ranges
}

// MakeReplicatedKeyRangesExceptLockTable returns all key ranges that are fully Raft
// replicated for the given Range, except for the lock table ranges. These are
// returned in the following sorted order:
// 1. Replicated range-id local key range
// 2. Range-local key range
// 3. User key range
func MakeReplicatedKeyRangesExceptLockTable(d *roachpb.RangeDescriptor) []KeyRange {
	return []KeyRange{
		MakeRangeIDLocalKeyRange(d.RangeID, true /* replicatedOnly */),
		makeRangeLocalKeyRange(d),
		MakeUserKeyRange(d),
	}
}

// MakeReplicatedKeyRangesExceptRangeID returns all key ranges that are fully Raft
// replicated for the given Range, except for the replicated range-id local key range.
// These are returned in the following sorted order:
// 1. Range-local key range
// 2. Lock-table key ranges
// 3. User key range
func MakeReplicatedKeyRangesExceptRangeID(d *roachpb.RangeDescriptor) []KeyRange {
	rangeLocal := makeRangeLocalKeyRange(d)
	rangeLockTable := makeRangeLockTableKeyRanges(d)
	user := MakeUserKeyRange(d)
	ranges := make([]KeyRange, 4)
	ranges[0] = rangeLocal
	if len(rangeLockTable) != 2 {
		panic("unexpected number of lock table ranges")
	}
	ranges[1] = rangeLockTable[0]
	ranges[2] = rangeLockTable[1]
	ranges[3] = user
	return ranges
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

// makeRangeLocalKeyRange returns the range local key range. Range-local keys
// are replicated keys that do not belong to the range they would naturally
// sort into. For example, /Local/Range/Table/1 would sort into [/Min,
// /System), but it actually belongs to [/Table/1, /Table/2).
func makeRangeLocalKeyRange(d *roachpb.RangeDescriptor) KeyRange {
	return KeyRange{
		Start: storage.MakeMVCCMetadataKey(keys.MakeRangeKeyPrefix(d.StartKey)),
		End:   storage.MakeMVCCMetadataKey(keys.MakeRangeKeyPrefix(d.EndKey)),
	}
}

// makeRangeLockTableKeyRanges returns the 2 lock table key ranges.
func makeRangeLockTableKeyRanges(d *roachpb.RangeDescriptor) [2]KeyRange {
	// Handle doubly-local lock table keys since range descriptor key
	// is a range local key that can have a replicated lock acquired on it.
	startRangeLocal, _ := keys.LockTableSingleKey(keys.MakeRangeKeyPrefix(d.StartKey), nil)
	endRangeLocal, _ := keys.LockTableSingleKey(keys.MakeRangeKeyPrefix(d.EndKey), nil)
	// The first range in the global keyspace can start earlier than LocalMax,
	// at RKeyMin, but the actual data starts at LocalMax. We need to make this
	// adjustment here to prevent [startRangeLocal, endRangeLocal) and
	// [startGlobal, endGlobal) from overlapping.
	globalStartKey := d.StartKey.AsRawKey()
	if d.StartKey.Equal(roachpb.RKeyMin) {
		globalStartKey = keys.LocalMax
	}
	startGlobal, _ := keys.LockTableSingleKey(globalStartKey, nil)
	endGlobal, _ := keys.LockTableSingleKey(roachpb.Key(d.EndKey), nil)
	return [2]KeyRange{
		{
			Start: storage.MakeMVCCMetadataKey(startRangeLocal),
			End:   storage.MakeMVCCMetadataKey(endRangeLocal),
		},
		{
			Start: storage.MakeMVCCMetadataKey(startGlobal),
			End:   storage.MakeMVCCMetadataKey(endGlobal),
		},
	}
}

// MakeUserKeyRange returns the user key range.
func MakeUserKeyRange(d *roachpb.RangeDescriptor) KeyRange {
	userKeys := d.KeySpan()
	return KeyRange{
		Start: storage.MakeMVCCMetadataKey(userKeys.Key.AsRawKey()),
		End:   storage.MakeMVCCMetadataKey(userKeys.EndKey.AsRawKey()),
	}
}

// NewReplicaMVCCDataIterator creates a ReplicaMVCCDataIterator for the given
// replica. It iterates over the replicated key ranges excluding the lock
// table key range. Separated locks are made to appear as interleaved. The
// iterator can do one of reverse or forward iteration, based on whether
// seekEnd is true or false, respectively. With reverse iteration, it is
// initially positioned at the end of the last range, else it is initially
// positioned at the start of the first range.
//
// The iterator requires the reader.ConsistentIterators is true, since it
// creates a different iterator for each replicated key range. This is because
// MVCCIterator only allows changing the upper-bound of an existing iterator,
// and not both upper and lower bound.
func NewReplicaMVCCDataIterator(
	d *roachpb.RangeDescriptor, reader storage.Reader, seekEnd bool,
) *ReplicaMVCCDataIterator {
	if !reader.ConsistentIterators() {
		panic("ReplicaMVCCDataIterator needs a Reader that provides ConsistentIterators")
	}
	ri := &ReplicaMVCCDataIterator{
		reader:  reader,
		ranges:  MakeReplicatedKeyRangesExceptLockTable(d),
		reverse: seekEnd,
	}
	if ri.reverse {
		ri.curIndex = len(ri.ranges) - 1
	} else {
		ri.curIndex = 0
	}
	ri.tryCloseAndCreateIter()
	return ri
}

func (ri *ReplicaMVCCDataIterator) tryCloseAndCreateIter() {
	for {
		if ri.it != nil {
			ri.it.Close()
			ri.it = nil
		}
		if ri.curIndex < 0 || ri.curIndex >= len(ri.ranges) {
			return
		}
		ri.it = ri.reader.NewMVCCIterator(
			storage.MVCCKeyAndIntentsIterKind,
			storage.IterOptions{
				LowerBound: ri.ranges[ri.curIndex].Start.Key,
				UpperBound: ri.ranges[ri.curIndex].End.Key,
			})
		if ri.reverse {
			ri.it.SeekLT(ri.ranges[ri.curIndex].End)
		} else {
			ri.it.SeekGE(ri.ranges[ri.curIndex].Start)
		}
		if valid, err := ri.it.Valid(); valid || err != nil {
			ri.err = err
			return
		}
		if ri.reverse {
			ri.curIndex--
		} else {
			ri.curIndex++
		}
	}
}

// Close the underlying iterator.
func (ri *ReplicaMVCCDataIterator) Close() {
	if ri.it != nil {
		ri.it.Close()
		ri.it = nil
	}
}

// Next advances to the next key in the iteration.
func (ri *ReplicaMVCCDataIterator) Next() {
	if ri.reverse {
		panic("Next called on reverse iterator")
	}
	ri.it.Next()
	valid, err := ri.it.Valid()
	if err != nil {
		ri.err = err
		return
	}
	if !valid {
		ri.curIndex++
		ri.tryCloseAndCreateIter()
	}
}

// Prev advances the iterator one key backwards.
func (ri *ReplicaMVCCDataIterator) Prev() {
	if !ri.reverse {
		panic("Prev called on forward iterator")
	}
	ri.it.Prev()
	valid, err := ri.it.Valid()
	if err != nil {
		ri.err = err
		return
	}
	if !valid {
		ri.curIndex--
		ri.tryCloseAndCreateIter()
	}
}

// Valid returns true if the iterator currently points to a valid value.
func (ri *ReplicaMVCCDataIterator) Valid() (bool, error) {
	if ri.err != nil {
		return false, ri.err
	}
	if ri.it == nil {
		return false, nil
	}
	return true, nil
}

// Key returns the current key. Only called in tests.
func (ri *ReplicaMVCCDataIterator) Key() storage.MVCCKey {
	return ri.it.Key()
}

// Value returns the current value. Only called in tests.
func (ri *ReplicaMVCCDataIterator) Value() []byte {
	return ri.it.Value()
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

// Value returns the current value. Only used in tests.
func (ri *ReplicaEngineDataIterator) Value() []byte {
	value := ri.it.UnsafeValue()
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	return valueCopy
}

// UnsafeKey returns the current key, but the memory is invalidated on the
// next call to {Next,Close}.
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
