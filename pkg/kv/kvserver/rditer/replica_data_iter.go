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
type KeyRange struct {
	Start, End roachpb.Key
}

// ReplicaDataIteratorOptions defines ReplicaMVCCDataIterator creation options.
type ReplicaDataIteratorOptions struct {
	// See NewReplicaMVCCDataIterator for details.
	Reverse bool
	// IterKind is passed to underlying iterator to select desired value types.
	IterKind storage.MVCCIterKind
	// KeyTypes is passed to underlying iterator to select desired key types.
	KeyTypes storage.IterKeyType
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
	ReplicaDataIteratorOptions

	reader   storage.Reader
	curIndex int
	ranges   []KeyRange
	// When it is non-nil, it represents the iterator for curIndex.
	// A non-nil it is valid, else it is either done, or err != nil.
	it  storage.MVCCIterator
	err error
}

// ReplicaEngineDataIterator provides a complete iteration over all data in a
// range, including system-local metadata and user data. The ranges KeyRange
// slice specifies the key ranges which comprise the range's data.
//
// The iterator iterates over both point keys and range keys (i.e. MVCC range
// tombstones), but in a somewhat peculiar order: for each key range, it first
// iterates over all point keys in order, then over all range keys in order,
// signalled via HasPointAndRange(). This allows efficient non-interleaved
// iteration of point/range keys, and keeps them grouped by key range for
// efficient Raft snapshot ingestion into a single SST per key range.
//
// TODO(erikgrinaker): Reconsider the above ordering/scheme for point/range
// keys.
type ReplicaEngineDataIterator struct {
	reader     storage.Reader
	curIndex   int
	curKeyType storage.IterKeyType
	ranges     []KeyRange
	it         storage.EngineIterator
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
		Start: sysRangeIDKey,
		End:   sysRangeIDKey.PrefixEnd(),
	}
}

// makeRangeLocalKeyRange returns the range local key range. Range-local keys
// are replicated keys that do not belong to the range they would naturally
// sort into. For example, /Local/Range/Table/1 would sort into [/Min,
// /System), but it actually belongs to [/Table/1, /Table/2).
func makeRangeLocalKeyRange(d *roachpb.RangeDescriptor) KeyRange {
	return KeyRange{
		Start: keys.MakeRangeKeyPrefix(d.StartKey),
		End:   keys.MakeRangeKeyPrefix(d.EndKey),
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
			Start: startRangeLocal,
			End:   endRangeLocal,
		},
		{
			Start: startGlobal,
			End:   endGlobal,
		},
	}
}

// MakeUserKeyRange returns the user key range.
func MakeUserKeyRange(d *roachpb.RangeDescriptor) KeyRange {
	userKeys := d.KeySpan()
	return KeyRange{
		Start: userKeys.Key.AsRawKey(),
		End:   userKeys.EndKey.AsRawKey(),
	}
}

// NewReplicaMVCCDataIterator creates a ReplicaMVCCDataIterator for the given
// replica. It iterates over the replicated key ranges excluding the lock
// table key range. Separated locks are made to appear as interleaved. The
// iterator can do one of reverse or forward iteration, based on whether
// Reverse is true or false in ReplicaDataIteratorOptions, respectively.
// With reverse iteration, it is initially positioned at the end of the last
// range, else it is initially positioned at the start of the first range.
//
// The iterator requires the reader.ConsistentIterators is true, since it
// creates a different iterator for each replicated key range. This is because
// MVCCIterator only allows changing the upper-bound of an existing iterator,
// and not both upper and lower bound.
//
// TODO(erikgrinaker): ReplicaMVCCDataIterator does not support MVCC range keys.
// This should be deprecated in favor of e.g. ReplicaEngineDataIterator.
func NewReplicaMVCCDataIterator(
	d *roachpb.RangeDescriptor, reader storage.Reader, opts ReplicaDataIteratorOptions,
) *ReplicaMVCCDataIterator {
	if !reader.ConsistentIterators() {
		panic("ReplicaMVCCDataIterator needs a Reader that provides ConsistentIterators")
	}
	ri := &ReplicaMVCCDataIterator{
		ReplicaDataIteratorOptions: opts,
		reader:                     reader,
		ranges:                     MakeReplicatedKeyRangesExceptLockTable(d),
	}
	if ri.Reverse {
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
			ri.IterKind,
			storage.IterOptions{
				LowerBound: ri.ranges[ri.curIndex].Start,
				UpperBound: ri.ranges[ri.curIndex].End,
				KeyTypes:   ri.KeyTypes,
			})
		if ri.Reverse {
			ri.it.SeekLT(storage.MakeMVCCMetadataKey(ri.ranges[ri.curIndex].End))
		} else {
			ri.it.SeekGE(storage.MakeMVCCMetadataKey(ri.ranges[ri.curIndex].Start))
		}
		if valid, err := ri.it.Valid(); valid || err != nil {
			ri.err = err
			return
		}
		if ri.Reverse {
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
	if ri.Reverse {
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
	if !ri.Reverse {
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

// RangeBounds returns the range bounds for the current range key, or an
// empty span if there are none. The returned keys are only valid until the
// next iterator call.
func (ri *ReplicaMVCCDataIterator) RangeBounds() roachpb.Span {
	return ri.it.RangeBounds()
}

// UnsafeValue returns the same value as Value, but the memory is invalidated on
// the next call to {Next,Prev,Close}.
func (ri *ReplicaMVCCDataIterator) UnsafeValue() []byte {
	return ri.it.UnsafeValue()
}

// RangeKeys exposes RangeKeys from underlying iterator. See
// storage.SimpleMVCCIterator for details.
func (ri *ReplicaMVCCDataIterator) RangeKeys() []storage.MVCCRangeKeyValue {
	return ri.it.RangeKeys()
}

// HasPointAndRange exposes HasPointAndRange from underlying iterator. See
// storage.SimpleMVCCIterator for details.
func (ri *ReplicaMVCCDataIterator) HasPointAndRange() (bool, bool) {
	return ri.it.HasPointAndRange()
}

// NewReplicaEngineDataIterator creates a ReplicaEngineDataIterator for the
// given replica.
func NewReplicaEngineDataIterator(
	desc *roachpb.RangeDescriptor, reader storage.Reader, replicatedOnly bool,
) *ReplicaEngineDataIterator {
	if !reader.ConsistentIterators() {
		panic("ReplicaEngineDataIterator requires consistent iterators")
	}

	var ranges []KeyRange
	if replicatedOnly {
		ranges = MakeReplicatedKeyRanges(desc)
	} else {
		ranges = MakeAllKeyRanges(desc)
	}

	return &ReplicaEngineDataIterator{
		reader: reader,
		ranges: ranges,
	}
}

// nextIter creates an iterator for the next non-empty key range/type and seeks
// it, closing the existing iterator if any. Returns false if all key ranges and
// key types have been exhausted.
//
// TODO(erikgrinaker): Rather than creating a new iterator for each key range,
// we could expose an API to reconfigure the iterator with new bounds. However,
// the caller could also use e.g. a pebbleReadOnly which reuses the iterator
// internally. This should be benchmarked.
func (ri *ReplicaEngineDataIterator) nextIter() (bool, error) {
	for {
		if ri.it == nil {
			ri.curIndex = 0
			ri.curKeyType = storage.IterKeyTypePointsOnly
		} else if ri.curKeyType == storage.IterKeyTypePointsOnly {
			ri.curKeyType = storage.IterKeyTypeRangesOnly
		} else if ri.curIndex+1 < len(ri.ranges) {
			ri.curIndex++
			ri.curKeyType = storage.IterKeyTypePointsOnly
		} else {
			break
		}
		if ri.it != nil {
			ri.it.Close()
		}
		keyRange := ri.ranges[ri.curIndex]
		ri.it = ri.reader.NewEngineIterator(storage.IterOptions{
			KeyTypes:   ri.curKeyType,
			LowerBound: keyRange.Start,
			UpperBound: keyRange.End,
		})
		if ok, err := ri.it.SeekEngineKeyGE(storage.EngineKey{Key: keyRange.Start}); ok || err != nil {
			return ok, err
		}
	}
	return false, nil
}

// Close the underlying iterator.
func (ri *ReplicaEngineDataIterator) Close() {
	if ri.it != nil {
		ri.it.Close()
	}
}

// SeekStart seeks the iterator to the start of the key ranges.
// It returns false if the iterator did not find any data.
func (ri *ReplicaEngineDataIterator) SeekStart() (bool, error) {
	if ri.it != nil {
		ri.it.Close()
		ri.it = nil
	}
	return ri.nextIter()
}

// Next advances to the next key in the iteration.
func (ri *ReplicaEngineDataIterator) Next() (bool, error) {
	ok, err := ri.it.NextEngineKey()
	if !ok && err == nil {
		ok, err = ri.nextIter()
	}
	return ok, err
}

// Value returns the current value. Only used in tests.
func (ri *ReplicaEngineDataIterator) Value() []byte {
	return append([]byte{}, ri.it.UnsafeValue()...)
}

// UnsafeKey returns the current key, but the memory is invalidated on the
// next call to {Next,Close}.
func (ri *ReplicaEngineDataIterator) UnsafeKey() (storage.EngineKey, error) {
	return ri.it.UnsafeEngineKey()
}

// UnsafeValue returns the same value as Value, but the memory is invalidated on
// the next call to {Next,Close}.
func (ri *ReplicaEngineDataIterator) UnsafeValue() []byte {
	return ri.it.UnsafeValue()
}

// HasPointAndRange returns whether the current position has a point or range
// key. ReplicaEngineDataIterator will never expose both a point key and range
// key on the same position. See struct comment for details.
func (ri *ReplicaEngineDataIterator) HasPointAndRange() (bool, bool) {
	return ri.curKeyType == storage.IterKeyTypePointsOnly,
		ri.curKeyType == storage.IterKeyTypeRangesOnly
}

// RangeBounds returns the current range key bounds, but the memory is
// invalidated on the next call to {Next,Close}.
func (ri *ReplicaEngineDataIterator) RangeBounds() (roachpb.Span, error) {
	return ri.it.EngineRangeBounds()
}

// RangeKeys returns the current range keys, but the memory is invalidated on the
// next call to {Next,Close}.
func (ri *ReplicaEngineDataIterator) RangeKeys() []storage.EngineRangeKeyValue {
	return ri.it.EngineRangeKeys()
}
