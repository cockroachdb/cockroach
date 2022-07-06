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

// ReplicaMVCCDataIterator provides a complete iteration over MVCC or unversioned
// (which can be made to look like an MVCCKey) key / value
// rows in a range, including system-local metadata and user data.
// The ranges keyRange slice specifies the key spans which comprise
// the range's data. This cannot be used to iterate over keys that are not
// representable as MVCCKeys, except when such non-MVCCKeys are limited to
// intents, which can be made to look like interleaved MVCCKeys. Most callers
// want the real keys, and should use ReplicaEngineDataIterator.
//
// A ReplicaMVCCDataIterator provides a subset of the engine.MVCCIterator interface.
//
// TODO(sumeer): merge with ReplicaEngineDataIterator. We can use an EngineIterator
// for MVCC key spans and convert from EngineKey to MVCCKey.
type ReplicaMVCCDataIterator struct {
	reader   storage.Reader
	curIndex int
	spans    []roachpb.Span
	// When it is non-nil, it represents the iterator for curIndex.
	// A non-nil it is valid, else it is either done, or err != nil.
	it      storage.MVCCIterator
	err     error
	reverse bool
}

// ReplicaEngineDataIterator provides a complete iteration over all data in a
// range, including system-local metadata and user data. The ranges Span slice
// specifies the key spans which comprise the range's data.
//
// The iterator iterates over both point keys and range keys (i.e. MVCC range
// tombstones), but in a somewhat peculiar order: for each key span, it first
// iterates over all point keys in order, then over all range keys in order,
// signalled via HasPointAndRange(). This allows efficient non-interleaved
// iteration of point/range keys, and keeps them grouped by key span for
// efficient Raft snapshot ingestion into a single SST per key span.
//
// TODO(erikgrinaker): Reconsider the above ordering/scheme for point/range
// keys.
type ReplicaEngineDataIterator struct {
	reader     storage.Reader
	curIndex   int
	curKeyType storage.IterKeyType
	spans      []roachpb.Span
	it         storage.EngineIterator
}

// MakeAllKeySpans returns all key spans for the given Range, in
// sorted order.
func MakeAllKeySpans(d *roachpb.RangeDescriptor) []roachpb.Span {
	return makeRangeKeySpans(d, false /* replicatedOnly */)
}

// MakeReplicatedKeySpans returns all key spans that are fully Raft
// replicated for the given Range.
//
// NOTE: The logic for receiving snapshot relies on this function returning the
// spans in the following sorted order:
//
// 1. Replicated range-id local key span.
// 2. Range-local key span.
// 3. Lock-table key spans.
// 4. User key span.
func MakeReplicatedKeySpans(d *roachpb.RangeDescriptor) []roachpb.Span {
	return makeRangeKeySpans(d, true /* replicatedOnly */)
}

func makeRangeKeySpans(d *roachpb.RangeDescriptor, replicatedOnly bool) []roachpb.Span {
	rangeIDLocal := MakeRangeIDLocalKeySpan(d.RangeID, replicatedOnly)
	rangeLocal := makeRangeLocalKeySpan(d)
	rangeLockTable := makeRangeLockTableKeySpans(d)
	user := MakeUserKeySpan(d)
	ranges := make([]roachpb.Span, 5)
	ranges[0] = rangeIDLocal
	ranges[1] = rangeLocal
	if len(rangeLockTable) != 2 {
		panic("unexpected number of lock table key spans")
	}
	ranges[2] = rangeLockTable[0]
	ranges[3] = rangeLockTable[1]
	ranges[4] = user
	return ranges
}

// MakeReplicatedKeySpansExceptLockTable returns all key spans that are fully Raft
// replicated for the given Range, except for the lock table spans. These are
// returned in the following sorted order:
// 1. Replicated range-id local key span.
// 2. Range-local key span.
// 3. User key span.
func MakeReplicatedKeySpansExceptLockTable(d *roachpb.RangeDescriptor) []roachpb.Span {
	return []roachpb.Span{
		MakeRangeIDLocalKeySpan(d.RangeID, true /* replicatedOnly */),
		makeRangeLocalKeySpan(d),
		MakeUserKeySpan(d),
	}
}

// MakeReplicatedKeySpansExceptRangeID returns all key spans that are fully Raft
// replicated for the given Range, except for the replicated range-id local key span.
// These are returned in the following sorted order:
// 1. Range-local key span.
// 2. Lock-table key spans.
// 3. User key span.
func MakeReplicatedKeySpansExceptRangeID(d *roachpb.RangeDescriptor) []roachpb.Span {
	rangeLocal := makeRangeLocalKeySpan(d)
	rangeLockTable := makeRangeLockTableKeySpans(d)
	user := MakeUserKeySpan(d)
	ranges := make([]roachpb.Span, 4)
	ranges[0] = rangeLocal
	if len(rangeLockTable) != 2 {
		panic("unexpected number of lock table key spans")
	}
	ranges[1] = rangeLockTable[0]
	ranges[2] = rangeLockTable[1]
	ranges[3] = user
	return ranges
}

// MakeRangeIDLocalKeySpan returns the range-id local key span. If
// replicatedOnly is true, then it returns only the replicated keys, otherwise,
// it only returns both the replicated and unreplicated keys.
func MakeRangeIDLocalKeySpan(rangeID roachpb.RangeID, replicatedOnly bool) roachpb.Span {
	var prefixFn func(roachpb.RangeID) roachpb.Key
	if replicatedOnly {
		prefixFn = keys.MakeRangeIDReplicatedPrefix
	} else {
		prefixFn = keys.MakeRangeIDPrefix
	}
	sysRangeIDKey := prefixFn(rangeID)
	return roachpb.Span{
		Key:    sysRangeIDKey,
		EndKey: sysRangeIDKey.PrefixEnd(),
	}
}

// makeRangeLocalKeySpan returns the range local key span. Range-local keys
// are replicated keys that do not belong to the span they would naturally
// sort into. For example, /Local/Range/Table/1 would sort into [/Min,
// /System), but it actually belongs to [/Table/1, /Table/2).
func makeRangeLocalKeySpan(d *roachpb.RangeDescriptor) roachpb.Span {
	return roachpb.Span{
		Key:    keys.MakeRangeKeyPrefix(d.StartKey),
		EndKey: keys.MakeRangeKeyPrefix(d.EndKey),
	}
}

// makeRangeLockTableKeySpans returns the 2 lock table key spans.
func makeRangeLockTableKeySpans(d *roachpb.RangeDescriptor) [2]roachpb.Span {
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
	return [2]roachpb.Span{
		{
			Key:    startRangeLocal,
			EndKey: endRangeLocal,
		},
		{
			Key:    startGlobal,
			EndKey: endGlobal,
		},
	}
}

// MakeUserKeySpan returns the user key span.
func MakeUserKeySpan(d *roachpb.RangeDescriptor) roachpb.Span {
	userKeys := d.KeySpan()
	return roachpb.Span{
		Key:    userKeys.Key.AsRawKey(),
		EndKey: userKeys.EndKey.AsRawKey(),
	}
}

// NewReplicaMVCCDataIterator creates a ReplicaMVCCDataIterator for the given
// replica. It iterates over the replicated key spans excluding the lock
// table key span. Separated locks are made to appear as interleaved. The
// iterator can do one of reverse or forward iteration, based on whether
// seekEnd is true or false, respectively. With reverse iteration, it is
// initially positioned at the end of the last range, else it is initially
// positioned at the start of the first range.
//
// The iterator requires the reader.ConsistentIterators is true, since it
// creates a different iterator for each replicated key span. This is because
// MVCCIterator only allows changing the upper-bound of an existing iterator,
// and not both upper and lower bound.
//
// TODO(erikgrinaker): ReplicaMVCCDataIterator does not support MVCC range keys.
// This should be deprecated in favor of e.g. ReplicaEngineDataIterator.
func NewReplicaMVCCDataIterator(
	d *roachpb.RangeDescriptor, reader storage.Reader, seekEnd bool,
) *ReplicaMVCCDataIterator {
	if !reader.ConsistentIterators() {
		panic("ReplicaMVCCDataIterator needs a Reader that provides ConsistentIterators")
	}
	ri := &ReplicaMVCCDataIterator{
		reader:  reader,
		spans:   MakeReplicatedKeySpansExceptLockTable(d),
		reverse: seekEnd,
	}
	if ri.reverse {
		ri.curIndex = len(ri.spans) - 1
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
		if ri.curIndex < 0 || ri.curIndex >= len(ri.spans) {
			return
		}
		ri.it = ri.reader.NewMVCCIterator(
			storage.MVCCKeyAndIntentsIterKind,
			storage.IterOptions{
				LowerBound: ri.spans[ri.curIndex].Key,
				UpperBound: ri.spans[ri.curIndex].EndKey,
			})
		if ri.reverse {
			ri.it.SeekLT(storage.MakeMVCCMetadataKey(ri.spans[ri.curIndex].EndKey))
		} else {
			ri.it.SeekGE(storage.MakeMVCCMetadataKey(ri.spans[ri.curIndex].Key))
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

// NewReplicaEngineDataIterator creates a ReplicaEngineDataIterator for the
// given replica.
func NewReplicaEngineDataIterator(
	desc *roachpb.RangeDescriptor, reader storage.Reader, replicatedOnly bool,
) *ReplicaEngineDataIterator {
	if !reader.ConsistentIterators() {
		panic("ReplicaEngineDataIterator requires consistent iterators")
	}

	var ranges []roachpb.Span
	if replicatedOnly {
		ranges = MakeReplicatedKeySpans(desc)
	} else {
		ranges = MakeAllKeySpans(desc)
	}

	return &ReplicaEngineDataIterator{
		reader: reader,
		spans:  ranges,
	}
}

// nextIter creates an iterator for the next non-empty key span/type and seeks
// it, closing the existing iterator if any. Returns false if all key spans and
// key types have been exhausted.
//
// TODO(erikgrinaker): Rather than creating a new iterator for each key span,
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
		} else if ri.curIndex+1 < len(ri.spans) {
			ri.curIndex++
			ri.curKeyType = storage.IterKeyTypePointsOnly
		} else {
			break
		}
		if ri.it != nil {
			ri.it.Close()
		}
		keySpan := ri.spans[ri.curIndex]
		ri.it = ri.reader.NewEngineIterator(storage.IterOptions{
			KeyTypes:   ri.curKeyType,
			LowerBound: keySpan.Key,
			UpperBound: keySpan.EndKey,
		})
		if ok, err := ri.it.SeekEngineKeyGE(storage.EngineKey{Key: keySpan.Key}); ok || err != nil {
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

// SeekStart seeks the iterator to the start of the key spans.
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
