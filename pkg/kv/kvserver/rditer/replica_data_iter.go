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
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
)

// ReplicaDataIteratorOptions defines ReplicaMVCCDataIterator creation options.
type ReplicaDataIteratorOptions struct {
	// See NewReplicaMVCCDataIterator for details.
	Reverse bool
	// IterKind is passed to underlying iterator to select desired value types.
	IterKind storage.MVCCIterKind
	// KeyTypes is passed to underlying iterator to select desired key types.
	KeyTypes storage.IterKeyType
	// ExcludeUserKeySpace removes UserKeySpace span portion. This is used if
	// fast path managed to remove this part of data using ClearRange.
	ExcludeUserKeySpace bool
}

// ReplicaMVCCDataIterator provides a complete iteration over MVCC or unversioned
// (which can be made to look like an MVCCKey) key / value
// rows in a range, including system-local metadata and user data.
// The ranges keyRange slice specifies the key spans which comprise
// the range's data. This cannot be used to iterate over keys that are not
// representable as MVCCKeys, except when such non-MVCCKeys are limited to
// intents, which can be made to look like interleaved MVCCKeys. Most callers
// want the real keys, and should use IterateReplicaKeySpans.
//
// A ReplicaMVCCDataIterator provides a subset of the engine.MVCCIterator interface.
//
// TODO(sumeer): merge with IterateReplicaKeySpans. We can use an EngineIterator
// for MVCC key spans and convert from EngineKey to MVCCKey.
type ReplicaMVCCDataIterator struct {
	ReplicaDataIteratorOptions

	reader   storage.Reader
	curIndex int
	spans    []roachpb.Span
	// When it is non-nil, it represents the iterator for curIndex.
	// A non-nil it is valid, else it is either done, or err != nil.
	it  storage.MVCCIterator
	err error
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

// MakeReplicatedKeySpansExcludingUserAndLockTable returns all key spans that are fully Raft
// replicated for the given Range, except for the lock table spans and user key span.
// These are returned in the following sorted order:
// 1. Replicated range-id local key span.
// 2. Range-local key span.
// 3. User key span.
func MakeReplicatedKeySpansExcludingUserAndLockTable(d *roachpb.RangeDescriptor) []roachpb.Span {
	return []roachpb.Span{
		MakeRangeIDLocalKeySpan(d.RangeID, true /* replicatedOnly */),
		makeRangeLocalKeySpan(d),
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
// Reverse is true or false in ReplicaDataIteratorOptions, respectively.
// With reverse iteration, it is initially positioned at the end of the last
// range, else it is initially positioned at the start of the first range.
//
// The iterator requires the reader.ConsistentIterators is true, since it
// creates a different iterator for each replicated key span. This is because
// MVCCIterator only allows changing the upper-bound of an existing iterator,
// and not both upper and lower bound.
//
// TODO(erikgrinaker): ReplicaMVCCDataIterator does not support MVCC range keys.
// This should be deprecated in favor of e.g. IterateReplicaKeySpans.
func NewReplicaMVCCDataIterator(
	d *roachpb.RangeDescriptor, reader storage.Reader, opts ReplicaDataIteratorOptions,
) *ReplicaMVCCDataIterator {
	if !reader.ConsistentIterators() {
		panic("ReplicaMVCCDataIterator needs a Reader that provides ConsistentIterators")
	}
	spans := MakeReplicatedKeySpansExceptLockTable(d)
	if opts.ExcludeUserKeySpace {
		spans = MakeReplicatedKeySpansExcludingUserAndLockTable(d)
	}
	ri := &ReplicaMVCCDataIterator{
		ReplicaDataIteratorOptions: opts,
		reader:                     reader,
		spans:                      spans,
	}
	if ri.Reverse {
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
			ri.IterKind,
			storage.IterOptions{
				LowerBound: ri.spans[ri.curIndex].Key,
				UpperBound: ri.spans[ri.curIndex].EndKey,
				KeyTypes:   ri.KeyTypes,
			})
		if ri.Reverse {
			ri.it.SeekLT(storage.MakeMVCCMetadataKey(ri.spans[ri.curIndex].EndKey))
		} else {
			ri.it.SeekGE(storage.MakeMVCCMetadataKey(ri.spans[ri.curIndex].Key))
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
func (ri *ReplicaMVCCDataIterator) RangeKeys() storage.MVCCRangeKeyStack {
	return ri.it.RangeKeys()
}

// HasPointAndRange exposes HasPointAndRange from underlying iterator. See
// storage.SimpleMVCCIterator for details.
func (ri *ReplicaMVCCDataIterator) HasPointAndRange() (bool, bool) {
	return ri.it.HasPointAndRange()
}

// IterateReplicaKeySpans iterates over each of a range's key spans, and calls
// the given visitor with an iterator over its data. Specifically, it iterates
// over the spans returned by either MakeAllKeySpans or MakeReplicatedKeySpans,
// and for each one provides first a point key iterator and then a range key
// iterator. This is the expected order for Raft snapshots.
//
// The iterator will be pre-seeked to the span, and is provided along with the
// key span and key type (point or range). Iterators that have no data are
// skipped (i.e. when the seek exhausts the iterator). The iterator will
// automatically be closed when done. To halt iteration over key spans, return
// iterutil.StopIteration().
//
// Must use a reader with consistent iterators.
func IterateReplicaKeySpans(
	desc *roachpb.RangeDescriptor,
	reader storage.Reader,
	replicatedOnly bool,
	visitor func(storage.EngineIterator, roachpb.Span, storage.IterKeyType) error,
) error {
	if !reader.ConsistentIterators() {
		panic("reader must provide consistent iterators")
	}
	var spans []roachpb.Span
	if replicatedOnly {
		spans = MakeReplicatedKeySpans(desc)
	} else {
		spans = MakeAllKeySpans(desc)
	}
	keyTypes := []storage.IterKeyType{storage.IterKeyTypePointsOnly, storage.IterKeyTypeRangesOnly}
	for _, span := range spans {
		for _, keyType := range keyTypes {
			iter := reader.NewEngineIterator(storage.IterOptions{
				KeyTypes:   keyType,
				LowerBound: span.Key,
				UpperBound: span.EndKey,
			})
			ok, err := iter.SeekEngineKeyGE(storage.EngineKey{Key: span.Key})
			if err == nil && ok {
				err = visitor(iter, span, keyType)
			}
			iter.Close()
			if err != nil {
				return iterutil.Map(err)
			}
		}
	}
	return nil
}
