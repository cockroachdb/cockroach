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
	// ExcludeUserKeySpan removes UserKeySpace span portion.
	ExcludeUserKeySpan bool
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

// makeAllKeySpans returns all key spans for the given Range, in
// sorted order.
func makeAllKeySpans(d *roachpb.RangeDescriptor) []roachpb.Span {
	return Select(d.RangeID, SelectOpts{
		ReplicatedBySpan:      d.RSpan(),
		ReplicatedByRangeID:   true,
		UnreplicatedByRangeID: true,
	})
}

// MakeReplicatedKeySpans returns all key spans that are fully Raft
// replicated for the given Range, in lexicographically sorted order:
//
// 1. Replicated range-id local key span.
// 2. "Local" key span (range descriptor, etc)
// 3. Lock-table key spans.
// 4. User key span.
func MakeReplicatedKeySpans(d *roachpb.RangeDescriptor) []roachpb.Span {
	return Select(d.RangeID, SelectOpts{
		ReplicatedBySpan:    d.RSpan(),
		ReplicatedByRangeID: true,
	})
}

// makeReplicatedKeySpansExceptLockTable returns all key spans that are fully Raft
// replicated for the given Range, except for the lock table spans. These are
// returned in the following sorted order:
// 1. Replicated range-id local key span.
// 2. Range-local key span.
// 3. User key span.
func makeReplicatedKeySpansExceptLockTable(d *roachpb.RangeDescriptor) []roachpb.Span {
	return []roachpb.Span{
		makeRangeIDReplicatedSpan(d.RangeID),
		makeRangeLocalKeySpan(d.RSpan()),
		d.KeySpan().AsRawSpanWithNoLocals(),
	}
}

// makeReplicatedKeySpansExcludingUserAndLockTable returns all key spans that are fully Raft
// replicated for the given Range, except for the lock table spans and user key span.
// These are returned in the following sorted order:
// 1. Replicated range-id local key span.
// 2. Range-local key span.
func makeReplicatedKeySpansExcludingUserAndLockTable(d *roachpb.RangeDescriptor) []roachpb.Span {
	return []roachpb.Span{
		makeRangeIDReplicatedSpan(d.RangeID),
		makeRangeLocalKeySpan(d.RSpan()),
	}
}

func makeRangeIDReplicatedSpan(rangeID roachpb.RangeID) roachpb.Span {
	prefix := keys.MakeRangeIDReplicatedPrefix(rangeID)
	return roachpb.Span{
		Key:    prefix,
		EndKey: prefix.PrefixEnd(),
	}
}

func makeRangeIDUnreplicatedSpan(rangeID roachpb.RangeID) roachpb.Span {
	prefix := keys.MakeRangeIDUnreplicatedPrefix(rangeID)
	return roachpb.Span{
		Key:    prefix,
		EndKey: prefix.PrefixEnd(),
	}
}

// makeRangeLocalKeySpan returns the range local key span. Range-local keys
// are replicated keys that do not belong to the span they would naturally
// sort into. For example, /Local/Range/Table/1 would sort into [/Min,
// /System), but it actually belongs to [/Table/1, /Table/2).
func makeRangeLocalKeySpan(sp roachpb.RSpan) roachpb.Span {
	return roachpb.Span{
		Key:    keys.MakeRangeKeyPrefix(sp.Key),
		EndKey: keys.MakeRangeKeyPrefix(sp.EndKey),
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
	spans := makeReplicatedKeySpansExceptLockTable(d)
	if opts.ExcludeUserKeySpan {
		spans = makeReplicatedKeySpansExcludingUserAndLockTable(d)
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
func (ri *ReplicaMVCCDataIterator) Value() ([]byte, error) {
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
func (ri *ReplicaMVCCDataIterator) UnsafeValue() ([]byte, error) {
	return ri.it.UnsafeValue()
}

// MVCCValueLenAndIsTombstone has the same behavior as
// SimpleMVCCIterator.MVCCValueLenAndIsTombstone.
func (ri *ReplicaMVCCDataIterator) MVCCValueLenAndIsTombstone() (int, bool, error) {
	return ri.it.MVCCValueLenAndIsTombstone()
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
// over the spans returned by either makeAllKeySpans or MakeReplicatedKeySpans,
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
		spans = makeAllKeySpans(desc)
	}
	keyTypes := []storage.IterKeyType{storage.IterKeyTypePointsOnly, storage.IterKeyTypeRangesOnly}
	for _, span := range spans {
		for _, keyType := range keyTypes {
			err := func() error {
				iter := reader.NewEngineIterator(storage.IterOptions{
					KeyTypes:   keyType,
					LowerBound: span.Key,
					UpperBound: span.EndKey,
				})
				defer iter.Close()
				ok, err := iter.SeekEngineKeyGE(storage.EngineKey{Key: span.Key})
				if err == nil && ok {
					err = visitor(iter, span, keyType)
				}
				return err
			}()
			if err != nil {
				return iterutil.Map(err)
			}
		}
	}
	return nil
}

// IterateOptions instructs how points and ranges should be presented to visitor
// and if iterators should be visited in forward or reverse order.
// Reverse iterator are also positioned at the end of the range prior to being
// passed to visitor.
type IterateOptions struct {
	CombineRangesAndPoints bool
	Reverse                bool
	ExcludeUserKeySpan     bool
}

// IterateMVCCReplicaKeySpans iterates over replica's key spans in the similar
// way to IterateReplicaKeySpans, but uses MVCCIterator and gives additional
// options to create reverse iterators and to combine keys are ranges.
func IterateMVCCReplicaKeySpans(
	desc *roachpb.RangeDescriptor,
	reader storage.Reader,
	options IterateOptions,
	visitor func(storage.MVCCIterator, roachpb.Span, storage.IterKeyType) error,
) error {
	if !reader.ConsistentIterators() {
		panic("reader must provide consistent iterators")
	}
	spans := makeReplicatedKeySpansExceptLockTable(desc)
	if options.ExcludeUserKeySpan {
		spans = makeReplicatedKeySpansExcludingUserAndLockTable(desc)
	}
	if options.Reverse {
		spanMax := len(spans) - 1
		for i := 0; i < len(spans)/2; i++ {
			spans[spanMax-i], spans[i] = spans[i], spans[spanMax-i]
		}
	}
	keyTypes := []storage.IterKeyType{storage.IterKeyTypePointsOnly, storage.IterKeyTypeRangesOnly}
	if options.CombineRangesAndPoints {
		keyTypes = []storage.IterKeyType{storage.IterKeyTypePointsAndRanges}
	}
	for _, span := range spans {
		for _, keyType := range keyTypes {
			err := func() error {
				iter := reader.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
					LowerBound: span.Key,
					UpperBound: span.EndKey,
					KeyTypes:   keyType,
				})
				defer iter.Close()
				if options.Reverse {
					iter.SeekLT(storage.MakeMVCCMetadataKey(span.EndKey))
				} else {
					iter.SeekGE(storage.MakeMVCCMetadataKey(span.Key))
				}
				ok, err := iter.Valid()
				if err == nil && ok {
					err = visitor(iter, span, keyType)
				}
				return err
			}()
			if err != nil {
				return iterutil.Map(err)
			}
		}
	}
	return nil
}
