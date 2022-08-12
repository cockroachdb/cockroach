// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// ReadAsOfIterator wraps a SimpleMVCCIterator and only surfaces the latest
// valid point key of a given MVCC key that is also below the asOf timestamp, if
// set. Further, the iterator does not surface point or range tombstones, nor
// any MVCC keys shadowed by tombstones below the asOf timestamp, if set. The
// iterator assumes that it will not encounter any write intents.
type ReadAsOfIterator struct {
	iter SimpleMVCCIterator

	// asOf is the latest timestamp of a key surfaced by the iterator.
	asOf hlc.Timestamp

	// valid tracks if the current key is valid
	valid bool

	// err tracks if iterating to the current key returned an error
	err error
}

var _ SimpleMVCCIterator = &ReadAsOfIterator{}

// Close closes the underlying iterator.
func (f *ReadAsOfIterator) Close() {
	f.iter.Close()
}

// SeekGE advances the iterator to the first key in the engine which is >= the
// provided key that obeys the ReadAsOfIterator key constraints.
func (f *ReadAsOfIterator) SeekGE(originalKey MVCCKey) {
	// To ensure SeekGE seeks to a key that isn't shadowed by a tombstone that the
	// ReadAsOfIterator would have skipped (i.e. a tombstone below asOf), seek to
	// the key with the latest possible timestamp that the iterator could surface
	// (i.e. asOf, if set) and iterate to the next valid key at or below the caller's
	// key that also obeys the iterator's constraints.
	synthetic := MVCCKey{Key: originalKey.Key, Timestamp: f.asOf}
	f.iter.SeekGE(synthetic)

	if f.advance(); f.valid && f.UnsafeKey().Less(originalKey) {
		// The following is true:
		// originalKey.Key == f.UnsafeKey &&
		// f.asOf timestamp (if set) >= current timestamp > originalKey timestamp.
		//
		// This implies the caller is seeking to a key that is shadowed by a valid
		// key that obeys the iterator 's constraints. The caller's key is NOT the
		// latest key of the given MVCC key; therefore, skip to the next MVCC key.
		f.NextKey()
	}
}

// Valid implements the simpleMVCCIterator.
func (f *ReadAsOfIterator) Valid() (bool, error) {
	return f.valid, f.err
}

// Next advances the iterator to the next valid MVCC key obeying the iterator's
// constraints. Note that Next and NextKey have the same implementation because
// the iterator only surfaces the latest valid key of a given MVCC key below the
// asOf timestamp.
func (f *ReadAsOfIterator) Next() {
	f.NextKey()
}

// NextKey advances the iterator to the next valid MVCC key obeying the
// iterator's constraints. NextKey() is only guaranteed to surface a key that
// obeys the iterator's constraints if the iterator was already on a key that
// obeys the constraints. To ensure this, initialize the iterator with a SeekGE
// call before any calls to NextKey().
func (f *ReadAsOfIterator) NextKey() {
	f.iter.NextKey()
	f.advance()
}

// UnsafeKey returns the current key, but the memory is invalidated on the next
// call to {NextKey,Seek}.
func (f *ReadAsOfIterator) UnsafeKey() MVCCKey {
	return f.iter.UnsafeKey()
}

// UnsafeValue returns the current value as a byte slice, but the memory is
// invalidated on the next call to {NextKey,Seek}.
func (f *ReadAsOfIterator) UnsafeValue() []byte {
	return f.iter.UnsafeValue()
}

// HasPointAndRange implements SimpleMVCCIterator.
func (f *ReadAsOfIterator) HasPointAndRange() (bool, bool) {
	return true, false
}

// RangeBounds always returns an empty span, since the iterator never surfaces
// rangekeys.
func (f *ReadAsOfIterator) RangeBounds() roachpb.Span {
	return roachpb.Span{}
}

// RangeKeys is always empty since this iterator never surfaces rangeKeys.
func (f *ReadAsOfIterator) RangeKeys() MVCCRangeKeyStack {
	return MVCCRangeKeyStack{}
}

// RangeKeyChanged implements SimpleMVCCIterator.
func (f *ReadAsOfIterator) RangeKeyChanged() bool {
	return false
}

// updateValid updates i.valid and i.err based on the underlying iterator, and
// returns true if valid.
func (f *ReadAsOfIterator) updateValid() bool {
	f.valid, f.err = f.iter.Valid()
	return f.valid
}

// advance moves past keys with timestamps later than f.asOf and skips MVCC keys
// whose latest value (subject to f.asOF) has been deleted by a point or range
// tombstone.
func (f *ReadAsOfIterator) advance() {
	for {
		if ok := f.updateValid(); !ok {
			return
		}

		key := f.iter.UnsafeKey()

		if f.asOf.Less(key.Timestamp) {
			// Skip keys above the asOf timestamp, regardless of type of key (e.g. point or range)
			f.iter.Next()
		} else if hasPoint, hasRange := f.iter.HasPointAndRange(); !hasPoint && hasRange {
			// Bare range keys get surfaced before the point key, even though the
			// point key shadows it; thus, because we can infer range key information
			// when the point key surfaces, skip over the bare range key, and reason
			// about shadowed keys at the surfaced point key.
			//
			// E.g. Scanning the keys below:
			//  2  a2
			//  1  o---o
			//     a   b
			//
			//  would result in two surfaced keys:
			//   {a-b}@1;
			//   a2, {a-b}@1

			f.iter.Next()
		} else if len(f.iter.UnsafeValue()) == 0 {
			// Skip to the next MVCC key if we find a point tombstone.
			f.iter.NextKey()
		} else if !hasRange {
			// On a valid key without a range key
			return
			// TODO (msbutler): ensure this caches range key values (#84379) before
			// the 22.2 branch cut, else we face a steep perf cliff for RESTORE with
			// range keys.
		} else if f.iter.RangeKeys().HasBetween(key.Timestamp, f.asOf) {
			// The latest range key, as of system time, shadows the latest point key.
			// This key is therefore deleted as of system time.
			f.iter.NextKey()
		} else {
			// On a valid key that potentially shadows range key(s).
			return
		}
	}
}

// NewReadAsOfIterator constructs a ReadAsOfIterator. If asOf is not set, the
// iterator reads the most recent data.
func NewReadAsOfIterator(iter SimpleMVCCIterator, asOf hlc.Timestamp) *ReadAsOfIterator {
	if asOf.IsEmpty() {
		asOf = hlc.MaxTimestamp
	}
	return &ReadAsOfIterator{iter: iter, asOf: asOf}
}
