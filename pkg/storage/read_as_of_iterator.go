// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
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

	// newestRangeTombstone contains the timestamp of the latest range
	// tombstone below asOf at the current position, if any.
	newestRangeTombstone hlc.Timestamp
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

	if f.advance(true /* seeked */); f.valid && f.UnsafeKey().Less(originalKey) {
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
	if util.RaceEnabled && f.valid {
		if err := f.assertInvariants(); err != nil {
			return false, err
		}
	}
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
	f.advance(false /* seeked */)
}

// UnsafeKey returns the current key, but the memory is invalidated on the next
// call to {NextKey,Seek}.
func (f *ReadAsOfIterator) UnsafeKey() MVCCKey {
	return f.iter.UnsafeKey()
}

// UnsafeValue returns the current value as a byte slice, but the memory is
// invalidated on the next call to {NextKey,Seek}.
func (f *ReadAsOfIterator) UnsafeValue() ([]byte, error) {
	return f.iter.UnsafeValue()
}

// MVCCValueLenAndIsTombstone implements the SimpleMVCCIterator interface.
func (f *ReadAsOfIterator) MVCCValueLenAndIsTombstone() (int, bool, error) {
	return f.iter.MVCCValueLenAndIsTombstone()
}

// ValueLen implements the SimpleMVCCIterator interface.
func (f *ReadAsOfIterator) ValueLen() int {
	return f.iter.ValueLen()
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
func (f *ReadAsOfIterator) advance(seeked bool) {
	for {
		if ok := f.updateValid(); !ok {
			return
		}

		// Detect range tombstones, and step forward to the next key if on a bare
		// range key.
		if seeked || f.iter.RangeKeyChanged() {
			seeked = false
			hasPoint, hasRange := f.iter.HasPointAndRange()
			f.newestRangeTombstone = hlc.Timestamp{}
			if hasRange {
				if v, ok := f.iter.RangeKeys().FirstAtOrBelow(f.asOf); ok {
					f.newestRangeTombstone = v.Timestamp
				}
			}
			if !hasPoint {
				f.iter.Next()
				continue
			}
		}

		// Process point keys.
		if key := f.iter.UnsafeKey(); f.asOf.Less(key.Timestamp) {
			// Skip keys above the asOf timestamp.
			f.iter.Next()
		} else {
			v, err := f.iter.UnsafeValue()
			if err != nil {
				f.valid, f.err = false, err
				return
			}
			if isTombstone, err := EncodedMVCCValueIsTombstone(v); err != nil {
				f.valid, f.err = false, err
				return
			} else if isTombstone {
				// Skip to the next MVCC key if we find a point tombstone.
				f.iter.NextKey()
			} else if key.Timestamp.LessEq(f.newestRangeTombstone) {
				// The latest range key, as of system time, shadows the latest point key.
				// This key is therefore deleted as of system time.
				f.iter.NextKey()
			} else {
				// On a valid key that potentially shadows range key(s).
				return
			}
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

// assertInvariants asserts iterator invariants. The iterator must be valid.
func (f *ReadAsOfIterator) assertInvariants() error {
	// Check general SimpleMVCCIterator API invariants.
	if err := assertSimpleMVCCIteratorInvariants(f); err != nil {
		return err
	}

	// asOf must be set.
	if f.asOf.IsEmpty() {
		return errors.AssertionFailedf("f.asOf is empty")
	}

	// The underlying iterator must be valid.
	if ok, err := f.iter.Valid(); !ok || err != nil {
		errMsg := err.Error()
		return errors.AssertionFailedf("invalid underlying iter with err=%s", errMsg)
	}

	// Keys can't be intents or inline values, and must have timestamps at or
	// below the readAsOf timestamp.
	key := f.UnsafeKey()
	if key.Timestamp.IsEmpty() {
		return errors.AssertionFailedf("emitted key %s has no timestamp", key)
	}
	if f.asOf.Less(key.Timestamp) {
		return errors.AssertionFailedf("emitted key %s above asOf timestamp %s", key, f.asOf)
	}

	// Tombstones should not be emitted.
	if _, isTombstone, err := f.MVCCValueLenAndIsTombstone(); err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "invalid value")
	} else if isTombstone {
		return errors.AssertionFailedf("emitted tombstone for key %s", key)
	}

	return nil
}
