// Copyright 2023 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// WithDeprecatedAPI returns if err is non-nil. Otherwise it returns the
// provided MVCCIterator, wrapped in the DeprecatedMVCCIterator struct.
func WithDeprecatedAPI(it MVCCIterator, err error) (DeprecatedMVCCIterator, error) {
	if err != nil {
		return DeprecatedMVCCIterator{}, err
	}
	return DeprecatedMVCCIterator{it}, nil
}

// WithDeprecatedSimpleAPI returns if err is non-nil. Otherwise it returns the
// provided SimpleMVCCIterator, wrapped in the DeprecatedSimpleMVCCIterator
// struct.
func WithDeprecatedSimpleAPI(
	it SimpleMVCCIterator, err error,
) (DeprecatedSimpleMVCCIterator, error) {
	if err != nil {
		return DeprecatedSimpleMVCCIterator{}, err
	}
	return DeprecatedSimpleMVCCIterator{it}, nil
}

// DeprecatedSimpleMVCCIterator wraps a SimpleMVCCIterator with the old,
// deprecated MVCCIterator interface that does not return validity or errors
// through return values. This type is a temporary, adapter type that should be
// removed once all SimpleMVCCIterator usages are updated to use the new
// interface.
type DeprecatedSimpleMVCCIterator struct {
	SimpleMVCCIterator
}

// Close frees up resources held by the iterator.
func (d DeprecatedSimpleMVCCIterator) Close() {
	d.SimpleMVCCIterator.Close()
}

// SeekGE advances the iterator to the first key in the engine which is >= the
// provided key. This may be in the middle of a bare range key straddling the
// seek key.
func (d DeprecatedSimpleMVCCIterator) SeekGE(key MVCCKey) {
	_, _ = d.SimpleMVCCIterator.SeekGE(key)
}

// Valid must be called after any call to Seek(), Next(), Prev(), or
// similar methods. It returns (true, nil) if the iterator points to
// a valid key (it is undefined to call Key(), Value(), or similar
// methods unless Valid() has returned (true, nil)). It returns
// (false, nil) if the iterator has moved past the end of the valid
// range, or (false, err) if an error has occurred. Valid() will
// never return true with a non-nil error.
func (d DeprecatedSimpleMVCCIterator) Valid() (bool, error) {
	return d.SimpleMVCCIterator.Valid()
}

// Next advances the iterator to the next key in the iteration. After this
// call, Valid() will be true if the iterator was not positioned at the last
// key.
func (d DeprecatedSimpleMVCCIterator) Next() {
	_, _ = d.SimpleMVCCIterator.Next()
}

// NextKey advances the iterator to the next MVCC key. This operation is
// distinct from Next which advances to the next version of the current key
// or the next key if the iterator is currently located at the last version
// for a key. NextKey must not be used to switch iteration direction from
// reverse iteration to forward iteration.
//
// If NextKey() lands on a bare range key, it is possible that there exists a
// versioned point key at the start key too. Calling NextKey() again would
// skip over this point key, since the start key was already emitted. If the
// caller wants to see it, it must call Next() to check for it. Note that
// this is not the case with intents: they don't have a timestamp, so the
// encoded key is identical to the range key's start bound, and they will
// be emitted together at that position.
func (d DeprecatedSimpleMVCCIterator) NextKey() {
	_, _ = d.SimpleMVCCIterator.NextKey()
}

// UnsafeKey returns the current key position. This may be a point key, or
// the current position inside a range key (typically the start key
// or the seek key when using SeekGE within its bounds).
//
// The memory is invalidated on the next call to {Next,NextKey,Prev,SeekGE,
// SeekLT,Close}. Use Key() if this is undesirable.
func (d DeprecatedSimpleMVCCIterator) UnsafeKey() MVCCKey {
	return d.SimpleMVCCIterator.UnsafeKey()
}

// UnsafeValue returns the current point key value as a byte slice.
// This must only be called when it is known that the iterator is positioned
// at a point value, i.e. HasPointAndRange has returned (true, *). If
// possible, use MVCCValueLenAndIsTombstone() instead.
//
// The memory is invalidated on the next call to {Next,NextKey,Prev,SeekGE,SeekLT,Close}.
// Use Value() if that is undesirable.
func (d DeprecatedSimpleMVCCIterator) UnsafeValue() ([]byte, error) {
	return d.SimpleMVCCIterator.UnsafeValue()
}

// MVCCValueLenAndIsTombstone should be called only for MVCC (i.e.,
// UnsafeKey().IsValue()) point values, when the actual point value is not
// needed, for example when updating stats and making GC decisions, and it
// is sufficient for the caller to know the length (len(UnsafeValue()), and
// whether the underlying MVCCValue is a tombstone
// (MVCCValue.IsTombstone()). This is an optimization that can allow the
// underlying storage layer to avoid retrieving the value.
// REQUIRES: HasPointAndRange() has returned (true, *).
func (d DeprecatedSimpleMVCCIterator) MVCCValueLenAndIsTombstone() (int, bool, error) {
	return d.SimpleMVCCIterator.MVCCValueLenAndIsTombstone()
}

// ValueLen can be called for MVCC or non-MVCC values, when only the value
// length is needed. This is an optimization that can allow the underlying
// storage layer to avoid retrieving the value.
// REQUIRES: HasPointAndRange() has returned (true, *).
func (d DeprecatedSimpleMVCCIterator) ValueLen() int {
	return d.SimpleMVCCIterator.ValueLen()
}

// HasPointAndRange returns whether the current iterator position has a point
// key and/or a range key. Must check Valid() first. At least one of these
// will always be true for a valid iterator. For details on range keys, see
// comment on SimpleMVCCIterator.
func (d DeprecatedSimpleMVCCIterator) HasPointAndRange() (bool, bool) {
	return d.SimpleMVCCIterator.HasPointAndRange()
}

// RangeBounds returns the range bounds for the current range key, or an
// empty span if there are none. The returned keys are valid until the
// range key changes, see RangeKeyChanged().
func (d DeprecatedSimpleMVCCIterator) RangeBounds() roachpb.Span {
	return d.SimpleMVCCIterator.RangeBounds()
}

// RangeKeys returns a stack of all range keys (with different timestamps) at
// the current key position. When at a point key, it will return all range
// keys overlapping that point key. The stack is valid until the range key
// changes, see RangeKeyChanged().
//
// For details on range keys, see SimpleMVCCIterator comment, or tech note:
// https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/mvcc-range-tombstones.md
func (d DeprecatedSimpleMVCCIterator) RangeKeys() MVCCRangeKeyStack {
	return d.SimpleMVCCIterator.RangeKeys()
}

// RangeKeyChanged returns true if the previous seek or step moved to a
// different range key (or none at all). Requires a valid iterator, but an
// exhausted iterator is considered to have had no range keys when calling
// this after repositioning.
func (d DeprecatedSimpleMVCCIterator) RangeKeyChanged() bool {
	return d.SimpleMVCCIterator.RangeKeyChanged()
}

// DeprecatedMVCCIterator wraps a MVCCIterator with the old, deprecated
// MVCCIterator interface that does not return validity or errors through return
// values. This type is a temporary, adapter type that should be removed once
// all MVCCIterator usages are updated to use the new interface.
type DeprecatedMVCCIterator struct {
	MVCCIterator
}

// Close frees up resources held by the iterator.
func (d DeprecatedMVCCIterator) Close() {
	d.MVCCIterator.Close()
}

// SeekGE advances the iterator to the first key in the engine which is >= the
// provided key. This may be in the middle of a bare range key straddling the
// seek key.
func (d DeprecatedMVCCIterator) SeekGE(key MVCCKey) {
	_, _ = d.MVCCIterator.SeekGE(key)
}

// SeekLT advances the iterator to the first key in the engine which is < the
// provided key. Unlike SeekGE, when calling SeekLT within range key bounds
// this will not land on the seek key, but rather on the closest point key
// overlapping the range key or the range key's start bound.
func (d DeprecatedMVCCIterator) SeekLT(key MVCCKey) {
	_, _ = d.MVCCIterator.SeekLT(key)
}

// Valid must be called after any call to Seek(), Next(), Prev(), or
// similar methods. It returns (true, nil) if the iterator points to
// a valid key (it is undefined to call Key(), Value(), or similar
// methods unless Valid() has returned (true, nil)). It returns
// (false, nil) if the iterator has moved past the end of the valid
// range, or (false, err) if an error has occurred. Valid() will
// never return true with a non-nil error.
func (d DeprecatedMVCCIterator) Valid() (bool, error) {
	return d.MVCCIterator.Valid()
}

// Next advances the iterator to the next key in the iteration. After this
// call, Valid() will be true if the iterator was not positioned at the last
// key.
func (d DeprecatedMVCCIterator) Next() {
	_, _ = d.MVCCIterator.Next()
}

// NextKey advances the iterator to the next MVCC key. This operation is
// distinct from Next which advances to the next version of the current key
// or the next key if the iterator is currently located at the last version
// for a key. NextKey must not be used to switch iteration direction from
// reverse iteration to forward iteration.
//
// If NextKey() lands on a bare range key, it is possible that there exists a
// versioned point key at the start key too. Calling NextKey() again would
// skip over this point key, since the start key was already emitted. If the
// caller wants to see it, it must call Next() to check for it. Note that
// this is not the case with intents: they don't have a timestamp, so the
// encoded key is identical to the range key's start bound, and they will
// be emitted together at that position.
func (d DeprecatedMVCCIterator) NextKey() {
	_, _ = d.MVCCIterator.NextKey()
}

// Prev moves the iterator backward to the previous key in the iteration.
// After this call, Valid() will be true if the iterator was not positioned at
// the first key.
func (d DeprecatedMVCCIterator) Prev() {
	_, _ = d.MVCCIterator.Prev()
}

// SeekIntentGE is a specialized version of SeekGE(MVCCKey{Key: key}), when
// the caller expects to find an intent, and additionally has the txnUUID
// for the intent it is looking for. When running with separated intents,
// this can optimize the behavior of the underlying Engine for write heavy
// keys by avoiding the need to iterate over many deleted intents.
func (d DeprecatedMVCCIterator) SeekIntentGE(key roachpb.Key, txnUUID uuid.UUID) {
	_, _ = d.MVCCIterator.SeekIntentGE(key, txnUUID)
}

// UnsafeKey returns the current key position. This may be a point key, or
// the current position inside a range key (typically the start key
// or the seek key when using SeekGE within its bounds).
//
// The memory is invalidated on the next call to {Next,NextKey,Prev,SeekGE,
// SeekLT,Close}. Use Key() if this is undesirable.
func (d DeprecatedMVCCIterator) UnsafeKey() MVCCKey {
	return d.MVCCIterator.UnsafeKey()
}

// UnsafeValue returns the current point key value as a byte slice.
// This must only be called when it is known that the iterator is positioned
// at a point value, i.e. HasPointAndRange has returned (true, *). If
// possible, use MVCCValueLenAndIsTombstone() instead.
//
// The memory is invalidated on the next call to {Next,NextKey,Prev,SeekGE,SeekLT,Close}.
// Use Value() if that is undesirable.
func (d DeprecatedMVCCIterator) UnsafeValue() ([]byte, error) {
	return d.MVCCIterator.UnsafeValue()
}

// MVCCValueLenAndIsTombstone should be called only for MVCC (i.e.,
// UnsafeKey().IsValue()) point values, when the actual point value is not
// needed, for example when updating stats and making GC decisions, and it
// is sufficient for the caller to know the length (len(UnsafeValue()), and
// whether the underlying MVCCValue is a tombstone
// (MVCCValue.IsTombstone()). This is an optimization that can allow the
// underlying storage layer to avoid retrieving the value.
// REQUIRES: HasPointAndRange() has returned (true, *).
func (d DeprecatedMVCCIterator) MVCCValueLenAndIsTombstone() (int, bool, error) {
	return d.MVCCIterator.MVCCValueLenAndIsTombstone()
}

// ValueLen can be called for MVCC or non-MVCC values, when only the value
// length is needed. This is an optimization that can allow the underlying
// storage layer to avoid retrieving the value.
// REQUIRES: HasPointAndRange() has returned (true, *).
func (d DeprecatedMVCCIterator) ValueLen() int {
	return d.MVCCIterator.ValueLen()
}

// HasPointAndRange returns whether the current iterator position has a point
// key and/or a range key. Must check Valid() first. At least one of these
// will always be true for a valid iterator. For details on range keys, see
// comment on SimpleMVCCIterator.
func (d DeprecatedMVCCIterator) HasPointAndRange() (bool, bool) {
	return d.MVCCIterator.HasPointAndRange()
}

// RangeBounds returns the range bounds for the current range key, or an
// empty span if there are none. The returned keys are valid until the
// range key changes, see RangeKeyChanged().
func (d DeprecatedMVCCIterator) RangeBounds() roachpb.Span {
	return d.MVCCIterator.RangeBounds()
}

// RangeKeys returns a stack of all range keys (with different timestamps) at
// the current key position. When at a point key, it will return all range
// keys overlapping that point key. The stack is valid until the range key
// changes, see RangeKeyChanged().
//
// For details on range keys, see SimpleMVCCIterator comment, or tech note:
// https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/mvcc-range-tombstones.md
func (d DeprecatedMVCCIterator) RangeKeys() MVCCRangeKeyStack {
	return d.MVCCIterator.RangeKeys()
}

// RangeKeyChanged returns true if the previous seek or step moved to a
// different range key (or none at all). Requires a valid iterator, but an
// exhausted iterator is considered to have had no range keys when calling
// this after repositioning.
func (d DeprecatedMVCCIterator) RangeKeyChanged() bool {
	return d.MVCCIterator.RangeKeyChanged()
}
