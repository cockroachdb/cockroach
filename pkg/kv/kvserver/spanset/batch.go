// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanset

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/rangekey"
)

// MVCCIterator wraps an storage.MVCCIterator and ensures that it can
// only be used to access spans in a SpanSet.
type MVCCIterator struct {
	i     storage.MVCCIterator
	spans *SpanSet

	// spansOnly controls whether or not timestamps associated with the
	// spans are considered when ensuring access. If set to true,
	// only span boundaries are checked.
	spansOnly bool

	// Timestamp the access is taking place. If timestamp is zero, access is
	// considered non-MVCC. If spansOnly is set to true, ts is not consulted.
	ts hlc.Timestamp

	// Seeking to an invalid key puts the iterator in an error state.
	err error
	// Reaching an out-of-bounds key with Next/Prev invalidates the
	// iterator but does not set err.
	invalid bool
}

var _ storage.MVCCIterator = &MVCCIterator{}

// NewIterator constructs an iterator that verifies access of the underlying
// iterator against the given SpanSet. Timestamps associated with the spans
// in the spanset are not considered, only the span boundaries are checked.
func NewIterator(iter storage.MVCCIterator, spans *SpanSet) *MVCCIterator {
	return &MVCCIterator{i: iter, spans: spans, spansOnly: true}
}

// NewIteratorAt constructs an iterator that verifies access of the underlying
// iterator against the given SpanSet at the given timestamp.
func NewIteratorAt(iter storage.MVCCIterator, spans *SpanSet, ts hlc.Timestamp) *MVCCIterator {
	return &MVCCIterator{i: iter, spans: spans, ts: ts}
}

// Close is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) Close() {
	i.i.Close()
}

// Valid is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) Valid() (bool, error) {
	if i.err != nil {
		return false, i.err
	}
	ok, err := i.i.Valid()
	if err != nil {
		return false, err
	}
	return ok && !i.invalid, nil
}

// SeekGE is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) SeekGE(key storage.MVCCKey) {
	i.i.SeekGE(key)
	i.checkAllowed(roachpb.Span{Key: key.Key}, true)
}

// SeekLT is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) SeekLT(key storage.MVCCKey) {
	i.i.SeekLT(key)
	// CheckAllowed{At} supports the span representation of [,key), which
	// corresponds to the span [key.Prev(),).
	i.checkAllowed(roachpb.Span{EndKey: key.Key}, true)
}

// Next is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) Next() {
	i.i.Next()
	i.checkAllowedCurrPosForward(false)
}

// Prev is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) Prev() {
	i.i.Prev()
	i.checkAllowedCurrPosForward(false)
}

// NextKey is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) NextKey() {
	i.i.NextKey()
	i.checkAllowedCurrPosForward(false)
}

// checkAllowedCurrPosForward checks the span starting at the current iterator
// position, if the current iterator position is valid.
func (i *MVCCIterator) checkAllowedCurrPosForward(errIfDisallowed bool) {
	i.invalid = false
	i.err = nil
	if ok, _ := i.i.Valid(); !ok {
		// If the iterator is invalid after the operation, there's nothing to
		// check. We allow uses of iterators to exceed the declared span bounds
		// as long as the iterator itself is configured with proper boundaries.
		return
	}
	i.checkAllowedValidPos(roachpb.Span{Key: i.UnsafeKey().Key}, errIfDisallowed)
}

// checkAllowed checks the provided span if the current iterator position is
// valid.
func (i *MVCCIterator) checkAllowed(span roachpb.Span, errIfDisallowed bool) {
	i.invalid = false
	i.err = nil
	if ok, _ := i.i.Valid(); !ok {
		// If the iterator is invalid after the operation, there's nothing to
		// check. We allow uses of iterators to exceed the declared span bounds
		// as long as the iterator itself is configured with proper boundaries.
		return
	}
	i.checkAllowedValidPos(span, errIfDisallowed)
}

func (i *MVCCIterator) checkAllowedValidPos(span roachpb.Span, errIfDisallowed bool) {
	var err error
	if i.spansOnly {
		err = i.spans.CheckAllowed(SpanReadOnly, span)
	} else {
		err = i.spans.CheckAllowedAt(SpanReadOnly, span, i.ts)
	}
	if errIfDisallowed {
		i.err = err
	} else {
		i.invalid = err != nil
	}
}

// Value is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) Value() ([]byte, error) {
	return i.i.Value()
}

// ValueProto is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) ValueProto(msg protoutil.Message) error {
	return i.i.ValueProto(msg)
}

// UnsafeKey is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) UnsafeKey() storage.MVCCKey {
	return i.i.UnsafeKey()
}

// UnsafeRawKey is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) UnsafeRawKey() []byte {
	return i.i.UnsafeRawKey()
}

// UnsafeRawMVCCKey is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) UnsafeRawMVCCKey() []byte {
	return i.i.UnsafeRawMVCCKey()
}

// UnsafeValue is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) UnsafeValue() ([]byte, error) {
	return i.i.UnsafeValue()
}

// MVCCValueLenAndIsTombstone implements the MVCCIterator interface.
func (i *MVCCIterator) MVCCValueLenAndIsTombstone() (int, bool, error) {
	return i.i.MVCCValueLenAndIsTombstone()
}

// ValueLen implements the MVCCIterator interface.
func (i *MVCCIterator) ValueLen() int {
	return i.i.ValueLen()
}

// HasPointAndRange implements SimpleMVCCIterator.
func (i *MVCCIterator) HasPointAndRange() (bool, bool) {
	return i.i.HasPointAndRange()
}

// RangeBounds implements SimpleMVCCIterator.
func (i *MVCCIterator) RangeBounds() roachpb.Span {
	return i.i.RangeBounds()
}

// RangeKeys implements SimpleMVCCIterator.
func (i *MVCCIterator) RangeKeys() storage.MVCCRangeKeyStack {
	return i.i.RangeKeys()
}

// RangeKeyChanged implements SimpleMVCCIterator.
func (i *MVCCIterator) RangeKeyChanged() bool {
	return i.i.RangeKeyChanged()
}

// FindSplitKey is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) FindSplitKey(
	start, end, minSplitKey roachpb.Key, targetSize int64,
) (storage.MVCCKey, error) {
	if i.spansOnly {
		if err := i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}); err != nil {
			return storage.MVCCKey{}, err
		}
	} else {
		if err := i.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}, i.ts); err != nil {
			return storage.MVCCKey{}, err
		}
	}
	return i.i.FindSplitKey(start, end, minSplitKey, targetSize)
}

// Stats is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) Stats() storage.IteratorStats {
	return i.i.Stats()
}

// IsPrefix is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) IsPrefix() bool {
	return i.i.IsPrefix()
}

// UnsafeLazyValue is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) UnsafeLazyValue() pebble.LazyValue {
	return i.i.UnsafeLazyValue()
}

// EngineIterator wraps a storage.EngineIterator and ensures that it can
// only be used to access spans in a SpanSet.
type EngineIterator struct {
	i         storage.EngineIterator
	spans     *SpanSet
	spansOnly bool
	ts        hlc.Timestamp
}

// Close is part of the storage.EngineIterator interface.
func (i *EngineIterator) Close() {
	i.i.Close()
}

// SeekEngineKeyGE is part of the storage.EngineIterator interface.
func (i *EngineIterator) SeekEngineKeyGE(key storage.EngineKey) (valid bool, err error) {
	valid, err = i.i.SeekEngineKeyGE(key)
	if !valid {
		return valid, err
	}
	if key.IsMVCCKey() && !i.spansOnly {
		mvccKey, _ := key.ToMVCCKey()
		if err := i.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: mvccKey.Key}, i.ts); err != nil {
			return false, err
		}
	} else if err = i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key.Key}); err != nil {
		return false, err
	}
	return valid, err
}

// SeekEngineKeyLT is part of the storage.EngineIterator interface.
func (i *EngineIterator) SeekEngineKeyLT(key storage.EngineKey) (valid bool, err error) {
	valid, err = i.i.SeekEngineKeyLT(key)
	if !valid {
		return valid, err
	}
	if key.IsMVCCKey() && !i.spansOnly {
		mvccKey, _ := key.ToMVCCKey()
		if err := i.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: mvccKey.Key}, i.ts); err != nil {
			return false, err
		}
	} else if err = i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{EndKey: key.Key}); err != nil {
		return false, err
	}
	return valid, err
}

// NextEngineKey is part of the storage.EngineIterator interface.
func (i *EngineIterator) NextEngineKey() (valid bool, err error) {
	valid, err = i.i.NextEngineKey()
	if !valid {
		return valid, err
	}
	return i.checkKeyAllowed()
}

// PrevEngineKey is part of the storage.EngineIterator interface.
func (i *EngineIterator) PrevEngineKey() (valid bool, err error) {
	valid, err = i.i.PrevEngineKey()
	if !valid {
		return valid, err
	}
	return i.checkKeyAllowed()
}

// SeekEngineKeyGEWithLimit is part of the storage.EngineIterator interface.
func (i *EngineIterator) SeekEngineKeyGEWithLimit(
	key storage.EngineKey, limit roachpb.Key,
) (state pebble.IterValidityState, err error) {
	state, err = i.i.SeekEngineKeyGEWithLimit(key, limit)
	if state != pebble.IterValid {
		return state, err
	}
	if err = i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key.Key}); err != nil {
		return pebble.IterExhausted, err
	}
	return state, err
}

// SeekEngineKeyLTWithLimit is part of the storage.EngineIterator interface.
func (i *EngineIterator) SeekEngineKeyLTWithLimit(
	key storage.EngineKey, limit roachpb.Key,
) (state pebble.IterValidityState, err error) {
	state, err = i.i.SeekEngineKeyLTWithLimit(key, limit)
	if state != pebble.IterValid {
		return state, err
	}
	if err = i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{EndKey: key.Key}); err != nil {
		return pebble.IterExhausted, err
	}
	return state, err
}

// NextEngineKeyWithLimit is part of the storage.EngineIterator interface.
func (i *EngineIterator) NextEngineKeyWithLimit(
	limit roachpb.Key,
) (state pebble.IterValidityState, err error) {
	return i.i.NextEngineKeyWithLimit(limit)
}

// PrevEngineKeyWithLimit is part of the storage.EngineIterator interface.
func (i *EngineIterator) PrevEngineKeyWithLimit(
	limit roachpb.Key,
) (state pebble.IterValidityState, err error) {
	return i.i.PrevEngineKeyWithLimit(limit)
}

func (i *EngineIterator) checkKeyAllowed() (valid bool, err error) {
	key, err := i.i.UnsafeEngineKey()
	if err != nil {
		return false, err
	}
	if key.IsMVCCKey() && !i.spansOnly {
		mvccKey, _ := key.ToMVCCKey()
		if err := i.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: mvccKey.Key}, i.ts); err != nil {
			// Invalid, but no error.
			return false, nil // nolint:returnerrcheck
		}
	} else if err = i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key.Key}); err != nil {
		// Invalid, but no error.
		return false, nil // nolint:returnerrcheck
	}
	return true, nil
}

// HasPointAndRange is part of the storage.EngineIterator interface.
func (i *EngineIterator) HasPointAndRange() (bool, bool) {
	return i.i.HasPointAndRange()
}

// EngineRangeBounds is part of the storage.EngineIterator interface.
func (i *EngineIterator) EngineRangeBounds() (roachpb.Span, error) {
	return i.i.EngineRangeBounds()
}

// EngineRangeKeys is part of the storage.EngineIterator interface.
func (i *EngineIterator) EngineRangeKeys() []storage.EngineRangeKeyValue {
	return i.i.EngineRangeKeys()
}

// RangeKeyChanged is part of the storage.EngineIterator interface.
func (i *EngineIterator) RangeKeyChanged() bool {
	return i.i.RangeKeyChanged()
}

// UnsafeEngineKey is part of the storage.EngineIterator interface.
func (i *EngineIterator) UnsafeEngineKey() (storage.EngineKey, error) {
	return i.i.UnsafeEngineKey()
}

// EngineKey is part of the storage.EngineIterator interface.
func (i *EngineIterator) EngineKey() (storage.EngineKey, error) {
	return i.i.EngineKey()
}

// UnsafeRawEngineKey is part of the storage.EngineIterator interface.
func (i *EngineIterator) UnsafeRawEngineKey() []byte {
	return i.i.UnsafeRawEngineKey()
}

// UnsafeValue is part of the storage.EngineIterator interface.
func (i *EngineIterator) UnsafeValue() ([]byte, error) {
	return i.i.UnsafeValue()
}

// UnsafeLazyValue is part of the storage.EngineIterator interface.
func (i *EngineIterator) UnsafeLazyValue() pebble.LazyValue {
	return i.i.UnsafeLazyValue()
}

// Value is part of the storage.EngineIterator interface.
func (i *EngineIterator) Value() ([]byte, error) {
	return i.i.Value()
}

// ValueLen is part of the storage.EngineIterator interface.
func (i *EngineIterator) ValueLen() int {
	return i.i.ValueLen()
}

// CloneContext is part of the storage.EngineIterator interface.
func (i *EngineIterator) CloneContext() storage.CloneContext {
	return i.i.CloneContext()
}

// Stats is part of the storage.EngineIterator interface.
func (i *EngineIterator) Stats() storage.IteratorStats {
	return i.i.Stats()
}

type spanSetReader struct {
	r     storage.Reader
	spans *SpanSet

	spansOnly bool
	ts        hlc.Timestamp
}

var _ storage.Reader = spanSetReader{}

func (s spanSetReader) ScanInternal(
	ctx context.Context,
	lower, upper roachpb.Key,
	visitPointKey func(key *pebble.InternalKey, value pebble.LazyValue, info pebble.IteratorLevel) error,
	visitRangeDel func(start []byte, end []byte, seqNum pebble.SeqNum) error,
	visitRangeKey func(start []byte, end []byte, keys []rangekey.Key) error,
	visitSharedFile func(sst *pebble.SharedSSTMeta) error,
	visitExternalFile func(sst *pebble.ExternalFile) error,
) error {
	return s.r.ScanInternal(ctx, lower, upper, visitPointKey, visitRangeDel, visitRangeKey, visitSharedFile, visitExternalFile)
}

func (s spanSetReader) Close() {
	s.r.Close()
}

func (s spanSetReader) Closed() bool {
	return s.r.Closed()
}

func (s spanSetReader) MVCCIterate(
	ctx context.Context,
	start, end roachpb.Key,
	iterKind storage.MVCCIterKind,
	keyTypes storage.IterKeyType,
	readCategory fs.ReadCategory,
	f func(storage.MVCCKeyValue, storage.MVCCRangeKeyStack) error,
) error {
	if s.spansOnly {
		if err := s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}); err != nil {
			return err
		}
	} else {
		if err := s.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}, s.ts); err != nil {
			return err
		}
	}
	return s.r.MVCCIterate(ctx, start, end, iterKind, keyTypes, readCategory, f)
}

func (s spanSetReader) NewMVCCIterator(
	ctx context.Context, iterKind storage.MVCCIterKind, opts storage.IterOptions,
) (storage.MVCCIterator, error) {
	mvccIter, err := s.r.NewMVCCIterator(ctx, iterKind, opts)
	if err != nil {
		return nil, err
	}
	if s.spansOnly {
		return NewIterator(mvccIter, s.spans), nil
	}
	return NewIteratorAt(mvccIter, s.spans, s.ts), nil
}

func (s spanSetReader) NewEngineIterator(
	ctx context.Context, opts storage.IterOptions,
) (storage.EngineIterator, error) {
	engineIter, err := s.r.NewEngineIterator(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &EngineIterator{
		i:         engineIter,
		spans:     s.spans,
		spansOnly: s.spansOnly,
		ts:        s.ts,
	}, nil
}

// ConsistentIterators implements the storage.Reader interface.
func (s spanSetReader) ConsistentIterators() bool {
	return s.r.ConsistentIterators()
}

// PinEngineStateForIterators implements the storage.Reader interface.
func (s spanSetReader) PinEngineStateForIterators(readCategory fs.ReadCategory) error {
	return s.r.PinEngineStateForIterators(readCategory)
}

type spanSetWriter struct {
	w     storage.Writer
	spans *SpanSet

	spansOnly bool
	ts        hlc.Timestamp
}

var _ storage.Writer = spanSetWriter{}

func (s spanSetWriter) ApplyBatchRepr(repr []byte, sync bool) error {
	// Assume that the constructor of the batch has bounded it correctly.
	return s.w.ApplyBatchRepr(repr, sync)
}

func (s spanSetWriter) checkAllowed(key roachpb.Key) error {
	if s.spansOnly {
		if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: key}); err != nil {
			return err
		}
	} else {
		if err := s.spans.CheckAllowedAt(SpanReadWrite, roachpb.Span{Key: key}, s.ts); err != nil {
			return err
		}
	}
	return nil
}

func (s spanSetWriter) ClearMVCC(key storage.MVCCKey, opts storage.ClearOptions) error {
	if err := s.checkAllowed(key.Key); err != nil {
		return err
	}
	return s.w.ClearMVCC(key, opts)
}

func (s spanSetWriter) ClearUnversioned(key roachpb.Key, opts storage.ClearOptions) error {
	if err := s.checkAllowed(key); err != nil {
		return err
	}
	return s.w.ClearUnversioned(key, opts)
}

func (s spanSetWriter) ClearEngineKey(key storage.EngineKey, opts storage.ClearOptions) error {
	if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
		return err
	}
	return s.w.ClearEngineKey(key, opts)
}

func (s spanSetWriter) SingleClearEngineKey(key storage.EngineKey) error {
	// Pass-through, since single clear is only used for the lock table, which
	// is not in the spans.
	return s.w.SingleClearEngineKey(key)
}

func (s spanSetWriter) checkAllowedRange(start, end roachpb.Key) error {
	if s.spansOnly {
		if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: start, EndKey: end}); err != nil {
			return err
		}
	} else {
		if err := s.spans.CheckAllowedAt(SpanReadWrite, roachpb.Span{Key: start, EndKey: end}, s.ts); err != nil {
			return err
		}
	}
	return nil
}

func (s spanSetWriter) ClearRawRange(start, end roachpb.Key, pointKeys, rangeKeys bool) error {
	if err := s.checkAllowedRange(start, end); err != nil {
		return err
	}
	return s.w.ClearRawRange(start, end, pointKeys, rangeKeys)
}

func (s spanSetWriter) ClearMVCCRange(start, end roachpb.Key, pointKeys, rangeKeys bool) error {
	if err := s.checkAllowedRange(start, end); err != nil {
		return err
	}
	return s.w.ClearMVCCRange(start, end, pointKeys, rangeKeys)
}

func (s spanSetWriter) ClearMVCCVersions(start, end storage.MVCCKey) error {
	if err := s.checkAllowedRange(start.Key, end.Key); err != nil {
		return err
	}
	return s.w.ClearMVCCVersions(start, end)
}

func (s spanSetWriter) ClearMVCCIteratorRange(
	start, end roachpb.Key, pointKeys, rangeKeys bool,
) error {
	if err := s.checkAllowedRange(start, end); err != nil {
		return err
	}
	return s.w.ClearMVCCIteratorRange(start, end, pointKeys, rangeKeys)
}

func (s spanSetWriter) PutMVCCRangeKey(
	rangeKey storage.MVCCRangeKey, value storage.MVCCValue,
) error {
	if err := s.checkAllowedRange(rangeKey.StartKey, rangeKey.EndKey); err != nil {
		return err
	}
	return s.w.PutMVCCRangeKey(rangeKey, value)
}

func (s spanSetWriter) PutRawMVCCRangeKey(rangeKey storage.MVCCRangeKey, value []byte) error {
	if err := s.checkAllowedRange(rangeKey.StartKey, rangeKey.EndKey); err != nil {
		return err
	}
	return s.w.PutRawMVCCRangeKey(rangeKey, value)
}

func (s spanSetWriter) PutEngineRangeKey(start, end roachpb.Key, suffix, value []byte) error {
	if !s.spansOnly {
		panic("cannot do timestamp checking for PutEngineRangeKey")
	}
	if err := s.checkAllowedRange(start, end); err != nil {
		return err
	}
	return s.w.PutEngineRangeKey(start, end, suffix, value)
}

func (s spanSetWriter) ClearEngineRangeKey(start, end roachpb.Key, suffix []byte) error {
	if !s.spansOnly {
		panic("cannot do timestamp checking for ClearEngineRangeKey")
	}
	if err := s.checkAllowedRange(start, end); err != nil {
		return err
	}
	return s.w.ClearEngineRangeKey(start, end, suffix)
}

func (s spanSetWriter) ClearMVCCRangeKey(rangeKey storage.MVCCRangeKey) error {
	if err := s.checkAllowedRange(rangeKey.StartKey, rangeKey.EndKey); err != nil {
		return err
	}
	return s.w.ClearMVCCRangeKey(rangeKey)
}

func (s spanSetWriter) Merge(key storage.MVCCKey, value []byte) error {
	if s.spansOnly {
		if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
			return err
		}
	} else {
		if err := s.spans.CheckAllowedAt(SpanReadWrite, roachpb.Span{Key: key.Key}, s.ts); err != nil {
			return err
		}
	}
	return s.w.Merge(key, value)
}

func (s spanSetWriter) PutMVCC(key storage.MVCCKey, value storage.MVCCValue) error {
	if err := s.checkAllowed(key.Key); err != nil {
		return err
	}
	return s.w.PutMVCC(key, value)
}

func (s spanSetWriter) PutRawMVCC(key storage.MVCCKey, value []byte) error {
	if err := s.checkAllowed(key.Key); err != nil {
		return err
	}
	return s.w.PutRawMVCC(key, value)
}

func (s spanSetWriter) PutUnversioned(key roachpb.Key, value []byte) error {
	if err := s.checkAllowed(key); err != nil {
		return err
	}
	return s.w.PutUnversioned(key, value)
}

func (s spanSetWriter) PutEngineKey(key storage.EngineKey, value []byte) error {
	if !s.spansOnly {
		panic("cannot do timestamp checking for putting EngineKey")
	}
	if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
		return err
	}
	return s.w.PutEngineKey(key, value)
}

func (s spanSetWriter) LogData(data []byte) error {
	return s.w.LogData(data)
}

func (s spanSetWriter) LogLogicalOp(
	op storage.MVCCLogicalOpType, details storage.MVCCLogicalOpDetails,
) {
	s.w.LogLogicalOp(op, details)
}

func (s spanSetWriter) ShouldWriteLocalTimestamps(ctx context.Context) bool {
	return s.w.ShouldWriteLocalTimestamps(ctx)
}

func (s spanSetWriter) BufferedSize() int {
	return s.w.BufferedSize()
}

// ReadWriter is used outside of the spanset package internally, in ccl.
type ReadWriter struct {
	spanSetReader
	spanSetWriter
}

var _ storage.ReadWriter = ReadWriter{}

func makeSpanSetReadWriter(rw storage.ReadWriter, spans *SpanSet) ReadWriter {
	spans = addLockTableSpans(spans)
	return ReadWriter{
		spanSetReader: spanSetReader{r: rw, spans: spans, spansOnly: true},
		spanSetWriter: spanSetWriter{w: rw, spans: spans, spansOnly: true},
	}
}

func makeSpanSetReadWriterAt(rw storage.ReadWriter, spans *SpanSet, ts hlc.Timestamp) ReadWriter {
	spans = addLockTableSpans(spans)
	return ReadWriter{
		spanSetReader: spanSetReader{r: rw, spans: spans, ts: ts},
		spanSetWriter: spanSetWriter{w: rw, spans: spans, ts: ts},
	}
}

// NewReader returns a storage.Reader that asserts access of the underlying
// Reader against the given SpanSet at a given timestamp. If zero timestamp is
// provided, accesses are considered non-MVCC.
func NewReader(r storage.Reader, spans *SpanSet, ts hlc.Timestamp) storage.Reader {
	spans = addLockTableSpans(spans)
	return spanSetReader{r: r, spans: spans, ts: ts}
}

// NewReadWriterAt returns a storage.ReadWriter that asserts access of the
// underlying ReadWriter against the given SpanSet at a given timestamp.
// If zero timestamp is provided, accesses are considered non-MVCC.
func NewReadWriterAt(rw storage.ReadWriter, spans *SpanSet, ts hlc.Timestamp) storage.ReadWriter {
	return makeSpanSetReadWriterAt(rw, spans, ts)
}

type spanSetBatch struct {
	ReadWriter
	b     storage.Batch
	spans *SpanSet

	spansOnly bool
	ts        hlc.Timestamp
}

var _ storage.Batch = spanSetBatch{}

func (s spanSetBatch) NewBatchOnlyMVCCIterator(
	ctx context.Context, opts storage.IterOptions,
) (storage.MVCCIterator, error) {
	panic("unimplemented")
}

func (s spanSetBatch) ScanInternal(
	ctx context.Context,
	lower, upper roachpb.Key,
	visitPointKey func(key *pebble.InternalKey, value pebble.LazyValue, info pebble.IteratorLevel) error,
	visitRangeDel func(start []byte, end []byte, seqNum pebble.SeqNum) error,
	visitRangeKey func(start []byte, end []byte, keys []rangekey.Key) error,
	visitSharedFile func(sst *pebble.SharedSSTMeta) error,
	visitExternalFile func(sst *pebble.ExternalFile) error,
) error {
	// Only used on Engine.
	panic("unimplemented")
}

func (s spanSetBatch) Commit(sync bool) error {
	return s.b.Commit(sync)
}

func (s spanSetBatch) CommitNoSyncWait() error {
	return s.b.CommitNoSyncWait()
}

func (s spanSetBatch) SyncWait() error {
	return s.b.CommitNoSyncWait()
}

func (s spanSetBatch) Empty() bool {
	return s.b.Empty()
}

func (s spanSetBatch) Count() uint32 {
	return s.b.Count()
}

func (s spanSetBatch) Len() int {
	return s.b.Len()
}

func (s spanSetBatch) Repr() []byte {
	return s.b.Repr()
}

func (s spanSetBatch) CommitStats() storage.BatchCommitStats {
	return s.b.CommitStats()
}

func (s spanSetBatch) PutInternalRangeKey(start, end []byte, key rangekey.Key) error {
	return s.b.PutInternalRangeKey(start, end, key)
}

func (s spanSetBatch) PutInternalPointKey(key *pebble.InternalKey, value []byte) error {
	return s.b.PutInternalPointKey(key, value)
}

func (s spanSetBatch) ClearRawEncodedRange(start, end []byte) error {
	return s.b.ClearRawEncodedRange(start, end)
}

// NewBatch returns a storage.Batch that asserts access of the underlying
// Batch against the given SpanSet. We only consider span boundaries, associated
// timestamps are not considered.
func NewBatch(b storage.Batch, spans *SpanSet) storage.Batch {
	return &spanSetBatch{
		ReadWriter: makeSpanSetReadWriter(b, spans),
		b:          b,
		spans:      spans,
		spansOnly:  true,
	}
}

// NewBatchAt returns an storage.Batch that asserts access of the underlying
// Batch against the given SpanSet at the given timestamp.
// If the zero timestamp is used, all accesses are considered non-MVCC.
func NewBatchAt(b storage.Batch, spans *SpanSet, ts hlc.Timestamp) storage.Batch {
	return &spanSetBatch{
		ReadWriter: makeSpanSetReadWriterAt(b, spans, ts),
		b:          b,
		spans:      spans,
		ts:         ts,
	}
}

// DisableReaderAssertions unwraps any storage.Reader implementations that may
// assert access against a given SpanSet.
func DisableReaderAssertions(reader storage.Reader) storage.Reader {
	switch v := reader.(type) {
	case ReadWriter:
		return DisableReaderAssertions(v.r)
	case *spanSetBatch:
		return DisableReaderAssertions(v.r)
	default:
		return reader
	}
}

// DisableReadWriterAssertions unwraps any storage.ReadWriter implementations
// that may assert access against a given SpanSet.
func DisableReadWriterAssertions(rw storage.ReadWriter) storage.ReadWriter {
	switch v := rw.(type) {
	case ReadWriter:
		return DisableReadWriterAssertions(v.w.(storage.ReadWriter))
	case *spanSetBatch:
		return DisableReadWriterAssertions(v.w.(storage.ReadWriter))
	default:
		return rw
	}
}

// addLockTableSpans adds corresponding lock table spans for the declared
// spans. This is to implicitly allow raw access to separated intents in the
// lock table for any declared keys. Explicitly declaring lock table spans is
// therefore illegal and will panic, as the implicit access would give
// insufficient isolation from concurrent requests that only declare lock table
// keys.
func addLockTableSpans(spans *SpanSet) *SpanSet {
	withLocks := spans.Copy()
	spans.Iterate(func(sa SpanAccess, _ SpanScope, span Span) {
		// We don't check for spans that contain the entire lock table within them,
		// because some commands (e.g. TransferLease) declare access to the entire
		// key space or very large chunks of it. Even though this could be unsafe,
		// we assume these callers know what they are doing.
		if bytes.HasPrefix(span.Key, keys.LocalRangeLockTablePrefix) ||
			bytes.HasPrefix(span.EndKey, keys.LocalRangeLockTablePrefix) {
			panic(fmt.Sprintf(
				"declaring raw lock table spans is illegal, use main key spans instead (found %s)", span))
		}
		ltKey, _ := keys.LockTableSingleKey(span.Key, nil)
		var ltEndKey roachpb.Key
		if span.EndKey != nil {
			ltEndKey, _ = keys.LockTableSingleKey(span.EndKey, nil)
		}
		if sa == SpanReadOnly && span.Timestamp == hlc.MaxTimestamp {
			// Shared lock acquisition uses a read-only latch access with
			// the maximum timestamp. This gives it sufficient isolation to
			// write to the lock table without having to declare a write
			// latch and be serialized with other shared lock acquisitions.
			// For details, see DefaultDeclareIsolatedKeys.
			//
			// For the sake of this function, we consider this to be strong
			// enough to declare write access to the lock table. This could
			// be made cleaner if latch spans operated on locking strengths
			// instead of read/write access.
			sa = SpanReadWrite
		}
		withLocks.AddNonMVCC(sa, roachpb.Span{Key: ltKey, EndKey: ltEndKey})
	})
	return withLocks
}

type spanSetEFOS struct {
	spanSetReader
	efos storage.EventuallyFileOnlyReader
}

// NewEventuallyFileOnlySnapshot returns a storage.EventuallyFileOnlyReader that
// asserts access of the underlying EFOS against the given SpanSet. We only
// consider span boundaries, associated timestamps are not considered.
func NewEventuallyFileOnlySnapshot(
	e storage.EventuallyFileOnlyReader, spans *SpanSet,
) storage.EventuallyFileOnlyReader {
	spans = addLockTableSpans(spans)
	return &spanSetEFOS{
		spanSetReader: spanSetReader{r: e, spans: spans, spansOnly: true},
		efos:          e,
	}
}

// WaitForFileOnly implements the storage.EventuallyFileOnlyReader interface.
func (e *spanSetEFOS) WaitForFileOnly(
	ctx context.Context, gracePeriodBeforeFlush time.Duration,
) error {
	return e.efos.WaitForFileOnly(ctx, gracePeriodBeforeFlush)
}
