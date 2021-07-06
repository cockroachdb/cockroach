// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanset

import (
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/pebble"
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

// SeekIntentGE is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) SeekIntentGE(key roachpb.Key, txnUUID uuid.UUID) {
	i.i.SeekIntentGE(key, txnUUID)
	i.checkAllowed(roachpb.Span{Key: key}, true)
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
	i.checkAllowed(roachpb.Span{Key: i.UnsafeKey().Key}, false)
}

// Prev is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) Prev() {
	i.i.Prev()
	i.checkAllowed(roachpb.Span{Key: i.UnsafeKey().Key}, false)
}

// NextKey is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) NextKey() {
	i.i.NextKey()
	i.checkAllowed(roachpb.Span{Key: i.UnsafeKey().Key}, false)
}

func (i *MVCCIterator) checkAllowed(span roachpb.Span, errIfDisallowed bool) {
	i.invalid = false
	i.err = nil
	if ok, _ := i.i.Valid(); !ok {
		// If the iterator is invalid after the operation, there's nothing to
		// check. We allow uses of iterators to exceed the declared span bounds
		// as long as the iterator itself is configured with proper boundaries.
		return
	}
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

// Key is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) Key() storage.MVCCKey {
	return i.i.Key()
}

// Value is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) Value() []byte {
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
func (i *MVCCIterator) UnsafeValue() []byte {
	return i.i.UnsafeValue()
}

// IsCurIntentSeparated implements the MVCCIterator interface.
func (i *MVCCIterator) IsCurIntentSeparated() bool {
	return i.i.IsCurIntentSeparated()
}

// ComputeStats is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) ComputeStats(
	start, end roachpb.Key, nowNanos int64,
) (enginepb.MVCCStats, error) {
	if i.spansOnly {
		if err := i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}); err != nil {
			return enginepb.MVCCStats{}, err
		}
	} else {
		if err := i.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}, i.ts); err != nil {
			return enginepb.MVCCStats{}, err
		}
	}
	return i.i.ComputeStats(start, end, nowNanos)
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

// CheckForKeyCollisions is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) CheckForKeyCollisions(
	sstData []byte, start, end roachpb.Key,
) (enginepb.MVCCStats, error) {
	return i.i.CheckForKeyCollisions(sstData, start, end)
}

// SetUpperBound is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) SetUpperBound(key roachpb.Key) {
	i.i.SetUpperBound(key)
}

// Stats is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) Stats() storage.IteratorStats {
	return i.i.Stats()
}

// SupportsPrev is part of the storage.MVCCIterator interface.
func (i *MVCCIterator) SupportsPrev() bool {
	return i.i.SupportsPrev()
}

// EngineIterator wraps a storage.EngineIterator and ensures that it can
// only be used to access spans in a SpanSet.
type EngineIterator struct {
	i     storage.EngineIterator
	spans *SpanSet
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
	if err = i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key.Key}); err != nil {
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
	if err = i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{EndKey: key.Key}); err != nil {
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
	if err = i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key.Key}); err != nil {
		// Invalid, but no error.
		return false, nil // nolint:returnerrcheck
	}
	return true, nil
}

// UnsafeEngineKey is part of the storage.EngineIterator interface.
func (i *EngineIterator) UnsafeEngineKey() (storage.EngineKey, error) {
	return i.i.UnsafeEngineKey()
}

// UnsafeValue is part of the storage.EngineIterator interface.
func (i *EngineIterator) UnsafeValue() []byte {
	return i.i.UnsafeValue()
}

// EngineKey is part of the storage.EngineIterator interface.
func (i *EngineIterator) EngineKey() (storage.EngineKey, error) {
	return i.i.EngineKey()
}

// Value is part of the storage.EngineIterator interface.
func (i *EngineIterator) Value() []byte {
	return i.i.Value()
}

// UnsafeRawEngineKey is part of the storage.EngineIterator interface.
func (i *EngineIterator) UnsafeRawEngineKey() []byte {
	return i.i.UnsafeRawEngineKey()
}

// SetUpperBound is part of the storage.EngineIterator interface.
func (i *EngineIterator) SetUpperBound(key roachpb.Key) {
	i.i.SetUpperBound(key)
}

// GetRawIter is part of the storage.EngineIterator interface.
func (i *EngineIterator) GetRawIter() *pebble.Iterator {
	return i.i.GetRawIter()
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

func (s spanSetReader) Close() {
	s.r.Close()
}

func (s spanSetReader) Closed() bool {
	return s.r.Closed()
}

// ExportMVCCToSst is part of the storage.Reader interface.
func (s spanSetReader) ExportMVCCToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	targetSize, maxSize uint64,
	useTBI bool,
	dest io.Writer,
) (roachpb.BulkOpSummary, roachpb.Key, error) {
	return s.r.ExportMVCCToSst(startKey, endKey, startTS, endTS, exportAllRevisions, targetSize,
		maxSize, useTBI, dest)
}

func (s spanSetReader) MVCCGet(key storage.MVCCKey) ([]byte, error) {
	if s.spansOnly {
		if err := s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key.Key}); err != nil {
			return nil, err
		}
	} else {
		if err := s.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: key.Key}, s.ts); err != nil {
			return nil, err
		}
	}
	//lint:ignore SA1019 implementing deprecated interface function (Get) is OK
	return s.r.MVCCGet(key)
}

func (s spanSetReader) MVCCGetProto(
	key storage.MVCCKey, msg protoutil.Message,
) (bool, int64, int64, error) {
	if s.spansOnly {
		if err := s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key.Key}); err != nil {
			return false, 0, 0, err
		}
	} else {
		if err := s.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: key.Key}, s.ts); err != nil {
			return false, 0, 0, err
		}
	}
	//lint:ignore SA1019 implementing deprecated interface function (MVCCGetProto) is OK
	return s.r.MVCCGetProto(key, msg)
}

func (s spanSetReader) MVCCIterate(
	start, end roachpb.Key, iterKind storage.MVCCIterKind, f func(storage.MVCCKeyValue) error,
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
	return s.r.MVCCIterate(start, end, iterKind, f)
}

func (s spanSetReader) NewMVCCIterator(
	iterKind storage.MVCCIterKind, opts storage.IterOptions,
) storage.MVCCIterator {
	if s.spansOnly {
		return NewIterator(s.r.NewMVCCIterator(iterKind, opts), s.spans)
	}
	return NewIteratorAt(s.r.NewMVCCIterator(iterKind, opts), s.spans, s.ts)
}

func (s spanSetReader) NewEngineIterator(opts storage.IterOptions) storage.EngineIterator {
	if !s.spansOnly {
		panic("cannot do timestamp checking for EngineIterator")
	}
	return &EngineIterator{
		i:     s.r.NewEngineIterator(opts),
		spans: s.spans,
	}
}

// ConsistentIterators implements the storage.Reader interface.
func (s spanSetReader) ConsistentIterators() bool {
	return s.r.ConsistentIterators()
}

// PinEngineStateForIterators implements the storage.Reader interface.
func (s spanSetReader) PinEngineStateForIterators() error {
	return s.r.PinEngineStateForIterators()
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

func (s spanSetWriter) ClearMVCC(key storage.MVCCKey) error {
	if err := s.checkAllowed(key.Key); err != nil {
		return err
	}
	return s.w.ClearMVCC(key)
}

func (s spanSetWriter) ClearUnversioned(key roachpb.Key) error {
	if err := s.checkAllowed(key); err != nil {
		return err
	}
	return s.w.ClearUnversioned(key)
}

func (s spanSetWriter) ClearIntent(
	key roachpb.Key, state storage.PrecedingIntentState, txnDidNotUpdateMeta bool, txnUUID uuid.UUID,
) (int, error) {
	if err := s.checkAllowed(key); err != nil {
		return 0, err
	}
	return s.w.ClearIntent(key, state, txnDidNotUpdateMeta, txnUUID)
}

func (s spanSetWriter) ClearEngineKey(key storage.EngineKey) error {
	if !s.spansOnly {
		panic("cannot do timestamp checking for clearing EngineKey")
	}
	if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
		return err
	}
	return s.w.ClearEngineKey(key)
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

func (s spanSetWriter) ClearRawRange(start, end roachpb.Key) error {
	if err := s.checkAllowedRange(start, end); err != nil {
		return err
	}
	return s.w.ClearRawRange(start, end)
}

func (s spanSetWriter) ClearMVCCRangeAndIntents(start, end roachpb.Key) error {
	if err := s.checkAllowedRange(start, end); err != nil {
		return err
	}
	return s.w.ClearMVCCRangeAndIntents(start, end)
}

func (s spanSetWriter) ClearMVCCRange(start, end storage.MVCCKey) error {
	if err := s.checkAllowedRange(start.Key, end.Key); err != nil {
		return err
	}
	return s.w.ClearMVCCRange(start, end)
}

func (s spanSetWriter) ClearIterRange(iter storage.MVCCIterator, start, end roachpb.Key) error {
	if err := s.checkAllowedRange(start, end); err != nil {
		return err
	}
	return s.w.ClearIterRange(iter, start, end)
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

func (s spanSetWriter) PutMVCC(key storage.MVCCKey, value []byte) error {
	if err := s.checkAllowed(key.Key); err != nil {
		return err
	}
	return s.w.PutMVCC(key, value)
}

func (s spanSetWriter) PutUnversioned(key roachpb.Key, value []byte) error {
	if err := s.checkAllowed(key); err != nil {
		return err
	}
	return s.w.PutUnversioned(key, value)
}

func (s spanSetWriter) PutIntent(
	ctx context.Context,
	key roachpb.Key,
	value []byte,
	state storage.PrecedingIntentState,
	txnDidNotUpdateMeta bool,
	txnUUID uuid.UUID,
) (int, error) {
	if err := s.checkAllowed(key); err != nil {
		return 0, err
	}
	return s.w.PutIntent(ctx, key, value, state, txnDidNotUpdateMeta, txnUUID)
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

func (s spanSetWriter) SafeToWriteSeparatedIntents(ctx context.Context) (bool, error) {
	return s.w.SafeToWriteSeparatedIntents(ctx)
}

func (s spanSetWriter) LogData(data []byte) error {
	return s.w.LogData(data)
}

func (s spanSetWriter) LogLogicalOp(
	op storage.MVCCLogicalOpType, details storage.MVCCLogicalOpDetails,
) {
	s.w.LogLogicalOp(op, details)
}

// ReadWriter is used outside of the spanset package internally, in ccl.
type ReadWriter struct {
	spanSetReader
	spanSetWriter
}

var _ storage.ReadWriter = ReadWriter{}

func makeSpanSetReadWriter(rw storage.ReadWriter, spans *SpanSet) ReadWriter {
	return ReadWriter{
		spanSetReader: spanSetReader{r: rw, spans: spans, spansOnly: true},
		spanSetWriter: spanSetWriter{w: rw, spans: spans, spansOnly: true},
	}
}

func makeSpanSetReadWriterAt(rw storage.ReadWriter, spans *SpanSet, ts hlc.Timestamp) ReadWriter {
	return ReadWriter{
		spanSetReader: spanSetReader{r: rw, spans: spans, ts: ts},
		spanSetWriter: spanSetWriter{w: rw, spans: spans, ts: ts},
	}
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

func (s spanSetBatch) Commit(sync bool) error {
	return s.b.Commit(sync)
}

func (s spanSetBatch) Empty() bool {
	return s.b.Empty()
}

func (s spanSetBatch) Len() int {
	return s.b.Len()
}

func (s spanSetBatch) Repr() []byte {
	return s.b.Repr()
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
