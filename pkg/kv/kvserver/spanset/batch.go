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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// Iterator wraps an engine.Iterator and ensures that it can
// only be used to access spans in a SpanSet.
type Iterator struct {
	i     storage.Iterator
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

var _ storage.Iterator = &Iterator{}
var _ storage.MVCCIterator = &Iterator{}

// NewIterator constructs an iterator that verifies access of the underlying
// iterator against the given SpanSet. Timestamps associated with the spans
// in the spanset are not considered, only the span boundaries are checked.
func NewIterator(iter storage.Iterator, spans *SpanSet) *Iterator {
	return &Iterator{i: iter, spans: spans, spansOnly: true}
}

// NewIteratorAt constructs an iterator that verifies access of the underlying
// iterator against the given SpanSet at the given timestamp.
func NewIteratorAt(iter storage.Iterator, spans *SpanSet, ts hlc.Timestamp) *Iterator {
	return &Iterator{i: iter, spans: spans, ts: ts}
}

// Close is part of the engine.Iterator interface.
func (i *Iterator) Close() {
	i.i.Close()
}

// Iterator returns the underlying engine.Iterator.
func (i *Iterator) Iterator() storage.Iterator {
	return i.i
}

// Valid is part of the engine.Iterator interface.
func (i *Iterator) Valid() (bool, error) {
	if i.err != nil {
		return false, i.err
	}
	ok, err := i.i.Valid()
	if err != nil {
		return false, err
	}
	return ok && !i.invalid, nil
}

// SeekGE is part of the engine.Iterator interface.
func (i *Iterator) SeekGE(key storage.MVCCKey) {
	i.i.SeekGE(key)
	i.checkAllowed(roachpb.Span{Key: key.Key}, true)
}

// SeekLT is part of the engine.Iterator interface.
func (i *Iterator) SeekLT(key storage.MVCCKey) {
	i.i.SeekLT(key)
	// CheckAllowed{At} supports the span representation of [,key), which
	// corresponds to the span [key.Prev(),).
	i.checkAllowed(roachpb.Span{EndKey: key.Key}, true)
}

// Next is part of the engine.Iterator interface.
func (i *Iterator) Next() {
	i.i.Next()
	i.checkAllowed(roachpb.Span{Key: i.UnsafeKey().Key}, false)
}

// Prev is part of the engine.Iterator interface.
func (i *Iterator) Prev() {
	i.i.Prev()
	i.checkAllowed(roachpb.Span{Key: i.UnsafeKey().Key}, false)
}

// NextKey is part of the engine.Iterator interface.
func (i *Iterator) NextKey() {
	i.i.NextKey()
	i.checkAllowed(roachpb.Span{Key: i.UnsafeKey().Key}, false)
}

func (i *Iterator) checkAllowed(span roachpb.Span, errIfDisallowed bool) {
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

// Key is part of the engine.Iterator interface.
func (i *Iterator) Key() storage.MVCCKey {
	return i.i.Key()
}

// Value is part of the engine.Iterator interface.
func (i *Iterator) Value() []byte {
	return i.i.Value()
}

// ValueProto is part of the engine.Iterator interface.
func (i *Iterator) ValueProto(msg protoutil.Message) error {
	return i.i.ValueProto(msg)
}

// UnsafeKey is part of the engine.Iterator interface.
func (i *Iterator) UnsafeKey() storage.MVCCKey {
	return i.i.UnsafeKey()
}

// UnsafeValue is part of the engine.Iterator interface.
func (i *Iterator) UnsafeValue() []byte {
	return i.i.UnsafeValue()
}

// ComputeStats is part of the engine.Iterator interface.
func (i *Iterator) ComputeStats(
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

// FindSplitKey is part of the engine.Iterator interface.
func (i *Iterator) FindSplitKey(
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

// CheckForKeyCollisions is part of the engine.Iterator interface.
func (i *Iterator) CheckForKeyCollisions(
	sstData []byte, start, end roachpb.Key,
) (enginepb.MVCCStats, error) {
	return i.i.CheckForKeyCollisions(sstData, start, end)
}

// SetUpperBound is part of the engine.Iterator interface.
func (i *Iterator) SetUpperBound(key roachpb.Key) {
	i.i.SetUpperBound(key)
}

// Stats is part of the engine.Iterator interface.
func (i *Iterator) Stats() storage.IteratorStats {
	return i.i.Stats()
}

// MVCCOpsSpecialized is part of the engine.MVCCIterator interface.
func (i *Iterator) MVCCOpsSpecialized() bool {
	if mvccIt, ok := i.i.(storage.MVCCIterator); ok {
		return mvccIt.MVCCOpsSpecialized()
	}
	return false
}

// MVCCGet is part of the engine.MVCCIterator interface.
func (i *Iterator) MVCCGet(
	key roachpb.Key, timestamp hlc.Timestamp, opts storage.MVCCGetOptions,
) (*roachpb.Value, *roachpb.Intent, error) {
	if i.spansOnly {
		if err := i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key}); err != nil {
			return nil, nil, err
		}
	} else {
		if err := i.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: key}, timestamp); err != nil {
			return nil, nil, err
		}
	}
	return i.i.(storage.MVCCIterator).MVCCGet(key, timestamp, opts)
}

// MVCCScan is part of the engine.MVCCIterator interface.
func (i *Iterator) MVCCScan(
	start, end roachpb.Key, timestamp hlc.Timestamp, opts storage.MVCCScanOptions,
) (storage.MVCCScanResult, error) {
	if i.spansOnly {
		if err := i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}); err != nil {
			return storage.MVCCScanResult{}, err
		}
	} else {
		if err := i.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}, timestamp); err != nil {
			return storage.MVCCScanResult{}, err
		}
	}
	return i.i.(storage.MVCCIterator).MVCCScan(start, end, timestamp, opts)
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

// ExportToSst is part of the engine.Reader interface.
func (s spanSetReader) ExportToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	targetSize, maxSize uint64,
	io storage.IterOptions,
) ([]byte, roachpb.BulkOpSummary, roachpb.Key, error) {
	return s.r.ExportToSst(startKey, endKey, startTS, endTS, exportAllRevisions, targetSize, maxSize, io)
}

func (s spanSetReader) Get(key storage.MVCCKey) ([]byte, error) {
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
	return s.r.Get(key)
}

func (s spanSetReader) GetProto(
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
	//lint:ignore SA1019 implementing deprecated interface function (GetProto) is OK
	return s.r.GetProto(key, msg)
}

func (s spanSetReader) Iterate(
	start, end roachpb.Key, f func(storage.MVCCKeyValue) (bool, error),
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
	return s.r.Iterate(start, end, f)
}

func (s spanSetReader) NewIterator(opts storage.IterOptions) storage.Iterator {
	if s.spansOnly {
		return NewIterator(s.r.NewIterator(opts), s.spans)
	}
	return NewIteratorAt(s.r.NewIterator(opts), s.spans, s.ts)
}

// GetDBEngine recursively searches for the underlying rocksDB engine.
func GetDBEngine(reader storage.Reader, span roachpb.Span) storage.Reader {
	switch v := reader.(type) {
	case ReadWriter:
		return GetDBEngine(getSpanReader(v, span), span)
	case *spanSetBatch:
		return GetDBEngine(getSpanReader(v.ReadWriter, span), span)
	default:
		return reader
	}
}

// getSpanReader is a getter to access the engine.Reader field of the
// spansetReader.
func getSpanReader(r ReadWriter, span roachpb.Span) storage.Reader {
	if err := r.spanSetReader.spans.CheckAllowed(SpanReadOnly, span); err != nil {
		panic("Not in the span")
	}

	return r.spanSetReader.r
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

func (s spanSetWriter) Clear(key storage.MVCCKey) error {
	if s.spansOnly {
		if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
			return err
		}
	} else {
		if err := s.spans.CheckAllowedAt(SpanReadWrite, roachpb.Span{Key: key.Key}, s.ts); err != nil {
			return err
		}
	}
	return s.w.Clear(key)
}

func (s spanSetWriter) SingleClear(key storage.MVCCKey) error {
	if s.spansOnly {
		if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
			return err
		}
	} else {
		if err := s.spans.CheckAllowedAt(SpanReadWrite, roachpb.Span{Key: key.Key}, s.ts); err != nil {
			return err
		}
	}
	return s.w.SingleClear(key)
}

func (s spanSetWriter) ClearRange(start, end storage.MVCCKey) error {
	if s.spansOnly {
		if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: start.Key, EndKey: end.Key}); err != nil {
			return err
		}
	} else {
		if err := s.spans.CheckAllowedAt(SpanReadWrite, roachpb.Span{Key: start.Key, EndKey: end.Key}, s.ts); err != nil {
			return err
		}
	}
	return s.w.ClearRange(start, end)
}

func (s spanSetWriter) ClearIterRange(iter storage.Iterator, start, end roachpb.Key) error {
	if s.spansOnly {
		if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: start, EndKey: end}); err != nil {
			return err
		}
	} else {
		if err := s.spans.CheckAllowedAt(SpanReadWrite, roachpb.Span{Key: start, EndKey: end}, s.ts); err != nil {
			return err
		}
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

func (s spanSetWriter) Put(key storage.MVCCKey, value []byte) error {
	if s.spansOnly {
		if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
			return err
		}
	} else {
		if err := s.spans.CheckAllowedAt(SpanReadWrite, roachpb.Span{Key: key.Key}, s.ts); err != nil {
			return err
		}
	}
	return s.w.Put(key, value)
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

// NewReadWriter returns an engine.ReadWriter that asserts access of the
// underlying ReadWriter against the given SpanSet.
func NewReadWriter(rw storage.ReadWriter, spans *SpanSet) storage.ReadWriter {
	return makeSpanSetReadWriter(rw, spans)
}

// NewReadWriterAt returns an engine.ReadWriter that asserts access of the
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

func (s spanSetBatch) Distinct() storage.ReadWriter {
	if s.spansOnly {
		return NewReadWriter(s.b.Distinct(), s.spans)
	}
	return NewReadWriterAt(s.b.Distinct(), s.spans, s.ts)
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

// NewBatch returns an engine.Batch that asserts access of the underlying
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

// NewBatchAt returns an engine.Batch that asserts access of the underlying
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
