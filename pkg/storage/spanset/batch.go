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
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// Iterator wraps an engine.Iterator and ensures that it can
// only be used to access spans in a SpanSet.
type Iterator struct {
	i     engine.Iterator
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

var _ engine.Iterator = &Iterator{}
var _ engine.MVCCIterator = &Iterator{}

// NewIterator constructs an iterator that verifies access of the underlying
// iterator against the given SpanSet. Timestamps associated with the spans
// in the spanset are not considered, only the span boundaries are checked.
func NewIterator(iter engine.Iterator, spans *SpanSet) *Iterator {
	return &Iterator{i: iter, spans: spans, spansOnly: true}
}

// NewIteratorAt constructs an iterator that verifies access of the underlying
// iterator against the given SpanSet at the given timestamp.
func NewIteratorAt(iter engine.Iterator, spans *SpanSet, ts hlc.Timestamp) *Iterator {
	return &Iterator{i: iter, spans: spans, ts: ts}
}

// Close is part of the engine.Iterator interface.
func (i *Iterator) Close() {
	i.i.Close()
}

// Iterator returns the underlying engine.Iterator.
func (i *Iterator) Iterator() engine.Iterator {
	return i.i
}

// SeekGE is part of the engine.Iterator interface.
func (i *Iterator) SeekGE(key engine.MVCCKey) {
	if i.spansOnly {
		i.err = i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key.Key})
	} else {
		i.err = i.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: key.Key}, i.ts)
	}
	if i.err == nil {
		i.invalid = false
	}
	i.i.SeekGE(key)
}

// SeekLT is part of the engine.Iterator interface.
func (i *Iterator) SeekLT(key engine.MVCCKey) {
	// CheckAllowed{At} supports the span representation of [,key), which
	// corresponds to the span [key.Prev(),).
	revSpan := roachpb.Span{EndKey: key.Key}
	if i.spansOnly {
		i.err = i.spans.CheckAllowed(SpanReadOnly, revSpan)
	} else {
		i.err = i.spans.CheckAllowedAt(SpanReadOnly, revSpan, i.ts)
	}
	if i.err == nil {
		i.invalid = false
	}
	i.i.SeekLT(key)
}

// Valid is part of the engine.Iterator interface.
func (i *Iterator) Valid() (bool, error) {
	if i.err != nil {
		return false, i.err
	}
	ok, err := i.i.Valid()
	if err != nil {
		return false, i.err
	}
	return ok && !i.invalid, nil
}

// Next is part of the engine.Iterator interface.
func (i *Iterator) Next() {
	i.i.Next()
	if i.spansOnly {
		if i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: i.UnsafeKey().Key}) != nil {
			i.invalid = true
		}
	} else {
		if i.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: i.UnsafeKey().Key}, i.ts) != nil {
			i.invalid = true
		}
	}
}

// Prev is part of the engine.Iterator interface.
func (i *Iterator) Prev() {
	i.i.Prev()
	if i.spansOnly {
		if i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: i.UnsafeKey().Key}) != nil {
			i.invalid = true
		}
	} else {
		if i.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: i.UnsafeKey().Key}, i.ts) != nil {
			i.invalid = true
		}
	}
}

// NextKey is part of the engine.Iterator interface.
func (i *Iterator) NextKey() {
	i.i.NextKey()
	if i.spansOnly {
		if i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: i.UnsafeKey().Key}) != nil {
			i.invalid = true
		}
	} else {
		if i.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: i.UnsafeKey().Key}, i.ts) != nil {
			i.invalid = true
		}
	}
}

// Key is part of the engine.Iterator interface.
func (i *Iterator) Key() engine.MVCCKey {
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
func (i *Iterator) UnsafeKey() engine.MVCCKey {
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
) (engine.MVCCKey, error) {
	if i.spansOnly {
		if err := i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}); err != nil {
			return engine.MVCCKey{}, err
		}
	} else {
		if err := i.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}, i.ts); err != nil {
			return engine.MVCCKey{}, err
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
func (i *Iterator) Stats() engine.IteratorStats {
	return i.i.Stats()
}

// MVCCOpsSpecialized is part of the engine.MVCCIterator interface.
func (i *Iterator) MVCCOpsSpecialized() bool {
	if mvccIt, ok := i.i.(engine.MVCCIterator); ok {
		return mvccIt.MVCCOpsSpecialized()
	}
	return false
}

// MVCCGet is part of the engine.MVCCIterator interface.
func (i *Iterator) MVCCGet(
	key roachpb.Key, timestamp hlc.Timestamp, opts engine.MVCCGetOptions,
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
	return i.i.(engine.MVCCIterator).MVCCGet(key, timestamp, opts)
}

// MVCCScan is part of the engine.MVCCIterator interface.
func (i *Iterator) MVCCScan(
	start, end roachpb.Key, max int64, timestamp hlc.Timestamp, opts engine.MVCCScanOptions,
) (engine.MVCCScanResult, error) {
	if i.spansOnly {
		if err := i.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}); err != nil {
			return engine.MVCCScanResult{}, err
		}
	} else {
		if err := i.spans.CheckAllowedAt(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}, timestamp); err != nil {
			return engine.MVCCScanResult{}, err
		}
	}
	return i.i.(engine.MVCCIterator).MVCCScan(start, end, max, timestamp, opts)
}

type spanSetReader struct {
	r     engine.Reader
	spans *SpanSet

	spansOnly bool
	ts        hlc.Timestamp
}

var _ engine.Reader = spanSetReader{}

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
	io engine.IterOptions,
) ([]byte, roachpb.BulkOpSummary, roachpb.Key, error) {
	return s.r.ExportToSst(startKey, endKey, startTS, endTS, exportAllRevisions, targetSize, maxSize, io)
}

func (s spanSetReader) Get(key engine.MVCCKey) ([]byte, error) {
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
	key engine.MVCCKey, msg protoutil.Message,
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
	start, end roachpb.Key, f func(engine.MVCCKeyValue) (bool, error),
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

func (s spanSetReader) NewIterator(opts engine.IterOptions) engine.Iterator {
	if s.spansOnly {
		return NewIterator(s.r.NewIterator(opts), s.spans)
	}
	return NewIteratorAt(s.r.NewIterator(opts), s.spans, s.ts)
}

// GetDBEngine recursively searches for the underlying rocksDB engine.
func GetDBEngine(reader engine.Reader, span roachpb.Span) engine.Reader {
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
func getSpanReader(r ReadWriter, span roachpb.Span) engine.Reader {
	if err := r.spanSetReader.spans.CheckAllowed(SpanReadOnly, span); err != nil {
		panic("Not in the span")
	}

	return r.spanSetReader.r
}

type spanSetWriter struct {
	w     engine.Writer
	spans *SpanSet

	spansOnly bool
	ts        hlc.Timestamp
}

var _ engine.Writer = spanSetWriter{}

func (s spanSetWriter) ApplyBatchRepr(repr []byte, sync bool) error {
	// Assume that the constructor of the batch has bounded it correctly.
	return s.w.ApplyBatchRepr(repr, sync)
}

func (s spanSetWriter) Clear(key engine.MVCCKey) error {
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

func (s spanSetWriter) SingleClear(key engine.MVCCKey) error {
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

func (s spanSetWriter) ClearRange(start, end engine.MVCCKey) error {
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

func (s spanSetWriter) ClearIterRange(iter engine.Iterator, start, end roachpb.Key) error {
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

func (s spanSetWriter) Merge(key engine.MVCCKey, value []byte) error {
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

func (s spanSetWriter) Put(key engine.MVCCKey, value []byte) error {
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
	op engine.MVCCLogicalOpType, details engine.MVCCLogicalOpDetails,
) {
	s.w.LogLogicalOp(op, details)
}

// ReadWriter is used outside of the spanset package internally, in ccl.
type ReadWriter struct {
	spanSetReader
	spanSetWriter
}

var _ engine.ReadWriter = ReadWriter{}

func makeSpanSetReadWriter(rw engine.ReadWriter, spans *SpanSet) ReadWriter {
	return ReadWriter{
		spanSetReader: spanSetReader{r: rw, spans: spans, spansOnly: true},
		spanSetWriter: spanSetWriter{w: rw, spans: spans, spansOnly: true},
	}
}

func makeSpanSetReadWriterAt(rw engine.ReadWriter, spans *SpanSet, ts hlc.Timestamp) ReadWriter {
	return ReadWriter{
		spanSetReader: spanSetReader{r: rw, spans: spans, ts: ts},
		spanSetWriter: spanSetWriter{w: rw, spans: spans, ts: ts},
	}
}

// NewReadWriter returns an engine.ReadWriter that asserts access of the
// underlying ReadWriter against the given SpanSet.
func NewReadWriter(rw engine.ReadWriter, spans *SpanSet) engine.ReadWriter {
	return makeSpanSetReadWriter(rw, spans)
}

// NewReadWriterAt returns an engine.ReadWriter that asserts access of the
// underlying ReadWriter against the given SpanSet at a given timestamp.
// If zero timestamp is provided, accesses are considered non-MVCC.
func NewReadWriterAt(rw engine.ReadWriter, spans *SpanSet, ts hlc.Timestamp) engine.ReadWriter {
	return makeSpanSetReadWriterAt(rw, spans, ts)
}

type spanSetBatch struct {
	ReadWriter
	b     engine.Batch
	spans *SpanSet

	spansOnly bool
	ts        hlc.Timestamp
}

var _ engine.Batch = spanSetBatch{}

func (s spanSetBatch) Commit(sync bool) error {
	return s.b.Commit(sync)
}

func (s spanSetBatch) Distinct() engine.ReadWriter {
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
func NewBatch(b engine.Batch, spans *SpanSet) engine.Batch {
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
func NewBatchAt(b engine.Batch, spans *SpanSet, ts hlc.Timestamp) engine.Batch {
	return &spanSetBatch{
		ReadWriter: makeSpanSetReadWriterAt(b, spans, ts),
		b:          b,
		spans:      spans,
		ts:         ts,
	}
}
