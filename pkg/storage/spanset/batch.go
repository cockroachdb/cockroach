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

	// Seeking to an invalid key puts the iterator in an error state.
	err error
	// Reaching an out-of-bounds key with Next/Prev invalidates the
	// iterator but does not set err.
	invalid bool
}

var _ engine.Iterator = &Iterator{}

// NewIterator constructs an iterator that verifies access of the underlying
// iterator against the given spans.
func NewIterator(iter engine.Iterator, spans *SpanSet) *Iterator {
	return &Iterator{
		i:     iter,
		spans: spans,
	}
}

// Stats is part of the engine.Iterator interface.
func (s *Iterator) Stats() engine.IteratorStats {
	return s.i.Stats()
}

// Close is part of the engine.Iterator interface.
func (s *Iterator) Close() {
	s.i.Close()
}

// Iterator returns the underlying engine.Iterator.
func (s *Iterator) Iterator() engine.Iterator {
	return s.i
}

// Seek is part of the engine.Iterator interface.
func (s *Iterator) Seek(key engine.MVCCKey) {
	s.err = s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key.Key})
	if s.err == nil {
		s.invalid = false
	}
	s.i.Seek(key)
}

// SeekReverse is part of the engine.Iterator interface.
func (s *Iterator) SeekReverse(key engine.MVCCKey) {
	s.err = s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key.Key})
	if s.err == nil {
		s.invalid = false
	}
	s.i.SeekReverse(key)
}

// Valid is part of the engine.Iterator interface.
func (s *Iterator) Valid() (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	ok, err := s.i.Valid()
	if err != nil {
		return false, s.err
	}
	return ok && !s.invalid, nil
}

// Next is part of the engine.Iterator interface.
func (s *Iterator) Next() {
	s.i.Next()
	if s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: s.UnsafeKey().Key}) != nil {
		s.invalid = true
	}
}

// Prev is part of the engine.Iterator interface.
func (s *Iterator) Prev() {
	s.i.Prev()
	if s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: s.UnsafeKey().Key}) != nil {
		s.invalid = true
	}
}

// NextKey is part of the engine.Iterator interface.
func (s *Iterator) NextKey() {
	s.i.NextKey()
	if s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: s.UnsafeKey().Key}) != nil {
		s.invalid = true
	}
}

// PrevKey is part of the engine.Iterator interface.
func (s *Iterator) PrevKey() {
	s.i.PrevKey()
	if s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: s.UnsafeKey().Key}) != nil {
		s.invalid = true
	}
}

// Key is part of the engine.Iterator interface.
func (s *Iterator) Key() engine.MVCCKey {
	return s.i.Key()
}

// Value is part of the engine.Iterator interface.
func (s *Iterator) Value() []byte {
	return s.i.Value()
}

// ValueProto is part of the engine.Iterator interface.
func (s *Iterator) ValueProto(msg protoutil.Message) error {
	return s.i.ValueProto(msg)
}

// UnsafeKey is part of the engine.Iterator interface.
func (s *Iterator) UnsafeKey() engine.MVCCKey {
	return s.i.UnsafeKey()
}

// UnsafeValue is part of the engine.Iterator interface.
func (s *Iterator) UnsafeValue() []byte {
	return s.i.UnsafeValue()
}

// ComputeStats is part of the engine.Iterator interface.
func (s *Iterator) ComputeStats(
	start, end engine.MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	if err := s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: start.Key, EndKey: end.Key}); err != nil {
		return enginepb.MVCCStats{}, err
	}
	return s.i.ComputeStats(start, end, nowNanos)
}

// FindSplitKey is part of the engine.Iterator interface.
func (s *Iterator) FindSplitKey(
	start, end, minSplitKey engine.MVCCKey, targetSize int64,
) (engine.MVCCKey, error) {
	if err := s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: start.Key, EndKey: end.Key}); err != nil {
		return engine.MVCCKey{}, err
	}
	return s.i.FindSplitKey(start, end, minSplitKey, targetSize)
}

// MVCCGet is part of the engine.Iterator interface.
func (s *Iterator) MVCCGet(
	key roachpb.Key, timestamp hlc.Timestamp, opts engine.MVCCGetOptions,
) (*roachpb.Value, *roachpb.Intent, error) {
	if err := s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key}); err != nil {
		return nil, nil, err
	}
	return s.i.MVCCGet(key, timestamp, opts)
}

// MVCCScan is part of the engine.Iterator interface.
func (s *Iterator) MVCCScan(
	start, end roachpb.Key, max int64, timestamp hlc.Timestamp, opts engine.MVCCScanOptions,
) (kvData [][]byte, numKVs int64, resumeSpan *roachpb.Span, intents []roachpb.Intent, err error) {
	if err := s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: start, EndKey: end}); err != nil {
		return nil, 0, nil, nil, err
	}
	return s.i.MVCCScan(start, end, max, timestamp, opts)
}

// SetUpperBound is part of the engine.Iterator interface.
func (s *Iterator) SetUpperBound(key roachpb.Key) {
	s.i.SetUpperBound(key)
}

type spanSetReader struct {
	r     engine.Reader
	spans *SpanSet
}

var _ engine.Reader = spanSetReader{}

func (s spanSetReader) Close() {
	s.r.Close()
}

func (s spanSetReader) Closed() bool {
	return s.r.Closed()
}

func (s spanSetReader) Get(key engine.MVCCKey) ([]byte, error) {
	if err := s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key.Key}); err != nil {
		return nil, err
	}
	//lint:ignore SA1019 implementing deprecated interface function (Get) is OK
	return s.r.Get(key)
}

func (s spanSetReader) GetProto(
	key engine.MVCCKey, msg protoutil.Message,
) (bool, int64, int64, error) {
	if err := s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: key.Key}); err != nil {
		return false, 0, 0, err
	}
	//lint:ignore SA1019 implementing deprecated interface function (GetProto) is OK
	return s.r.GetProto(key, msg)
}

func (s spanSetReader) Iterate(
	start, end engine.MVCCKey, f func(engine.MVCCKeyValue) (bool, error),
) error {
	if err := s.spans.CheckAllowed(SpanReadOnly, roachpb.Span{Key: start.Key, EndKey: end.Key}); err != nil {
		return err
	}
	return s.r.Iterate(start, end, f)
}

func (s spanSetReader) NewIterator(opts engine.IterOptions) engine.Iterator {
	return &Iterator{s.r.NewIterator(opts), s.spans, nil, false}
}

// GetDBEngine recursively searches for the underlying rocksDB engine.
func GetDBEngine(e engine.Reader, span roachpb.Span) engine.Reader {
	switch v := e.(type) {
	case ReadWriter:
		return GetDBEngine(getSpanReader(v, span), span)
	case *spanSetBatch:
		return GetDBEngine(getSpanReader(v.ReadWriter, span), span)
	default:
		return e
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
}

var _ engine.Writer = spanSetWriter{}

func (s spanSetWriter) ApplyBatchRepr(repr []byte, sync bool) error {
	// Assume that the constructor of the batch has bounded it correctly.
	return s.w.ApplyBatchRepr(repr, sync)
}

func (s spanSetWriter) Clear(key engine.MVCCKey) error {
	if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
		return err
	}
	return s.w.Clear(key)
}

func (s spanSetWriter) SingleClear(key engine.MVCCKey) error {
	if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
		return err
	}
	return s.w.SingleClear(key)
}

func (s spanSetWriter) ClearRange(start, end engine.MVCCKey) error {
	if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: start.Key, EndKey: end.Key}); err != nil {
		return err
	}
	return s.w.ClearRange(start, end)
}

func (s spanSetWriter) ClearIterRange(iter engine.Iterator, start, end engine.MVCCKey) error {
	if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: start.Key, EndKey: end.Key}); err != nil {
		return err
	}
	return s.w.ClearIterRange(iter, start, end)
}

func (s spanSetWriter) Merge(key engine.MVCCKey, value []byte) error {
	if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
		return err
	}
	return s.w.Merge(key, value)
}

func (s spanSetWriter) Put(key engine.MVCCKey, value []byte) error {
	if err := s.spans.CheckAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
		return err
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
		spanSetReader{
			r:     rw,
			spans: spans,
		},
		spanSetWriter{
			w:     rw,
			spans: spans,
		},
	}
}

// NewReadWriter returns an engine.ReadWriter that asserts access of the
// underlying ReadWriter against the given SpanSet.
func NewReadWriter(rw engine.ReadWriter, spans *SpanSet) engine.ReadWriter {
	return makeSpanSetReadWriter(rw, spans)
}

type spanSetBatch struct {
	ReadWriter
	b     engine.Batch
	spans *SpanSet
}

var _ engine.Batch = spanSetBatch{}

func (s spanSetBatch) Commit(sync bool) error {
	return s.b.Commit(sync)
}

func (s spanSetBatch) Distinct() engine.ReadWriter {
	return makeSpanSetReadWriter(s.b.Distinct(), s.spans)
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
// Batch against the given SpanSet.
func NewBatch(b engine.Batch, spans *SpanSet) engine.Batch {
	return &spanSetBatch{
		makeSpanSetReadWriter(b, spans),
		b,
		spans,
	}
}
