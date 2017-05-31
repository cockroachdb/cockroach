// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Ben Darnell

package storage

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

// SpanAccess records the intended mode of access in SpanSet.
type SpanAccess int

// Constants for SpanAccess. Higher-valued accesses imply lower-level ones.
const (
	SpanReadOnly SpanAccess = iota
	SpanReadWrite
	numSpanAccess
)

type spanScope int

const (
	spanGlobal spanScope = iota
	spanLocal
	numSpanScope
)

// SpanSet tracks the set of key spans touched by a command. The set
// is divided into subsets for access type (read-only or read/write)
// and key scope (local or global; used to facilitate use by the
// separate local and global command queues).
type SpanSet struct {
	spans [numSpanAccess][numSpanScope][]roachpb.Span
}

func (ss *SpanSet) len() int {
	var count int
	for i := SpanAccess(0); i < numSpanAccess; i++ {
		for j := spanScope(0); j < numSpanScope; j++ {
			count += len(ss.getSpans(i, j))
		}
	}
	return count
}

// reserve space for N additional keys.
func (ss *SpanSet) reserve(access SpanAccess, scope spanScope, n int) {
	existing := ss.spans[access][scope]
	ss.spans[access][scope] = make([]roachpb.Span, len(existing), n+cap(existing))
	copy(ss.spans[access][scope], existing)
}

// Add a span to the set.
func (ss *SpanSet) Add(access SpanAccess, span roachpb.Span) {
	scope := spanGlobal
	if keys.IsLocal(span.Key) {
		scope = spanLocal
	}
	ss.spans[access][scope] = append(ss.spans[access][scope], span)
}

// getSpans returns a slice of spans with the given parameters.
func (ss *SpanSet) getSpans(access SpanAccess, scope spanScope) []roachpb.Span {
	return ss.spans[access][scope]
}

func (ss *SpanSet) checkAllowed(access SpanAccess, span roachpb.Span) error {
	scope := spanGlobal
	if keys.IsLocal(span.Key) {
		scope = spanLocal
	}
	for ac := access; ac < numSpanAccess; ac++ {
		for _, s := range ss.spans[ac][scope] {
			if s.Key.Equal(keys.LocalMax) && s.EndKey.Equal(roachpb.KeyMax) {
				continue
			}
			if s.Contains(span) {
				return nil
			}
		}
	}
	action := "read"
	if access == SpanReadWrite {
		action = "write"
	}

	return errors.Errorf("cannot %s undeclared span %s", action, span)
}

// validate returns an error if any spans that have been added to the set
// are invalid.
func (ss *SpanSet) validate() error {
	for _, accessSpans := range ss.spans {
		for _, spans := range accessSpans {
			for _, span := range spans {
				if len(span.EndKey) > 0 && span.Key.Compare(span.EndKey) >= 0 {
					return errors.Errorf("inverted span %s %s", span.Key, span.EndKey)
				}
			}
		}
	}
	return nil
}

// SpanSetIterator wraps an engine.Iterator and ensures that it can
// only be used to access spans in a SpanSet.
type SpanSetIterator struct {
	i     engine.Iterator
	spans *SpanSet

	// Seeking to an invalid key puts the iterator in an error state.
	err error
	// Reaching an out-of-bounds key with Next/Prev invalidates the
	// iterator but does not set err.
	invalid bool
}

var _ engine.Iterator = &SpanSetIterator{}

// Close implements engine.Iterator.
func (s *SpanSetIterator) Close() {
	s.i.Close()
}

// Iterator returns the underlying engine.Iterator.
func (s *SpanSetIterator) Iterator() engine.Iterator {
	return s.i
}

// Seek implements engine.Iterator.
func (s *SpanSetIterator) Seek(key engine.MVCCKey) {
	s.err = s.spans.checkAllowed(SpanReadOnly, roachpb.Span{Key: key.Key})
	if s.err == nil {
		s.invalid = false
	}
	s.i.Seek(key)
}

// SeekReverse implements engine.Iterator.
func (s *SpanSetIterator) SeekReverse(key engine.MVCCKey) {
	s.err = s.spans.checkAllowed(SpanReadOnly, roachpb.Span{Key: key.Key})
	if s.err == nil {
		s.invalid = false
	}
	s.i.SeekReverse(key)
}

// Valid implements engine.Iterator.
func (s *SpanSetIterator) Valid() (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	ok, err := s.i.Valid()
	if err != nil {
		return false, s.err
	}
	return ok && !s.invalid, nil
}

// Next implements engine.Iterator.
func (s *SpanSetIterator) Next() {
	s.i.Next()
	if s.spans.checkAllowed(SpanReadOnly, roachpb.Span{Key: s.UnsafeKey().Key}) != nil {
		s.invalid = true
	}
}

// Prev implements engine.Iterator.
func (s *SpanSetIterator) Prev() {
	s.i.Prev()
	if s.spans.checkAllowed(SpanReadOnly, roachpb.Span{Key: s.UnsafeKey().Key}) != nil {
		s.invalid = true
	}
}

// NextKey implements engine.Iterator.
func (s *SpanSetIterator) NextKey() {
	s.i.NextKey()
	if s.spans.checkAllowed(SpanReadOnly, roachpb.Span{Key: s.UnsafeKey().Key}) != nil {
		s.invalid = true
	}
}

// PrevKey implements engine.Iterator.
func (s *SpanSetIterator) PrevKey() {
	s.i.PrevKey()
	if s.spans.checkAllowed(SpanReadOnly, roachpb.Span{Key: s.UnsafeKey().Key}) != nil {
		s.invalid = true
	}
}

// Key implements engine.Iterator.
func (s *SpanSetIterator) Key() engine.MVCCKey {
	return s.i.Key()
}

// Value implements engine.Iterator.
func (s *SpanSetIterator) Value() []byte {
	return s.i.Value()
}

// ValueProto implements engine.Iterator.
func (s *SpanSetIterator) ValueProto(msg proto.Message) error {
	return s.i.ValueProto(msg)
}

// UnsafeKey implements engine.Iterator.
func (s *SpanSetIterator) UnsafeKey() engine.MVCCKey {
	return s.i.UnsafeKey()
}

// UnsafeValue implements engine.Iterator.
func (s *SpanSetIterator) UnsafeValue() []byte {
	return s.i.UnsafeValue()
}

// Less implements engine.Iterator.
func (s *SpanSetIterator) Less(key engine.MVCCKey) bool {
	return s.i.Less(key)
}

// ComputeStats implements engine.Iterator.
func (s *SpanSetIterator) ComputeStats(
	start, end engine.MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	if err := s.spans.checkAllowed(SpanReadOnly, roachpb.Span{Key: start.Key, EndKey: end.Key}); err != nil {
		return enginepb.MVCCStats{}, err
	}
	return s.i.ComputeStats(start, end, nowNanos)
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
	if err := s.spans.checkAllowed(SpanReadOnly, roachpb.Span{Key: key.Key}); err != nil {
		return nil, err
	}
	return s.r.Get(key)
}

func (s spanSetReader) GetProto(key engine.MVCCKey, msg proto.Message) (bool, int64, int64, error) {
	if err := s.spans.checkAllowed(SpanReadOnly, roachpb.Span{Key: key.Key}); err != nil {
		return false, 0, 0, err
	}
	return s.r.GetProto(key, msg)
}

func (s spanSetReader) Iterate(
	start, end engine.MVCCKey, f func(engine.MVCCKeyValue) (bool, error),
) error {
	if err := s.spans.checkAllowed(SpanReadOnly, roachpb.Span{Key: start.Key, EndKey: end.Key}); err != nil {
		return err
	}
	return s.r.Iterate(start, end, f)
}

func (s spanSetReader) NewIterator(prefix bool) engine.Iterator {
	return &SpanSetIterator{s.r.NewIterator(prefix), s.spans, nil, false}
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
	if err := s.spans.checkAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
		return err
	}
	return s.w.Clear(key)
}

func (s spanSetWriter) ClearRange(start, end engine.MVCCKey) error {
	if err := s.spans.checkAllowed(SpanReadWrite, roachpb.Span{Key: start.Key, EndKey: end.Key}); err != nil {
		return err
	}
	return s.w.ClearRange(start, end)
}

func (s spanSetWriter) ClearIterRange(iter engine.Iterator, start, end engine.MVCCKey) error {
	if err := s.spans.checkAllowed(SpanReadWrite, roachpb.Span{Key: start.Key, EndKey: end.Key}); err != nil {
		return err
	}
	return s.w.ClearIterRange(iter, start, end)
}

func (s spanSetWriter) Merge(key engine.MVCCKey, value []byte) error {
	if err := s.spans.checkAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
		return err
	}
	return s.w.Merge(key, value)
}

func (s spanSetWriter) Put(key engine.MVCCKey, value []byte) error {
	if err := s.spans.checkAllowed(SpanReadWrite, roachpb.Span{Key: key.Key}); err != nil {
		return err
	}
	return s.w.Put(key, value)
}

type spanSetReadWriter struct {
	spanSetReader
	spanSetWriter
}

var _ engine.ReadWriter = spanSetReadWriter{}

func makeSpanSetReadWriter(rw engine.ReadWriter, spans *SpanSet) spanSetReadWriter {
	return spanSetReadWriter{
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

type spanSetBatch struct {
	spanSetReadWriter
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

func (s spanSetBatch) Repr() []byte {
	return s.b.Repr()
}

func makeSpanSetBatch(b engine.Batch, spans *SpanSet) engine.Batch {
	return &spanSetBatch{
		makeSpanSetReadWriter(b, spans),
		b,
		spans,
	}
}
