// Copyright 2016 The Cockroach Authors.
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
// permissions and limitations under the License.
//
// Author: Radu Berinde (radu@cockroachlabs.com)
// Author: Andrei Matei (andreimatei1@gmail.com)
//
// Input synchronizers are used by processors to merge incoming rows from
// (potentially) multiple streams; see docs/RFCS/distributed_sql.md

package distsqlrun

import (
	"container/heap"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

type srcInfo struct {
	src RowSource
	// row is the last row received from src.
	row sqlbase.EncDatumRow
}

// srcIdx refers to the index of a source inside a []srcInfo array.
type srcIdx int

type orderedSynchronizerState int

const (
	// notInitialized means that the heap has not yet been constructed. A row
	// needs to be read from each source to build the heap.
	notInitialized orderedSynchronizerState = iota
	// returningRows is the regular operation mode of the orderedSynchronizer.
	// Rows and metadata records are returning to the consumer.
	returningRows
	// draining means the orderedSynchronizer will ignore everything but metadata
	// records. On the first call to NextRow() while in draining mode, all the
	// sources are read until exhausted and metadata is accumulated. The state is
	// then transitioned to drained.
	draining
	// In the drainBuffered mode, all the sources of the orderedSynchronizer have been
	// exhausted, and we might have some buffered metadata. Metadata records are
	// going to be returned, one by one.
	drainBuffered
)

// orderedSynchronizer receives rows from multiple streams and produces a single
// stream of rows, ordered according to a set of columns. The rows in each input
// stream are assumed to be ordered according to the same set of columns
// (intra-stream ordering).
type orderedSynchronizer struct {
	ordering sqlbase.ColumnOrdering
	evalCtx  *parser.EvalContext

	sources []srcInfo

	// state dictates the operation mode.
	state orderedSynchronizerState

	// heap of source indexes, ordered by the current row. Sources with no more
	// rows are not in the heap.
	heap []srcIdx
	// needsAdvance is set when the row at the root of the heap has already been
	// consumed and thus producing a new row requires the root to be advanced.
	// This is usually set after a row is produced, but is not set when a metadata
	// row has just been produced, as that means that the heap is in good state to
	// serve the next row without advancing anything.
	needsAdvance bool

	// err can be set by the Less function (used by the heap implementation)
	err error

	alloc sqlbase.DatumAlloc

	// metadata is accumulated from all the sources and is passed on as soon as
	// possible.
	metadata []*ProducerMetadata
}

var _ RowSource = &orderedSynchronizer{}

// Types is part of the RowSource interface.
func (s *orderedSynchronizer) Types() []sqlbase.ColumnType {
	return s.sources[0].src.Types()
}

// Len is part of heap.Interface and is only meant to be used internally.
func (s *orderedSynchronizer) Len() int {
	return len(s.heap)
}

// Less is part of heap.Interface and is only meant to be used internally.
func (s *orderedSynchronizer) Less(i, j int) bool {
	si := &s.sources[s.heap[i]]
	sj := &s.sources[s.heap[j]]
	cmp, err := si.row.Compare(&s.alloc, s.ordering, s.evalCtx, sj.row)
	if err != nil {
		s.err = err
		return false
	}
	return cmp < 0
}

// Swap is part of heap.Interface and is only meant to be used internally.
func (s *orderedSynchronizer) Swap(i, j int) {
	s.heap[i], s.heap[j] = s.heap[j], s.heap[i]
}

// Push is part of heap.Interface; it's not used as we never insert elements to
// the heap (we initialize it with all sources, see initHeap).
func (s *orderedSynchronizer) Push(x interface{}) { panic("unimplemented") }

// Pop is part of heap.Interface and is only meant to be used internally.
func (s *orderedSynchronizer) Pop() interface{} {
	s.heap = s.heap[:len(s.heap)-1]
	return nil
}

// initHeap grabs a row from each source and initializes the heap.
func (s *orderedSynchronizer) initHeap() error {
	for i := range s.sources {
		src := &s.sources[i]
		if err := s.consumeMetadata(src, stopOnRowOrError); err != nil {
			return err
		}
		if src.row != nil {
			// Add to the heap array (it won't be a heap until we call heap.Init).
			s.heap = append(s.heap, srcIdx(i))
		}
	}
	heap.Init(s)
	// heap operations might set s.err (see Less)
	return s.err
}

type consumeMetadataOption int

const (
	// stopOnRowOrError means that consumeMetadata() will stop consuming as soon
	// as a row or metadata record with an error is received. The idea is to allow
	// this row to be placed in the heap, or for the error to be passed to the
	// consumer as soon as possible.
	stopOnRowOrError consumeMetadataOption = iota
	// drain means that we're going to fully consume the source, accumulating all
	// metadata records and ignoring all rows.
	drain
)

// consumeMetadata accumulates metadata from a source. Depending on mode, it
// will stop on the first row or error, or it will completely consume the
// source.
//
// In the stopOnRowOrError mode, src.row will be updated to the first row
// received (or to nil if src has been exhausted).
//
// Metadata records are accumulated in s.metadata. With the stopOnRowOrError
// mode, if a metadata record with an error is encountered, further metadata is
// not consumed and the error is returned. With the drain mode, metadata records
// with error are accumulated like all the others and this method doesn't return
// any errors.
func (s *orderedSynchronizer) consumeMetadata(src *srcInfo, mode consumeMetadataOption) error {
	for {
		row, meta := src.src.Next()
		if meta.Err != nil && mode == stopOnRowOrError {
			return meta.Err
		}
		if !meta.Empty() {
			s.metadata = append(s.metadata, &meta)
			continue
		}
		if mode == stopOnRowOrError {
			src.row = row
			return nil
		}
		if row == nil && meta.Empty() {
			return nil
		}
	}
}

// advanceRoot retrieves the next row for the source at the root of the heap and
// updates the heap accordingly.
//
// Metadata records from the source currently at the root are accumulated.
//
// If an error is returned, then either the heap is in a bad state (s.err has
// been set), or one of the sources is borked. In either case, advanceRoot()
// should not be called again - the caller should update the
// orderedSynchronizer.state accordingly.
func (s *orderedSynchronizer) advanceRoot() error {
	if s.state != returningRows {
		return errors.Errorf("advanceRoot() called in unsupported state: %d", s.state)
	}
	if len(s.heap) == 0 {
		return nil
	}
	src := &s.sources[s.heap[0]]
	if src.row == nil {
		return errors.Errorf("trying to advance closed source")
	}

	oldRow := src.row
	if err := s.consumeMetadata(src, stopOnRowOrError); err != nil {
		return err
	}

	if src.row == nil {
		heap.Remove(s, 0)
	} else {
		heap.Fix(s, 0)
		// TODO(radu): this check may be costly, we could disable it in production
		if cmp, err := oldRow.Compare(&s.alloc, s.ordering, s.evalCtx, src.row); err != nil {
			return err
		} else if cmp > 0 {
			return errors.Errorf("incorrectly ordered stream %s after %s", src.row, oldRow)
		}
	}
	// heap operations might set s.err (see Less)
	return s.err
}

// drainSources consumes all the rows from the sources. All the data is
// discarded, except the metadata records which are accumulated in s.metadata.
func (s *orderedSynchronizer) drainSources() {
	for _, srcIdx := range s.heap {
		if err := s.consumeMetadata(&s.sources[srcIdx], drain); err != nil {
			log.Fatalf(context.TODO(), "unexpected draining error: %s", err)
		}
	}
}

// Next is part of the RowSource interface.
func (s *orderedSynchronizer) Next() (sqlbase.EncDatumRow, ProducerMetadata) {
	if s.state == notInitialized {
		if err := s.initHeap(); err != nil {
			s.ConsumerDone()
			return nil, ProducerMetadata{Err: err}
		}
		s.state = returningRows
	} else if s.state == returningRows && s.needsAdvance {
		// Last row returned was from the source at the root of the heap; get
		// the next row for that source.
		if err := s.advanceRoot(); err != nil {
			s.ConsumerDone()
			return nil, ProducerMetadata{Err: err}
		}
	}

	if s.state == draining {
		// ConsumerDone(), or an error, has put us in draining mode. All subsequent
		// Next() calls will return metadata records.
		s.drainSources()
		s.state = drainBuffered
		s.heap = nil
	}

	if len(s.metadata) != 0 {
		// TODO(andrei): We return the metadata records one by one. The interface
		// should support returning all of them at once.
		var meta *ProducerMetadata
		meta, s.metadata = s.metadata[0], s.metadata[1:]
		s.needsAdvance = false
		return nil, *meta
	}

	if len(s.heap) == 0 {
		return nil, ProducerMetadata{}
	}

	s.needsAdvance = true
	return s.sources[s.heap[0]].row, ProducerMetadata{}
}

// ConsumerDone is part of the RowSource interface.
func (s *orderedSynchronizer) ConsumerDone() {
	// We're entering draining mode. Only metadata will be forwarded from now on.
	if s.state != draining {
		s.state = draining
		s.consumerStatusChanged(RowSource.ConsumerDone)
	}
}

// ConsumerClosed is part of the RowSource interface.
func (s *orderedSynchronizer) ConsumerClosed() {
	// The state should matter, as no further methods should be called, but we'll
	// set it to something other than the default.
	s.state = drainBuffered
	s.consumerStatusChanged(RowSource.ConsumerClosed)
}

// consumerStatusChanged calls a RowSource method on all the non-exhausted
// sources.
func (s *orderedSynchronizer) consumerStatusChanged(f func(RowSource)) {
	if s.state == notInitialized {
		for i := range s.sources {
			f(s.sources[i].src)
		}
	} else {
		// The sources that are not in the heap have been consumed already. It would
		// be ok to call ConsumerDone()/ConsumerClosed() on them too, but avoiding
		// the call may be a bit faster (in most cases there should be no sources
		// left).
		for _, sIdx := range s.heap {
			f(s.sources[sIdx].src)
		}
	}
}

func makeOrderedSync(
	ordering sqlbase.ColumnOrdering, evalCtx *parser.EvalContext, sources []RowSource,
) (RowSource, error) {
	if len(sources) < 2 {
		return nil, errors.Errorf("only %d sources for ordered synchronizer", len(sources))
	}
	s := &orderedSynchronizer{
		state:    notInitialized,
		sources:  make([]srcInfo, len(sources)),
		heap:     make([]srcIdx, 0, len(sources)),
		ordering: ordering,
		evalCtx:  evalCtx,
	}
	for i := range s.sources {
		s.sources[i].src = sources[i]
	}
	return s, nil
}
