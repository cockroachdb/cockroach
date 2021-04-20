// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// Input synchronizers are used by processors to merge incoming rows from
// (potentially) multiple streams; see docs/RFCS/distributed_sql.md

package rowflow

import (
	"container/heap"
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type srcInfo struct {
	src execinfra.RowSource
	// row is the last row received from src.
	row rowenc.EncDatumRow
}

// srcIdx refers to the index of a source inside a []srcInfo array.
type srcIdx int

type serialSynchronizerState int

const (
	// notInitialized means that the heap has not yet been constructed. A row
	// needs to be read from each source to build the heap.
	notInitialized serialSynchronizerState = iota
	// returningRows is the regular mode of operation.
	// Rows and metadata records are returning to the consumer.
	returningRows
	// draining means the synchronizer will ignore everything but metadata
	// records. On the first call to NextRow() while in draining mode, all the
	// sources are read until exhausted and metadata is accumulated. The state is
	// then transitioned to drained.
	draining
	// In the drainBuffered mode, all the sources of the serialOrderedSynchronizer
	// have been exhausted, and we might have some buffered metadata. Metadata
	// records are going to be returned, one by one.   serialUnorderedSynchronizer
	// doesn't buffer metadata and doesn't use this.
	drainBuffered
)

// Let setupProcessors peek at sources w/o caring about type of synchronizer.
type serialSynchronizer interface {
	getSources() []srcInfo
}

type serialSynchronizerBase struct {
	state serialSynchronizerState

	types    []*types.T
	sources  []srcInfo
	rowAlloc rowenc.EncDatumRowAlloc
}

var _ serialSynchronizer = &serialSynchronizerBase{}

func (s *serialSynchronizerBase) getSources() []srcInfo {
	return s.sources
}

// Start is part of the RowSource interface.
func (s *serialSynchronizerBase) Start(ctx context.Context) {
	for _, src := range s.sources {
		src.src.Start(ctx)
	}
}

// OutputTypes is part of the RowSource interface.
func (s *serialSynchronizerBase) OutputTypes() []*types.T {
	return s.types
}

// serialOrderedSynchronizer receives rows from multiple streams and produces a single
// stream of rows, ordered according to a set of columns. The rows in each input
// stream are assumed to be ordered according to the same set of columns
// (intra-stream ordering).
type serialOrderedSynchronizer struct {
	serialSynchronizerBase

	// ordering dictates the way in which rows compare. This can't be nil.
	ordering colinfo.ColumnOrdering

	evalCtx *tree.EvalContext
	// heap of source indexes. In state notInitialized, heap holds all source
	// indexes. Once initialized (initHeap is called), heap will be ordered by the
	// current row from each source and will only contain source indexes of
	// sources that are not done.
	heap []srcIdx
	// needsAdvance is set when the row at the root of the heap has already been
	// consumed and thus producing a new row requires the root to be advanced.
	// This is usually set after a row is produced, but is not set when a metadata
	// row has just been produced, as that means that the heap is in good state to
	// serve the next row without advancing anything.
	needsAdvance bool

	// err can be set by the Less function (used by the heap implementation)
	err error

	alloc rowenc.DatumAlloc

	// metadata is accumulated from all the sources and is passed on as soon as
	// possible.
	metadata []*execinfrapb.ProducerMetadata
}

var _ execinfra.RowSource = &serialOrderedSynchronizer{}

// Len is part of heap.Interface and is only meant to be used internally.
func (s *serialOrderedSynchronizer) Len() int {
	return len(s.heap)
}

// Less is part of heap.Interface and is only meant to be used internally.
func (s *serialOrderedSynchronizer) Less(i, j int) bool {
	si := &s.sources[s.heap[i]]
	sj := &s.sources[s.heap[j]]
	cmp, err := si.row.Compare(s.types, &s.alloc, s.ordering, s.evalCtx, sj.row)
	if err != nil {
		s.err = err
		return false
	}
	return cmp < 0
}

// Swap is part of heap.Interface and is only meant to be used internally.
func (s *serialOrderedSynchronizer) Swap(i, j int) {
	s.heap[i], s.heap[j] = s.heap[j], s.heap[i]
}

// Push is part of heap.Interface; it's not used as we never insert elements to
// the heap (we initialize it with all sources, see initHeap).
func (s *serialOrderedSynchronizer) Push(x interface{}) { panic("unimplemented") }

// Pop is part of heap.Interface and is only meant to be used internally.
func (s *serialOrderedSynchronizer) Pop() interface{} {
	s.heap = s.heap[:len(s.heap)-1]
	return nil
}

// initHeap grabs a row from each source and initializes the heap. Any given
// source will be on the heap (even if an error was encountered while reading
// from it) unless there are no more rows to read from it.
// If an error is returned, heap.Init() has not been called, so s.heap is not
// an actual heap. In this case, all members of the heap need to be drained.
func (s *serialOrderedSynchronizer) initHeap() error {
	// consumeErr is the last error encountered while consuming metadata.
	var consumeErr error

	// s.heap contains all the sources. Go through the sources and get the first
	// row from each source, removing them if they're already finished.
	toDelete := 0
	for i, srcIdx := range s.heap {
		src := &s.sources[srcIdx]
		err := s.consumeMetadata(src, stopOnRowOrError)
		if err != nil {
			consumeErr = err
		}
		if src.row == nil && err == nil {
			// Source is done. Swap current element with the first of the valid
			// sources then truncate array after the loop so that i is still valid.
			s.heap[toDelete], s.heap[i] = s.heap[i], s.heap[toDelete]
			toDelete++
		}
	}
	s.heap = s.heap[toDelete:]
	if consumeErr != nil {
		return consumeErr
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
func (s *serialOrderedSynchronizer) consumeMetadata(
	src *srcInfo, mode consumeMetadataOption,
) error {
	for {
		row, meta := src.src.Next()
		if meta != nil {
			if meta.Err != nil && mode == stopOnRowOrError {
				return meta.Err
			}
			s.metadata = append(s.metadata, meta)
			continue
		}
		if mode == stopOnRowOrError {
			if row != nil {
				row = s.rowAlloc.CopyRow(row)
			}
			src.row = row
			return nil
		}
		if row == nil && meta == nil {
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
// serialOrderedSynchronizer.state accordingly.
func (s *serialOrderedSynchronizer) advanceRoot() error {
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
		if cmp, err := oldRow.Compare(s.types, &s.alloc, s.ordering, s.evalCtx, src.row); err != nil {
			return err
		} else if cmp > 0 {
			return errors.Errorf(
				"incorrectly ordered stream %s after %s (ordering: %v)",
				src.row.String(s.types), oldRow.String(s.types), s.ordering,
			)
		}
	}
	// heap operations might set s.err (see Less)
	return s.err
}

// drainSources consumes all the rows from the sources. All the data is
// discarded, except the metadata records which are accumulated in s.metadata.
func (s *serialOrderedSynchronizer) drainSources() {
	for _, srcIdx := range s.heap {
		if err := s.consumeMetadata(&s.sources[srcIdx], drain); err != nil {
			log.Fatalf(context.TODO(), "unexpected draining error: %s", err)
		}
	}
}

// Next is part of the RowSource interface.
func (s *serialOrderedSynchronizer) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if s.state == notInitialized {
		if err := s.initHeap(); err != nil {
			s.ConsumerDone()
			return nil, &execinfrapb.ProducerMetadata{Err: err}
		}
		s.state = returningRows
	} else if s.state == returningRows && s.needsAdvance {
		// Last row returned was from the source at the root of the heap; get
		// the next row for that source.
		if err := s.advanceRoot(); err != nil {
			s.ConsumerDone()
			return nil, &execinfrapb.ProducerMetadata{Err: err}
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
		var meta *execinfrapb.ProducerMetadata
		meta, s.metadata = s.metadata[0], s.metadata[1:]
		s.needsAdvance = false
		return nil, meta
	}

	if len(s.heap) == 0 {
		return nil, nil
	}

	s.needsAdvance = true
	return s.sources[s.heap[0]].row, nil
}

// ConsumerDone is part of the RowSource interface.
func (s *serialOrderedSynchronizer) ConsumerDone() {
	// We're entering draining mode. Only metadata will be forwarded from now on.
	if s.state != draining {
		s.consumerStatusChanged(draining, execinfra.RowSource.ConsumerDone)
	}
}

// ConsumerClosed is part of the RowSource interface.
func (s *serialOrderedSynchronizer) ConsumerClosed() {
	// The state shouldn't matter, as no further methods should be called, but
	// we'll set it to something other than the default.
	s.consumerStatusChanged(drainBuffered, execinfra.RowSource.ConsumerClosed)
}

// consumerStatusChanged calls a RowSource method on all the non-exhausted
// sources.
func (s *serialOrderedSynchronizer) consumerStatusChanged(
	newState serialSynchronizerState, f func(execinfra.RowSource),
) {
	if s.state == notInitialized {
		for i := range s.sources {
			f(s.sources[i].src)
		}
	} else {
		// The sources that are not in the heap have been consumed already. It
		// would be ok to call ConsumerDone()/ConsumerClosed() on them too, but
		// avoiding the call may be a bit faster (in most cases there should be
		// no sources left).
		for _, sIdx := range s.heap {
			f(s.sources[sIdx].src)
		}
	}
	s.state = newState
}

// serialUnorderedSynchronizer exhausts its sources one at a time until
// each is drained.  It's necessary to have the row engine support locality
// optimized scans.
type serialUnorderedSynchronizer struct {
	serialSynchronizerBase

	srcIndex int
}

var _ execinfra.RowSource = &serialUnorderedSynchronizer{}

func (u *serialUnorderedSynchronizer) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for u.srcIndex < len(u.sources) {
		row, metadata := u.sources[u.srcIndex].src.Next()

		// If we're draining only return metadata.
		if u.state == draining && row != nil {
			continue
		}

		// If we see nil,nil go to next source.
		if row == nil && metadata == nil {
			u.srcIndex++
			continue
		}

		if row != nil {
			row = u.rowAlloc.CopyRow(row)
		}

		return row, metadata
	}

	return nil, nil
}

// ConsumerDone is part of the RowSource interface.
func (u *serialUnorderedSynchronizer) ConsumerDone() {
	if u.state != draining {
		for i := range u.sources {
			u.sources[i].src.ConsumerDone()
		}
		u.state = draining
	}
}

// ConsumerClosed is part of the RowSource interface.
func (u *serialUnorderedSynchronizer) ConsumerClosed() {
	for i := range u.sources {
		u.sources[i].src.ConsumerClosed()
	}
}

// makeSerialSync creates a serialOrderedSynchronizer or a
// serialUnorderedSynchronizer. ordering dictates how rows are to be compared.
// Use colinfo.NoOrdering to indicate that the row ordering doesn't matter and
// sources should be consumed in index order (which is useful when you intend to
// fuse the synchronizer and its inputs later; see FuseAggressively).
func makeSerialSync(
	ordering colinfo.ColumnOrdering, evalCtx *tree.EvalContext, sources []execinfra.RowSource,
) (execinfra.RowSource, error) {
	if len(sources) < 2 {
		return nil, errors.Errorf("only %d sources for serial synchronizer", len(sources))
	}

	base := serialSynchronizerBase{
		state:   notInitialized,
		sources: make([]srcInfo, len(sources)),
		types:   sources[0].OutputTypes(),
	}

	for i := range base.sources {
		base.sources[i].src = sources[i]
	}

	var sync execinfra.RowSource

	if len(ordering) > 0 {
		os := &serialOrderedSynchronizer{
			serialSynchronizerBase: base,
			heap:                   make([]srcIdx, 0, len(sources)),
			ordering:               ordering,
			evalCtx:                evalCtx,
		}
		for i := range os.sources {
			os.heap = append(os.heap, srcIdx(i))
		}
		sync = os
	} else {
		sync = &serialUnorderedSynchronizer{
			serialSynchronizerBase: base,
		}
	}

	return sync, nil
}
