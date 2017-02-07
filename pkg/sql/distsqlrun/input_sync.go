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
//
// Input synchronizers are used by processors to merge incoming rows from
// (potentially) multiple streams; see docs/RFCS/distributed_sql.md

package distsqlrun

import (
	"container/heap"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

type srcInfo struct {
	src RowSource
	// row is the last row received from src.
	row sqlbase.EncDatumRow
}

// srcIdx refers to the index of a source inside a []srcInfo array.
type srcIdx int

// orderedSynchronizer receives rows from multiple streams and produces a single
// stream of rows, ordered according to a set of columns. The rows in each input
// stream are assumed to be ordered according to the same set of columns
// (intra-stream ordering).
type orderedSynchronizer struct {
	ordering sqlbase.ColumnOrdering

	sources []srcInfo

	// heap of source indexes, ordered by the current row. Sources with no more
	// rows are not in the heap.
	heap        []srcIdx
	initialized bool
	// needsAdvance is set when the row at the root of the heap has already been
	// consumed and thus producing a new row required the root to be advanced.
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

	// If draining is set, the orderedSynchronizer will ignore everything but
	// metadata records.
	draining bool
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
	cmp, err := si.row.Compare(&s.alloc, s.ordering, sj.row)
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
		if err := s.consumeMetadata(src); err != nil {
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

// consumeMetadata keeps reading from a source until the first data row.
// Metadata records are accumulated in s.metadata. If a metadata record with an
// error is encountered, further metadata is not consumed and the error is
// returned.
func (s *orderedSynchronizer) consumeMetadata(src *srcInfo) error {
	for {
		row, meta := src.src.NextRow()
		if meta.Err != nil {
			s.err = meta.Err
			return meta.Err
		}
		if !meta.Empty() {
			s.metadata = append(s.metadata, &meta)
			continue
		}
		src.row = row
		return nil
	}
}

// advanceRoot retrieves the next row for the source at the root of the heap and
// updates the heap accordingly.
func (s *orderedSynchronizer) advanceRoot() error {
	if len(s.heap) == 0 {
		return nil
	}
	src := &s.sources[s.heap[0]]
	if src.row == nil {
		panic("trying to advance closed source")
	}

	oldRow := src.row
	if err := s.consumeMetadata(src); err != nil {
		return err
	}

	if src.row == nil {
		heap.Remove(s, 0)
	} else {
		heap.Fix(s, 0)
		// TODO(radu): this check may be costly, we could disable it in production
		if cmp, err := oldRow.Compare(&s.alloc, s.ordering, src.row); err != nil {
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
func (s *orderedSynchronizer) drainSources() error {
	s.needsAdvance = true
	for len(s.heap) > 0 {
		if err := s.advanceRoot(); err != nil {
			return err
		}
	}
	return nil
}

// NextRow is part of the RowSource interface.
func (s *orderedSynchronizer) NextRow() (sqlbase.EncDatumRow, ProducerMetadata) {
	if !s.initialized {
		if err := s.initHeap(); err != nil {
			return nil, ProducerMetadata{Err: err}
		}
		s.initialized = true
	} else if s.needsAdvance {
		// Last row returned was from the source at the root of the heap; get
		// the next row for that source.
		if err := s.advanceRoot(); err != nil {
			return nil, ProducerMetadata{Err: err}
		}
	}
	if s.draining {
		// ConsumerDone() has put us in draining mode. We're going to clear the heap
		// and all subsequent NextRow() calls will return metadata records.
		s.draining = false
		if err := s.drainSources(); err != nil {
			return nil, ProducerMetadata{Err: err}
		}
		if len(s.heap) != 0 {
			panic("drainSources() didn't clear the heap")
		}
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
	s.draining = true
	s.consumerStatusChanged(RowSource.ConsumerDone)
}

// ConsumerClosed is part of the RowSource interface.
func (s *orderedSynchronizer) ConsumerClosed() {
	s.consumerStatusChanged(RowSource.ConsumerClosed)
}

// consumerStatusChanged calls a RowSource method on all the non-exhausted
// sources.
func (s *orderedSynchronizer) consumerStatusChanged(f func(RowSource)) {
	if !s.initialized {
		for i := range s.sources {
			f(s.sources[i].src)
		}
	} else {
		// The sources that are not in the heap have been consumed already. It would
		// be ok to call ConsumerDone() on them too, but avoiding the call may be a
		// bit faster (in most cases there should be no sources left).
		for _, sIdx := range s.heap {
			f(s.sources[sIdx].src)
		}
	}
}

func makeOrderedSync(ordering sqlbase.ColumnOrdering, sources []RowSource) (RowSource, error) {
	if len(sources) < 2 {
		return nil, errors.Errorf("only %d sources for ordered synchronizer", len(sources))
	}
	s := &orderedSynchronizer{
		sources:  make([]srcInfo, len(sources)),
		heap:     make([]srcIdx, 0, len(sources)),
		ordering: ordering,
	}
	for i := range s.sources {
		s.sources[i].src = sources[i]
	}
	return s, nil
}
