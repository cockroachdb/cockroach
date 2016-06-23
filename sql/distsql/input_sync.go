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

package distsql

import (
	"container/heap"

	"github.com/cockroachdb/cockroach/sql/sqlbase"
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

	// err can be set by the Less function (used by the heap implementation)
	err error

	alloc sqlbase.DatumAlloc
}

var _ RowSource = &orderedSynchronizer{}

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
		var err error
		src.row, err = src.src.NextRow()
		if err != nil {
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
	var err error
	src.row, err = src.src.NextRow()
	if err != nil {
		s.err = err
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

// NextRow is part of the RowSource interface.
func (s *orderedSynchronizer) NextRow() (sqlbase.EncDatumRow, error) {
	if !s.initialized {
		if err := s.initHeap(); err != nil {
			return nil, err
		}
		s.initialized = true
	} else {
		// Last row returned was from the source at the root of the heap; get
		// the next row for that source.
		if err := s.advanceRoot(); err != nil {
			return nil, err
		}
	}
	if len(s.heap) == 0 {
		return nil, nil
	}
	return s.sources[s.heap[0]].row, nil
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
