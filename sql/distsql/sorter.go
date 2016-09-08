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
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package distsql

import (
	"container/heap"
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/log"
)

// rowIdx refers to the index of a row inside a sqlbase.EncDatumRows array.
// This level of indirection is added so as to avoid copying/moving entire rows
// while sorting and instead operating on their positions within the array.
type rowIdx int

// sorter sorts the input rows according to the column ordering specified by
// 'ordering'. Note that this is a no-grouping aggregator and therefore it does
// not produce a global ordering but simply guarantees an intra-stream ordering
// on the physical output stream.
type sorter struct {
	input  RowSource
	output RowReceiver
	heap   []rowIdx
	ctx    context.Context

	// err can be set by the Less function, used by the heap implementation.
	err error

	ordering sqlbase.ColumnOrdering
	alloc    sqlbase.DatumAlloc
	rows     sqlbase.EncDatumRows
}

var _ processor = &sorter{}

// Len is part of heap.Interface and is only meant to be used internally.
func (s *sorter) Len() int {
	return len(s.heap)
}

// Less is part of heap.Interface and is only meant to be used internally.
func (s *sorter) Less(i, j int) bool {
	ri := s.rows[s.heap[i]]
	rj := s.rows[s.heap[j]]
	cmp, err := ri.Compare(&s.alloc, s.ordering, rj)
	if err != nil {
		s.err = err
		return false
	}
	return cmp < 0
}

// Swap is part of heap.Interface and is only meant to be used internally.
func (s *sorter) Swap(i, j int) {
	s.heap[i], s.heap[j] = s.heap[j], s.heap[i]
}

// Push is part of heap.Interface; it's not used as we never insert elements to
// the heap (we initialize it with all rows in initHeap).
func (s *sorter) Push(x interface{}) {
	panic("unimplemented")
}

// Pop is part of heap.Interface and is only meant to be used internally.
func (s *sorter) Pop() interface{} {
	x := s.heap[len(s.heap)-1]
	s.heap = s.heap[:len(s.heap)-1]
	return x
}

func (s *sorter) initHeap() error {
	for i := 0; ; i++ {
		row, err := s.input.NextRow()
		if err != nil {
			s.err = err
			return err
		}
		if row == nil {
			break
		}
		s.rows = append(s.rows, row)
		s.heap = append(s.heap, rowIdx(i))
	}

	heap.Init(s)

	// heap operations might set s.err (see Less)
	return s.err
}

func (s *sorter) nextRow() (sqlbase.EncDatumRow, error) {
	if len(s.heap) == 0 {
		return nil, nil
	}
	idx := heap.Pop(s).(rowIdx)
	return s.rows[idx], s.err
}

func (s *sorter) flush() error {
	for {
		outRow, err := s.nextRow()
		if err != nil {
			return err
		}
		if outRow == nil {
			return nil
		}

		if log.V(3) {
			log.Infof(s.ctx, "pushing row %s\n", outRow)
		}

		// Push the row to the output; stop if they don't need more rows.
		if !s.output.PushRow(outRow) {
			if log.V(2) {
				log.Infof(s.ctx, "no more rows required")
			}
			return nil
		}
	}
}

func newSorter(
	flowCtx *FlowCtx, spec *SorterSpec, input RowSource, output RowReceiver,
) *sorter {
	return &sorter{
		input:  input,
		output: output,
		ctx:    log.WithLogTag(flowCtx.Context, "Sorter", nil),

		ordering: convertToColumnOrdering(spec.Ordering),
	}
}

// Run is part of the processor interface.
func (s *sorter) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	if log.V(2) {
		log.Infof(s.ctx, "starting sorter run")
		defer log.Infof(s.ctx, "exiting sorter run")
	}

	if err := s.initHeap(); err != nil {
		log.Errorf(s.ctx, "heap initialization error: %s", err)
		s.output.Close(err)
	}

	if err := s.flush(); err != nil {
		log.Errorf(s.ctx, "error flushing results: %s", err)
		s.output.Close(err)
	}

	s.output.Close(nil)
}
