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
	"sort"
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/log"
)

// sorter sorts the input rows according to the column ordering specified by
// 'ordering'. Note that this is a no-grouping aggregator and therefore it does
// not produce a global ordering but simply guarantees an intra-stream ordering
// on the physical output stream.
type sorter struct {
	input  RowSource
	output RowReceiver
	ctx    context.Context

	// This additional level of indirection is added so as to avoid having to
	// copy/move entire rows while sorting and instead operating on their
	// positions within the rows array.
	rowIndex []int

	// err can be set by the Less function.
	err error

	ordering sqlbase.ColumnOrdering
	alloc    sqlbase.DatumAlloc
	rows     sqlbase.EncDatumRows
}

var _ processor = &sorter{}
var _ sort.Interface = &sorter{}

// Len is part of sort.Interface and is only meant to be used internally.
func (s *sorter) Len() int {
	return len(s.rows)
}

// Less is part of sort.Interface and is only meant to be used internally.
func (s *sorter) Less(i, j int) bool {
	ri := s.rows[s.rowIndex[i]]
	rj := s.rows[s.rowIndex[j]]
	cmp, err := ri.Compare(&s.alloc, s.ordering, rj)
	if err != nil {
		s.err = err
		return false
	}

	return cmp < 0
}

// Swap is part of sort.Interface and is only meant to be used internally.
func (s *sorter) Swap(i, j int) {
	s.rowIndex[i], s.rowIndex[j] = s.rowIndex[j], s.rowIndex[i]
}

// sort buffers in all rows from the input RowSource into memory and sorts them
// by rearranging their corresponding indexes in rowIndex to reflect the new
// sorted ordering.
func (s *sorter) sort() error {
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
		s.rowIndex = append(s.rowIndex, i)
	}

	sort.Sort(s)
	return s.err
}

func (s *sorter) nextRow() (sqlbase.EncDatumRow, error) {
	if len(s.rowIndex) == 0 {
		return nil, nil
	}

	idx := s.rowIndex[0]
	s.rowIndex = s.rowIndex[1:]
	return s.rows[idx], s.err
}

// flush streams out the sorted rows in order to the output RowReceiver.
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

	if err := s.sort(); err != nil {
		log.Errorf(s.ctx, "error sorting rows: %s", err)
		s.output.Close(err)
	}

	if err := s.flush(); err != nil {
		log.Errorf(s.ctx, "error flushing results: %s", err)
		s.output.Close(err)
	}

	s.output.Close(nil)
}
