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

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

type sorterStrategy interface {
	// Execute runs the main execution loop of the strategy on a given sorter.
	Execute(*sorter) error

	// Push adds a single element to the strategy.
	Push(sqlbase.EncDatumRow)

	// Process sorts all the values that have been currently added to the
	// strategy. It suffices to call this only once unless you need to sort
	// batches of rows at a time (as is in the case of possible optimizations
	// made for partial column ordering matches).
	Process() error

	// Peek returns the value of the next element without removing it
	// from the strategy.
	//
	// Illegal to call if new elements have been added to the strategy since
	// the last call to Sort.
	Peek() sqlbase.EncDatumRow

	// Pop retrieves the next row whilst removing it from the strategy.
	// Returns a nil row if there are no more rows.
	//
	// Illegal to call if new elements have been added to the strategy since
	// the last call to Sort.
	Pop() sqlbase.EncDatumRow
}

// All rows for each sorting strategy are added to the wrapped sorterValues.
type sortStrategyBase struct {
	sValues *sorterValues
}

func (ss *sortStrategyBase) Push(row sqlbase.EncDatumRow) {
	ss.sValues.PushRow(row)
}

func (ss *sortStrategyBase) Process() error {
	return ss.sValues.Sort()
}

func (ss *sortStrategyBase) Peek() sqlbase.EncDatumRow {
	return ss.sValues.Peek()
}

func (ss *sortStrategyBase) Pop() sqlbase.EncDatumRow {
	return ss.sValues.PopRow()
}

// sortAllStrategy reads in all values into the wrapped sValues and
// uses sort.Sort to sort all values in-place. It has a worst-case time
// complexity of O(n*log(n)) and a worst-case space complexity of O(n).
//
// The strategy is intended to be used when all values need to be sorted.
type sortAllStrategy struct {
	sortStrategyBase
}

var _ sorterStrategy = &sortAllStrategy{}

func newSortAllStrategy(sValues *sorterValues) sorterStrategy {
	return &sortAllStrategy{
		sortStrategyBase: sortStrategyBase{
			sValues: sValues,
		},
	}
}

// The execution loop for the SortAll strategy is trivial in that it simply
// loads all rows into memory, runs sort.Sort to sort rows in place following
// which it sends each row out to the output stream.
func (ss *sortAllStrategy) Execute(s *sorter) error {
	for {
		row, err := s.input.NextRow()
		if err != nil {
			return err
		}
		if row == nil {
			break
		}
		ss.Push(row)
	}

	err := ss.Process()
	if err != nil {
		return err
	}

	for {
		row := ss.Pop()
		if row == nil {
			break
		}

		if log.V(3) {
			log.Infof(s.ctx, "pushing row %s\n", row)
		}

		// Push the row to the output; stop if they don't need more rows.
		if !s.output.PushRow(row) {
			if log.V(2) {
				log.Infof(s.ctx, "no more rows required")
			}
			break
		}
	}

	return nil
}

// sortTopKStrategy creates a max-heap in its wrapped sValues and keeps
// this heap populated with only the top k values seen. It accomplishes this
// by comparing new values (before the deep copy) with the top of the heap.
// If the new value is less than the current top, the top will be replaced
// and the heap will be fixed. If not, the new value is dropped. When finished,
// the max heap is converted to a min-heap effectively sorting the values
// correctly in-place. It has a worst-case time complexity of O(n*log(k)) and a
// worst-case space complexity of O(k).
//
// The strategy is intended to be used when exactly k values need to be sorted,
// where k is known before sorting begins.
//
// TODO(irfansharif): (taken from TODO found in sql/sort.go) There are better
// algorithms that can achieve a sorted top k in a worst-case time complexity
// of O(n + k*log(k)) while maintaining a worst-case space complexity of O(k).
// For instance, the top k can be found in linear time, and then this can be
// sorted in linearithmic time.
type sortTopKStrategy struct {
	sortStrategyBase
	k int64
}

var _ sorterStrategy = &sortTopKStrategy{}

func newSortTopKStrategy(sValues *sorterValues, k int64) sorterStrategy {
	ss := &sortTopKStrategy{
		sortStrategyBase: sortStrategyBase{
			sValues: sValues,
		},
		k: k,
	}
	ss.sValues.InitMaxHeap()

	return ss
}

// The execution loop for the SortTopK strategy is completely identical to that
// of the SortAll strategy, the key difference comes about in the Push
// implementation shown below.
func (ss *sortTopKStrategy) Execute(s *sorter) error {
	for {
		row, err := s.input.NextRow()
		if err != nil {
			return err
		}
		if row == nil {
			break
		}
		ss.Push(row)
	}

	err := ss.Process()
	if err != nil {
		return err
	}

	for {
		row := ss.Pop()
		if row == nil {
			break
		}

		if log.V(3) {
			log.Infof(s.ctx, "pushing row %s\n", row)
		}

		// Push the row to the output; stop if they don't need more rows.
		if !s.output.PushRow(row) {
			if log.V(2) {
				log.Infof(s.ctx, "no more rows required")
			}
			break
		}
	}

	return nil
}

func (ss *sortTopKStrategy) Push(row sqlbase.EncDatumRow) {
	switch {
	case int64(ss.sValues.Len()) < ss.k:
		// The first k values all go into the max-heap.
		ss.sValues.PushRow(row)
	case ss.sValues.RowLess(row, ss.Peek()):
		// Once the heap is full, only replace the top
		// value if a new value is less than it. If so
		// replace and fix the heap.
		ss.sValues.rows[0] = row
		heap.Fix(ss.sValues, 0)
	}
}

// If we're scanning an index with a prefix matching an ordering prefix, we only accumulate values
// for equal fields in this prefix, sort the accumulated chunk and then output.
type sortChunksStrategy struct {
	sortStrategyBase
	alloc sqlbase.DatumAlloc
}

var _ sorterStrategy = &sortChunksStrategy{}

func newSortChunksStrategy(sValues *sorterValues) sorterStrategy {
	return &sortChunksStrategy{
		sortStrategyBase: sortStrategyBase{
			sValues: sValues,
		},
	}
}

func (ss *sortChunksStrategy) Execute(s *sorter) error {
	// pivoted is a helper function that determines if the given row shares the same values for the
	// first s.matchLen ordering columns with the given pivot.
	pivoted := func(row, pivot sqlbase.EncDatumRow) (bool, error) {
		for _, ord := range s.ordering[:s.matchLen] {
			cmp, err := row[ord.ColIdx].Compare(&ss.alloc, &pivot[ord.ColIdx])
			if err != nil || cmp != 0 {
				return false, err
			}
		}
		return true, nil
	}

	nextRow, err := s.input.NextRow()
	if err != nil || nextRow == nil {
		return err
	}

	for {
		pivot := nextRow

		// We will accumulate rows to form a chunk such that they all share the same values
		// for the first s.matchLen ordering columns.
		for {
			if log.V(3) {
				log.Infof(s.ctx, "pushing row %s\n", nextRow)
			}
			ss.Push(nextRow)

			nextRow, err = s.input.NextRow()
			if err != nil {
				return err
			}
			if nextRow == nil {
				break
			}

			p, err := pivoted(nextRow, pivot)
			if err != nil {
				return err
			}
			if p {
				continue
			}

			// We verify if the nextRow here is infact 'greater' than pivot.
			if cmp, err := nextRow.Compare(&ss.alloc, s.ordering, pivot); err != nil {
				return err
			} else if cmp < 0 {
				return errors.Errorf("incorrectly ordered row %s before %s", pivot, nextRow)
			}
			break
		}

		// Process all the rows that have been pushed onto the buffer.
		err = ss.Process()
		if err != nil {
			return err
		}

		// Stream out sorted rows in order to row receiver.
		for {
			res := ss.Pop()
			if res == nil {
				break
			}

			// We don't need any more rows, we will clear out ss so to not hold on to that memory.
			if !s.output.PushRow(res) {
				ss = &sortChunksStrategy{}
				return nil
			}
		}

		// We've reached the end of the table.
		if nextRow == nil {
			if log.V(2) {
				log.Infof(s.ctx, "no more rows required")
			}
			break
		}
	}

	return nil
}
