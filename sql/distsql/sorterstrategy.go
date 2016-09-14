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
	"errors"

	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/log"
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

// When re-initialized to a MinHeap it essentially pops all values in the heap,
// resulting in the inverted ordering being sorted in reverse. Therefore, the
// slice is ordered correctly in-place.
func (ss *sortTopKStrategy) Process() error {
	ss.sValues.InitMinHeap()
	return nil
}

// If we're scanning an index with a prefix matching an ordering prefix, we can
// only accumulate values for equal fields in this prefix, sort the accumulated
// chunk and then output.
type sortChunksStrategy struct {
	sortStrategyBase
	matchLen int
	alloc    sqlbase.DatumAlloc
}

var _ sorterStrategy = &sortChunksStrategy{}

func newSortChunksStrategy(sValues *sorterValues, matchLen int) sorterStrategy {
	return &sortChunksStrategy{
		sortStrategyBase: sortStrategyBase{
			sValues: sValues,
		},
		matchLen: matchLen,
	}
}

// The main execution loop here is by nature recursive, we traverse down the
// columns until we locate a 'chunk', defined to be a set of rows with a common
// prefix matching an ordering prefix, we sort the accumulated chunk and the
// output. We then repeat this procedure for the next chunk until there's none
// left. We additionally define a few terms here, 'depth' and
// 'pivot'/'pivoted'. For a table sorted by the first column:
//  (nil)
//      a, b
//      a, d
//      a, c
//      a, a
//      b, c
//      b, a
//      b, b
//      b, d
// We say the 1st column has a depth of zero, the 2nd column a depth of 1, etc.
// (zero indexed column numbers from the left). Given the first four rows share
// the common prefix matching an ordering prefix ('a'), we say that the first
// tuple to appear matching this criteria (a, b) is a pivot while the following
// three rows ((a, d), (a, c), (a, a)) are pivoted under (a, b) at depth 0.
// We should note that all rows are pivoted under the nil row at depth -1.
func (ss *sortChunksStrategy) Execute(s *sorter) error {
	// pivoted is a helper function to determine if row is pivoted under a
	// pivot row at a specified depth.
	pivoted := func(row, pivot sqlbase.EncDatumRow, depth int) bool {
		if depth < 0 {
			return true
		}
		cmp, err := row[depth].Compare(&ss.alloc, &pivot[depth])
		if err != nil {
			return false
		}

		return cmp == 0
	}

	// csort, chunky sort, buffers in all rows of the following chunk pivoted
	// at given pivot.
	var csort func(int, sqlbase.EncDatumRow) (sqlbase.EncDatumRow, error)
	csort = func(depth int, pivot sqlbase.EncDatumRow) (
		ret sqlbase.EncDatumRow,
		err error) {
		// If at max depth, we simply keep buffering all rows until we reach
		// row that is not pivoted under given pivot. Once we reach this point
		// (or end of table), we sort the contents of the current buffer and
		// push it out to the output streams.
		// At the end of our computation we will return the first row that was
		// not pivoted under given pivot.
		if depth == ss.matchLen {
			// We will push in the pivot first, then proceed to
			// recursively operate on all the rows pivoted under pivot.
			ss.Push(pivot)

			for {
				row, err := s.input.NextRow()
				if err != nil {
					return nil, err
				}
				if row == nil {
					ret = nil
					break
				}

				// If current row is not pivoted under the given pivot at the
				// previous column, we can stop adding rows to our buffer and
				// start processing. We will also return the non-pivoted row as
				// this now serves to be our next pivot.
				if !pivoted(row, pivot, depth-1) {
					ret = row
					break
				}
				ss.Push(row)
			}

			// Process all the rows that have been pushed onto the buffer.
			err = ss.Process()
			if err != nil {
				return nil, err
			}

			// Stream out sorted rows in order to row receiver.
			for {
				res := ss.Pop()
				if res == nil {
					break
				}

				if !s.output.PushRow(res) {
					break
				}
			}

			return ret, nil
		}
		// npivot here refers to our 'next' pivot, we immediately call
		// csort recursively in order to traverse as deep as we can.
		npivot, err := csort(depth+1, pivot)
		for {
			if err != nil {
				return nil, err
			}
			if npivot == nil {
				ret = nil
				break
			}

			// Given that we are not yet at max depth npivot here is a
			// nested pivot. If we have a table sorted by the 1st column
			// then the 2nd:
			//      a, b, c
			//      a, d, a
			//      a, d, t
			//      a, d, k
			// When operating on rows (a, d, a), (a, d, t), (a, d, k) and
			// at maximal depth 'a' is the outermost pivot and 'd' is the
			// nested pivot.
			// If the next pivot is not pivoted under the outer pivot at the
			// previous column, we will return the non-pivoted row as this
			// now serves to be our next pivot in the calling function scope.
			if !pivoted(npivot, pivot, depth-1) {
				ret = npivot
				break
			}
			npivot, err = csort(depth+1, npivot)
		}
		return ret, nil

	}

	first, err := s.input.NextRow()
	if err != nil {
		return err
	}
	if first == nil {
		// No rows to sort.
		return nil
	}

	last, err := csort(0, first)
	if err != nil {
		return err
	}
	if last != nil {
		return errors.New("unable to sort all rows")
	}

	return nil
}
