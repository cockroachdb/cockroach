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

package distsqlrun

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// sorterStrategy is an interface implemented by structs that know how to sort
// rows on behalf of a sorter processor.
type sorterStrategy interface {
	// Execute performs a sorter processor's sorting work. Rows are read from the
	// sorter's input and, after being sorted, passed to the sorter's
	// post-processing stage.
	//
	// It returns once either all the input has been exhausted or the consumer
	// indicated that no more rows are needed. In any case, the caller is
	// responsible for draining and closing the producer and the consumer.
	Execute(context.Context, *sorter) error
}

// sortAllStrategy reads in all values into the wrapped rows and
// uses sort.Sort to sort all values in-place. It has a worst-case time
// complexity of O(n*log(n)) and a worst-case space complexity of O(n).
//
// The strategy is intended to be used when all values need to be sorted.
type sortAllStrategy struct {
	rows rowContainer
}

var _ sorterStrategy = &sortAllStrategy{}

func newSortAllStrategy(rows rowContainer) sorterStrategy {
	return &sortAllStrategy{
		rows: rows,
	}
}

// The execution loop for the SortAll strategy:
//  - loads all rows into memory;
//  - runs sort.Sort to sort rows in place;
//  - sends each row out to the output stream.
func (ss *sortAllStrategy) Execute(ctx context.Context, s *sorter) error {
	defer ss.rows.Close(ctx)
	for {
		row, err := s.input.NextRow()
		if err != nil {
			return err
		}
		if row == nil {
			break
		}
		if err := ss.rows.AddRow(ctx, row); err != nil {
			return err
		}
	}
	ss.rows.Sort()

	for ss.rows.Len() > 0 {
		// Push the row to the output; stop if they don't need more rows.
		consumerStatus, err := s.out.emitRow(ctx, ss.rows.EncRow(0))
		if err != nil || consumerStatus != NeedMoreRows {
			return err
		}
		ss.rows.PopFirst()
	}
	return nil
}

// sortTopKStrategy creates a max-heap in its wrapped rows and keeps
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
	rows rowContainer
	k    int64
}

var _ sorterStrategy = &sortTopKStrategy{}

func newSortTopKStrategy(rows rowContainer, k int64) sorterStrategy {
	ss := &sortTopKStrategy{
		rows: rows,
		k:    k,
	}

	return ss
}

// The execution loop for the SortTopK strategy is similar to that of the
// SortAll strategy; the difference is that we push rows into a max-heap of size
// at most K, and only sort those.
func (ss *sortTopKStrategy) Execute(ctx context.Context, s *sorter) error {
	defer ss.rows.Close(ctx)
	heapCreated := false
	for {
		row, err := s.input.NextRow()
		if err != nil {
			return err
		}
		if row == nil {
			break
		}

		if int64(ss.rows.Len()) < ss.k {
			// Accumulate up to k values.
			if err := ss.rows.AddRow(ctx, row); err != nil {
				return err
			}
		} else {
			if !heapCreated {
				// Arrange the k values into a max-heap.
				ss.rows.InitMaxHeap()
				heapCreated = true
			}
			// Replace the max value if the new row is smaller, maintaining the
			// max-heap.
			if err := ss.rows.MaybeReplaceMax(row); err != nil {
				return err
			}
		}
	}

	ss.rows.Sort()

	for ss.rows.Len() > 0 {
		// Push the row to the output; stop if they don't need more rows.
		consumerStatus, err := s.out.emitRow(ctx, ss.rows.EncRow(0))
		if err != nil || consumerStatus != NeedMoreRows {
			return err
		}
		ss.rows.PopFirst()
	}
	return nil
}

// If we're scanning an index with a prefix matching an ordering prefix, we only accumulate values
// for equal fields in this prefix, sort the accumulated chunk and then output.
type sortChunksStrategy struct {
	rows  rowContainer
	alloc sqlbase.DatumAlloc
}

var _ sorterStrategy = &sortChunksStrategy{}

func newSortChunksStrategy(rows rowContainer) sorterStrategy {
	return &sortChunksStrategy{
		rows: rows,
	}
}

func (ss *sortChunksStrategy) Execute(ctx context.Context, s *sorter) error {
	defer ss.rows.Close(ctx)
	// pivoted is a helper function that determines if the given row shares the same values for the
	// first s.matchLen ordering columns with the given pivot.
	pivoted := func(row, pivot sqlbase.EncDatumRow) (bool, error) {
		for _, ord := range s.ordering[:s.matchLen] {
			cmp, err := row[ord.ColIdx].Compare(&ss.alloc, ss.rows.evalCtx, &pivot[ord.ColIdx])
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
				log.Infof(ctx, "pushing row %s", nextRow)
			}
			if err := ss.rows.AddRow(ctx, nextRow); err != nil {
				return err
			}

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
			if cmp, err := nextRow.Compare(&ss.alloc, s.ordering, ss.rows.evalCtx, pivot); err != nil {
				return err
			} else if cmp < 0 {
				return errors.Errorf("incorrectly ordered row %s before %s", pivot, nextRow)
			}
			break
		}

		// Sort the rows that have been pushed onto the buffer.
		ss.rows.Sort()

		// Stream out sorted rows in order to row receiver.
		for ss.rows.Len() > 0 {
			consumerStatus, err := s.out.emitRow(ctx, ss.rows.EncRow(0))
			if err != nil || consumerStatus != NeedMoreRows {
				// We don't need any more rows; clear out ss so to not hold on to that
				// memory.
				ss = &sortChunksStrategy{}
				return err
			}
			ss.rows.PopFirst()
		}
		ss.rows.Clear(ctx)

		if nextRow == nil {
			// We've reached the end of the table.
			break
		}
	}

	return nil
}
