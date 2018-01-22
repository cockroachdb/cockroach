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

package distsqlrun

import (
	"context"

	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// sortAllStrategy reads in all values into the wrapped rows and
// uses sort.Sort to sort all values in-place. It has a worst-case time
// complexity of O(n*log(n)) and a worst-case space complexity of O(n).
//
// The strategy is intended to be used when all values need to be sorted.
type sortAllStrategy struct {
	sorterBase

	i              rowIterator
	rows           *memRowContainer
	diskContainer  diskRowContainer
	useTempStorage bool
	closed         bool
}

var _ sortImplementation = &sortAllStrategy{}

func newSortAllStrategy(s *sorterBase, rows *memRowContainer, useTempStorage bool) sortImplementation {
	ss := sortAllStrategy{
		sorterBase:     *s,
		rows:           rows,
		useTempStorage: useTempStorage,
	}

	return &ss
}

func (ss *sortAllStrategy) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	if ss.maybeStart("sortAllStrategy", "sortAllStrategy") {
		ctx := ss.evalCtx.Ctx()
		// Attempt an in memory implementation of a sort. If this run fails with a
		// memory error, fall back to use disk.
		row, err := ss.fill(ctx, ss.rows)
		// TODO(asubiotto): A memory error could also be returned if a limit other
		// than the COCKROACH_WORK_MEM was reached. We should distinguish between
		// these cases and log the event to facilitate debugging of queries that
		// may be slow for this reason.
		// We return the memory error if the row is nil because this case implies
		// that we received the memory error from a code path that was not adding
		// a row (e.g. from an upstream processor).
		if pgErr, ok := pgerror.GetPGCause(err); !(ok && pgErr.Code == pgerror.CodeOutOfMemoryError) || row == nil {
			return nil, ss.producerMeta(err)
		}
		if !ss.useTempStorage {
			return nil, ss.producerMeta(errors.Wrap(err, "external storage for large queries disabled"))
		}
		log.VEventf(ctx, 2, "falling back to disk")
		ss.diskContainer = makeDiskRowContainer(
			ctx, ss.flowCtx.diskMonitor, ss.rows.types, ss.rows.ordering, ss.tempStorage,
		)

		// Transfer the rows from memory to disk. Note that this frees up the
		// memory taken up by ss.rows.
		i := ss.rows.NewIterator(ctx)
		defer i.Close()
		for i.Rewind(); ; i.Next() {
			if ok, err := i.Valid(); err != nil {
				return nil, ss.producerMeta(err)
			} else if !ok {
				break
			}
			memRow, err := i.Row()
			if err != nil {
				return nil, ss.producerMeta(err)
			}
			if err := ss.diskContainer.AddRow(ctx, memRow); err != nil {
				return nil, ss.producerMeta(err)
			}
		}

		// Add the row that caused the memory container to run out of memory.
		if err := ss.diskContainer.AddRow(ctx, row); err != nil {
			return nil, ss.producerMeta(err)
		}
		if _, err := ss.fill(ctx, &ss.diskContainer); err != nil {
			return nil, ss.producerMeta(err)
		}
		ss.rows.Close(ctx)
	}

	if ok, err := ss.i.Valid(); err != nil {
		return nil, ss.producerMeta(err)
	} else if !ok {
		return nil, nil
	}

	row, err := ss.i.Row()
	if err != nil {
		return nil, ss.producerMeta(err)
	}
	ss.i.Next()
	return row, nil
}

// The execution loop for the SortAll strategy:
//  - loads all rows into memory. If the memory budget is not high enough, all
//    rows are stored on disk.
//  - runs sort.Sort to sort rows in place. In the disk-backed case, the rows
//    are already kept in sorted order.
//  - sends each row out to the output stream.
//
// If an error occurs while adding a row to the given container, the row is
// returned in order to not lose it.
func (ss *sortAllStrategy) fill(ctx context.Context, r sortableRowContainer,
) (sqlbase.EncDatumRow, error) {
	for {
		row, meta := ss.input.Next()
		if meta != nil {
			if meta.Err != nil {
				return nil, meta.Err
			} else {
				return nil, errors.Errorf("sortAllStrategy cannot handle non-error metadata")
			}
		}
		if row == nil {
			break
		}
		if err := r.AddRow(ctx, row); err != nil {
			return row, err
		}
	}
	r.Sort(ctx)

	ss.i = r.NewIterator(ctx)
	ss.i.Rewind()

	return nil, nil
}

func (s *sortAllStrategy) Run(wg *sync.WaitGroup) {
	if s.out.output == nil {
		panic("sorter output not initialized for emitting rows")
	}
	Run(s.flowCtx.Ctx, s, s.out.output)
	if wg != nil {
		wg.Done()
	}
}

func (s *sortAllStrategy) close() {
	if s.internalClose() {
		// We are done sorting rows, close the iterators we have open.
		if s.i != nil {
			s.i.Close()
		}
		if s.useTempStorage {
			s.diskContainer.Close(s.evalCtx.Ctx())
		}
		s.rows.Close(s.evalCtx.Ctx())
		s.input.ConsumerClosed()
	}
}

// ConsumerDone is part of the RowSource interface.
func (s *sortAllStrategy) ConsumerDone() {
	s.input.ConsumerDone()
}

// ConsumerClosed is part of the RowSource interface.
func (s *sortAllStrategy) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	s.close()
}

// producerMeta constructs the ProducerMetadata after consumption of rows has
// terminated, either due to being indicated by the consumer, or because the
// processor ran out of rows or encountered an error. It is ok for err to be
// nil indicating that we're done producing rows even though no error occurred.
func (s *sortAllStrategy) producerMeta(err error) *ProducerMetadata {
	var meta *ProducerMetadata
	if !s.closed {
		if err != nil {
			meta = &ProducerMetadata{Err: err}
		} else if trace := getTraceData(s.ctx); trace != nil {
			meta = &ProducerMetadata{TraceData: trace}
		}
		// We need to close as soon as we send producer metadata as we're done
		// sending rows. The consumer is allowed to not call ConsumerDone().
		s.close()
	}
	return meta
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
//
// TODO(asubiotto): Use diskRowContainer for these other strategies.
type sortTopKStrategy struct {
	sorterBase

	rows *memRowContainer
	k    int64
}

var _ sortImplementation = &sortTopKStrategy{}

func newSortTopKStrategy(s *sorterBase, rows *memRowContainer, k int64) sortImplementation {
	ss := sortTopKStrategy{
		sorterBase: *s,
		rows:       rows,
		k:          k,
	}

	return &ss
}

func (ss *sortTopKStrategy) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	if ss.maybeStart("sortTopK", "SortTopK") {
		// The execution loop for the SortTopK strategy is similar to that of the
		// SortAll strategy; the difference is that we push rows into a max-heap
		// of size at most K, and only sort those.
		ctx := ss.evalCtx.Ctx()

		heapCreated := false
		for {
			row, meta := ss.input.Next()
			if meta != nil {
				return nil, meta
			}
			if row == nil {
				break
			}

			if int64(ss.rows.Len()) < ss.k {
				// Accumulate up to k values.
				if err := ss.rows.AddRow(ctx, row); err != nil {
					return nil, ss.producerMeta(err)
				}
			} else {
				if !heapCreated {
					// Arrange the k values into a max-heap.
					ss.rows.InitMaxHeap()
					heapCreated = true
				}
				// Replace the max value if the new row is smaller, maintaining the
				// max-heap.
				if err := ss.rows.MaybeReplaceMax(ctx, row); err != nil {
					return nil, ss.producerMeta(err)
				}
			}
		}
		ss.rows.Sort(ctx)
	}

	if ss.rows.Len() > 0 {
		row := ss.rows.EncRow(0)
		ss.rows.PopFirst()
		return row, nil
	}
	return nil, nil
}

func (s *sortTopKStrategy) Run(wg *sync.WaitGroup) {
	if s.out.output == nil {
		panic("sorter output not initialized for emitting rows")
	}
	Run(s.flowCtx.Ctx, s, s.out.output)
	if wg != nil {
		wg.Done()
	}
}

func (s *sortTopKStrategy) close() {
	if s.internalClose() {
		s.rows.Close(s.evalCtx.Ctx())
		s.input.ConsumerClosed()
	}
}

// ConsumerDone is part of the RowSource interface.
func (s *sortTopKStrategy) ConsumerDone() {
	s.input.ConsumerDone()
}

// ConsumerClosed is part of the RowSource interface.
func (s *sortTopKStrategy) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	s.close()
}

// producerMeta constructs the ProducerMetadata after consumption of rows has
// terminated, either due to being indicated by the consumer, or because the
// processor ran out of rows or encountered an error. It is ok for err to be
// nil indicating that we're done producing rows even though no error occurred.
func (s *sortTopKStrategy) producerMeta(err error) *ProducerMetadata {
	var meta *ProducerMetadata
	if !s.closed {
		if err != nil {
			meta = &ProducerMetadata{Err: err}
		} else if trace := getTraceData(s.ctx); trace != nil {
			meta = &ProducerMetadata{TraceData: trace}
		}
		// We need to close as soon as we send producer metadata as we're done
		// sending rows. The consumer is allowed to not call ConsumerDone().
		s.close()
	}
	return meta
}

// If we're scanning an index with a prefix matching an ordering prefix, we only accumulate values
// for equal fields in this prefix, sort the accumulated chunk and then output.
type sortChunksStrategy struct {
	sorterBase

	rows  *memRowContainer
	alloc sqlbase.DatumAlloc

	// sortChunksStrategy accumulates rows that are equal on a prefix, until it
	// encounters a row that is greater. It stores that greater row in nextChunkRow
	prefix       sqlbase.EncDatumRow
	nextChunkRow sqlbase.EncDatumRow
}

var _ sortImplementation = &sortChunksStrategy{}

func newSortChunksStrategy(s *sorterBase, rows *memRowContainer) sortImplementation {
	ss := sortChunksStrategy{
		sorterBase: *s,
		rows:       rows,
	}

	return &ss
}

// chunkCompleted is a helper function that determines if the given row shares the same
// values for the first matchLen ordering columns with the given prefix.
func (ss *sortChunksStrategy) chunkCompleted(
	ordering sqlbase.ColumnOrdering,
	matchLen uint32,
	row, prefix sqlbase.EncDatumRow,
) (bool, error) {
	for _, ord := range ordering[:matchLen] {
		col := ord.ColIdx
		cmp, err := row[col].Compare(&ss.rows.types[col], &ss.alloc, ss.rows.evalCtx, &prefix[col])
		if err != nil || cmp > 0 {
			return true, err
		} else if cmp < 0 {
			return true, errors.Errorf(
				"incorrectly ordered row %s before %s",
				ss.prefix.String(ss.rows.types),
				row.String(ss.rows.types),
			)
		}
	}
	return false, nil
}

// fill one chunk of rows from the input and sort them.
func (ss *sortChunksStrategy) fill() *ProducerMetadata {
	ctx := ss.evalCtx.Ctx()

	// First we get a single row, and save it in prefix.
	var meta *ProducerMetadata
	if ss.nextChunkRow != nil {
		if err := ss.rows.AddRow(ctx, ss.nextChunkRow); err != nil {
			return ss.producerMeta(err)
		}
	}

	ss.nextChunkRow, meta = ss.input.Next()
	if meta != nil || ss.nextChunkRow == nil {
		return meta
	}

	ss.prefix = ss.nextChunkRow

	// We will accumulate rows to form a chunk such that they all share the same values
	// as prefix for the first s.matchLen ordering columns.
	for {
		if err := ss.rows.AddRow(ctx, ss.nextChunkRow); err != nil {
			return ss.producerMeta(err)
		}

		ss.nextChunkRow, meta = ss.input.Next()
		if meta != nil {
			return meta
		}
		if ss.nextChunkRow == nil {
			break
		}

		chunkCompleted, err := ss.chunkCompleted(ss.ordering, ss.matchLen, ss.nextChunkRow, ss.prefix)
		if err != nil {
			return ss.producerMeta(err)
		}
		if chunkCompleted {
			break
		}
	}

	ss.rows.Sort(ctx)
	return nil
}

func (ss *sortChunksStrategy) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	ss.maybeStart("sortChunks", "SortChunks")

	// If we have an active chunk, get a row from it.
	if ss.rows.Len() > 0 {
		row := ss.rows.EncRow(0)
		ss.rows.PopFirst()
		return row, nil
	}

	// If we don't have an active chunk, clear and refill it.
	ss.rows.Clear(ss.rows.evalCtx.Ctx())
	meta := ss.fill()
	if meta != nil {
		return nil, meta
	}

	// If refilling it resulted in nothing, we've reached the end of the input.
	if ss.rows.Len() == 0 {
		return nil, nil
	}
	return ss.Next()
}

func (s *sortChunksStrategy) Run(wg *sync.WaitGroup) {
	if s.out.output == nil {
		panic("sorter output not initialized for emitting rows")
	}
	Run(s.flowCtx.Ctx, s, s.out.output)
	if wg != nil {
		wg.Done()
	}
}

func (s *sortChunksStrategy) close() {
	if s.internalClose() {
		s.rows.Close(s.evalCtx.Ctx())
		s.input.ConsumerClosed()
	}
}

// ConsumerDone is part of the RowSource interface.
func (s *sortChunksStrategy) ConsumerDone() {
	s.input.ConsumerDone()
}

// ConsumerClosed is part of the RowSource interface.
func (s *sortChunksStrategy) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	s.close()
}

// producerMeta constructs the ProducerMetadata after consumption of rows has
// terminated, either due to being indicated by the consumer, or because the
// processor ran out of rows or encountered an error. It is ok for err to be
// nil indicating that we're done producing rows even though no error occurred.
func (s *sortChunksStrategy) producerMeta(err error) *ProducerMetadata {
	var meta *ProducerMetadata
	if !s.closed {
		if err != nil {
			meta = &ProducerMetadata{Err: err}
		} else if trace := getTraceData(s.ctx); trace != nil {
			meta = &ProducerMetadata{TraceData: trace}
		}
		// We need to close as soon as we send producer metadata as we're done
		// sending rows. The consumer is allowed to not call ConsumerDone().
		s.close()
	}
	return meta
}

func (s *sorterBase) FlowCtx() *FlowCtx {
	return s.flowCtx
}
