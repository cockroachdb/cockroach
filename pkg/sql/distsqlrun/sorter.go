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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/pkg/errors"
)

// sorter sorts the input rows according to the specified ordering.
type sorterBase struct {
	processorBase

	evalCtx *tree.EvalContext

	input    RowSource
	ordering sqlbase.ColumnOrdering
	matchLen uint32
	// count is the maximum number of rows that the sorter will push to the
	// ProcOutputHelper. 0 if the sorter should sort and push all the rows from
	// the input.
	count int64
	// tempStorage is used to store rows when the working set is larger than can
	// be stored in memory.
	tempStorage engine.Engine
}

func newSorter(
	flowCtx *FlowCtx, spec *SorterSpec, input RowSource, post *PostProcessSpec, output RowReceiver,
) (Processor, error) {
	count := int64(0)
	if post.Limit != 0 {
		// The sorter needs to produce Offset + Limit rows. The ProcOutputHelper
		// will discard the first Offset ones.
		count = int64(post.Limit) + int64(post.Offset)
	}

	s := &sorterBase{
		input:       input,
		ordering:    convertToColumnOrdering(spec.OutputOrdering),
		matchLen:    spec.OrderingMatchLen,
		count:       count,
		tempStorage: flowCtx.TempStorage,
		evalCtx:     flowCtx.NewEvalCtx(),
	}

	if err := s.init(post, input.OutputTypes(), flowCtx, s.evalCtx, output); err != nil {
		return nil, err
	}

	// Choose the optimal sorterStrategy.
	if s.matchLen == 0 {
		if s.count == 0 {
			// No specified ordering match length and unspecified limit; no
			// optimizations are possible so we simply load all rows into memory and
			// sort all values in-place. It has a worst-case time complexity of
			// O(n*log(n)) and a worst-case space complexity of O(n).
			return newSortAllStrategy(s), nil
		}
		// No specified ordering match length but specified limit; we can optimize
		// our sort procedure by maintaining a max-heap populated with only the
		// smallest k rows seen. It has a worst-case time complexity of
		// O(n*log(k)) and a worst-case space complexity of O(k).
		return newSortTopKStrategy(s, s.count), nil
	}
	// Ordering match length is specified. We will be able to use existing
	// ordering in order to avoid loading all the rows into memory. If we're
	// scanning an index with a prefix matching an ordering prefix, we can only
	// accumulate values for equal fields in this prefix, sort the accumulated
	// chunk and then output.
	// TODO(irfansharif): Add optimization for case where both ordering match
	// length and limit is specified.
	return newSortChunksStrategy(s), nil

}

// sortAllStrategy reads in all values into the wrapped rows and
// uses sort.Sort to sort all values in-place. It has a worst-case time
// complexity of O(n*log(n)) and a worst-case space complexity of O(n).
//
// The strategy is intended to be used when all values need to be sorted.
type sortAllStrategy struct {
	*sorterBase

	useTempStorage  bool
	diskContainer   *diskRowContainer
	rows            *memRowContainer
	rowContainerMon *mon.BytesMonitor

	// The following variables are used by the state machine, and are used by Next()
	// to determine where to resume emitting rows.
	i      rowIterator
	closed bool

	// sortAllStrategy first calls Next() on its input stream to completion before
	// outputting a single row. Thus when it receives ProducerMetadata, it has to
	// cache it and output the rows its received so far, before emitting that metadata.
	meta *ProducerMetadata
}

var _ Processor = &sortAllStrategy{}

func newSortAllStrategy(s *sorterBase) Processor {
	var rows memRowContainer
	rowContainerMon := s.flowCtx.EvalCtx.Mon
	useTempStorage := settingUseTempStorageSorts.Get(&s.flowCtx.Settings.SV) ||
		s.flowCtx.testingKnobs.MemoryLimitBytes > 0
	if useTempStorage {
		// We will use the sortAllStrategy in this case and potentially fall
		// back to disk.
		// Limit the memory use by creating a child monitor with a hard limit.
		// The strategy will overflow to disk if this limit is not enough.
		limit := s.flowCtx.testingKnobs.MemoryLimitBytes
		if limit <= 0 {
			limit = settingWorkMemBytes.Get(&s.flowCtx.Settings.SV)
		}
		limitedMon := mon.MakeMonitorInheritWithLimit(
			"sortall-limited", limit, s.flowCtx.EvalCtx.Mon,
		)
		limitedMon.Start(s.flowCtx.Ctx, s.flowCtx.EvalCtx.Mon, mon.BoundAccount{})

		rowContainerMon = &limitedMon
	}
	rows.initWithMon(s.ordering, s.input.OutputTypes(), s.flowCtx.NewEvalCtx(), rowContainerMon)

	return &sortAllStrategy{
		sorterBase:      s,
		rows:            &rows,
		rowContainerMon: rowContainerMon,
		useTempStorage:  useTempStorage,
	}
}

func (s *sortAllStrategy) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	if s.closed {
		return nil, nil
	}
	if s.maybeStart("sortAllStrategy", "sortAllStrategy") {
		if err := s.fill(); err != nil {
			return nil, s.producerMeta(err)
		}
	}

	if ok, err := s.i.Valid(); err != nil {
		return nil, s.producerMeta(err)
	} else if !ok {
		return s.trailingMetadata()
	}

	for {
		row, err := s.i.Row()
		if err != nil {
			return nil, s.producerMeta(err)
		}
		s.i.Next()

		outRow, status, err := s.out.ProcessRow(s.evalCtx.Ctx(), row)
		if outRow != nil {
			return outRow, nil
		}
		if outRow == nil && err == nil && status == NeedMoreRows {
			continue
		}
		return nil, s.producerMeta(err)
	}
}

// Sort processors read all their input before outputting a single output. Thus
// they can't stop sending results when they encounter metadata. Instead they
// must cache metadata, and output it after outputting all their non-metadata rows.
// trailingMetadata() returns any cached metadata to output to Next(), and evicts
// its cache so that subsequent calls return nil.
// TODO(arjun): This is unnecessary. Metadata can be emitted immediately.
// This should be refactored away.
func (s *sortAllStrategy) trailingMetadata() (sqlbase.EncDatumRow, *ProducerMetadata) {
	s.close()
	if s.meta != nil {
		meta := s.meta
		s.meta = nil
		return nil, meta
	}
	return nil, nil
}

func (s *sortAllStrategy) fill() error {
	ctx := s.evalCtx.Ctx()
	// Attempt an in memory implementation of a sort. If this run fails with a
	// memory error, fall back to use disk.
	row, meta, err := s.fillWithContainer(ctx, s.rows)
	if meta != nil {
		s.meta = meta
	} else if err != nil {
		// TODO(asubiotto): A memory error could also be returned if a limit other
		// than the COCKROACH_WORK_MEM was reached. We should distinguish between
		// these cases and log the event to facilitate debugging of queries that
		// may be slow for this reason.
		// We return the memory error if the row is nil because this case implies
		// that we received the memory error from a code path that was not adding
		// a row (e.g. from an upstream processor).
		if pgErr, ok := pgerror.GetPGCause(err); !(ok && pgErr.Code == pgerror.CodeOutOfMemoryError) || row == nil {
			return err
		}
		if !s.useTempStorage {
			return errors.Wrap(err, "external storage for large queries disabled")
		}
		log.VEventf(ctx, 2, "falling back to disk")
		diskContainer := makeDiskRowContainer(
			ctx, s.flowCtx.diskMonitor, s.rows.types, s.rows.ordering, s.tempStorage,
		)
		s.diskContainer = &diskContainer

		// Transfer the rows from memory to disk. This frees up the memory taken up by s.rows.
		i := s.rows.NewIterator(ctx)
		for i.Rewind(); ; i.Next() {
			if ok, err := i.Valid(); err != nil {
				return err
			} else if !ok {
				break
			}
			memRow, err := i.Row()
			if err != nil {
				return err
			}
			if err := s.diskContainer.AddRow(ctx, memRow); err != nil {
				return err
			}
		}
		s.i = nil

		// Add the row that caused the memory container to run out of memory.
		if err := s.diskContainer.AddRow(ctx, row); err != nil {
			return err
		}

		// Continue and fill the rest of the rows from the input.
		if _, meta, err := s.fillWithContainer(ctx, s.diskContainer); meta != nil {
			// if we encounter metadata, we have to be careful: we can't return it
			// right away. We save it for sending after we send all the rows we've
			// already buffered.
			s.meta = meta
		} else if err != nil {
			return err
		}
	}
	return nil
}

// fill the rows from the input into the given container. If an error occurs,
// or the input sends metadata while adding a row to the given container, the
// row is returned in order to not lose it.
func (s *sortAllStrategy) fillWithContainer(
	ctx context.Context, r sortableRowContainer,
) (sqlbase.EncDatumRow, *ProducerMetadata, error) {
	var meta *ProducerMetadata
	for {
		row, inputMeta := s.input.Next()
		if inputMeta != nil {
			meta = inputMeta
			continue
		}
		if row == nil {
			break
		}

		if err := r.AddRow(ctx, row); err != nil {
			return row, nil, err
		}
	}
	r.Sort(ctx)

	s.i = r.NewIterator(ctx)
	s.i.Rewind()

	return nil, meta, nil
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
	// We are done sorting rows, close the iterators we have open. The row
	// containers require a context, so must be called before internalClose().
	if !s.closed {
		if s.i != nil {
			s.i.Close()
		}
		ctx := s.evalCtx.Ctx()
		if s.diskContainer != nil {
			s.diskContainer.Close(ctx)
		}
		s.rows.Close(ctx)
		s.rowContainerMon.Stop(ctx)
	}
	if s.internalClose() {
		s.input.ConsumerClosed()
		s.closed = true
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

	rows            *memRowContainer
	rowContainerMon *mon.BytesMonitor
	k               int64

	meta *ProducerMetadata
}

var _ Processor = &sortTopKStrategy{}

func newSortTopKStrategy(s *sorterBase, k int64) Processor {
	var rows memRowContainer
	rowContainerMon := s.flowCtx.EvalCtx.Mon
	rows.initWithMon(s.ordering, s.input.OutputTypes(), s.flowCtx.NewEvalCtx(), rowContainerMon)
	return &sortTopKStrategy{
		sorterBase:      *s,
		rows:            &rows,
		rowContainerMon: rowContainerMon,
		k:               k,
	}
}

func (s *sortTopKStrategy) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	if s.maybeStart("sortTopK", "SortTopK") {
		// The execution loop for the SortTopK strategy is similar to that of the
		// SortAll strategy; the difference is that we push rows into a max-heap
		// of size at most K, and only sort those.
		ctx := s.evalCtx.Ctx()

		heapCreated := false
		for {
			row, meta := s.input.Next()
			if meta != nil {
				s.meta = meta
				continue
			}
			if row == nil {
				break
			}

			if int64(s.rows.Len()) < s.k {
				// Accumulate up to k values.
				if err := s.rows.AddRow(ctx, row); err != nil {
					return nil, s.producerMeta(err)
				}
			} else {
				if !heapCreated {
					// Arrange the k values into a max-heap.
					s.rows.InitMaxHeap()
					heapCreated = true
				}
				// Replace the max value if the new row is smaller, maintaining the
				// max-heap.
				if err := s.rows.MaybeReplaceMax(ctx, row); err != nil {
					return nil, s.producerMeta(err)
				}
			}
		}
		s.rows.Sort(ctx)
	}

	if s.rows.Len() == 0 {
		return s.trailingMetadata()
	}
	for {
		row := s.rows.EncRow(0)
		s.rows.PopFirst()

		outRow, status, err := s.out.ProcessRow(s.evalCtx.Ctx(), row)
		if outRow != nil {
			return outRow, nil
		}
		if outRow == nil && err == nil && status == NeedMoreRows {
			continue
		}
		return nil, s.producerMeta(err)
	}
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
		ctx := s.evalCtx.Ctx()
		s.rows.Close(ctx)
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

// Sort processors read all their input before outputting a single output. Thus
// they can't stop sending results when they encounter metadata. Instead they
// must cache metadata, and output it after outputting all their non-metadata rows.
// trailingMetadata() returns any cached metadata to output to Next(), and evicts
// its cache so that subsequent calls return nil.
func (s *sortTopKStrategy) trailingMetadata() (sqlbase.EncDatumRow, *ProducerMetadata) {
	s.close()
	if s.meta != nil {
		meta := s.meta
		s.meta = nil
		return nil, meta
	}
	return nil, nil
}

// If we're scanning an index with a prefix matching an ordering prefix, we only accumulate values
// for equal fields in this prefix, sort the accumulated chunk and then output.
type sortChunksStrategy struct {
	sorterBase

	rows            *memRowContainer
	rowContainerMon *mon.BytesMonitor
	alloc           sqlbase.DatumAlloc

	// sortChunksStrategy accumulates rows that are equal on a prefix, until it
	// encounters a row that is greater. It stores that greater row in nextChunkRow
	prefix       sqlbase.EncDatumRow
	nextChunkRow sqlbase.EncDatumRow
	meta         *ProducerMetadata
}

var _ Processor = &sortChunksStrategy{}

func newSortChunksStrategy(s *sorterBase) Processor {
	var rows memRowContainer
	rowContainerMon := s.flowCtx.EvalCtx.Mon
	rows.initWithMon(s.ordering, s.input.OutputTypes(), s.flowCtx.NewEvalCtx(), rowContainerMon)

	return &sortChunksStrategy{
		sorterBase:      *s,
		rows:            &rows,
		rowContainerMon: rowContainerMon,
	}
}

// chunkCompleted is a helper function that determines if the given row shares the same
// values for the first matchLen ordering columns with the given prefix.
func (s *sortChunksStrategy) chunkCompleted() (bool, error) {
	for _, ord := range s.ordering[:s.matchLen] {
		col := ord.ColIdx
		cmp, err := s.nextChunkRow[col].Compare(&s.rows.types[col], &s.alloc, s.rows.evalCtx, &s.prefix[col])
		if cmp != 0 || err != nil {
			return true, err
		}
	}
	return false, nil
}

// fill one chunk of rows from the input and sort them.
func (s *sortChunksStrategy) fill() (*ProducerMetadata, error) {
	ctx := s.evalCtx.Ctx()

	var meta *ProducerMetadata

	if s.nextChunkRow == nil {
		s.nextChunkRow, meta = s.input.Next()
		if meta != nil || s.nextChunkRow == nil {
			return meta, nil
		}
	}
	s.prefix = s.nextChunkRow

	// Add the chunk
	if err := s.rows.AddRow(ctx, s.nextChunkRow); err != nil {
		return nil, err
	}

	defer s.rows.Sort(ctx)

	// We will accumulate rows to form a chunk such that they all share the same values
	// as prefix for the first s.matchLen ordering columns.
	for {
		s.nextChunkRow, meta = s.input.Next()
		if meta != nil {
			return meta, nil
		}
		if s.nextChunkRow == nil {
			break
		}

		chunkCompleted, err := s.chunkCompleted()
		if err != nil {
			return nil, err
		}
		if chunkCompleted {
			break
		}

		if err := s.rows.AddRow(ctx, s.nextChunkRow); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func (s *sortChunksStrategy) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	s.maybeStart("sortChunks", "SortChunks")
	for {
		// If we don't have an active chunk, clear and refill it.
		if s.rows.Len() == 0 {

			s.rows.Clear(s.rows.evalCtx.Ctx())
			meta, err := s.fill()
			if err != nil {
				return nil, s.producerMeta(err)
			} else if meta != nil {
				s.meta = meta
			}
			// If that didn't result in an active chunk, we are done.
			if s.rows.Len() == 0 {
				return s.trailingMetadata()
			}
		}

		// If we have an active chunk, get a row from it.
		row := s.rows.EncRow(0)
		s.rows.PopFirst()

		outRow, status, err := s.out.ProcessRow(s.evalCtx.Ctx(), row)
		if outRow != nil {
			return outRow, nil
		}
		if outRow == nil && err == nil && status == NeedMoreRows {
			continue
		}
		return nil, s.producerMeta(err)
	}
}

func (s *sortChunksStrategy) trailingMetadata() (sqlbase.EncDatumRow, *ProducerMetadata) {
	s.close()
	if s.meta != nil {
		meta := s.meta
		s.meta = nil
		return nil, meta
	}
	return nil, nil
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
		ctx := s.evalCtx.Ctx()
		s.rows.Close(ctx)
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
