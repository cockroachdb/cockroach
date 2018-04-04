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

	// meta stores metadata that the sorter has accumulated for pushing later.
	meta []ProducerMetadata
	// close is a callback provided by the sorter that is called the first time
	// producerMeta is called.
	close func()
}

func (s *sorterBase) init(
	flowCtx *FlowCtx,
	input RowSource,
	post *PostProcessSpec,
	output RowReceiver,
	ordering sqlbase.ColumnOrdering,
	matchLen uint32,
	close func(),
) error {
	count := int64(0)
	if post.Limit != 0 {
		// The sorter needs to produce Offset + Limit rows. The ProcOutputHelper
		// will discard the first Offset ones.
		count = int64(post.Limit) + int64(post.Offset)
	}

	s.input = input
	s.ordering = ordering
	s.matchLen = matchLen
	s.count = count
	s.tempStorage = flowCtx.TempStorage
	s.evalCtx = flowCtx.NewEvalCtx()
	s.close = close
	return s.processorBase.init(post, input.OutputTypes(), flowCtx, s.evalCtx, output)
}

// closeAndQueueTrailingMeta closes input and puts the processor in a mode where
// future calls to Next() will return only "trailing metadata". The first such
// piece of metadata is returned. The next ones have to be retrieved with
// popTrailingMeta().
//
// If an error is passed in, it will be part of the trailing metadata.
//
// This method is to be called when the processor is done producing rows and
// draining its inputs (if it wants to drain them).
func (s *sorterBase) closeAndQueueTrailingMeta(err error) *ProducerMetadata {
	if s.closed {
		log.Fatalf(s.ctx, "closeAndQueueTrailingMeta() called after close. err: %v", err)
	}

	if err != nil {
		s.meta = append(s.meta, ProducerMetadata{Err: err})
	}
	if trace := getTraceData(s.ctx); trace != nil {
		s.meta = append(s.meta, ProducerMetadata{TraceData: trace})
	}
	s.close()

	return s.popTrailingMeta()
}

// popTrailingMeta peels off one piece of trailing metadata.
func (s *sorterBase) popTrailingMeta() *ProducerMetadata {
	if len(s.meta) > 0 {
		meta := &s.meta[0]
		s.meta = s.meta[1:]
		return meta
	}
	return nil
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

	// Choose the optimal processor.
	if spec.OrderingMatchLen == 0 {
		if count == 0 {
			// No specified ordering match length and unspecified limit; no
			// optimizations are possible so we simply load all rows into memory and
			// sort all values in-place. It has a worst-case time complexity of
			// O(n*log(n)) and a worst-case space complexity of O(n).
			return newSortAllProcessor(flowCtx, spec, input, post, output)
		}
		// No specified ordering match length but specified limit; we can optimize
		// our sort procedure by maintaining a max-heap populated with only the
		// smallest k rows seen. It has a worst-case time complexity of
		// O(n*log(k)) and a worst-case space complexity of O(k).
		return newSortTopKProcessor(flowCtx, spec, input, post, output, count)
	}
	// Ordering match length is specified. We will be able to use existing
	// ordering in order to avoid loading all the rows into memory. If we're
	// scanning an index with a prefix matching an ordering prefix, we can only
	// accumulate values for equal fields in this prefix, sort the accumulated
	// chunk and then output.
	// TODO(irfansharif): Add optimization for case where both ordering match
	// length and limit is specified.
	return newSortChunksProcessor(flowCtx, spec, input, post, output)

}

// sortAllProcessor reads in all values into the wrapped rows and
// uses sort.Sort to sort all values in-place. It has a worst-case time
// complexity of O(n*log(n)) and a worst-case space complexity of O(n).
//
// This processor is intended to be used when all values need to be sorted.
type sortAllProcessor struct {
	sorterBase

	useTempStorage  bool
	diskContainer   *diskRowContainer
	rows            memRowContainer
	rowContainerMon *mon.BytesMonitor

	// The following variables are used by the state machine, and are used by Next()
	// to determine where to resume emitting rows.
	i      rowIterator
	closed bool
}

var _ Processor = &sortAllProcessor{}
var _ RowSource = &sortAllProcessor{}

func newSortAllProcessor(
	flowCtx *FlowCtx, spec *SorterSpec, input RowSource, post *PostProcessSpec, out RowReceiver,
) (Processor, error) {
	ordering := convertToColumnOrdering(spec.OutputOrdering)
	useTempStorage := settingUseTempStorageSorts.Get(&flowCtx.Settings.SV) ||
		flowCtx.testingKnobs.MemoryLimitBytes > 0

	rowContainerMon := flowCtx.EvalCtx.Mon
	if useTempStorage {
		// We will use the sortAllProcessor in this case and potentially fall
		// back to disk.
		// Limit the memory use by creating a child monitor with a hard limit.
		// The processor will overflow to disk if this limit is not enough.
		limit := flowCtx.testingKnobs.MemoryLimitBytes
		if limit <= 0 {
			limit = settingWorkMemBytes.Get(&flowCtx.Settings.SV)
		}
		limitedMon := mon.MakeMonitorInheritWithLimit(
			"sortall-limited", limit, flowCtx.EvalCtx.Mon,
		)
		limitedMon.Start(flowCtx.Ctx, rowContainerMon, mon.BoundAccount{})
		rowContainerMon = &limitedMon
	}

	proc := &sortAllProcessor{
		rowContainerMon: rowContainerMon,
		useTempStorage:  useTempStorage,
	}
	proc.rows.initWithMon(ordering, input.OutputTypes(), flowCtx.NewEvalCtx(), rowContainerMon)
	if err := proc.sorterBase.init(
		flowCtx, input, post, out,
		convertToColumnOrdering(spec.OutputOrdering),
		spec.OrderingMatchLen, proc.close,
	); err != nil {
		return nil, err
	}
	return proc, nil
}

func (s *sortAllProcessor) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	if s.maybeStart("sortAllProcessor", "sortAllProcessor") {
		if err := s.fill(); err != nil {
			return nil, s.closeAndQueueTrailingMeta(err)
		}
	}
	if s.closed {
		return nil, s.popTrailingMeta()
	}

	for {
		if ok, err := s.i.Valid(); err != nil || !ok {
			return nil, s.closeAndQueueTrailingMeta(err)
		}

		row, err := s.i.Row()
		if err != nil {
			return nil, s.closeAndQueueTrailingMeta(err)
		}
		s.i.Next()

		outRow, status, err := s.out.ProcessRow(s.evalCtx.Ctx(), row)
		if outRow != nil {
			return outRow, nil
		}
		if outRow == nil && err == nil && status == NeedMoreRows {
			continue
		}
		return nil, s.closeAndQueueTrailingMeta(err)
	}
}

func (s *sortAllProcessor) fill() error {
	ctx := s.evalCtx.Ctx()
	// Attempt an in memory implementation of a sort. If this run fails with a
	// memory error, fall back to use disk.
	row, err := s.fillWithContainer(ctx, &s.rows)
	if err != nil {
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
		if _, err := s.fillWithContainer(ctx, s.diskContainer); err != nil {
			return err
		}
	}
	return nil
}

// fill the rows from the input into the given container. If an error occurs,
// or the input sends metadata while adding a row to the given container, the
// row is returned in order to not lose it.
func (s *sortAllProcessor) fillWithContainer(
	ctx context.Context, r sortableRowContainer,
) (sqlbase.EncDatumRow, error) {
	for {
		row, meta := s.input.Next()
		if meta != nil {
			s.meta = append(s.meta, *meta)
			continue
		}
		if row == nil {
			break
		}

		if err := r.AddRow(ctx, row); err != nil {
			return row, err
		}
	}
	r.Sort(ctx)

	s.i = r.NewIterator(ctx)
	s.i.Rewind()

	return nil, nil
}

func (s *sortAllProcessor) Run(wg *sync.WaitGroup) {
	if s.out.output == nil {
		panic("sorter output not initialized for emitting rows")
	}
	Run(s.flowCtx.Ctx, s, s.out.output)
	if wg != nil {
		wg.Done()
	}
}

func (s *sortAllProcessor) close() {
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
func (s *sortAllProcessor) ConsumerDone() {
	s.input.ConsumerDone()
}

// ConsumerClosed is part of the RowSource interface.
func (s *sortAllProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	s.close()
}

// sortTopKProcessor creates a max-heap in its wrapped rows and keeps
// this heap populated with only the top k values seen. It accomplishes this
// by comparing new values (before the deep copy) with the top of the heap.
// If the new value is less than the current top, the top will be replaced
// and the heap will be fixed. If not, the new value is dropped. When finished,
// the max heap is converted to a min-heap effectively sorting the values
// correctly in-place. It has a worst-case time complexity of O(n*log(k)) and a
// worst-case space complexity of O(k).
//
// This processor is intended to be used when exactly k values need to be sorted,
// where k is known before sorting begins.
//
// TODO(irfansharif): (taken from TODO found in sql/sort.go) There are better
// algorithms that can achieve a sorted top k in a worst-case time complexity
// of O(n + k*log(k)) while maintaining a worst-case space complexity of O(k).
// For instance, the top k can be found in linear time, and then this can be
// sorted in linearithmic time.
//
// TODO(asubiotto): Use diskRowContainer for these other strategies.
type sortTopKProcessor struct {
	sorterBase

	rows            memRowContainer
	rowContainerMon *mon.BytesMonitor
	k               int64
}

var _ Processor = &sortTopKProcessor{}
var _ RowSource = &sortTopKProcessor{}

func newSortTopKProcessor(
	flowCtx *FlowCtx,
	spec *SorterSpec,
	input RowSource,
	post *PostProcessSpec,
	out RowReceiver,
	k int64,
) (Processor, error) {
	rowContainerMon := flowCtx.EvalCtx.Mon
	ordering := convertToColumnOrdering(spec.OutputOrdering)
	proc := &sortTopKProcessor{
		rowContainerMon: rowContainerMon,
		k:               k,
	}
	proc.rows.initWithMon(ordering, input.OutputTypes(), flowCtx.NewEvalCtx(), rowContainerMon)
	if err := proc.sorterBase.init(
		flowCtx, input, post, out,
		ordering, spec.OrderingMatchLen, proc.close,
	); err != nil {
		return nil, err
	}
	return proc, nil
}

func (s *sortTopKProcessor) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	if s.maybeStart("sortTopK", "SortTopK") {
		// The execution loop for the SortTopK processor is similar to that of the
		// SortAll processor; the difference is that we push rows into a max-heap
		// of size at most K, and only sort those.
		ctx := s.evalCtx.Ctx()

		heapCreated := false
		for {
			row, meta := s.input.Next()
			if meta != nil {
				s.meta = append(s.meta, *meta)
				continue
			}
			if row == nil {
				break
			}

			if int64(s.rows.Len()) < s.k {
				// Accumulate up to k values.
				if err := s.rows.AddRow(ctx, row); err != nil {
					return nil, s.closeAndQueueTrailingMeta(err)
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
					return nil, s.closeAndQueueTrailingMeta(err)
				}
			}
		}
		s.rows.Sort(ctx)
	}

	if s.closed {
		return nil, s.popTrailingMeta()
	}

	if s.rows.Len() == 0 {
		return nil, s.closeAndQueueTrailingMeta(nil /* err */)
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
		return nil, s.closeAndQueueTrailingMeta(err)
	}
}

func (s *sortTopKProcessor) Run(wg *sync.WaitGroup) {
	if s.out.output == nil {
		panic("sorter output not initialized for emitting rows")
	}
	Run(s.flowCtx.Ctx, s, s.out.output)
	if wg != nil {
		wg.Done()
	}
}

func (s *sortTopKProcessor) close() {
	if s.internalClose() {
		ctx := s.evalCtx.Ctx()
		s.rows.Close(ctx)
		s.input.ConsumerClosed()
	}
}

// ConsumerDone is part of the RowSource interface.
func (s *sortTopKProcessor) ConsumerDone() {
	s.input.ConsumerDone()
}

// ConsumerClosed is part of the RowSource interface.
func (s *sortTopKProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	s.close()
}

// If we're scanning an index with a prefix matching an ordering prefix, we only accumulate values
// for equal fields in this prefix, sort the accumulated chunk and then output.
type sortChunksProcessor struct {
	sorterBase

	rows            memRowContainer
	rowContainerMon *mon.BytesMonitor
	alloc           sqlbase.DatumAlloc

	// sortChunksProcessor accumulates rows that are equal on a prefix, until it
	// encounters a row that is greater. It stores that greater row in nextChunkRow
	prefix       sqlbase.EncDatumRow
	nextChunkRow sqlbase.EncDatumRow
}

var _ Processor = &sortChunksProcessor{}
var _ RowSource = &sortChunksProcessor{}

func newSortChunksProcessor(
	flowCtx *FlowCtx, spec *SorterSpec, input RowSource, post *PostProcessSpec, out RowReceiver,
) (Processor, error) {
	rowContainerMon := flowCtx.EvalCtx.Mon
	ordering := convertToColumnOrdering(spec.OutputOrdering)

	proc := &sortChunksProcessor{
		rowContainerMon: rowContainerMon,
	}
	proc.rows.initWithMon(ordering, input.OutputTypes(), flowCtx.NewEvalCtx(), rowContainerMon)
	if err := proc.sorterBase.init(
		flowCtx, input, post, out, ordering, spec.OrderingMatchLen, proc.close,
	); err != nil {
		return nil, err
	}
	return proc, nil
}

// chunkCompleted is a helper function that determines if the given row shares the same
// values for the first matchLen ordering columns with the given prefix.
func (s *sortChunksProcessor) chunkCompleted() (bool, error) {
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
func (s *sortChunksProcessor) fill() error {
	ctx := s.evalCtx.Ctx()

	var meta *ProducerMetadata

	for s.nextChunkRow == nil {
		s.nextChunkRow, meta = s.input.Next()
		if meta != nil {
			s.meta = append(s.meta, *meta)
			continue
		} else if s.nextChunkRow == nil {
			return nil
		}
		break
	}
	s.prefix = s.nextChunkRow

	// Add the chunk
	if err := s.rows.AddRow(ctx, s.nextChunkRow); err != nil {
		return err
	}

	defer s.rows.Sort(ctx)

	// We will accumulate rows to form a chunk such that they all share the same values
	// as prefix for the first s.matchLen ordering columns.
	for {
		s.nextChunkRow, meta = s.input.Next()

		if meta != nil {
			s.meta = append(s.meta, *meta)
			continue
		}
		if s.nextChunkRow == nil {
			break
		}

		chunkCompleted, err := s.chunkCompleted()

		if err != nil {
			return err
		}
		if chunkCompleted {
			break
		}

		if err := s.rows.AddRow(ctx, s.nextChunkRow); err != nil {
			return err
		}
	}

	return nil
}

func (s *sortChunksProcessor) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	s.maybeStart("sortChunks", "SortChunks")
	if s.closed {
		return nil, s.popTrailingMeta()
	}

	for {
		// If we don't have an active chunk, clear and refill it.
		if s.rows.Len() == 0 {
			s.rows.Clear(s.rows.evalCtx.Ctx())
			// If that errored or didn't result in an active chunk, we are done.
			if err := s.fill(); err != nil || s.rows.Len() == 0 {
				return nil, s.closeAndQueueTrailingMeta(err)
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
		return nil, s.closeAndQueueTrailingMeta(err)
	}
}

func (s *sortChunksProcessor) Run(wg *sync.WaitGroup) {
	if s.out.output == nil {
		panic("sorter output not initialized for emitting rows")
	}
	Run(s.flowCtx.Ctx, s, s.out.output)
	if wg != nil {
		wg.Done()
	}
}

func (s *sortChunksProcessor) close() {
	if s.internalClose() {
		ctx := s.evalCtx.Ctx()
		s.rows.Close(ctx)
		s.input.ConsumerClosed()
	}
}

// ConsumerDone is part of the RowSource interface.
func (s *sortChunksProcessor) ConsumerDone() {
	s.input.ConsumerDone()
}

// ConsumerClosed is part of the RowSource interface.
func (s *sortChunksProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	s.close()
}
