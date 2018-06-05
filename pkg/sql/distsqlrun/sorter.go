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
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/pkg/errors"
)

// sorter sorts the input rows according to the specified ordering.
type sorterBase struct {
	processorBase

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

func (s *sorterBase) init(
	flowCtx *FlowCtx,
	processorID int32,
	input RowSource,
	post *PostProcessSpec,
	output RowReceiver,
	ordering sqlbase.ColumnOrdering,
	matchLen uint32,
	opts procStateOpts,
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
	return s.processorBase.init(post, input.OutputTypes(), flowCtx, processorID, output, opts)
}

func newSorter(
	ctx context.Context,
	flowCtx *FlowCtx,
	processorID int32,
	spec *SorterSpec,
	input RowSource,
	post *PostProcessSpec,
	output RowReceiver,
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
			return newSortAllProcessor(ctx, flowCtx, processorID, spec, input, post, output)
		}
		// No specified ordering match length but specified limit; we can optimize
		// our sort procedure by maintaining a max-heap populated with only the
		// smallest k rows seen. It has a worst-case time complexity of
		// O(n*log(k)) and a worst-case space complexity of O(k).
		return newSortTopKProcessor(flowCtx, processorID, spec, input, post, output, count)
	}
	// Ordering match length is specified. We will be able to use existing
	// ordering in order to avoid loading all the rows into memory. If we're
	// scanning an index with a prefix matching an ordering prefix, we can only
	// accumulate values for equal fields in this prefix, sort the accumulated
	// chunk and then output.
	// TODO(irfansharif): Add optimization for case where both ordering match
	// length and limit is specified.
	return newSortChunksProcessor(flowCtx, processorID, spec, input, post, output)
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
	i rowIterator
}

var _ Processor = &sortAllProcessor{}
var _ RowSource = &sortAllProcessor{}

const sortAllProcName = "sortAll"

func newSortAllProcessor(
	ctx context.Context,
	flowCtx *FlowCtx,
	processorID int32,
	spec *SorterSpec,
	input RowSource,
	post *PostProcessSpec,
	out RowReceiver,
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
		limitedMon.Start(ctx, rowContainerMon, mon.BoundAccount{})
		rowContainerMon = &limitedMon
	}

	proc := &sortAllProcessor{
		rowContainerMon: rowContainerMon,
		useTempStorage:  useTempStorage,
	}
	if err := proc.sorterBase.init(
		flowCtx, processorID, input, post, out,
		convertToColumnOrdering(spec.OutputOrdering),
		spec.OrderingMatchLen,
		procStateOpts{
			inputsToDrain: []RowSource{input},
			trailingMetaCallback: func() []ProducerMetadata {
				proc.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	proc.rows.initWithMon(ordering, input.OutputTypes(), proc.evalCtx, rowContainerMon)
	return proc, nil
}

// Start is part of the RowSource interface.
func (s *sortAllProcessor) Start(ctx context.Context) context.Context {
	s.input.Start(ctx)
	ctx = s.startInternal(ctx, sortAllProcName)

	valid, err := s.fill()
	if !valid || err != nil {
		s.moveToDraining(err)
	}
	return ctx
}

// Next is part of the RowSource interface.
func (s *sortAllProcessor) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for s.state == stateRunning {
		if ok, err := s.i.Valid(); err != nil || !ok {
			s.moveToDraining(err)
			break
		}

		row, err := s.i.Row()
		if err != nil {
			s.moveToDraining(err)
			break
		}
		s.i.Next()

		if outRow := s.processRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, s.drainHelper()
}

// fill fills the container with the input's rows.
//
// It returns true if the container is valid, false otherwise. If false is
// returned, the caller is expected to drain.
// It is possible for ok to be false even if no error is returned - in case an
// error metadata was received.
func (s *sortAllProcessor) fill() (ok bool, _ error) {
	ctx := s.evalCtx.Ctx()
	// Attempt an in memory implementation of a sort. If this run fails with a
	// memory error, fall back to use disk.
	valid, row, err := s.fillWithContainer(ctx, &s.rows)
	if err == nil {
		return valid, nil
	}
	// TODO(asubiotto): A memory error could also be returned if a limit other
	// than the COCKROACH_WORK_MEM was reached. We should distinguish between
	// these cases and log the event to facilitate debugging of queries that
	// may be slow for this reason.
	// We return the memory error if the row is nil because this case implies
	// that we received the memory error from a code path that was not adding
	// a row (e.g. from an upstream processor).
	if pgErr, ok := pgerror.GetPGCause(err); !(ok && pgErr.Code == pgerror.CodeOutOfMemoryError) || row == nil {
		return false, err
	}
	if !s.useTempStorage {
		return false, errors.Wrap(err, "external storage for large queries disabled")
	}
	log.VEventf(ctx, 2, "falling back to disk")
	diskContainer := makeDiskRowContainer(
		ctx, s.flowCtx.diskMonitor, s.rows.types, s.rows.ordering, s.tempStorage,
	)
	s.diskContainer = &diskContainer

	// Transfer the rows from memory to disk. This frees up the memory taken up by s.rows.
	i := s.rows.NewFinalIterator(ctx)
	for i.Rewind(); ; i.Next() {
		if ok, err := i.Valid(); err != nil {
			return false, err
		} else if !ok {
			break
		}
		memRow, err := i.Row()
		if err != nil {
			return false, err
		}
		if err := s.diskContainer.AddRow(ctx, memRow); err != nil {
			return false, err
		}
	}
	s.i = nil

	// Add the row that caused the memory container to run out of memory.
	if err := s.diskContainer.AddRow(ctx, row); err != nil {
		return false, err
	}

	// Continue and fill the rest of the rows from the input.
	valid, _ /* row */, err = s.fillWithContainer(ctx, s.diskContainer)
	return valid, err
}

// fill the rows from the input into the given container.
//
// Metadata is buffered in s.trailingMeta.
//
// If an error occurs, while adding a row to the given container, the row is
// returned together with the error in order to not lose it.
//
// The ok retval is true if the container is valid, false otherwise (if an
// error occurred or if the input returned an error metadata record). The caller
// is expected to inspect the error (if any) and drain if it's not recoverable.
// It is possible for ok to be false even if no error is returned - in case an
// error metadata was received.
func (s *sortAllProcessor) fillWithContainer(
	ctx context.Context, r sortableRowContainer,
) (ok bool, _ sqlbase.EncDatumRow, _ error) {
	for {
		row, meta := s.input.Next()
		if meta != nil {
			s.trailingMeta = append(s.trailingMeta, *meta)
			if meta.Err != nil {
				return false, nil, nil
			}
			continue
		}
		if row == nil {
			break
		}

		if err := r.AddRow(ctx, row); err != nil {
			return false, row, err
		}
	}
	r.Sort(ctx)

	s.i = r.NewFinalIterator(ctx)
	s.i.Rewind()

	return true, nil, nil
}

func (s *sortAllProcessor) Run(ctx context.Context, wg *sync.WaitGroup) {
	if s.out.output == nil {
		panic("sorter output not initialized for emitting rows")
	}
	ctx = s.Start(ctx)
	Run(ctx, s, s.out.output)
	if wg != nil {
		wg.Done()
	}
}

func (s *sortAllProcessor) close() {
	// We are done sorting rows, close the iterators we have open. The row
	// containers require a context, so must be called before internalClose().
	if s.internalClose() {
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

const sortTopKProcName = "sortTopK"

func newSortTopKProcessor(
	flowCtx *FlowCtx,
	processorID int32,
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
	if err := proc.sorterBase.init(
		flowCtx, processorID, input, post, out,
		ordering, spec.OrderingMatchLen,
		procStateOpts{
			inputsToDrain: []RowSource{input},
			trailingMetaCallback: func() []ProducerMetadata {
				proc.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	proc.rows.initWithMon(ordering, input.OutputTypes(), proc.evalCtx, rowContainerMon)
	return proc, nil
}

// Start is part of the RowSource interface.
func (s *sortTopKProcessor) Start(ctx context.Context) context.Context {
	s.input.Start(ctx)
	ctx = s.startInternal(ctx, sortTopKProcName)

	// The execution loop for the SortTopK processor is similar to that of the
	// SortAll processor; the difference is that we push rows into a max-heap
	// of size at most K, and only sort those.
	heapCreated := false
	for {
		row, meta := s.input.Next()
		if meta != nil {
			s.trailingMeta = append(s.trailingMeta, *meta)
			continue
		}
		if row == nil {
			break
		}

		if int64(s.rows.Len()) < s.k {
			// Accumulate up to k values.
			if err := s.rows.AddRow(ctx, row); err != nil {
				s.moveToDraining(err)
				break
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
				s.moveToDraining(err)
				break
			}
		}
	}
	s.rows.Sort(ctx)
	return ctx
}

// Next is part of the RowSource interface.
func (s *sortTopKProcessor) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for s.state == stateRunning {
		if s.rows.Len() == 0 {
			s.moveToDraining(nil /* err */)
			break
		}

		row := s.rows.EncRow(0)
		s.rows.PopFirst()

		if outRow := s.processRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, s.drainHelper()
}

func (s *sortTopKProcessor) Run(ctx context.Context, wg *sync.WaitGroup) {
	if s.out.output == nil {
		panic("sorter output not initialized for emitting rows")
	}
	ctx = s.Start(ctx)
	Run(ctx, s, s.out.output)
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
	rowAlloc        sqlbase.EncDatumRowAlloc

	// sortChunksProcessor accumulates rows that are equal on a prefix, until it
	// encounters a row that is greater. It stores that greater row in nextChunkRow
	nextChunkRow sqlbase.EncDatumRow
}

var _ Processor = &sortChunksProcessor{}
var _ RowSource = &sortChunksProcessor{}

const sortChunksProcName = "sortChunks"

func newSortChunksProcessor(
	flowCtx *FlowCtx,
	processorID int32,
	spec *SorterSpec,
	input RowSource,
	post *PostProcessSpec,
	out RowReceiver,
) (Processor, error) {
	rowContainerMon := flowCtx.EvalCtx.Mon
	ordering := convertToColumnOrdering(spec.OutputOrdering)

	proc := &sortChunksProcessor{
		rowContainerMon: rowContainerMon,
	}
	if err := proc.sorterBase.init(
		flowCtx, processorID, input, post, out, ordering, spec.OrderingMatchLen,
		procStateOpts{
			inputsToDrain: []RowSource{input},
			trailingMetaCallback: func() []ProducerMetadata {
				proc.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	proc.rows.initWithMon(ordering, input.OutputTypes(), proc.evalCtx, rowContainerMon)
	return proc, nil
}

// chunkCompleted is a helper function that determines if the given row shares the same
// values for the first matchLen ordering columns with the given prefix.
func (s *sortChunksProcessor) chunkCompleted(
	nextChunkRow, prefix sqlbase.EncDatumRow,
) (bool, error) {
	for _, ord := range s.ordering[:s.matchLen] {
		col := ord.ColIdx
		cmp, err := nextChunkRow[col].Compare(&s.rows.types[col], &s.alloc, s.rows.evalCtx, &prefix[col])
		if cmp != 0 || err != nil {
			return true, err
		}
	}
	return false, nil
}

// fill one chunk of rows from the input and sort them.
//
// Metadata is buffered in s.trailingMeta. Returns true if a valid chunk of rows
// has been read and sorted, false otherwise (if the input had no more rows or
// if a metadata record was encountered). The caller is expected to drain when
// this returns false.
func (s *sortChunksProcessor) fill() (bool, error) {
	ctx := s.evalCtx.Ctx()

	var meta *ProducerMetadata

	nextChunkRow := s.nextChunkRow
	s.nextChunkRow = nil
	for nextChunkRow == nil {
		nextChunkRow, meta = s.input.Next()
		if meta != nil {
			s.trailingMeta = append(s.trailingMeta, *meta)
			if meta.Err != nil {
				return false, nil
			}
			continue
		} else if nextChunkRow == nil {
			return false, nil
		}
		break
	}
	prefix := nextChunkRow

	// Add the chunk
	if err := s.rows.AddRow(ctx, nextChunkRow); err != nil {
		return false, err
	}

	defer s.rows.Sort(ctx)

	// We will accumulate rows to form a chunk such that they all share the same values
	// as prefix for the first s.matchLen ordering columns.
	for {
		nextChunkRow, meta = s.input.Next()

		if meta != nil {
			s.trailingMeta = append(s.trailingMeta, *meta)
			continue
		}
		if nextChunkRow == nil {
			break
		}

		chunkCompleted, err := s.chunkCompleted(nextChunkRow, prefix)

		if err != nil {
			return false, err
		}
		if chunkCompleted {
			s.nextChunkRow = s.rowAlloc.CopyRow(nextChunkRow)
			break
		}

		if err := s.rows.AddRow(ctx, nextChunkRow); err != nil {
			return false, err
		}
	}

	return true, nil
}

// Start is part of the RowSource interface.
func (s *sortChunksProcessor) Start(ctx context.Context) context.Context {
	s.input.Start(ctx)
	return s.startInternal(ctx, sortChunksProcName)
}

// Next is part of the RowSource interface.
func (s *sortChunksProcessor) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for s.state == stateRunning {
		// If we don't have an active chunk, clear and refill it.
		if s.rows.Len() == 0 {
			s.rows.Clear(s.rows.evalCtx.Ctx())
			valid, err := s.fill()
			if !valid || err != nil {
				s.moveToDraining(err)
				break
			}
		}

		// If we have an active chunk, get a row from it.
		row := s.rows.EncRow(0)
		s.rows.PopFirst()

		if outRow := s.processRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, s.drainHelper()
}

func (s *sortChunksProcessor) Run(ctx context.Context, wg *sync.WaitGroup) {
	if s.out.output == nil {
		panic("sorter output not initialized for emitting rows")
	}
	ctx = s.Start(ctx)
	Run(ctx, s, s.out.output)
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
	s.moveToDraining(nil /* err */)
}

// ConsumerClosed is part of the RowSource interface.
func (s *sortChunksProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	s.close()
}
