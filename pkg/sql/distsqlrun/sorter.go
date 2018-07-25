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

	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	opentracing "github.com/opentracing/opentracing-go"
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

	rows sortableRowContainer
	i    rowIterator

	// Only set if the ability to spill to disk is enabled.
	diskMonitor *mon.BytesMonitor
}

func (s *sorterBase) init(
	self RowSource,
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

	ctx := flowCtx.EvalCtx.Ctx()
	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		input = NewInputStatCollector(input)
		s.finishTrace = s.outputStatsToTrace
	}

	useTempStorage := settingUseTempStorageSorts.Get(&flowCtx.Settings.SV) ||
		flowCtx.testingKnobs.MemoryLimitBytes > 0
	var memMonitor *mon.BytesMonitor
	if useTempStorage {
		// Limit the memory use by creating a child monitor with a hard limit.
		// The processor will overflow to disk if this limit is not enough.
		limit := flowCtx.testingKnobs.MemoryLimitBytes
		if limit <= 0 {
			limit = settingWorkMemBytes.Get(&flowCtx.Settings.SV)
		}
		limitedMon := mon.MakeMonitorInheritWithLimit(
			"sortall-limited", limit, flowCtx.EvalCtx.Mon,
		)
		limitedMon.Start(ctx, flowCtx.EvalCtx.Mon, mon.BoundAccount{})
		memMonitor = &limitedMon
	} else {
		memMonitor = newMonitor(ctx, flowCtx.EvalCtx.Mon, "sorter-mem")
	}

	if err := s.processorBase.init(
		self, post, input.OutputTypes(), flowCtx, processorID, output, memMonitor, opts,
	); err != nil {
		memMonitor.Stop(ctx)
		return err
	}

	if useTempStorage {
		s.diskMonitor = newMonitor(ctx, flowCtx.diskMonitor, "sorter-disk")
		rc := diskBackedRowContainer{}
		rc.init(
			ordering,
			input.OutputTypes(),
			s.evalCtx,
			flowCtx.TempStorage,
			memMonitor,
			s.diskMonitor,
		)
		s.rows = &rc
	} else {
		rc := memRowContainer{}
		rc.initWithMon(ordering, input.OutputTypes(), s.evalCtx, memMonitor)
		s.rows = &rc
	}

	s.input = input
	s.ordering = ordering
	s.matchLen = matchLen
	s.count = count
	return nil
}

// Next is part of the RowSource interface. It is extracted into sorterBase
// because this implementation of next is shared between the sortAllProcessor
// and the sortTopKProcessor.
func (s *sorterBase) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for s.state == stateRunning {
		if ok, err := s.i.Valid(); err != nil || !ok {
			s.moveToDraining(nil /* err */)
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

func (s *sorterBase) close() {
	// We are done sorting rows, close the iterator we have open.
	if s.internalClose() {
		if s.i != nil {
			s.i.Close()
		}
		ctx := s.evalCtx.Ctx()
		s.rows.Close(ctx)
		s.memMonitor.Stop(ctx)
		if s.diskMonitor != nil {
			s.diskMonitor.Stop(ctx)
		}
	}
}

var _ DistSQLSpanStats = &SorterStats{}

const sorterTagPrefix = "sorter."

// Stats implements the SpanStats interface.
func (ss *SorterStats) Stats() map[string]string {
	statsMap := ss.InputStats.Stats(sorterTagPrefix)
	statsMap[sorterTagPrefix+maxMemoryTagSuffix] = humanizeutil.IBytes(ss.MaxAllocatedMem)
	statsMap[sorterTagPrefix+maxDiskTagSuffix] = humanizeutil.IBytes(ss.MaxAllocatedDisk)
	return statsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (ss *SorterStats) StatsForQueryPlan() []string {
	return append(
		ss.InputStats.StatsForQueryPlan("" /* prefix */),
		fmt.Sprintf("%s: %s", maxMemoryQueryPlanSuffix, humanizeutil.IBytes(ss.MaxAllocatedMem)),
		fmt.Sprintf("%s: %s", maxDiskQueryPlanSuffix, humanizeutil.IBytes(ss.MaxAllocatedDisk)),
	)
}

// outputStatsToTrace outputs the collected sorter stats to the trace. Will fail
// silently if stats are not being collected.
func (s *sorterBase) outputStatsToTrace() {
	is, ok := getInputStats(s.flowCtx, s.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(s.ctx); sp != nil {
		tracing.SetSpanStats(
			sp,
			&SorterStats{
				InputStats:       is,
				MaxAllocatedMem:  s.memMonitor.MaximumBytes(),
				MaxAllocatedDisk: s.diskMonitor.MaximumBytes(),
			},
		)
	}
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
	proc := &sortAllProcessor{}
	if err := proc.sorterBase.init(
		proc, flowCtx, processorID, input, post, out,
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

// fill fills s.rows with the input's rows.
//
// Metadata is buffered in s.trailingMeta.
//
// The ok retval is false if an error occurred or if the input returned an error
// metadata record. The caller is expected to inspect the error (if any) and
// drain if it's not recoverable. It is possible for ok to be false even if no
// error is returned - in case an error metadata was received.
func (s *sortAllProcessor) fill() (ok bool, _ error) {
	ctx := s.evalCtx.Ctx()

	for {
		row, meta := s.input.Next()
		if meta != nil {
			s.trailingMeta = append(s.trailingMeta, *meta)
			if meta.Err != nil {
				return false, nil
			}
			continue
		}
		if row == nil {
			break
		}

		if err := s.rows.AddRow(ctx, row); err != nil {
			return false, err
		}
	}
	s.rows.Sort(ctx)

	s.i = s.rows.NewFinalIterator(ctx)
	s.i.Rewind()
	return true, nil
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
type sortTopKProcessor struct {
	sorterBase
	k int64
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
	ordering := convertToColumnOrdering(spec.OutputOrdering)
	proc := &sortTopKProcessor{k: k}
	if err := proc.sorterBase.init(
		proc, flowCtx, processorID, input, post, out,
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
				s.rows.InitTopK()
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
	s.i = s.rows.NewFinalIterator(ctx)
	s.i.Rewind()
	return ctx
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

	alloc sqlbase.DatumAlloc

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
	ordering := convertToColumnOrdering(spec.OutputOrdering)

	proc := &sortChunksProcessor{}
	if err := proc.sorterBase.init(
		proc, flowCtx, processorID, input, post, out, ordering, spec.OrderingMatchLen,
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
	proc.i = proc.rows.NewFinalIterator(proc.ctx)
	return proc, nil
}

// chunkCompleted is a helper function that determines if the given row shares the same
// values for the first matchLen ordering columns with the given prefix.
func (s *sortChunksProcessor) chunkCompleted(
	nextChunkRow, prefix sqlbase.EncDatumRow,
) (bool, error) {
	types := s.input.OutputTypes()
	for _, ord := range s.ordering[:s.matchLen] {
		col := ord.ColIdx
		cmp, err := nextChunkRow[col].Distinct(&types[col], &s.alloc, s.evalCtx, &prefix[col])
		if cmp || err != nil {
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
			s.nextChunkRow = nextChunkRow
			break
		}

		if err := s.rows.AddRow(ctx, nextChunkRow); err != nil {
			return false, err
		}
	}

	s.rows.Sort(ctx)

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
		ok, err := s.i.Valid()
		if err != nil {
			s.moveToDraining(err)
			break
		}
		// If we don't have an active chunk, clear and refill it.
		if !ok {
			ctx := s.evalCtx.Ctx()
			if err := s.rows.UnsafeReset(ctx); err != nil {
				s.moveToDraining(err)
				break
			}
			valid, err := s.fill()
			if !valid || err != nil {
				s.moveToDraining(err)
				break
			}
			s.i.Close()
			s.i = s.rows.NewFinalIterator(ctx)
			s.i.Rewind()
			if ok, err := s.i.Valid(); err != nil || !ok {
				s.moveToDraining(err)
				break
			}
		}

		// If we have an active chunk, get a row from it.
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

// ConsumerDone is part of the RowSource interface.
func (s *sortChunksProcessor) ConsumerDone() {
	s.moveToDraining(nil /* err */)
}

// ConsumerClosed is part of the RowSource interface.
func (s *sortChunksProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	s.close()
}
