// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

// sorter sorts the input rows according to the specified ordering.
type sorterBase struct {
	execinfra.ProcessorBase

	input    execinfra.RowSource
	ordering sqlbase.ColumnOrdering
	matchLen uint32

	rows rowcontainer.SortableRowContainer
	i    rowcontainer.RowIterator

	// Only set if the ability to spill to disk is enabled.
	diskMonitor *mon.BytesMonitor
}

func (s *sorterBase) init(
	self execinfra.RowSource,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	processorName string,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
	ordering sqlbase.ColumnOrdering,
	matchLen uint32,
	opts execinfra.ProcStateOpts,
) error {
	ctx := flowCtx.EvalCtx.Ctx()
	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		input = newInputStatCollector(input)
		s.FinishTrace = s.outputStatsToTrace
	}

	// Limit the memory use by creating a child monitor with a hard limit.
	// The processor will overflow to disk if this limit is not enough.
	memMonitor := execinfra.NewLimitedMonitor(ctx, flowCtx.EvalCtx.Mon, flowCtx.Cfg, fmt.Sprintf("%s-limited", processorName))
	if err := s.ProcessorBase.Init(
		self, post, input.OutputTypes(), flowCtx, processorID, output, memMonitor, opts,
	); err != nil {
		memMonitor.Stop(ctx)
		return err
	}

	s.diskMonitor = execinfra.NewMonitor(ctx, flowCtx.Cfg.DiskMonitor, fmt.Sprintf("%s-disk", processorName))
	rc := rowcontainer.DiskBackedRowContainer{}
	rc.Init(
		ordering,
		input.OutputTypes(),
		s.EvalCtx,
		flowCtx.Cfg.TempStorage,
		memMonitor,
		s.diskMonitor,
		0, /* rowCapacity */
	)
	s.rows = &rc

	s.input = input
	s.ordering = ordering
	s.matchLen = matchLen
	return nil
}

// Next is part of the RowSource interface. It is extracted into sorterBase
// because this implementation of next is shared between the sortAllProcessor
// and the sortTopKProcessor.
func (s *sorterBase) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for s.State == execinfra.StateRunning {
		if ok, err := s.i.Valid(); err != nil || !ok {
			s.MoveToDraining(err)
			break
		}

		row, err := s.i.Row()
		if err != nil {
			s.MoveToDraining(err)
			break
		}
		s.i.Next()

		if outRow := s.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, s.DrainHelper()
}

func (s *sorterBase) close() {
	// We are done sorting rows, close the iterator we have open.
	if s.InternalClose() {
		if s.i != nil {
			s.i.Close()
		}
		ctx := s.Ctx
		s.rows.Close(ctx)
		s.MemMonitor.Stop(ctx)
		if s.diskMonitor != nil {
			s.diskMonitor.Stop(ctx)
		}
	}
}

var _ execinfrapb.DistSQLSpanStats = &SorterStats{}

const sorterTagPrefix = "sorter."

// Stats implements the SpanStats interface.
func (ss *SorterStats) Stats() map[string]string {
	statsMap := ss.InputStats.Stats(sorterTagPrefix)
	statsMap[sorterTagPrefix+MaxMemoryTagSuffix] = humanizeutil.IBytes(ss.MaxAllocatedMem)
	statsMap[sorterTagPrefix+MaxDiskTagSuffix] = humanizeutil.IBytes(ss.MaxAllocatedDisk)
	return statsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (ss *SorterStats) StatsForQueryPlan() []string {
	stats := ss.InputStats.StatsForQueryPlan("" /* prefix */)

	if ss.MaxAllocatedMem != 0 {
		stats = append(stats,
			fmt.Sprintf("%s: %s", MaxMemoryQueryPlanSuffix, humanizeutil.IBytes(ss.MaxAllocatedMem)))
	}

	if ss.MaxAllocatedDisk != 0 {
		stats = append(stats,
			fmt.Sprintf("%s: %s", MaxDiskQueryPlanSuffix, humanizeutil.IBytes(ss.MaxAllocatedDisk)))
	}

	return stats
}

// outputStatsToTrace outputs the collected sorter stats to the trace. Will fail
// silently if stats are not being collected.
func (s *sorterBase) outputStatsToTrace() {
	is, ok := getInputStats(s.FlowCtx, s.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(s.Ctx); sp != nil {
		tracing.SetSpanStats(
			sp,
			&SorterStats{
				InputStats:       is,
				MaxAllocatedMem:  s.MemMonitor.MaximumBytes(),
				MaxAllocatedDisk: s.diskMonitor.MaximumBytes(),
			},
		)
	}
}

func newSorter(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.SorterSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	count := uint64(0)
	if post.Limit != 0 && post.Filter.Empty() {
		// The sorter needs to produce Offset + Limit rows. The ProcOutputHelper
		// will discard the first Offset ones.
		// LIMIT and OFFSET should each never be greater than math.MaxInt64, the
		// parser ensures this.
		if post.Limit > math.MaxInt64 || post.Offset > math.MaxInt64 {
			return nil, errors.AssertionFailedf(
				"error creating sorter: limit %d offset %d too large",
				errors.Safe(post.Limit), errors.Safe(post.Offset))
		}
		count = post.Limit + post.Offset
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

var _ execinfra.Processor = &sortAllProcessor{}
var _ execinfra.RowSource = &sortAllProcessor{}

const sortAllProcName = "sortAll"

func newSortAllProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.SorterSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	out execinfra.RowReceiver,
) (execinfra.Processor, error) {
	proc := &sortAllProcessor{}
	if err := proc.sorterBase.init(
		proc, flowCtx, processorID, sortAllProcName, input, post, out,
		execinfrapb.ConvertToColumnOrdering(spec.OutputOrdering),
		spec.OrderingMatchLen,
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{input},
			TrailingMetaCallback: func(context.Context) []execinfrapb.ProducerMetadata {
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
	ctx = s.StartInternal(ctx, sortAllProcName)

	valid, err := s.fill()
	if !valid || err != nil {
		s.MoveToDraining(err)
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
	ctx := s.EvalCtx.Ctx()

	for {
		row, meta := s.input.Next()
		if meta != nil {
			s.AppendTrailingMeta(*meta)
			if meta.Err != nil {
				return false, nil //nolint:returnerrcheck
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
	k uint64
}

var _ execinfra.Processor = &sortTopKProcessor{}
var _ execinfra.RowSource = &sortTopKProcessor{}

const sortTopKProcName = "sortTopK"

var errSortTopKZeroK = errors.New("invalid value 0 for k")

func newSortTopKProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.SorterSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	out execinfra.RowReceiver,
	k uint64,
) (execinfra.Processor, error) {
	if k == 0 {
		return nil, errors.NewAssertionErrorWithWrappedErrf(errSortTopKZeroK,
			"error creating top k sorter")
	}
	ordering := execinfrapb.ConvertToColumnOrdering(spec.OutputOrdering)
	proc := &sortTopKProcessor{k: k}
	if err := proc.sorterBase.init(
		proc, flowCtx, processorID, sortTopKProcName, input, post, out,
		ordering, spec.OrderingMatchLen,
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{input},
			TrailingMetaCallback: func(context.Context) []execinfrapb.ProducerMetadata {
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
	ctx = s.StartInternal(ctx, sortTopKProcName)

	// The execution loop for the SortTopK processor is similar to that of the
	// SortAll processor; the difference is that we push rows into a max-heap
	// of size at most K, and only sort those.
	heapCreated := false
	for {
		row, meta := s.input.Next()
		if meta != nil {
			s.AppendTrailingMeta(*meta)
			if meta.Err != nil {
				s.MoveToDraining(nil /* err */)
				break
			}
			continue
		}
		if row == nil {
			break
		}

		if uint64(s.rows.Len()) < s.k {
			// Accumulate up to k values.
			if err := s.rows.AddRow(ctx, row); err != nil {
				s.MoveToDraining(err)
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
				s.MoveToDraining(err)
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

var _ execinfra.Processor = &sortChunksProcessor{}
var _ execinfra.RowSource = &sortChunksProcessor{}

const sortChunksProcName = "sortChunks"

func newSortChunksProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.SorterSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	out execinfra.RowReceiver,
) (execinfra.Processor, error) {
	ordering := execinfrapb.ConvertToColumnOrdering(spec.OutputOrdering)

	proc := &sortChunksProcessor{}
	if err := proc.sorterBase.init(
		proc, flowCtx, processorID, sortChunksProcName, input, post, out, ordering, spec.OrderingMatchLen,
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{input},
			TrailingMetaCallback: func(context.Context) []execinfrapb.ProducerMetadata {
				proc.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	proc.i = proc.rows.NewFinalIterator(proc.Ctx)
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
		cmp, err := nextChunkRow[col].Compare(types[col], &s.alloc, s.EvalCtx, &prefix[col])
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
	ctx := s.Ctx

	var meta *execinfrapb.ProducerMetadata

	nextChunkRow := s.nextChunkRow
	s.nextChunkRow = nil
	for nextChunkRow == nil {
		nextChunkRow, meta = s.input.Next()
		if meta != nil {
			s.AppendTrailingMeta(*meta)
			if meta.Err != nil {
				return false, nil //nolint:returnerrcheck
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
			s.AppendTrailingMeta(*meta)
			if meta.Err != nil {
				return false, nil //nolint:returnerrcheck
			}
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
	return s.StartInternal(ctx, sortChunksProcName)
}

// Next is part of the RowSource interface.
func (s *sortChunksProcessor) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	ctx := s.Ctx
	for s.State == execinfra.StateRunning {
		ok, err := s.i.Valid()
		if err != nil {
			s.MoveToDraining(err)
			break
		}
		// If we don't have an active chunk, clear and refill it.
		if !ok {
			if err := s.rows.UnsafeReset(ctx); err != nil {
				s.MoveToDraining(err)
				break
			}
			valid, err := s.fill()
			if !valid || err != nil {
				s.MoveToDraining(err)
				break
			}
			s.i.Close()
			s.i = s.rows.NewFinalIterator(ctx)
			s.i.Rewind()
			if ok, err := s.i.Valid(); err != nil || !ok {
				s.MoveToDraining(err)
				break
			}
		}

		// If we have an active chunk, get a row from it.
		row, err := s.i.Row()
		if err != nil {
			s.MoveToDraining(err)
			break
		}
		s.i.Next()

		if outRow := s.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, s.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (s *sortChunksProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	s.close()
}
