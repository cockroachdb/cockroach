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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/errors"
)

// sorter sorts the input rows according to the specified ordering.
type sorterBase struct {
	execinfra.ProcessorBase

	input    execinfra.RowSource
	ordering colinfo.ColumnOrdering
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
	ordering colinfo.ColumnOrdering,
	matchLen uint32,
	opts execinfra.ProcStateOpts,
) error {
	ctx := flowCtx.EvalCtx.Ctx()
	if execinfra.ShouldCollectStats(ctx, flowCtx) {
		input = newInputStatCollector(input)
		s.ExecStatsForTrace = s.execStatsForTrace
	}

	// Limit the memory use by creating a child monitor with a hard limit.
	// The processor will overflow to disk if this limit is not enough.
	memMonitor := execinfra.NewLimitedMonitor(ctx, flowCtx.EvalCtx.Mon, flowCtx, fmt.Sprintf("%s-limited", processorName))
	if err := s.ProcessorBase.Init(
		self, post, input.OutputTypes(), flowCtx, processorID, output, memMonitor, opts,
	); err != nil {
		memMonitor.Stop(ctx)
		return err
	}

	s.diskMonitor = execinfra.NewMonitor(ctx, flowCtx.DiskMonitor, fmt.Sprintf("%s-disk", processorName))
	rc := rowcontainer.DiskBackedRowContainer{}
	rc.Init(
		ordering,
		input.OutputTypes(),
		s.EvalCtx,
		flowCtx.Cfg.TempStorage,
		memMonitor,
		s.diskMonitor,
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
func (s *sorterBase) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
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
		s.rows.Close(s.Ctx)
		s.MemMonitor.Stop(s.Ctx)
		if s.diskMonitor != nil {
			s.diskMonitor.Stop(s.Ctx)
		}
	}
}

// execStatsForTrace implements ProcessorBase.ExecStatsForTrace.
func (s *sorterBase) execStatsForTrace() *execinfrapb.ComponentStats {
	is, ok := getInputStats(s.input)
	if !ok {
		return nil
	}
	return &execinfrapb.ComponentStats{
		Inputs: []execinfrapb.InputStats{is},
		Exec: execinfrapb.ExecStats{
			MaxAllocatedMem:  optional.MakeUint(uint64(s.MemMonitor.MaximumBytes())),
			MaxAllocatedDisk: optional.MakeUint(uint64(s.diskMonitor.MaximumBytes())),
		},
		Output: s.OutputHelper.Stats(),
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
	if post.Limit != 0 {
		// The sorter needs to produce Offset + Limit rows. The ProcOutputHelper
		// will discard the first Offset ones.
		if post.Limit <= math.MaxUint64-post.Offset {
			count = post.Limit + post.Offset
		}
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
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
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
func (s *sortAllProcessor) Start(ctx context.Context) {
	ctx = s.StartInternal(ctx, sortAllProcName)
	s.input.Start(ctx)

	valid, err := s.fill()
	if !valid || err != nil {
		s.MoveToDraining(err)
	}
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
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
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
func (s *sortTopKProcessor) Start(ctx context.Context) {
	ctx = s.StartInternal(ctx, sortTopKProcName)
	s.input.Start(ctx)

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

	alloc rowenc.DatumAlloc

	// sortChunksProcessor accumulates rows that are equal on a prefix, until it
	// encounters a row that is greater. It stores that greater row in nextChunkRow
	nextChunkRow rowenc.EncDatumRow
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
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
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
	nextChunkRow, prefix rowenc.EncDatumRow,
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
func (s *sortChunksProcessor) Start(ctx context.Context) {
	ctx = s.StartInternal(ctx, sortChunksProcName)
	s.input.Start(ctx)
}

// Next is part of the RowSource interface.
func (s *sortChunksProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
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
