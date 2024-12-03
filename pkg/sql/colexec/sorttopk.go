// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"container/heap"
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

const (
	topKVecIdx  = 0
	inputVecIdx = 1
)

// NewTopKSorter returns a new sort operator, which sorts its input on the
// columns given in orderingCols and returns the first K rows. The inputTypes
// must correspond 1-1 with the columns in the input operator. If matchLen is
// non-zero, then the input tuples must be sorted on first matchLen columns.
func NewTopKSorter(
	allocator *colmem.Allocator,
	input colexecop.Operator,
	inputTypes []*types.T,
	orderingCols []execinfrapb.Ordering_Column,
	matchLen int,
	k uint64,
	maxOutputBatchMemSize int64,
) colexecop.ResettableOperator {
	if matchLen < 0 {
		colexecerror.InternalError(errors.AssertionFailedf("invalid matchLen %v", matchLen))
	}
	base := &topKSorter{
		allocator:       allocator,
		OneInputNode:    colexecop.NewOneInputNode(input),
		inputTypes:      inputTypes,
		orderingCols:    orderingCols,
		k:               k,
		hasPartialOrder: matchLen > 0,
		matchLen:        matchLen,
	}
	base.helper.Init(allocator, maxOutputBatchMemSize)
	if base.hasPartialOrder {
		base.heaper = &topKPartialOrderHeaper{base}
		partialOrderCols := make([]uint32, matchLen)
		for i := range partialOrderCols {
			partialOrderCols[i] = orderingCols[i].ColIdx
		}
		base.orderState.distincterInput = &colexecop.FeedOperator{}
		base.orderState.distincter, base.orderState.distinctOutput = colexecbase.OrderedDistinctColsToOperators(
			base.orderState.distincterInput, partialOrderCols, inputTypes, false, /* nullsAreDistinct */
		)
	} else {
		base.heaper = &topKHeaper{base}
	}
	return base
}

var _ colexecop.BufferingInMemoryOperator = &topKSorter{}
var _ colexecop.Resetter = &topKSorter{}

// topKSortState represents the state of the sort operator.
type topKSortState int

const (
	// topKSortSpooling is the initial state of the operator, where it spools
	// its input.
	topKSortSpooling topKSortState = iota
	// topKSortEmitting is the second state of the operator, indicating that
	// each call to Next will return another batch of the sorted data.
	topKSortEmitting
	// topKSortDone is the final state of the operator, where it always returns
	// a zero batch.
	topKSortDone
)

type topKSorter struct {
	colexecop.OneInputNode
	colexecop.InitHelper
	// allocator is used for the topK append-only buffered batch and the output
	// batch (the latter via the helper).
	allocator       *colmem.Allocator
	helper          colmem.AccountingHelper
	orderingCols    []execinfrapb.Ordering_Column
	inputTypes      []*types.T
	k               uint64
	matchLen        int
	hasPartialOrder bool
	// state is the current state of the sort.
	state topKSortState
	// inputBatch is the last read batch from the input.
	inputBatch coldata.Batch
	// firstUnprocessedTupleIdx indicates the index of the first tuple in
	// inputBatch that hasn't been processed yet.
	firstUnprocessedTupleIdx int
	// comparators stores one comparator per ordering column.
	comparators []vecComparator
	// topK stores the top K rows. It is not sorted internally.
	topK *colexecutils.AppendOnlyBufferedBatch
	// heap is a max heap which stores indices into topK.
	heap   []int
	heaper heap.Interface
	// sel is a selection vector which specifies an ordering on topK.
	sel []int
	// emitted is the count of rows which have been emitted so far.
	emitted int
	output  coldata.Batch

	cancelChecker colexecutils.CancelChecker

	exportedFromTopK  int
	exportedFromBatch int
	windowedBatch     coldata.Batch
	exportComplete    bool

	// orderState stores fields useful for computing top K for partially ordered inputs.
	orderState struct {
		// group stores the group number associated with each entry in topK.
		group []int
		// distincter is an operator that groups an input batch by its partially
		// ordered column values.
		distincterInput *colexecop.FeedOperator
		distincter      colexecop.Operator
		distinctOutput  []bool
	}
}

func (t *topKSorter) Init(ctx context.Context) {
	if !t.InitHelper.Init(ctx) {
		return
	}
	t.Input.Init(t.Ctx)
	t.topK = colexecutils.NewAppendOnlyBufferedBatch(t.allocator, t.inputTypes, nil /* colsToStore */)
	// TODO(harding): We only need to create a vecComparator for the input vectors
	// in orderingCols.
	t.comparators = make([]vecComparator, len(t.inputTypes))
	for i, typ := range t.inputTypes {
		t.comparators[i] = GetVecComparator(typ, 2)
	}
	if t.hasPartialOrder {
		t.orderState.distincter.Init(t.Ctx)
		t.orderState.group = make([]int, t.k)
	}
	t.cancelChecker.Init(t.Ctx)
}

func (t *topKSorter) Next() coldata.Batch {
	for {
		switch t.state {
		case topKSortSpooling:
			t.spool()
			t.state = topKSortEmitting
		case topKSortEmitting:
			output := t.emit()
			if output.Length() == 0 {
				t.state = topKSortDone
				continue
			}
			return output
		case topKSortDone:
			return coldata.ZeroBatch
		default:
			colexecerror.InternalError(errors.AssertionFailedf("invalid sort state %v", t.state))
			// This code is unreachable, but the compiler cannot infer that.
			return nil
		}
	}
}

func (t *topKSorter) Reset(ctx context.Context) {
	if r, ok := t.Input.(colexecop.Resetter); ok {
		r.Reset(ctx)
	}
	t.state = topKSortSpooling
	t.firstUnprocessedTupleIdx = 0
	t.topK.ResetInternalBatch()
	t.emitted = 0
	t.exportedFromTopK = 0
	t.exportedFromBatch = 0
	t.exportComplete = false
	if t.hasPartialOrder {
		t.orderState.distincter.(colexecop.Resetter).Reset(t.Ctx)
	}
}

func (t *topKSorter) emit() coldata.Batch {
	toEmit := t.topK.Length() - t.emitted
	if toEmit == 0 {
		// We're done.
		return coldata.ZeroBatch
	}
	t.output, _ = t.helper.ResetMaybeReallocate(t.inputTypes, t.output, toEmit)
	if toEmit > t.output.Capacity() {
		toEmit = t.output.Capacity()
	}
	for i := range t.inputTypes {
		vec := t.output.ColVec(i)
		// At this point, we have already fully sorted the input. It is ok to do
		// this Copy outside of the allocator - the work has been done, but
		// theoretically it is possible to hit the limit here (mainly with
		// variable-sized types like Bytes). Nonetheless, for performance reasons
		// it would be sad to fallback to disk at this point.
		vec.Copy(
			coldata.SliceArgs{
				Src:         t.topK.ColVec(i),
				Sel:         t.sel,
				SrcStartIdx: t.emitted,
				SrcEndIdx:   t.emitted + toEmit,
			},
		)
	}
	t.output.SetLength(toEmit)
	t.emitted += toEmit
	return t.output
}

func (t *topKSorter) updateComparators(vecIdx int, batch coldata.Batch) {
	for i := range t.inputTypes {
		t.comparators[i].setVec(vecIdx, batch.ColVec(i))
	}
}

func (t *topKSorter) ExportBuffered(colexecop.Operator) coldata.Batch {
	if t.exportComplete {
		return coldata.ZeroBatch
	}
	if t.windowedBatch == nil {
		// TODO(yuzefovich): switch to calling this method on allocator. This
		// will require plumbing unlimited allocator to work correctly in tests
		// with memory limit of 1.
		t.windowedBatch = coldata.NewMemBatchNoCols(t.inputTypes, coldata.BatchSize())
	}
	topKLen := t.topK.Length()
	// First, we check whether we have exported all tuples from the topK vector.
	if t.exportedFromTopK < topKLen {
		newExportedFromTopK := t.exportedFromTopK + coldata.BatchSize()
		if newExportedFromTopK > topKLen {
			newExportedFromTopK = topKLen
		}
		for i := range t.inputTypes {
			window := t.topK.ColVec(i).Window(t.exportedFromTopK, newExportedFromTopK)
			t.windowedBatch.ReplaceCol(window, i)
		}
		t.windowedBatch.SetSelection(false)
		t.windowedBatch.SetLength(newExportedFromTopK - t.exportedFromTopK)
		t.exportedFromTopK = newExportedFromTopK
		return t.windowedBatch
	}
	// Next, we check whether we have exported all tuples from the last read
	// batch.
	if t.inputBatch != nil && t.firstUnprocessedTupleIdx+t.exportedFromBatch < t.inputBatch.Length() {
		colexecutils.MakeWindowIntoBatch(
			t.windowedBatch, t.inputBatch, t.firstUnprocessedTupleIdx, t.inputBatch.Length(), t.inputTypes,
		)
		t.exportedFromBatch = t.windowedBatch.Length()
		return t.windowedBatch
	}
	t.exportComplete = true
	return coldata.ZeroBatch
}

// ReleaseBeforeExport implements the colexecop.BufferingInMemoryOperator
// interface.
func (t *topKSorter) ReleaseBeforeExport() {}

// ReleaseAfterExport implements the colexecop.BufferingInMemoryOperator
// interface.
func (t *topKSorter) ReleaseAfterExport(colexecop.Operator) {
	if t.allocator == nil {
		// Resources have already been released.
		return
	}
	defer t.allocator.ReleaseAll()
	*t = topKSorter{exportComplete: true}
}

// Len is part of heap.Interface and is only meant to be used internally.
func (t *topKSorter) Len() int {
	return len(t.heap)
}

// Swap is part of heap.Interface and is only meant to be used internally.
func (t *topKSorter) Swap(i, j int) {
	t.heap[i], t.heap[j] = t.heap[j], t.heap[i]
}

// Push is part of heap.Interface and is only meant to be used internally.
func (t *topKSorter) Push(x interface{}) {
	t.heap = append(t.heap, x.(int))
}

// Pop is part of heap.Interface and is only meant to be used internally.
func (t *topKSorter) Pop() interface{} {
	x := t.heap[len(t.heap)-1]
	t.heap = t.heap[:len(t.heap)-1]
	return x
}
