// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// NewSorter returns a new sort operator, which sorts its input on the columns
// given in orderingCols. The inputTypes must correspond 1-1 with the columns
// in the input operator.
func NewSorter(
	allocator *colmem.Allocator,
	input colexecop.Operator,
	inputTypes []*types.T,
	orderingCols []execinfrapb.Ordering_Column,
) (colexecop.Operator, error) {
	return newSorter(allocator, newAllSpooler(allocator, input, inputTypes), inputTypes, orderingCols)
}

func newSorter(
	allocator *colmem.Allocator,
	input spooler,
	inputTypes []*types.T,
	orderingCols []execinfrapb.Ordering_Column,
) (colexecop.ResettableOperator, error) {
	partitioners := make([]partitioner, len(orderingCols)-1)

	var err error
	for i, ord := range orderingCols {
		if !isSorterSupported(inputTypes[ord.ColIdx], ord.Direction) {
			return nil, errors.Errorf("sorter for type: %s and direction: %s not supported", inputTypes[ord.ColIdx], ord.Direction)
		}
		if i < len(orderingCols)-1 {
			partitioners[i], err = newPartitioner(inputTypes[ord.ColIdx], false /* nullsAreDistinct */)
			if err != nil {
				return nil, err
			}
		}
	}

	return &sortOp{
		allocator:    allocator,
		input:        input,
		inputTypes:   inputTypes,
		sorters:      make([]colSorter, len(orderingCols)),
		partitioners: partitioners,
		orderingCols: orderingCols,
		state:        sortSpooling,
	}, nil
}

// spooler is a column vector operator that spools the data from its input.
type spooler interface {
	execinfra.OpNode

	// init initializes this spooler and will be called once at the setup time.
	init(context.Context)
	// spool performs the actual spooling.
	spool()
	// getValues returns ith Vec of the already spooled data.
	getValues(i int) coldata.Vec
	// getNumTuples returns the number of spooled tuples.
	getNumTuples() int
	// getPartitionsCol returns a partitions column vector in which every true
	// value indicates a start of a different partition (i.e. "chunk") within
	// spooled tuples. It should return nil if all the tuples belong to the same
	// partition.
	getPartitionsCol() []bool
	// getWindowedBatch returns a batch that is a "window" into all Vecs of the
	// already spooled data, with tuples in range [startIdx, endIdx). This batch
	// is not allowed to be modified and is only safe to use until the next call
	// to this method.
	// TODO(yuzefovich): one idea we might want to implement at some point is
	// adding a wrapper on top of a coldata.Batch that is coldata.ImmutableBatch
	// that returns coldata.ImmutableVecs to enforce immutability.
	getWindowedBatch(startIdx, endIdx int) coldata.Batch
}

// allSpooler is the spooler that spools all tuples from the input. It is used
// by the general sorter over the whole input.
type allSpooler struct {
	colexecop.OneInputNode
	colexecop.NonExplainable

	allocator *colmem.Allocator
	// inputTypes contains the types of all of the columns from the input.
	inputTypes []*types.T
	// bufferedTuples stores all the values from the input after spooling. Each
	// Vec in this batch is the entire column from the input.
	bufferedTuples *colexecutils.AppendOnlyBufferedBatch
	// spooled indicates whether spool() has already been called.
	spooled       bool
	windowedBatch coldata.Batch
}

var _ spooler = &allSpooler{}
var _ colexecop.Resetter = &allSpooler{}

func newAllSpooler(
	allocator *colmem.Allocator, input colexecop.Operator, inputTypes []*types.T,
) spooler {
	return &allSpooler{
		OneInputNode: colexecop.NewOneInputNode(input),
		allocator:    allocator,
		inputTypes:   inputTypes,
	}
}

func (p *allSpooler) init(ctx context.Context) {
	p.Input.Init(ctx)
	p.bufferedTuples = colexecutils.NewAppendOnlyBufferedBatch(p.allocator, p.inputTypes, nil /* colsToStore */)
	p.windowedBatch = p.allocator.NewMemBatchWithFixedCapacity(p.inputTypes, 0 /* size */)
}

func (p *allSpooler) spool() {
	if p.spooled {
		colexecerror.InternalError(errors.AssertionFailedf("spool() is called for the second time"))
	}
	p.spooled = true
	for batch := p.Input.Next(); batch.Length() != 0; batch = p.Input.Next() {
		p.allocator.PerformAppend(p.bufferedTuples, func() {
			p.bufferedTuples.AppendTuples(batch, 0 /* startIdx */, batch.Length())
		})
	}
}

func (p *allSpooler) getValues(i int) coldata.Vec {
	if !p.spooled {
		colexecerror.InternalError(errors.AssertionFailedf("getValues() is called before spool()"))
	}
	return p.bufferedTuples.ColVec(i)
}

func (p *allSpooler) getNumTuples() int {
	return p.bufferedTuples.Length()
}

func (p *allSpooler) getPartitionsCol() []bool {
	if !p.spooled {
		colexecerror.InternalError(errors.AssertionFailedf("getPartitionsCol() is called before spool()"))
	}
	return nil
}

func (p *allSpooler) getWindowedBatch(startIdx, endIdx int) coldata.Batch {
	// We don't need to worry about selection vectors here because if these were
	// present on the original input batches, they have been removed when we were
	// buffering up tuples.
	for i := range p.inputTypes {
		window := p.bufferedTuples.ColVec(i).Window(startIdx, endIdx)
		p.windowedBatch.ReplaceCol(window, i)
	}
	p.windowedBatch.SetSelection(false)
	p.windowedBatch.SetLength(endIdx - startIdx)
	return p.windowedBatch
}

func (p *allSpooler) Reset(ctx context.Context) {
	if r, ok := p.Input.(colexecop.Resetter); ok {
		r.Reset(ctx)
	}
	p.spooled = false
	p.bufferedTuples.ResetInternalBatch()
}

type sortOp struct {
	colexecop.InitHelper

	allocator *colmem.Allocator
	input     spooler

	// inputTypes contains the types of all of the columns from input.
	inputTypes []*types.T
	// orderingCols is the ordered list of column orderings that the sorter should
	// sort on.
	orderingCols []execinfrapb.Ordering_Column
	// sorters contains one colSorter per sort column. The instantiation of
	// sorters occurs within the sort method rather than during construction
	// of the sortOp so that we can correctly choose a sorter based on
	// whether the input has nulls or not.
	sorters []colSorter
	// partitioners contains one partitioner per sort column except for the last,
	// which doesn't need to be partitioned.
	partitioners []partitioner

	// order maintains the order of tuples in the batch, after sorting. The value
	// at index i in order is the ordinal value of the tuple in the input that
	// belongs at index i. For example, if the input column to sort was
	// [c,b,a,d], the order vector after sorting would be [2,1,0,3].
	order []int
	// emitted is the number of tuples emitted so far.
	emitted int
	// state is the current state of the sort.
	state sortState

	output coldata.Batch

	exported int
}

var _ colexecop.BufferingInMemoryOperator = &sortOp{}
var _ colexecop.Resetter = &sortOp{}

// colSorter is a single-column sorter, specialized on a particular type.
type colSorter interface {
	// init prepares this sorter, given a particular Vec and an order vector,
	// which must be the same size as the input Vec and will be permuted with
	// the same swaps as the column.
	init(ctx context.Context, col coldata.Vec, order []int)
	// sort globally sorts this sorter's column.
	sort()
	// sortPartitions sorts this sorter's column once for every partition in the
	// partition slice.
	sortPartitions(partitions []int)
}

func (p *sortOp) Init(ctx context.Context) {
	if !p.InitHelper.Init(ctx) {
		return
	}
	p.input.init(p.Ctx)
}

// sortState represents the state of the sort operator.
type sortState int

const (
	// sortSpooling is the initial state of the operator, where it spools its
	// input.
	sortSpooling sortState = iota
	// sortSorting is the second state of the operator, where it actually sorts
	// all the spooled data.
	sortSorting
	// sortEmitting is the third state of the operator, indicating that each call
	// to Next will return another batch of the sorted data.
	sortEmitting
	// sortDone is the final state of the operator, where it always returns a
	// zero batch.
	sortDone
)

func (p *sortOp) Next() coldata.Batch {
	for {
		switch p.state {
		case sortSpooling:
			p.input.spool()
			p.state = sortSorting
		case sortSorting:
			p.sort()
			p.state = sortEmitting
		case sortEmitting:
			toEmit := p.input.getNumTuples() - p.emitted
			if toEmit == 0 {
				p.state = sortDone
				continue
			}
			if toEmit > coldata.BatchSize() {
				toEmit = coldata.BatchSize()
			}
			// For now, we don't enforce any footprint-based memory limit.
			// TODO(yuzefovich): refactor this.
			const maxBatchMemSize = math.MaxInt64
			p.output, _ = p.allocator.ResetMaybeReallocate(p.inputTypes, p.output, toEmit, maxBatchMemSize)
			newEmitted := p.emitted + toEmit
			for j := 0; j < len(p.inputTypes); j++ {
				// At this point, we have already fully sorted the input. It is ok to do
				// this Copy outside of the allocator - the work has been done, but
				// theoretically it is possible to hit the limit here (mainly with
				// variable-sized types like Bytes). Nonetheless, for performance reasons
				// it would be sad to fallback to disk at this point.
				p.output.ColVec(j).Copy(
					coldata.CopySliceArgs{
						SliceArgs: coldata.SliceArgs{
							Sel:         p.order,
							Src:         p.input.getValues(j),
							SrcStartIdx: p.emitted,
							SrcEndIdx:   newEmitted,
						},
					},
				)
			}
			p.output.SetLength(toEmit)
			p.emitted = newEmitted
			return p.output
		case sortDone:
			return coldata.ZeroBatch
		default:
			colexecerror.InternalError(errors.AssertionFailedf("invalid sort state %v", p.state))
			// This code is unreachable, but the compiler cannot infer that.
			return nil
		}
	}
}

// sort sorts the spooled tuples, so it must be called after spool() has been
// performed.
func (p *sortOp) sort() {
	spooledTuples := p.input.getNumTuples()
	if spooledTuples == 0 {
		// There is nothing to sort.
		return
	}
	// Allocate p.order and p.workingSpace if it hasn't been allocated yet or the
	// underlying memory is insufficient.
	if p.order == nil || cap(p.order) < spooledTuples {
		p.order = make([]int, spooledTuples)
	}
	p.order = p.order[:spooledTuples]

	// Initialize the order vector to the ordinal positions within the input set.
	for i := 0; i < len(p.order); i++ {
		p.order[i] = i
	}

	for i := range p.orderingCols {
		inputVec := p.input.getValues(int(p.orderingCols[i].ColIdx))
		p.sorters[i] = newSingleSorter(p.inputTypes[p.orderingCols[i].ColIdx], p.orderingCols[i].Direction, inputVec.MaybeHasNulls())
		p.sorters[i].init(p.Ctx, inputVec, p.order)
	}

	// Now, sort each column in turn.
	sorters := p.sorters
	partitionsCol := p.input.getPartitionsCol()
	omitNextPartitioning := false
	offset := 0
	if partitionsCol == nil {
		// All spooled tuples belong to the same partition, so the first column
		// doesn't need special treatment - we just globally sort it.
		p.sorters[0].sort()
		if len(p.sorters) == 1 {
			// We're done sorting. Transition to emitting.
			return
		}
		sorters = sorters[1:]
		partitionsCol = make([]bool, spooledTuples)
	} else {
		// There are at least two partitions already, so the first column needs the
		// same special treatment as all others. The general sequence is as
		// follows: global sort -> partition -> sort partitions -> partition ->
		// -> sort partitions -> partition -> sort partitions -> ..., but in this
		// case, global sort doesn't make sense and partitioning has already been
		// done, so we want to skip the first partitioning step and sort partitions
		// right away. Also, in order to account for not performed global sort, we
		// introduce an offset of 1 for partitioners.
		omitNextPartitioning = true
		offset = 1
	}

	// The rest of the columns need p sorts, one per partition in the previous
	// column. For example, in a two column sort:
	//
	// 1  b
	// 2  b
	// 1  a
	// 2  a
	//
	// We'll first sort the first column:
	//
	// 1  b
	// 1  a
	// 2  b
	// 2  a
	//
	// Then, for each group in the sorted, first column, we sort the second column:
	//
	// 1 a
	// 1 b
	// 2 a
	// 2 b

	partitions := make([]int, 0, 16)
	for i, sorter := range sorters {
		if !omitNextPartitioning {
			// We partition the previous column by running an ordered distinct operation
			// on it, ORing the results together with each subsequent column. This
			// produces a distinct vector (a boolean vector that has true in each
			// position that is different from the last position).
			p.partitioners[i-offset].partitionWithOrder(p.input.getValues(int(p.orderingCols[i-offset].ColIdx)), p.order,
				partitionsCol, spooledTuples)
		} else {
			omitNextPartitioning = false
		}
		// Convert the distinct vector into a selection vector - a vector of indices
		// that were true in the distinct vector.
		partitions = boolVecToSel64(partitionsCol, partitions[:0])
		// For each partition (set of tuples that are identical in all of the sort
		// columns we've seen so far), sort based on the new column.
		sorter.sortPartitions(partitions)
	}
}

func (p *sortOp) Reset(ctx context.Context) {
	if r, ok := p.input.(colexecop.Resetter); ok {
		r.Reset(ctx)
	}
	p.emitted = 0
	p.exported = 0
	p.state = sortSpooling
}

func (p *sortOp) ChildCount(verbose bool) int {
	return 1
}

func (p *sortOp) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		return p.input
	}
	colexecerror.InternalError(errors.AssertionFailedf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

func (p *sortOp) ExportBuffered(colexecop.Operator) coldata.Batch {
	if p.exported == p.input.getNumTuples() {
		return coldata.ZeroBatch
	}
	newExported := p.exported + coldata.BatchSize()
	if newExported > p.input.getNumTuples() {
		newExported = p.input.getNumTuples()
	}
	b := p.input.getWindowedBatch(p.exported, newExported)
	p.exported = newExported
	return b
}
