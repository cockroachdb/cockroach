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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/pkg/errors"
)

// NewSorter returns a new sort operator, which sorts its input on the columns
// given in orderingCols. The inputTypes must correspond 1-1 with the columns
// in the input operator.
func NewSorter(
	allocator *Allocator,
	input Operator,
	inputTypes []coltypes.T,
	orderingCols []execinfrapb.Ordering_Column,
) (Operator, error) {
	return newSorter(allocator, newAllSpooler(allocator, input, inputTypes), inputTypes, orderingCols)
}

func newSorter(
	allocator *Allocator,
	input spooler,
	inputTypes []coltypes.T,
	orderingCols []execinfrapb.Ordering_Column,
) (resettableOperator, error) {
	partitioners := make([]partitioner, len(orderingCols)-1)

	var err error
	for i, ord := range orderingCols {
		if !isSorterSupported(inputTypes[ord.ColIdx], ord.Direction) {
			return nil, errors.Errorf("sorter for type: %s and direction: %s not supported", inputTypes[ord.ColIdx], ord.Direction)
		}
		if i < len(orderingCols)-1 {
			partitioners[i], err = newPartitioner(inputTypes[ord.ColIdx])
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
	init()
	// spool performs the actual spooling.
	spool(context.Context)
	// getValues returns ith Vec of the already spooled data.
	getValues(i int) coldata.Vec
	// getNumTuples returns the number of spooled tuples.
	getNumTuples() uint64
	// getPartitionsCol returns a partitions column vector in which every true
	// value indicates a start of a different partition (i.e. "chunk") within
	// spooled tuples. It should return nil if all the tuples belong to the same
	// partition.
	getPartitionsCol() []bool
}

// allSpooler is the spooler that spools all tuples from the input. It is used
// by the general sorter over the whole input.
type allSpooler struct {
	OneInputNode
	NonExplainable

	allocator *Allocator
	// inputTypes contains the types of all of the columns from the input.
	inputTypes []coltypes.T
	// values stores all the values from the input after spooling. Each Vec in
	// this slice is the entire column from the input.
	values []coldata.Vec
	// spooledTuples is the number of tuples spooled.
	spooledTuples uint64
	// spooled indicates whether spool() has already been called.
	spooled bool
}

var _ spooler = &allSpooler{}

func newAllSpooler(allocator *Allocator, input Operator, inputTypes []coltypes.T) spooler {
	return &allSpooler{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,
		inputTypes:   inputTypes,
	}
}

func (p *allSpooler) init() {
	p.input.Init()
	p.values = make([]coldata.Vec, len(p.inputTypes))
	for i := 0; i < len(p.inputTypes); i++ {
		p.values[i] = p.allocator.NewMemColumn(p.inputTypes[i], 0)
	}
}

func (p *allSpooler) spool(ctx context.Context) {
	if p.spooled {
		execerror.VectorizedInternalPanic("spool() is called for the second time")
	}
	p.spooled = true
	batch := p.input.Next(ctx)
	var nTuples uint64
	for ; batch.Length() != 0; batch = p.input.Next(ctx) {
		for i := 0; i < len(p.values); i++ {
			p.allocator.Append(
				p.values[i],
				coldata.SliceArgs{
					ColType:   p.inputTypes[i],
					Src:       batch.ColVec(i),
					Sel:       batch.Selection(),
					DestIdx:   nTuples,
					SrcEndIdx: uint64(batch.Length()),
				},
			)
		}
		nTuples += uint64(batch.Length())
	}
	p.spooledTuples = nTuples
}

func (p *allSpooler) getValues(i int) coldata.Vec {
	if !p.spooled {
		execerror.VectorizedInternalPanic("getValues() is called before spool()")
	}
	return p.values[i]
}

func (p *allSpooler) getNumTuples() uint64 {
	if !p.spooled {
		execerror.VectorizedInternalPanic("getNumTuples() is called before spool()")
	}
	return p.spooledTuples
}

func (p *allSpooler) getPartitionsCol() []bool {
	if !p.spooled {
		execerror.VectorizedInternalPanic("getPartitionsCol() is called before spool()")
	}
	return nil
}

func (p *allSpooler) reset() {
	p.spooledTuples = 0
	if r, ok := p.input.(resetter); ok {
		r.reset()
	}
}

type sortOp struct {
	allocator *Allocator
	input     spooler

	// inputTypes contains the types of all of the columns from input.
	inputTypes []coltypes.T
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
	order []uint64
	// emitted is the number of tuples emitted so far.
	emitted uint64
	// state is the current state of the sort.
	state sortState

	output coldata.Batch
}

var _ Operator = &sortOp{}

// colSorter is a single-column sorter, specialized on a particular type.
type colSorter interface {
	// init prepares this sorter, given a particular Vec and an order vector,
	// which must be the same size as the input Vec and will be permuted with
	// the same swaps as the column.
	init(col coldata.Vec, order []uint64)
	// sort globally sorts this sorter's column.
	sort(ctx context.Context)
	// sortPartitions sorts this sorter's column once for every partition in the
	// partition slice.
	sortPartitions(ctx context.Context, partitions []uint64)
}

func (p *sortOp) Init() {
	p.input.init()
	p.output = p.allocator.NewMemBatch(p.inputTypes)
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
)

func (p *sortOp) Next(ctx context.Context) coldata.Batch {
	switch p.state {
	case sortSpooling:
		p.input.spool(ctx)
		p.state = sortSorting
		fallthrough
	case sortSorting:
		p.sort(ctx)
		p.state = sortEmitting
		fallthrough
	case sortEmitting:
		newEmitted := p.emitted + uint64(coldata.BatchSize())
		if newEmitted > p.input.getNumTuples() {
			newEmitted = p.input.getNumTuples()
		}
		p.output.ResetInternalBatch()
		p.output.SetLength(uint16(newEmitted - p.emitted))
		if p.output.Length() == 0 {
			return p.output
		}

		for j := 0; j < len(p.inputTypes); j++ {
			p.allocator.Copy(
				p.output.ColVec(j),
				coldata.CopySliceArgs{
					SliceArgs: coldata.SliceArgs{
						ColType:     p.inputTypes[j],
						Src:         p.input.getValues(j),
						SrcStartIdx: p.emitted,
						SrcEndIdx:   p.emitted + uint64(p.output.Length()),
					},
					Sel64: p.order,
				},
			)
		}
		p.emitted = newEmitted
		return p.output
	}
	execerror.VectorizedInternalPanic(fmt.Sprintf("invalid sort state %v", p.state))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// sort sorts the spooled tuples, so it must be called after spool() has been
// performed.
func (p *sortOp) sort(ctx context.Context) {
	spooledTuples := p.input.getNumTuples()
	if spooledTuples == 0 {
		// There is nothing to sort.
		return
	}
	// Allocate p.order and p.workingSpace if it hasn't been allocated yet or the
	// underlying memory is insufficient.
	if p.order == nil || uint64(cap(p.order)) < spooledTuples {
		p.order = make([]uint64, spooledTuples)
	}
	p.order = p.order[:spooledTuples]

	// Initialize the order vector to the ordinal positions within the input set.
	for i := uint64(0); i < uint64(len(p.order)); i++ {
		p.order[i] = i
	}

	for i := range p.orderingCols {
		inputVec := p.input.getValues(int(p.orderingCols[i].ColIdx))
		p.sorters[i] = newSingleSorter(p.inputTypes[p.orderingCols[i].ColIdx], p.orderingCols[i].Direction, inputVec.MaybeHasNulls())
		p.sorters[i].init(inputVec, p.order)
	}

	// Now, sort each column in turn.
	sorters := p.sorters
	partitionsCol := p.input.getPartitionsCol()
	omitNextPartitioning := false
	offset := 0
	if partitionsCol == nil {
		// All spooled tuples belong to the same partition, so the first column
		// doesn't need special treatment - we just globally sort it.
		p.sorters[0].sort(ctx)
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

	partitions := make([]uint64, 0, 16)
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
		sorter.sortPartitions(ctx, partitions)
	}
}

func (p *sortOp) reset() {
	if r, ok := p.input.(resetter); ok {
		r.reset()
	}
	p.emitted = 0
	p.state = sortSpooling
}

func (p *sortOp) ChildCount() int {
	return 1
}

func (p *sortOp) Child(nth int) execinfra.OpNode {
	if nth == 0 {
		return p.input
	}
	execerror.VectorizedInternalPanic(fmt.Sprintf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}
