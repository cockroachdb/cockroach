// Copyright 2018 The Cockroach Authors.
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

package exec

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// NewSorter returns a new sort operator, which sorts its input on the columns
// given in sortColIdxs. The inputTypes must correspond 1-1 with the columns in
// the input operator.
func NewSorter(
	input Operator, inputTypes []types.T, orderingCols []distsqlpb.Ordering_Column,
) (Operator, error) {
	sorters := make([]colSorter, len(orderingCols))
	partitioners := make([]partitioner, len(orderingCols)-1)
	isOrderingCol := make([]bool, len(inputTypes))

	var err error
	for i, ord := range orderingCols {
		sorters[i], err = newSingleSorter(inputTypes[ord.ColIdx], ord.Direction)
		if err != nil {
			return nil, err
		}
		if i < len(orderingCols)-1 {
			partitioners[i], err = newPartitioner(inputTypes[ord.ColIdx])
			if err != nil {
				return nil, err
			}
		}
		// All ordering columns will have been sorted properly by the time the spool
		// phase is over - only the columns that weren't sort columns will need to
		// be reordered.
		isOrderingCol[ord.ColIdx] = true
	}

	return &sortOp{
		input:         input,
		inputTypes:    inputTypes,
		sorters:       sorters,
		partitioners:  partitioners,
		orderingCols:  orderingCols,
		isOrderingCol: isOrderingCol,
		state:         sortSpooling,
	}, nil
}

type sortOp struct {
	input Operator

	// inputTypes contains the types of all of the columns from input.
	inputTypes []types.T
	// orderingCols is the ordered list of column orderings that the sorter should
	// sort on.
	orderingCols []distsqlpb.Ordering_Column
	// isOrderingCol is set to true for every column that will have been pre-sorted
	// by the time the spool phase is finished. This will be true for all of the
	// sort columns except for the final one. The rest of the columns will not be
	// sorted yet, and will need to be sorted before outputting by rearrangement
	// to the order specified by the order field.
	isOrderingCol []bool
	// sorters contains one colSorter per sort column.
	sorters []colSorter
	// partitioners contains one partitioner per sort solumn except for the last,
	// which doesn't need to be partitioned.
	partitioners []partitioner

	// values stores all the values from the input after spooling. Each ColVec in
	// this slice is the entire column from the input, since this operator needs
	// to globally sort its input.
	values []ColVec
	// order maintains the order of tuples in the batch, after sorting. The value
	// at index i in order is the ordinal value of the tuple in the input that
	// belongs at index i. For example, if the input column to sort was
	// [c,b,a,d], the order vector after sorting would be [2,1,0,3].
	order []uint64
	// spooledTuples is the number of tuples spooled.
	spooledTuples uint64
	// emitted is the number of tuples emitted so far.
	emitted uint64
	// state is the current state of the sort.
	state sortState

	output ColBatch
}

// colSorter is a single-column sorter, specialized on a particular type.
type colSorter interface {
	// init prepares this sorter, given a particular ColVec and an order vector,
	// which must be the same size as the input ColVec and will be permuted with
	// the same swaps as the column. workingSpace is a vector of the same size as
	// the column that is needed for temporary space.
	init(col ColVec, order []uint64, workingSpace []uint64)
	// sort globally sorts this sorter's column.
	sort()
	// sortPartitions sorts this sorter's column once for every partition in the
	// partition slice.
	sortPartitions(partitions []uint64)
	// reorder reorders this sorter's column according to its order vector.
	reorder()
}

func (p *sortOp) Init() {
	p.input.Init()
	p.output = NewMemBatch(p.inputTypes)
	p.values = make([]ColVec, len(p.inputTypes))
	for i := 0; i < len(p.inputTypes); i++ {
		p.values[i] = newMemColumn(p.inputTypes[i], 0)
	}
}

// sortState represents the state of the sort operator.
type sortState int

const (
	// sortSpooling is the initial state of the operator, where it must spool all
	// input data and sort it.
	sortSpooling sortState = iota
	// sortEmitting is the second state of the operator, indicating that each call
	// to Next will return another chunk of the sorted data.
	sortEmitting
)

func (p *sortOp) Next() ColBatch {
	switch p.state {
	case sortSpooling:
		p.spoolAndSort()
		p.state = sortEmitting
		fallthrough
	case sortEmitting:
		newEmitted := p.emitted + ColBatchSize
		if newEmitted > p.spooledTuples {
			newEmitted = p.spooledTuples
		}
		p.output.SetLength(uint16(newEmitted - p.emitted))
		if p.output.Length() == 0 {
			return p.output
		}

		for j := 0; j < len(p.values); j++ {
			if p.isOrderingCol[j] {
				// the vec is already sorted, so just fill it directly.
				p.output.ColVec(j).Copy(p.values[j], p.emitted, newEmitted, p.inputTypes[j])
			} else {
				p.output.ColVec(j).CopyWithSelInt64(p.values[j], p.order[p.emitted:], p.output.Length(), p.inputTypes[j])
			}
		}
		p.emitted = newEmitted
		return p.output
	}
	panic(fmt.Sprintf("invalid sort state %v", p.state))
}

func (p *sortOp) spoolAndSort() {
	batch := p.input.Next()
	var nTuples uint64
	// First, copy all vecs into values.
	for ; batch.Length() != 0; batch = p.input.Next() {
		for i := 0; i < len(p.values); i++ {
			if batch.Selection() == nil {
				p.values[i].Append(batch.ColVec(i),
					p.inputTypes[i],
					nTuples,
					batch.Length(),
				)
			} else {
				p.values[i].AppendWithSel(batch.ColVec(i),
					batch.Selection(),
					batch.Length(),
					p.inputTypes[i],
					nTuples,
				)
			}
		}
		nTuples += uint64(batch.Length())
	}
	p.spooledTuples = nTuples

	// Initialize the order vector to the ordinal positions within the input set.
	p.order = make([]uint64, nTuples)
	for i := uint64(0); i < uint64(len(p.order)); i++ {
		p.order[i] = i
	}

	workingSpace := make([]uint64, nTuples)
	for i := range p.orderingCols {
		p.sorters[i].init(p.values[p.orderingCols[i].ColIdx], p.order, workingSpace)
	}

	// Now, sort each column in turn. The first column is doesn't need special
	// treatment - we just globally sort it.

	p.sorters[0].sort()
	if len(p.sorters) == 1 {
		// We're done sorting. Transition to emitting.
		return
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
	// Then, for each group in the sorted, first column, we sort the second col:
	//
	// 1 a
	// 1 b
	// 2 a
	// 2 b

	outputCol := make([]bool, nTuples)
	partitions := make([]uint64, 0, 16)
	for i, sorter := range p.sorters[1:] {
		// We partition the previous column by running an ordered distinct operation
		// on it, ORing the results together with each subsequent column. This
		// produces a distinct vector (a boolean vector that has true in each
		// position that is different from the last position).
		p.partitioners[i].partition(p.values[p.orderingCols[i].ColIdx], outputCol, nTuples)
		// Convert the distinct vector into a selection vector - a vector of indices
		// that were true in the distinct vector.
		partitions = boolVecToSel64(outputCol, partitions[:0])
		// Reorder the column we're about to sort, based on the swaps we've seen in
		// the sort so far.
		sorter.reorder()
		// For each partition (set of tuples that are identical in all of the sort
		// columns we've seen so far), sort based on the new column.
		sorter.sortPartitions(partitions)
	}
}
