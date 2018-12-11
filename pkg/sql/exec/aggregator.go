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
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/pkg/errors"
)

// aggregateFunc is an aggregate function that performs computation on a batch
// when Compute(batch) is called and writes the output to the ColVec passed in
// in Init. The aggregateFunc performs an aggregation per group and outputs the
// aggregation once the end of the group is reached. If the end of the group is
// not reached before the batch is finished, the aggregateFunc will store a
// carry value that it will use next time Compute is called. Note that this
// carry value is stored at the output index. Therefore if any memory
// modification of the output vector is made, the caller *MUST* copy the value
// at the current index inclusive for a correct aggregation.
type aggregateFunc interface {
	// Init sets the groups for the aggregation and the output vector. Each index
	// in groups corresponds to a column value in the input batch. true represents
	// the first value of a new group.
	Init(groups []bool, vec ColVec)

	// Reset resets the aggregate function for another run. Primarily used for
	// benchmarks.
	Reset()

	// CurrentOutputIndex returns the current index in the output vector that the
	// aggregate function is writing to. All indices < the index returned are
	// finished aggregations for previous groups. A negative index may be returned
	// to signify an aggregate function that has not yet performed any
	// computation.
	CurrentOutputIndex() int
	// SetOutputIndex sets the output index to write to. The value for the current
	// index is carried over. Note that calling SetOutputIndex is a noop if
	// CurrentOutputIndex returns a negative value (i.e. the aggregate function
	// has not yet performed any computation). This method also has the side
	// effect of clearing the output buffer past the given index.
	SetOutputIndex(idx int)

	// Compute computes the aggregation on the input batch. A zero-length input
	// batch tells the aggregate function that it should flush its results.
	Compute(batch ColBatch, inputIdxs []uint32)
}

// orderedAggregator is an aggregator that performs arbitrary aggregations on
// input ordered by a set of grouping columns. Before performing any
// aggregations, the aggregator sets up a chain of distinct operators that will
// produce a vector of booleans (referenced in groupCol) that specifies whether
// or not the corresponding columns in the input batch are part of a new group.
// The memory is modified by the distinct operator flow.
// Every aggregate function will change the shape of the data. i.e. a new column
// value will be output for each input group. Since the number of input groups
// is variable and the number of output values is constant, care must be taken
// not to overflow the output buffer. To avoid having to perform bounds checks
// for the aggregate functions, the aggregator allocates twice the size of the
// input batch for the functions to write to. Before the next batch is
// processed, the aggregator checks what index the functions are outputting to.
// If greater than the expected output batch size by downstream operators, the
// overflow values are copied to the start of the batch. Since the input batch
// size is not necessarily the same as the output batch size, more than one copy
// and return must be performed until the aggregator is in a state where its
// functions are in a state where the output indices would not overflow the
// output batch if a worst case input batch is encountered (one where every
// value is part of a new group).
type orderedAggregator struct {
	input Operator

	done bool

	aggCols [][]uint32
	aggTyps [][]types.T

	// scratch is the ColBatch to output and variables related to it. Aggregate
	// function operators write directly to this output batch.
	scratch struct {
		ColBatch
		// resumeIdx is the index at which the aggregation functions should start
		// writing to on the next iteration of Next().
		resumeIdx int
		// outputSize is ColBatchSize by default.
		outputSize int
	}

	// groupCol is the slice that aggregateFuncs use to determine whether a value
	// is part of the current aggregation group. See aggregateFunc.Init for more
	// information.
	groupCol []bool
	// aggregateFuncs are the aggregator's aggregate function operators.
	aggregateFuncs []aggregateFunc
}

var _ Operator = &orderedAggregator{}

// NewOrderedAggregator creates an ordered aggregator on the given grouping
// columns. aggCols is a slice where each index represents a new aggregation
// function. The slice at that index specifies the columns of the input batch
// that the aggregate function should work on. aggTyps specifies the associated
// types of these input columns.
// TODO(asubiotto): Take in distsqlrun.AggregatorSpec_Func. This is currently
// impossible due to an import cycle so we hack around it by taking in the raw
// integer specifier.
func NewOrderedAggregator(
	input Operator,
	groupCols []uint32,
	groupTyps []types.T,
	aggFns []int,
	aggCols [][]uint32,
	aggTyps [][]types.T,
) (Operator, error) {
	if len(aggFns) != len(aggCols) || len(aggFns) != len(aggTyps) {
		return nil,
			errors.Errorf(
				"mismatched aggregation spec lengths: aggFns(%d), aggCols(%d), aggTyps(%d)",
				len(aggFns),
				len(aggCols),
				len(aggTyps),
			)
	}
	op, groupCol, err := orderedDistinctColsToOperators(input, groupCols, groupTyps)
	if err != nil {
		return nil, err
	}

	a := &orderedAggregator{}
	if len(groupCols) == 0 {
		// If there were no groupCols, we can't rely on the distinct operators to
		// mark the first row as distinct, so we have to do it ourselves. Set up a
		// oneShotOp to set the first row to distinct.
		op = &oneShotOp{
			input: op,
			fn: func(batch ColBatch) {
				if batch.Length() == 0 {
					return
				}
				if sel := batch.Selection(); sel != nil {
					groupCol[sel[0]] = true
				} else {
					groupCol[0] = true
				}
			},
			outputSourceRef: &a.input,
		}
	}

	*a = orderedAggregator{
		input:    op,
		aggCols:  aggCols,
		aggTyps:  aggTyps,
		groupCol: groupCol,
	}
	a.aggregateFuncs = make([]aggregateFunc, len(aggCols))
	for i := range aggFns {
		if len(aggCols[i]) != 1 {
			return nil, errors.Errorf(
				"malformed input columns at index %d, expected 1 col got %d",
				i, len(aggCols[i]),
			)
		}
		var err error
		switch aggFns[i] {
		// AVG.
		case 1:
			a.aggregateFuncs[i], err = newAvgAgg(aggTyps[i][0])
		// SUM, SUM_INT.
		case 10, 11:
			a.aggregateFuncs[i], err = newSumAgg(aggTyps[i][0])
		default:
			return nil, errors.Errorf("unsupported columnar aggregate function %d", aggFns[i])
		}
		if err != nil {
			return nil, err
		}
	}

	return a, nil
}

func (a *orderedAggregator) initWithBatchSize(inputSize, outputSize int) {
	a.input.Init()

	// Output types are the input types for now.
	oTypes := make([]types.T, len(a.aggregateFuncs))
	for i := range oTypes {
		oTypes[i] = a.aggTyps[i][0]
	}
	// Twice the input batchSize is allocated to avoid having to check for
	// overflow when outputting.
	a.scratch.ColBatch = NewMemBatchWithSize(oTypes, inputSize*2)
	for i := 0; i < len(oTypes); i++ {
		vec := a.scratch.ColVec(i)
		a.aggregateFuncs[i].Init(a.groupCol, vec)
	}
	a.scratch.outputSize = outputSize
}

func (a *orderedAggregator) Init() {
	a.initWithBatchSize(ColBatchSize, ColBatchSize)
}

func (a *orderedAggregator) Next() ColBatch {
	if a.done {
		a.scratch.SetLength(0)
		return a.scratch
	}
	if a.scratch.resumeIdx >= a.scratch.outputSize {
		// Copy the second part of the output batch into the first and resume from
		// there.
		for i := 0; i < len(a.aggTyps); i++ {
			// According to the aggregate function interface contract, the value at
			// the current index must also be copied.
			a.scratch.ColVec(i).Copy(a.scratch.ColVec(i),
				uint64(a.scratch.outputSize),
				uint64(a.scratch.resumeIdx)+1,
				a.aggTyps[i][0])
			a.scratch.resumeIdx = a.scratch.resumeIdx - a.scratch.outputSize
			if a.scratch.resumeIdx >= a.scratch.outputSize {
				// We still have overflow output values.
				a.scratch.SetLength(uint16(a.scratch.outputSize))
				return a.scratch
			}
			a.aggregateFuncs[i].SetOutputIndex(a.scratch.resumeIdx)
		}
	}

	for a.scratch.resumeIdx < a.scratch.outputSize {
		batch := a.input.Next()
		for i, fn := range a.aggregateFuncs {
			fn.Compute(batch, a.aggCols[i])
		}
		a.scratch.resumeIdx = a.aggregateFuncs[0].CurrentOutputIndex()
		if batch.Length() == 0 {
			a.done = true
			break
		}
		// zero out a.groupCol. This is necessary because distinct ors the
		// uniqueness of a value with the groupCol, allowing the operators to be
		// linked.
		copy(a.groupCol, zeroBoolVec)
	}

	if a.scratch.resumeIdx > a.scratch.outputSize {
		a.scratch.SetLength(uint16(a.scratch.outputSize))
	} else {
		a.scratch.SetLength(uint16(a.scratch.resumeIdx))
	}

	return a.scratch
}

// Reset resets the orderedAggregator for another run. Primarily used for
// benchmarks.
func (a *orderedAggregator) Reset() {
	a.done = false
	a.scratch.resumeIdx = 0
	for _, fn := range a.aggregateFuncs {
		fn.Reset()
	}
}
