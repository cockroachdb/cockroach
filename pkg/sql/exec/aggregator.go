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
// carry value that it will use next time Compute is called.
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
	// has not yet performed any computation).
	SetOutputIndex(idx int)

	// Compute computes the aggregation on the input batch. A zero-length input
	// batch tells the aggregate function that it should flush its results.
	Compute(batch ColBatch, inputIdxs []uint32)
}

type orderedAggregator struct {
	input Operator

	done bool

	aggCols [][]uint32

	// scratch is the ColBatch to output and variables related to it. Aggregate
	// function operators write directly to this output batch.
	scratch struct {
		ColBatch
		// storage points to the memory that the ColBatch references. This is kept
		// around to manipulate output memory in Next().
		// TODO(asubiotto): This will eventually be a reference to the output
		// ColVecs and the copying code will be templated after #31703 is in.
		storage [][]int64
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
// columns. Currently, the aggregator is only set up to perform an int64 sum
// on the input columns in the same order. Each entry in aggCols specifies the
// input column for a separate aggregate func.
// TODO(asubiotto): Add aggregate function specifiers.
func NewOrderedAggregator(
	input Operator, groupCols []uint32, aggCols [][]uint32, typs []types.T,
) (Operator, error) {
	op, groupCol, err := orderedDistinctColsToOperators(input, groupCols, typs)
	if err != nil {
		return nil, err
	}

	a := &orderedAggregator{
		input:    op,
		aggCols:  aggCols,
		groupCol: groupCol,
	}
	a.aggregateFuncs = make([]aggregateFunc, len(aggCols))
	for i, cols := range aggCols {
		if len(cols) != 1 {
			return nil, errors.Errorf(
				"malformed input columns at index %d, expected 1 col got %d",
				i, len(cols),
			)
		}
		a.aggregateFuncs[i] = &sumInt64Agg{
			input: input,
		}
	}

	return a, nil
}

func (a *orderedAggregator) initWithBatchSize(inputSize, outputSize int) {
	a.input.Init()

	oTypes := make([]types.T, len(a.aggregateFuncs))
	for i := range oTypes {
		oTypes[i] = types.Int64
	}
	// Twice the input batchSize is allocated to avoid having to check for
	// overflow when outputting.
	a.scratch.ColBatch = NewMemBatchWithSize(oTypes, inputSize*2)
	a.scratch.storage = make([][]int64, len(oTypes))
	for i := 0; i < len(oTypes); i++ {
		vec := a.scratch.ColVec(i)
		a.scratch.storage[i] = vec.Int64()
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
	// Copy the second part of the output batch into the first and resume from
	// there.
	if a.scratch.resumeIdx >= a.scratch.outputSize {
		for i, outCol := range a.scratch.storage {
			copy(outCol, outCol[a.scratch.outputSize:a.scratch.resumeIdx])
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
		// zero out a.groupCol. This is necessary because distinct ors the
		// uniqueness of a value with the groupCol, allowing the operators to be
		// linked.
		copy(a.groupCol, zeroBoolVec)
		batch := a.input.Next()
		for i, fn := range a.aggregateFuncs {
			fn.Compute(batch, a.aggCols[i])
		}
		a.scratch.resumeIdx = a.aggregateFuncs[0].CurrentOutputIndex()
		if batch.Length() == 0 {
			a.done = true
			break
		}
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
