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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/pkg/errors"
)

// aggregateFunc is an aggregate function that performs computation on a batch
// when Compute(batch) is called and writes the output to the Vec passed in
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
	Init(groups []bool, vec coldata.Vec)

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
	Compute(batch coldata.Batch, inputIdxs []uint32)

	// HandleEmptyInputScalar populates the output for a case of an empty input
	// when the aggregate function is in scalar context. The output must always
	// be a single value (either null or zero, depending on the function).
	// TODO(yuzefovich): we can pull scratch field of aggregates into a shared
	// aggregator and implement this method once on the shared base.
	HandleEmptyInputScalar()
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
	OneInputNode

	done bool

	aggCols  [][]uint32
	aggTypes [][]coltypes.T

	outputTypes []coltypes.T

	// scratch is the Batch to output and variables related to it. Aggregate
	// function operators write directly to this output batch.
	scratch struct {
		coldata.Batch
		// shouldResetInternalBatch keeps track of whether the scratch.Batch should
		// be reset. It is false in cases where we have overflow results still to
		// return and therefore do not want to modify the batch.
		shouldResetInternalBatch bool
		// resumeIdx is the index at which the aggregation functions should start
		// writing to on the next iteration of Next().
		resumeIdx int
		// outputSize is col.BatchSize() by default.
		outputSize int
	}

	// unsafeBatch is a coldata.Batch returned when only a subset of the
	// scratch.Batch results is returned (i.e. work needs to be resumed on the
	// next Next call). The values to return are copied into this batch to protect
	// against downstream modification of the internal batch.
	unsafeBatch coldata.Batch

	// groupCol is the slice that aggregateFuncs use to determine whether a value
	// is part of the current aggregation group. See aggregateFunc.Init for more
	// information.
	groupCol []bool
	// aggregateFuncs are the aggregator's aggregate function operators.
	aggregateFuncs []aggregateFunc
	// isScalar indicates whether an aggregator is in scalar context.
	isScalar bool
	// seenNonEmptyBatch indicates whether a non-empty input batch has been
	// observed.
	seenNonEmptyBatch bool
}

var _ StaticMemoryOperator = &orderedAggregator{}

// NewOrderedAggregator creates an ordered aggregator on the given grouping
// columns. aggCols is a slice where each index represents a new aggregation
// function. The slice at that index specifies the columns of the input batch
// that the aggregate function should work on.
func NewOrderedAggregator(
	input Operator,
	colTypes []coltypes.T,
	aggFns []execinfrapb.AggregatorSpec_Func,
	groupCols []uint32,
	aggCols [][]uint32,
	isScalar bool,
) (Operator, error) {
	if len(aggFns) != len(aggCols) {
		return nil,
			errors.Errorf(
				"mismatched aggregation lengths: aggFns(%d), aggCols(%d)",
				len(aggFns),
				len(aggCols),
			)
	}

	aggTypes := extractAggTypes(aggCols, colTypes)

	op, groupCol, err := OrderedDistinctColsToOperators(input, groupCols, colTypes)
	if err != nil {
		return nil, err
	}

	a := &orderedAggregator{}
	if len(groupCols) == 0 {
		// If there were no groupCols, we can't rely on the distinct operators to
		// mark the first row as distinct, so we have to do it ourselves. Set up a
		// oneShotOp to set the first row to distinct.
		op = &oneShotOp{
			OneInputNode: NewOneInputNode(op),
			fn: func(batch coldata.Batch) {
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
		OneInputNode: NewOneInputNode(op),

		aggCols:  aggCols,
		aggTypes: aggTypes,
		groupCol: groupCol,
		isScalar: isScalar,
	}

	a.aggregateFuncs, a.outputTypes, err = makeAggregateFuncs(aggTypes, aggFns)

	if err != nil {
		return nil, err
	}

	return a, nil
}

func makeAggregateFuncs(
	aggTyps [][]coltypes.T, aggFns []execinfrapb.AggregatorSpec_Func,
) ([]aggregateFunc, []coltypes.T, error) {
	funcs := make([]aggregateFunc, len(aggFns))
	outTyps := make([]coltypes.T, len(aggFns))

	for i := range aggFns {
		var err error
		switch aggFns[i] {
		case execinfrapb.AggregatorSpec_ANY_NOT_NULL:
			funcs[i], err = newAnyNotNullAgg(aggTyps[i][0])
		case execinfrapb.AggregatorSpec_AVG:
			funcs[i], err = newAvgAgg(aggTyps[i][0])
		case execinfrapb.AggregatorSpec_SUM, execinfrapb.AggregatorSpec_SUM_INT:
			funcs[i], err = newSumAgg(aggTyps[i][0])
		case execinfrapb.AggregatorSpec_COUNT_ROWS:
			funcs[i] = newCountRowAgg()
		case execinfrapb.AggregatorSpec_COUNT:
			funcs[i] = newCountAgg()
		case execinfrapb.AggregatorSpec_MIN:
			funcs[i], err = newMinAgg(aggTyps[i][0])
		case execinfrapb.AggregatorSpec_MAX:
			funcs[i], err = newMaxAgg(aggTyps[i][0])
		default:
			return nil, nil, errors.Errorf("unsupported columnar aggregate function %s", aggFns[i].String())
		}

		// Set the output type of the aggregate.
		switch aggFns[i] {
		case execinfrapb.AggregatorSpec_COUNT_ROWS, execinfrapb.AggregatorSpec_COUNT:
			// TODO(jordan): this is a somewhat of a hack. The aggregate functions
			// should come with their own output types, somehow.
			outTyps[i] = coltypes.Int64
		default:
			// Output types are the input types for now.
			outTyps[i] = aggTyps[i][0]
		}

		if err != nil {
			return nil, nil, err
		}
	}

	return funcs, outTyps, nil
}

func (a *orderedAggregator) EstimateStaticMemoryUsage() int {
	return EstimateBatchSizeBytes(a.outputTypes, int(coldata.BatchSize()*2))
}

func (a *orderedAggregator) initWithOutputBatchSize(outputSize uint16) {
	a.initWithInputAndOutputBatchSize(int(coldata.BatchSize()), int(outputSize))
}

func (a *orderedAggregator) initWithInputAndOutputBatchSize(inputSize, outputSize int) {
	a.input.Init()

	// Twice the input batchSize is allocated to avoid having to check for
	// overflow when outputting.
	a.scratch.Batch = coldata.NewMemBatchWithSize(a.outputTypes, inputSize*2)
	for i := 0; i < len(a.outputTypes); i++ {
		vec := a.scratch.ColVec(i)
		a.aggregateFuncs[i].Init(a.groupCol, vec)
	}
	a.unsafeBatch = coldata.NewMemBatchWithSize(a.outputTypes, outputSize)
	a.scratch.outputSize = outputSize
}

func (a *orderedAggregator) Init() {
	a.initWithInputAndOutputBatchSize(int(coldata.BatchSize()), int(coldata.BatchSize()))
}

func (a *orderedAggregator) Next(ctx context.Context) coldata.Batch {
	a.unsafeBatch.ResetInternalBatch()
	if a.scratch.shouldResetInternalBatch {
		a.scratch.ResetInternalBatch()
		a.scratch.shouldResetInternalBatch = false
	}
	if a.done {
		a.scratch.SetLength(0)
		return a.scratch
	}
	if a.scratch.resumeIdx >= a.scratch.outputSize {
		// Copy the second part of the output batch into the first and resume from
		// there.
		newResumeIdx := a.scratch.resumeIdx - a.scratch.outputSize
		for i := 0; i < len(a.outputTypes); i++ {
			// According to the aggregate function interface contract, the value at
			// the current index must also be copied.
			a.scratch.ColVec(i).Copy(
				coldata.CopySliceArgs{
					SliceArgs: coldata.SliceArgs{
						Src:         a.scratch.ColVec(i),
						ColType:     a.outputTypes[i],
						SrcStartIdx: uint64(a.scratch.outputSize),
						SrcEndIdx:   uint64(a.scratch.resumeIdx + 1),
					},
				},
			)
			a.aggregateFuncs[i].SetOutputIndex(newResumeIdx)
		}
		a.scratch.resumeIdx = newResumeIdx
		if a.scratch.resumeIdx >= a.scratch.outputSize {
			// We still have overflow output values.
			a.scratch.SetLength(uint16(a.scratch.outputSize))
			for i := 0; i < len(a.outputTypes); i++ {
				a.unsafeBatch.ColVec(i).Copy(
					coldata.CopySliceArgs{
						SliceArgs: coldata.SliceArgs{
							Src:         a.scratch.ColVec(i),
							ColType:     a.outputTypes[i],
							SrcStartIdx: 0,
							SrcEndIdx:   uint64(a.scratch.Length()),
						},
					},
				)
			}
			a.unsafeBatch.SetLength(a.scratch.Length())
			a.scratch.shouldResetInternalBatch = false
			return a.unsafeBatch
		}
	}

	for a.scratch.resumeIdx < a.scratch.outputSize {
		batch := a.input.Next(ctx)
		a.seenNonEmptyBatch = a.seenNonEmptyBatch || batch.Length() > 0
		if !a.seenNonEmptyBatch {
			// The input has zero rows.
			if a.isScalar {
				for _, fn := range a.aggregateFuncs {
					fn.HandleEmptyInputScalar()
				}
				// All aggregate functions will output a single value.
				a.scratch.resumeIdx = 1
			} else {
				// There should be no output in non-scalar context for all aggregate
				// functions.
				a.scratch.resumeIdx = 0
			}
		} else {
			for i, fn := range a.aggregateFuncs {
				fn.Compute(batch, a.aggCols[i])
			}
			a.scratch.resumeIdx = a.aggregateFuncs[0].CurrentOutputIndex()
		}
		if batch.Length() == 0 {
			a.done = true
			break
		}
		// zero out a.groupCol. This is necessary because distinct ors the
		// uniqueness of a value with the groupCol, allowing the operators to be
		// linked.
		copy(a.groupCol, zeroBoolColumn)
	}

	batchToReturn := a.scratch.Batch
	if a.scratch.resumeIdx > a.scratch.outputSize {
		a.scratch.SetLength(uint16(a.scratch.outputSize))
		for i := 0; i < len(a.outputTypes); i++ {
			a.unsafeBatch.ColVec(i).Copy(
				coldata.CopySliceArgs{
					SliceArgs: coldata.SliceArgs{
						Src:         a.scratch.ColVec(i),
						ColType:     a.outputTypes[i],
						SrcStartIdx: 0,
						SrcEndIdx:   uint64(a.scratch.Length()),
					},
				},
			)
		}
		a.unsafeBatch.SetLength(a.scratch.Length())
		batchToReturn = a.unsafeBatch
		a.scratch.shouldResetInternalBatch = false
	} else {
		a.scratch.SetLength(uint16(a.scratch.resumeIdx))
		a.scratch.shouldResetInternalBatch = true
	}

	return batchToReturn
}

// reset resets the orderedAggregator for another run. Primarily used for
// benchmarks.
func (a *orderedAggregator) reset() {
	if resetter, ok := a.input.(resetter); ok {
		resetter.reset()
	}
	a.done = false
	a.seenNonEmptyBatch = false
	a.scratch.resumeIdx = 0
	for _, fn := range a.aggregateFuncs {
		fn.Reset()
	}
}

// extractAggTypes returns a nested array representing the input types
// corresponding to each aggregation function.
func extractAggTypes(aggCols [][]uint32, colTypes []coltypes.T) [][]coltypes.T {
	aggTyps := make([][]coltypes.T, len(aggCols))

	for aggIdx := range aggCols {
		aggTyps[aggIdx] = make([]coltypes.T, len(aggCols[aggIdx]))
		for i, colIdx := range aggCols[aggIdx] {
			aggTyps[aggIdx][i] = colTypes[colIdx]
		}
	}

	return aggTyps
}
