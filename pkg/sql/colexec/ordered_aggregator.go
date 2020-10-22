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
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecagg"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

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

	allocator *colmem.Allocator
	spec      *execinfrapb.AggregatorSpec
	done      bool

	outputTypes        []*types.T
	inputArgsConverter *colconv.VecToDatumConverter

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
	}

	// unsafeBatch is a coldata.Batch returned when only a subset of the
	// scratch.Batch results is returned (i.e. work needs to be resumed on the
	// next Next call). The values to return are copied into this batch to protect
	// against downstream modification of the internal batch.
	unsafeBatch coldata.Batch

	// groupCol is the slice that aggregateFuncs use to determine whether a value
	// is part of the current aggregation group. See colexecagg.AggregateFunc.Init
	// for more information.
	groupCol []bool
	// bucket is the aggregation bucket that is reused for all aggregation
	// groups.
	bucket    aggBucket
	aggHelper aggregatorHelper
	// isScalar indicates whether an aggregator is in scalar context.
	isScalar bool
	// seenNonEmptyBatch indicates whether a non-empty input batch has been
	// observed.
	seenNonEmptyBatch bool
	datumAlloc        rowenc.DatumAlloc
	toClose           colexecbase.Closers
}

var _ closableOperator = &orderedAggregator{}

// NewOrderedAggregator creates an ordered aggregator on the given grouping
// columns. aggCols is a slice where each index represents a new aggregation
// function. The slice at that index specifies the columns of the input batch
// that the aggregate function should work on.
func NewOrderedAggregator(
	allocator *colmem.Allocator,
	memAccount *mon.BoundAccount,
	input colexecbase.Operator,
	inputTypes []*types.T,
	spec *execinfrapb.AggregatorSpec,
	evalCtx *tree.EvalContext,
	constructors []execinfrapb.AggregateConstructor,
	constArguments []tree.Datums,
	outputTypes []*types.T,
	isScalar bool,
) (colexecbase.Operator, error) {
	for _, aggFn := range spec.Aggregations {
		if aggFn.FilterColIdx != nil {
			return nil, errors.AssertionFailedf("filtering ordered aggregation is not supported")
		}
	}
	op, groupCol, err := OrderedDistinctColsToOperators(input, spec.GroupCols, inputTypes)
	if err != nil {
		return nil, err
	}

	a := &orderedAggregator{}
	// The contract of AggregateFunc.Init requires that the very first group in
	// the whole input is not marked as a start of a new group with 'true'
	// value in groupCol. In order to satisfy that requirement we plan a
	// oneShotOp that explicitly sets groupCol for the very first tuple it
	// sees to 'false' and then deletes itself from the operator tree.
	op = &oneShotOp{
		OneInputNode: NewOneInputNode(op),
		fn: func(batch coldata.Batch) {
			if batch.Length() == 0 {
				return
			}
			if sel := batch.Selection(); sel != nil {
				groupCol[sel[0]] = false
			} else {
				groupCol[0] = false
			}
		},
		outputSourceRef: &a.input,
	}

	// We will be reusing the same aggregate functions, so we use 1 as the
	// allocation size.
	funcsAlloc, inputArgsConverter, toClose, err := colexecagg.NewAggregateFuncsAlloc(
		allocator, inputTypes, spec, evalCtx, constructors, constArguments,
		outputTypes, 1 /* allocSize */, false, /* isHashAgg */
	)
	if err != nil {
		return nil, errors.AssertionFailedf(
			"this error should have been checked in isAggregateSupported\n%+v", err,
		)
	}

	*a = orderedAggregator{
		OneInputNode:       NewOneInputNode(op),
		allocator:          allocator,
		spec:               spec,
		groupCol:           groupCol,
		bucket:             aggBucket{fns: funcsAlloc.MakeAggregateFuncs()},
		isScalar:           isScalar,
		outputTypes:        outputTypes,
		inputArgsConverter: inputArgsConverter,
		toClose:            toClose,
	}
	a.aggHelper = newAggregatorHelper(allocator, memAccount, inputTypes, spec, &a.datumAlloc, false /* isHashAgg */, coldata.BatchSize())
	return a, nil
}

func (a *orderedAggregator) Init() {
	a.input.Init()
	// Twice the batchSize is allocated to avoid having to check for overflow
	// when outputting.
	a.scratch.Batch = a.allocator.NewMemBatchWithFixedCapacity(a.outputTypes, 2*coldata.BatchSize())
	a.bucket.init(a.scratch.Batch, a.bucket.fns, a.aggHelper.makeSeenMaps(), a.groupCol)
	// Note that we use a batch with fixed capacity because aggregate functions
	// hold onto the vectors passed in into their Init method, so we cannot
	// simply reallocate the output batch.
	// TODO(yuzefovich): consider changing AggregateFunc interface to allow for
	// updating the output vector.
	a.unsafeBatch = a.allocator.NewMemBatchWithFixedCapacity(a.outputTypes, coldata.BatchSize())
}

func (a *orderedAggregator) Next(ctx context.Context) coldata.Batch {
	if a.done {
		return coldata.ZeroBatch
	}
	a.unsafeBatch.ResetInternalBatch()
	if a.scratch.shouldResetInternalBatch {
		a.scratch.ResetInternalBatch()
		a.scratch.shouldResetInternalBatch = false
	}
	if a.scratch.resumeIdx >= coldata.BatchSize() {
		// Copy the second part of the output batch into the first and resume from
		// there.
		newResumeIdx := a.scratch.resumeIdx - coldata.BatchSize()
		a.allocator.PerformOperation(a.scratch.ColVecs(), func() {
			for i := 0; i < len(a.outputTypes); i++ {
				vec := a.scratch.ColVec(i)
				// According to the aggregate function interface contract, the value at
				// the current index must also be copied.
				// Note that we're using Append here instead of Copy because we want the
				// "truncation" behavior, i.e. we want to copy over the remaining tuples
				// such the "lengths" of the vectors are equal to the number of copied
				// elements.
				vec.Append(
					coldata.SliceArgs{
						Src:         vec,
						DestIdx:     0,
						SrcStartIdx: coldata.BatchSize(),
						SrcEndIdx:   a.scratch.resumeIdx + 1,
					},
				)
				// Now we need to restore the desired length for the Vec.
				vec.SetLength(2 * coldata.BatchSize())
				a.bucket.fns[i].SetOutputIndex(newResumeIdx)
				// There might have been some NULLs set in the part that we
				// have just copied over, so we need to unset the NULLs.
				a.scratch.ColVec(i).Nulls().UnsetNullsAfter(newResumeIdx + 1)
			}
		})
		a.scratch.resumeIdx = newResumeIdx
	}

	for a.scratch.resumeIdx < coldata.BatchSize() {
		batch := a.input.Next(ctx)
		batchLength := batch.Length()
		a.seenNonEmptyBatch = a.seenNonEmptyBatch || batchLength > 0
		if !a.seenNonEmptyBatch {
			// The input has zero rows.
			if a.isScalar {
				for _, fn := range a.bucket.fns {
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
			if batchLength > 0 {
				a.inputArgsConverter.ConvertBatch(batch)
				a.aggHelper.performAggregation(
					ctx, batch.ColVecs(), batchLength, batch.Selection(), &a.bucket, a.groupCol,
				)
			} else {
				a.allocator.PerformOperation(a.scratch.ColVecs(), func() {
					for _, fn := range a.bucket.fns {
						// The aggregate function itself is responsible for
						// tracking the output index, so we pass in an invalid
						// index which will allow us to catch cases when the
						// implementation is misbehaving.
						fn.Flush(-1 /* outputIdx */)
					}
				})
			}
			a.scratch.resumeIdx = a.bucket.fns[0].CurrentOutputIndex()
		}
		if batchLength == 0 {
			a.done = true
			break
		}
		// zero out a.groupCol. This is necessary because distinct ORs the
		// uniqueness of a value with the groupCol, allowing the operators to be
		// linked.
		copy(a.groupCol, zeroBoolColumn)
	}

	batchToReturn := a.scratch.Batch
	if a.scratch.resumeIdx > coldata.BatchSize() {
		a.scratch.SetLength(coldata.BatchSize())
		a.allocator.PerformOperation(a.unsafeBatch.ColVecs(), func() {
			for i := 0; i < len(a.outputTypes); i++ {
				a.unsafeBatch.ColVec(i).Copy(
					coldata.CopySliceArgs{
						SliceArgs: coldata.SliceArgs{
							Src:         a.scratch.ColVec(i),
							SrcStartIdx: 0,
							SrcEndIdx:   a.scratch.Length(),
						},
					},
				)
			}
			a.unsafeBatch.SetLength(a.scratch.Length())
		})
		batchToReturn = a.unsafeBatch
		a.scratch.shouldResetInternalBatch = false
	} else {
		a.scratch.SetLength(a.scratch.resumeIdx)
		a.scratch.shouldResetInternalBatch = true
	}

	return batchToReturn
}

// reset resets the orderedAggregator for another run. Primarily used for
// benchmarks.
func (a *orderedAggregator) reset(ctx context.Context) {
	if r, ok := a.input.(resetter); ok {
		r.reset(ctx)
	}
	a.done = false
	a.seenNonEmptyBatch = false
	a.scratch.resumeIdx = 0
	for _, fn := range a.bucket.fns {
		fn.Reset()
	}
}

func (a *orderedAggregator) Close(ctx context.Context) error {
	return a.toClose.Close(ctx)
}
