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
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecagg"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// orderedAggregatorState represents the state of the ordered aggregator.
type orderedAggregatorState int

const (
	// orderedAggregatorAggregating is the state in which the ordered aggregator
	// processes the next input batch. If the scratch batch might not have
	// enough capacity for that next input batch, the aggregator transitions to
	// orderedAggregatorReallocating state. If the aggregator has already
	// accumulated coldata.BatchSize() number of output tuples or if it receives
	// the zero-length batch, it transitions to orderedAggregatorOutputting
	// state.
	orderedAggregatorAggregating orderedAggregatorState = iota
	// orderedAggregatorReallocating is the state in which the ordered
	// aggregator reallocates the scratch batch with the capacity determined by
	// the last read batch. Old scratch batch is discarded. From this state the
	// aggregator always transitions to orderedAggregatorAggregating state.
	orderedAggregatorReallocating
	// orderedAggregatorOutputting is the state in which the ordered aggregator
	// populates and returns an output batch. If the scratch batch contains more
	// tuples than can fit in a single output batch, the aggregator will copy
	// over the first coldata.BatchSize() tuples into a special "unsafe" batch
	// and will shift all other tuples to the beginning of the scratch batch.
	// It is the only state that needs to know what next state to transition to.
	orderedAggregatorOutputting
	// orderedAggregatorDone is the final state of the ordered aggregator in
	// which it always returns a zero-length batch.
	orderedAggregatorDone
	// orderedAggregatorUnknown is an invalid state of the ordered aggregator
	// used as a sanity check that we always specify the state to transition to
	// from orderedAggregatorOutputting.
	orderedAggregatorUnknown
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
	colexecop.OneInputNode
	colexecop.InitHelper

	state orderedAggregatorState

	allocator *colmem.Allocator
	spec      *execinfrapb.AggregatorSpec

	outputTypes        []*types.T
	inputArgsConverter *colconv.VecToDatumConverter

	// scratch is the Batch to output and variables related to it. Aggregate
	// function operators write directly to this output batch.
	scratch struct {
		coldata.Batch
		// tempBuffer is used when we need to shift the second part of the
		// scratch batch into the beginning. This can occur when we aggregated
		// more tuples than can fit into a single output batch, and we need some
		// scratch space to copy the overflow tuples into before resetting the
		// scratch.Batch.
		tempBuffer coldata.Batch
		// shouldResetInternalBatch keeps track of whether the scratch.Batch should
		// be reset. It is false in cases where we have overflow results still to
		// return and therefore do not want to modify the batch.
		shouldResetInternalBatch bool
		// resumeIdx is the index at which the aggregation functions should start
		// writing to on the next iteration of Next().
		resumeIdx int
	}

	// lastReadBatch is the last batch that we read from the input that hasn't
	// been processed yet.
	lastReadBatch coldata.Batch

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
	// seenNonEmptyBatch indicates whether a non-empty input batch has been
	// observed.
	seenNonEmptyBatch bool
	datumAlloc        rowenc.DatumAlloc
	toClose           colexecop.Closers
}

var _ colexecop.ResettableOperator = &orderedAggregator{}
var _ colexecop.ClosableOperator = &orderedAggregator{}

// NewOrderedAggregator creates an ordered aggregator.
func NewOrderedAggregator(
	args *colexecagg.NewAggregatorArgs,
) (colexecop.ResettableOperator, error) {
	for _, aggFn := range args.Spec.Aggregations {
		if aggFn.FilterColIdx != nil {
			return nil, errors.AssertionFailedf("filtering ordered aggregation is not supported")
		}
	}
	op, groupCol, err := colexecbase.OrderedDistinctColsToOperators(
		args.Input, args.Spec.GroupCols, args.InputTypes, false, /* nullsAreDistinct */
	)
	if err != nil {
		return nil, err
	}

	// We will be reusing the same aggregate functions, so we use 1 as the
	// allocation size.
	funcsAlloc, inputArgsConverter, toClose, err := colexecagg.NewAggregateFuncsAlloc(
		args, 1 /* allocSize */, false, /* isHashAgg */
	)
	if err != nil {
		return nil, errors.AssertionFailedf(
			"this error should have been checked in isAggregateSupported\n%+v", err,
		)
	}

	a := &orderedAggregator{
		OneInputNode:       colexecop.NewOneInputNode(op),
		allocator:          args.Allocator,
		spec:               args.Spec,
		groupCol:           groupCol,
		bucket:             aggBucket{fns: funcsAlloc.MakeAggregateFuncs()},
		outputTypes:        args.OutputTypes,
		inputArgsConverter: inputArgsConverter,
		toClose:            toClose,
	}
	a.aggHelper = newAggregatorHelper(args, &a.datumAlloc, false /* isHashAgg */, coldata.BatchSize())
	return a, nil
}

func (a *orderedAggregator) Init(ctx context.Context) {
	if !a.InitHelper.Init(ctx) {
		return
	}
	a.Input.Init(a.Ctx)
	a.bucket.init(a.bucket.fns, a.aggHelper.makeSeenMaps(), a.groupCol)
}

func (a *orderedAggregator) Next() coldata.Batch {
	stateAfterOutputting := orderedAggregatorUnknown
	for {
		switch a.state {
		case orderedAggregatorAggregating:
			if a.scratch.shouldResetInternalBatch {
				a.scratch.ResetInternalBatch()
				a.scratch.shouldResetInternalBatch = false
			}
			if a.scratch.resumeIdx >= coldata.BatchSize() {
				a.state = orderedAggregatorOutputting
				stateAfterOutputting = orderedAggregatorAggregating
				continue
			}

			batch := a.lastReadBatch
			a.lastReadBatch = nil
			if batch == nil {
				batch = a.Input.Next()
			}
			batchLength := batch.Length()

			if a.scratch.Batch == nil || a.scratch.Capacity() <= a.scratch.resumeIdx+batchLength {
				// Our scratch.Batch might not have enough capacity to
				// accommodate all possible aggregation groups from batch, so we
				// need to reallocate it with increased capacity.
				a.lastReadBatch = batch
				if a.scratch.resumeIdx > 0 {
					// We already have some results in the current
					// scratch.Batch, so we want to emit them.
					a.state = orderedAggregatorOutputting
					stateAfterOutputting = orderedAggregatorReallocating
					continue
				}
				// We don't have any results yet, so we simply want to restart
				// the aggregation with the scratch.Batch of increased capacity.
				a.state = orderedAggregatorReallocating
				continue
			}

			a.seenNonEmptyBatch = a.seenNonEmptyBatch || batchLength > 0
			if !a.seenNonEmptyBatch {
				// The input has zero rows.
				if a.spec.IsScalar() {
					for _, fn := range a.bucket.fns {
						fn.HandleEmptyInputScalar()
					}
					// All aggregate functions will output a single value.
					a.scratch.resumeIdx = 1
				} else {
					// There should be no output in non-scalar context for all
					// aggregate functions.
					a.scratch.resumeIdx = 0
				}
			} else {
				if batchLength > 0 {
					a.inputArgsConverter.ConvertBatch(batch)
					a.aggHelper.performAggregation(
						a.Ctx, batch.ColVecs(), batchLength, batch.Selection(), &a.bucket, a.groupCol,
					)
				} else {
					a.allocator.PerformOperation(a.scratch.ColVecs(), func() {
						for _, fn := range a.bucket.fns {
							// The aggregate function itself is responsible for
							// tracking the output index, so we pass in an
							// invalid index which will allow us to catch cases
							// when the implementation is misbehaving.
							fn.Flush(-1 /* outputIdx */)
						}
					})
				}
				a.scratch.resumeIdx = a.bucket.fns[0].CurrentOutputIndex()
			}
			if batchLength == 0 {
				a.state = orderedAggregatorOutputting
				stateAfterOutputting = orderedAggregatorDone
				continue
			}
			// zero out a.groupCol. This is necessary because distinct ORs the
			// uniqueness of a value with the groupCol, allowing the operators
			// to be linked.
			copy(a.groupCol[:batchLength], colexecutils.ZeroBoolColumn)

		case orderedAggregatorReallocating:
			// The ordered aggregator *cannot* limit the capacities of its
			// internal batches because it works under the assumption that any
			// input batch can be handled in a single pass, so we don't use a
			// memory limit here. It is up to the input to limit the size of
			// batches based on the memory footprint.
			const maxBatchMemSize = math.MaxInt64
			// Twice the batchSize is allocated to avoid having to check for
			// overflow when outputting.
			newMinCapacity := 2 * a.lastReadBatch.Length()
			if newMinCapacity == 0 {
				// If batchLength is 0, we still need to flush the last group,
				// so we need to have the capacity of at least 1.
				newMinCapacity = 1
			}
			if newMinCapacity > coldata.BatchSize() {
				// ResetMaybeReallocate truncates the capacity to
				// coldata.BatchSize(), but we actually want a batch with larger
				// capacity, so we choose to instantiate the batch with fixed
				// maximal capacity that can be needed by the aggregator.
				a.allocator.ReleaseMemory(colmem.GetBatchMemSize(a.scratch.Batch))
				a.scratch.Batch = a.allocator.NewMemBatchWithFixedCapacity(a.outputTypes, 2*coldata.BatchSize())
			} else {
				a.scratch.Batch, _ = a.allocator.ResetMaybeReallocate(
					a.outputTypes, a.scratch.Batch, newMinCapacity, maxBatchMemSize,
				)
			}
			// We will never copy more than coldata.BatchSize() into the
			// temporary buffer, so a half of the scratch's capacity will always
			// be sufficient.
			tempBufferCapacity := newMinCapacity / 2
			if tempBufferCapacity == 0 {
				tempBufferCapacity = 1
			}
			a.scratch.tempBuffer, _ = a.allocator.ResetMaybeReallocate(
				a.outputTypes, a.scratch.tempBuffer, tempBufferCapacity, maxBatchMemSize,
			)
			for fnIdx, fn := range a.bucket.fns {
				fn.SetOutput(a.scratch.ColVec(fnIdx))
			}
			a.scratch.shouldResetInternalBatch = false
			a.state = orderedAggregatorAggregating
			continue

		case orderedAggregatorOutputting:
			batchToReturn := a.scratch.Batch
			if a.scratch.resumeIdx > coldata.BatchSize() {
				// We already have more result tuples that can fit into a single
				// batch, so we will copy first coldata.BatchSize() of them into
				// a separate unsafe batch to output and shift the second part
				// of the scratch batch into the beginning preparing for the
				// next iteration.
				if a.unsafeBatch == nil {
					a.unsafeBatch = a.allocator.NewMemBatchWithFixedCapacity(a.outputTypes, coldata.BatchSize())
				} else {
					a.unsafeBatch.ResetInternalBatch()
				}
				a.allocator.PerformOperation(a.unsafeBatch.ColVecs(), func() {
					for i := 0; i < len(a.outputTypes); i++ {
						a.unsafeBatch.ColVec(i).Copy(
							coldata.CopySliceArgs{
								SliceArgs: coldata.SliceArgs{
									Src:         a.scratch.ColVec(i),
									SrcStartIdx: 0,
									SrcEndIdx:   coldata.BatchSize(),
								},
							},
						)
					}
					a.unsafeBatch.SetLength(coldata.BatchSize())
				})
				batchToReturn = a.unsafeBatch

				// Copy the second part of the scratch batch into the temporary
				// buffer first, reset the scratch batch, and copy over that
				// second part into the beginning of the scratch batch.
				//
				// This two-step process is necessary because we cannot do
				// resetting (needed to copy to the beginning) and copying (in
				// order to move the data) on the same scratch batch since then
				// the source and the destination would be the same, and
				// resetting it would lead to the loss of data.
				newResumeIdx := a.scratch.resumeIdx - coldata.BatchSize()
				a.scratch.tempBuffer.ResetInternalBatch()
				a.allocator.PerformOperation(a.scratch.tempBuffer.ColVecs(), func() {
					for i := 0; i < len(a.outputTypes); i++ {
						a.scratch.tempBuffer.ColVec(i).Copy(
							coldata.CopySliceArgs{
								SliceArgs: coldata.SliceArgs{
									Src:         a.scratch.ColVec(i),
									SrcStartIdx: coldata.BatchSize(),
									SrcEndIdx:   a.scratch.resumeIdx,
								},
							},
						)
					}
				})
				a.scratch.ResetInternalBatch()
				a.allocator.PerformOperation(a.scratch.ColVecs(), func() {
					for i := 0; i < len(a.outputTypes); i++ {
						a.scratch.ColVec(i).Copy(
							coldata.CopySliceArgs{
								SliceArgs: coldata.SliceArgs{
									Src:       a.scratch.tempBuffer.ColVec(i),
									SrcEndIdx: newResumeIdx,
								},
							},
						)
					}
				})
				a.scratch.resumeIdx = newResumeIdx
			} else {
				a.scratch.SetLength(a.scratch.resumeIdx)
				a.scratch.resumeIdx = 0
				a.scratch.shouldResetInternalBatch = true
			}
			for _, fn := range a.bucket.fns {
				fn.SetOutputIndex(a.scratch.resumeIdx)
			}
			a.state = stateAfterOutputting
			stateAfterOutputting = orderedAggregatorUnknown
			return batchToReturn

		case orderedAggregatorDone:
			return coldata.ZeroBatch

		default:
			colexecerror.InternalError(errors.AssertionFailedf("unexpected orderedAggregatorState %d", a.state))
		}
	}
}

func (a *orderedAggregator) Reset(ctx context.Context) {
	if r, ok := a.Input.(colexecop.Resetter); ok {
		r.Reset(ctx)
	}
	a.state = orderedAggregatorAggregating
	// In some cases we might reset the aggregator before Next() is called for
	// the first time, so there might not be a scratch batch allocated yet.
	a.scratch.shouldResetInternalBatch = a.scratch.Batch != nil
	a.scratch.resumeIdx = 0
	a.lastReadBatch = nil
	a.seenNonEmptyBatch = false
	for _, fn := range a.bucket.fns {
		fn.Reset()
	}
}

func (a *orderedAggregator) Close() error {
	return a.toClose.Close()
}
