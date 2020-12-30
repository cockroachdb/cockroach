// Copyright 2021 The Cockroach Authors.
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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecagg"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecjoin"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// IsHashGroupJoinerSupported determines whether we can plan a vectorized hash
// group-join operator instead of a hash join followed by a hash aggregator
// (with possible projection in-between).
//
// numLeftInputCols and numRightInputCols specify the width of inputs to the
// hash joiner.
func IsHashGroupJoinerSupported(
	numLeftInputCols, numRightInputCols uint32,
	hj *execinfrapb.HashJoinerSpec,
	joinPostProcessSpec *execinfrapb.PostProcessSpec,
	agg *execinfrapb.AggregatorSpec,
) bool {
	// Check that the hash join core is supported.
	if hj.Type != descpb.InnerJoin && hj.Type != descpb.RightOuterJoin {
		// TODO(yuzefovich): relax this for left/full outer joins.
		return false
	}
	if !hj.OnExpr.Empty() {
		return false
	}
	// Check that the post-process spec only has a projection.
	if joinPostProcessSpec.RenderExprs != nil ||
		joinPostProcessSpec.Limit != 0 ||
		joinPostProcessSpec.Offset != 0 {
		return false
	}

	joinOutputProjection := joinPostProcessSpec.OutputColumns
	if !joinPostProcessSpec.Projection {
		joinOutputProjection = make([]uint32, numLeftInputCols+numRightInputCols)
		for i := range joinOutputProjection {
			joinOutputProjection[i] = uint32(i)
		}
	}

	// Check that the aggregator's grouping columns are the same as the join's
	// equality columns.
	if len(agg.GroupCols) != len(hj.LeftEqColumns) {
		return false
	}
	for _, groupCol := range agg.GroupCols {
		// Grouping columns refer to the output columns of the join, so we need
		// to remap it using the projection slice.
		joinOutCol := joinOutputProjection[groupCol]
		found := false
		eqCols := hj.LeftEqColumns
		if joinOutCol >= numLeftInputCols {
			// This grouping column comes from the right side, so we'll look at
			// the right equality columns.
			joinOutCol -= numLeftInputCols
			eqCols = hj.RightEqColumns
		}
		for _, eqCol := range eqCols {
			if joinOutCol == eqCol {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	for i := range agg.Aggregations {
		aggFn := &agg.Aggregations[i]
		if !colexecagg.IsAggOptimized(aggFn.Func) {
			// We currently only support the optimized aggregate functions.
			// TODO(yuzefovich): add support for this.
			return false
		}
		if aggFn.Distinct || aggFn.FilterColIdx != nil {
			// We currently don't support distinct and filtering aggregations.
			// TODO(yuzefovich): add support for this.
			return false
		}
	}

	return true
}

type hashGroupJoinerState int

const (
	hgjBuilding hashGroupJoinerState = iota
	hgjProbing
	hgjProcessingUnmatched
	hgjOutputting
	hgjDone
)

var UnwrapStatsCollector func(colexecop.Operator) colexecop.Operator

func TryHashGroupJoiner(args *colexecagg.NewAggregatorArgs) (colexecop.Operator, error) {
	input := args.Input
	input = UnwrapStatsCollector(input)
	input = MaybeUnwrapInvariantsChecker(input)
	var joinOutputProjection []uint32
	input, joinOutputProjection = colexecbase.UnwrapIfSimpleProjectOp(input)
	dp, isDiskSpiller := input.(*diskSpillerBase)
	if !isDiskSpiller {
		return nil, errors.Newf("not a disk spiller: %T", input)
	}
	hj, isHashJoiner := dp.inMemoryOp.(*colexecjoin.HashJoiner)
	if !isHashJoiner {
		return nil, errors.Newf("not a hash joiner: %T", dp.inMemoryOp)
	}
	args.Allocator = hj.OutputUnlimitedAllocator
	return newHashGroupJoiner(
		hj.OutputUnlimitedAllocator,
		hj.Spec,
		hj.InputOne,
		hj.InputTwo,
		joinOutputProjection,
		args,
	), nil
}

// joinOutputProjection specifies a simple projection on top of the hash join
// output, it can be nil if all columns from the hash join are output.
func newHashGroupJoiner(
	unlimitedAllocator *colmem.Allocator,
	spec colexecjoin.HashJoinerSpec,
	leftSource, rightSource colexecop.Operator,
	joinOutputProjection []uint32,
	args *colexecagg.NewAggregatorArgs,
) colexecop.Operator {
	switch spec.JoinType {
	case descpb.InnerJoin, descpb.RightOuterJoin, descpb.FullOuterJoin:
	default:
		colexecerror.InternalError(errors.AssertionFailedf("hash group join is only supported for inner, right/full outer joins"))
	}
	var da rowenc.DatumAlloc
	helper := newAggregatorHelper(args, &da, true /* isHashAgg */, coldata.BatchSize())
	if _, ok := helper.(*defaultAggregatorHelper); !ok {
		colexecerror.InternalError(errors.AssertionFailedf("only non-distinct and non-filtering aggregations are supported"))
	}
	for _, aggFn := range args.Spec.Aggregations {
		if !colexecagg.IsAggOptimized(aggFn.Func) {
			colexecerror.InternalError(errors.AssertionFailedf("only optimized aggregate functions are supported"))
		}
	}
	aggFnsAlloc, _, toClose, err := colexecagg.NewAggregateFuncsAlloc(
		args, args.Spec.Aggregations, hashAggregatorAllocSize, colexecagg.HashAggKind,
	)
	if err != nil {
		colexecerror.InternalError(err)
	}
	if joinOutputProjection == nil {
		joinOutputProjection = make([]uint32, len(spec.Left.SourceTypes)+len(spec.Right.SourceTypes))
		for i := range joinOutputProjection {
			joinOutputProjection[i] = uint32(i)
		}
	}
	aggFnUsesLeft := make([]bool, len(args.Spec.Aggregations))
	for i := range args.Spec.Aggregations {
		var usesLeftColumn, usesRightColumn bool
		for _, colIdx := range args.Spec.Aggregations[i].ColIdx {
			joinOutColIdx := joinOutputProjection[colIdx]
			if int(joinOutColIdx) < len(spec.Left.SourceTypes) {
				usesLeftColumn = true
			} else {
				usesRightColumn = true
			}
		}
		if usesLeftColumn && usesRightColumn {
			colexecerror.InternalError(errors.AssertionFailedf(
				"hash group join with an aggregate function that uses columns from both sides of the join is not supported",
			))
		}
		aggFnUsesLeft[i] = usesLeftColumn
	}
	hj := colexecjoin.NewHashJoiner(
		unlimitedAllocator,
		unlimitedAllocator,
		spec,
		leftSource,
		rightSource,
		colexecjoin.HashJoinerInitialNumBuckets,
	)
	// TODO(yuzefovich): this is a bit hacky.
	hj.AllowNullEquality = true
	return &hashGroupJoiner{
		hj:                   hj,
		allocator:            unlimitedAllocator,
		joinOutputProjection: joinOutputProjection,
		aggSpec:              args.Spec,
		aggFnUsesLeft:        aggFnUsesLeft,
		aggFnsAlloc:          aggFnsAlloc,
		rightSideFakeSel:     make([]int, coldata.MaxBatchSize),
		outputTypes:          args.OutputTypes,
		toClose:              toClose,
	}
}

type hashGroupJoiner struct {
	hj *colexecjoin.HashJoiner

	state     hashGroupJoinerState
	allocator *colmem.Allocator

	joinOutputProjection []uint32
	aggSpec              *execinfrapb.AggregatorSpec
	aggFnUsesLeft        []bool
	aggInputVecsScratch  []coldata.Vec

	// rightTupleIdxToBucketIdx maps from the position of the tuple in the right
	// input to the bucket that it belongs to.
	rightTupleIdxToBucketIdx []int
	// bucketIdxToBucketStart maps from the bucket index to the first tuple in
	// rightTuplesArrangedByBuckets that belongs to the bucket.
	bucketIdxToBucketStart []int
	// rightTuplesArrangedByBuckets are concatenated "selection vectors" that
	// arrange the indices of right tuples in such a manner that all tuples
	// from the same bucket are contiguous. It holds that within a bucket the
	// indices are increasing and that all tuples from the same bucket are
	// determined by
	// rightTuplesArrangedByBuckets[
	//   bucketIdxToBucketStart[bucket] : bucketIdxToBucketStart[bucket+1]
	// ].
	rightTuplesArrangedByBuckets []int
	buckets                      []aggBucket
	bucketMatched                []bool
	aggFnsAlloc                  *colexecagg.AggregateFuncsAlloc
	rightSideFakeSel             []int

	output      coldata.Batch
	outputTypes []*types.T
	bucketIdx   int

	toClose colexecop.Closers
}

var _ colexecop.ClosableOperator = &hashGroupJoiner{}

func (h *hashGroupJoiner) Init(ctx context.Context) {
	h.hj.Init(ctx)
}

func (h *hashGroupJoiner) Next() coldata.Batch {
	isInnerJoin := h.hj.Spec.JoinType == descpb.InnerJoin
	for {
		switch h.state {
		case hgjBuilding:
			h.hj.Build(true /* storeHashCodes */)
			// TODO(yuzefovich): check whether the hash table is empty and
			// optimize the behavior if so.
			if h.hj.Ht.Vals.Length() == 0 {
				colexecerror.InternalError(errors.AssertionFailedf("empty right input is not supported"))
			}

			h.aggInputVecsScratch = make([]coldata.Vec, len(h.joinOutputProjection))
			for i, joinOutColIdx := range h.joinOutputProjection {
				if int(joinOutColIdx) >= len(h.hj.Spec.Left.SourceTypes) {
					h.aggInputVecsScratch[i] = h.hj.Ht.Vals.ColVec(int(joinOutColIdx) - len(h.hj.Spec.Left.SourceTypes))
				}
			}

			// Divide up all tuples from the right input into buckets. All
			// tuples non-distinct on the equality columns will have the same
			// headID value which equals the position of the earliest tuple from
			// this set in h.hj.Ht.Vals. By construction, whenever a larger headID
			// is seen, it is the first tuple of the new bucket.
			//
			// We need to explicitly create Distinct slice since it is used by
			// CheckProbeForDistinct, yet it isn't created by SetupLimitedSlices
			// because the hash table is not used in the distinct build mode.
			// TODO(yuzefovich): think through whether this can be cleaned up.
			h.hj.Ht.ProbeScratch.Distinct = colexecutils.MaybeAllocateBoolArray(h.hj.Ht.ProbeScratch.Distinct, h.hj.Ht.Vals.Length())
			h.hj.Ht.FindBuckets(
				h.hj.Ht.Vals, h.hj.Ht.Keys, h.hj.Ht.BuildScratch.First,
				h.hj.Ht.BuildScratch.Next, h.hj.Ht.CheckProbeForDistinct,
			)
			// TODO: memory accounting.
			h.rightTupleIdxToBucketIdx = make([]int, h.hj.Ht.Vals.Length())
			numBuckets := 1
			largestHeadID := uint64(1)
			h.rightTupleIdxToBucketIdx[0] = 0
			for i := 1; i < h.hj.Ht.Vals.Length(); i++ {
				curHeadID := h.hj.Ht.ProbeScratch.HeadID[i]
				if curHeadID > largestHeadID {
					largestHeadID = curHeadID
					h.rightTupleIdxToBucketIdx[i] = numBuckets
					numBuckets++
				} else {
					h.rightTupleIdxToBucketIdx[i] = h.rightTupleIdxToBucketIdx[curHeadID-1]
				}
			}

			// Populate other internal slices for efficient lookups of all right
			// tuples from a bucket.
			// TODO: we can skip this if right equality columns form a key.
			h.rightTuplesArrangedByBuckets = make([]int, h.hj.Ht.Vals.Length())
			for i := range h.rightTuplesArrangedByBuckets {
				h.rightTuplesArrangedByBuckets[i] = i
			}
			sort.SliceStable(h.rightTuplesArrangedByBuckets, func(i, j int) bool {
				return h.rightTupleIdxToBucketIdx[h.rightTuplesArrangedByBuckets[i]] < h.rightTupleIdxToBucketIdx[h.rightTuplesArrangedByBuckets[j]]
			})
			h.bucketIdxToBucketStart = make([]int, numBuckets+1)
			bucketIdx := 0
			prevRightTupleIdx := 0
			for i, rightTupleIdx := range h.rightTuplesArrangedByBuckets[1:] {
				if h.rightTupleIdxToBucketIdx[prevRightTupleIdx] != h.rightTupleIdxToBucketIdx[rightTupleIdx] {
					bucketIdx++
					h.bucketIdxToBucketStart[bucketIdx] = i + 1
				}
				prevRightTupleIdx = rightTupleIdx
			}
			h.bucketIdxToBucketStart[numBuckets] = h.hj.Ht.Vals.Length()

			h.buckets = make([]aggBucket, numBuckets)
			h.bucketMatched = make([]bool, numBuckets)
			// TODO: this is very hacky.
			h.hj.Spec.RightDistinct = true
			h.state = hgjProbing
			continue

		case hgjProbing:
			batch := h.hj.InputOne.Next()
			if batch.Length() == 0 {
				if isInnerJoin {
					h.state = hgjOutputting
				} else {
					h.state = hgjProcessingUnmatched
				}
				continue
			}
			for i, joinOutColIdx := range h.joinOutputProjection {
				if int(joinOutColIdx) < len(h.hj.Spec.Left.SourceTypes) {
					h.aggInputVecsScratch[i] = batch.ColVec(int(joinOutColIdx))
				}
			}

			if nResults := h.hj.InitialProbeAndCollect(batch); nResults > 0 {
				// TODO(yuzefovich): pay attention to
				// h.hj.probeState.probeRowUnmatched for full outer joins.
				for i := 0; i < nResults; {
					firstMatchIdx := i
					rightSideTupleIdx := h.hj.ProbeState.BuildIdx[i]
					i++
					// All matches with the same tuple on the right side are
					// contiguous.
					for ; i < nResults && h.hj.ProbeState.BuildIdx[i] == rightSideTupleIdx; i++ {
					}
					firstNonMatchIdx := i
					bucketIdx := h.rightTupleIdxToBucketIdx[rightSideTupleIdx]
					bucket := &h.buckets[bucketIdx]
					if !h.bucketMatched[bucketIdx] {
						h.bucketMatched[bucketIdx] = true
						bucket.fns = h.aggFnsAlloc.MakeAggregateFuncs()
						for _, fn := range bucket.fns {
							fn.Init(nil /* groups */)
						}
					}
					numMatched := firstNonMatchIdx - firstMatchIdx
					bucketStart := h.bucketIdxToBucketStart[bucketIdx]
					bucketEnd := h.bucketIdxToBucketStart[bucketIdx+1]
					for _, rightSideTupleIdx := range h.rightTuplesArrangedByBuckets[bucketStart:bucketEnd] {
						for j := 0; j < numMatched; j++ {
							h.rightSideFakeSel[j] = rightSideTupleIdx
						}
						for fnIdx, fn := range bucket.fns {
							sel := h.rightSideFakeSel
							if h.aggFnUsesLeft[fnIdx] {
								sel = h.hj.ProbeState.ProbeIdx[firstMatchIdx:firstNonMatchIdx]
							}
							fn.Compute(
								h.aggInputVecsScratch, h.aggSpec.Aggregations[fnIdx].ColIdx, 0 /* startIdx */, numMatched, sel,
							)
						}
					}
				}
			}

		case hgjProcessingUnmatched:
			for bucketIdx := range h.buckets {
				if !h.bucketMatched[bucketIdx] {
					bucket := &h.buckets[bucketIdx]
					bucket.fns = h.aggFnsAlloc.MakeAggregateFuncs()
					for _, fn := range bucket.fns {
						fn.Init(nil /* groups */)
					}
					bucketStart := h.bucketIdxToBucketStart[bucketIdx]
					bucketEnd := h.bucketIdxToBucketStart[bucketIdx+1]
					numTuplesProcessed := 0
					sel := h.rightSideFakeSel
					for numTuplesProcessed < bucketEnd-bucketStart {
						n := copy(sel, h.rightTuplesArrangedByBuckets[bucketStart+numTuplesProcessed:bucketEnd])
						for fnIdx, fn := range bucket.fns {
							// We only have anything to compute if the aggregate
							// function takes in an argument from the right
							// side.
							if !h.aggFnUsesLeft[fnIdx] {
								fn.Compute(
									h.aggInputVecsScratch, h.aggSpec.Aggregations[fnIdx].ColIdx, 0, n, sel,
								)
							}
						}
						numTuplesProcessed += n
					}
				}
			}
			h.state = hgjOutputting

		case hgjOutputting:
			// TODO: memory limit.
			h.output, _ = h.allocator.ResetMaybeReallocate(h.outputTypes, h.output, len(h.buckets), math.MaxInt64)
			curOutputIdx := 0
			h.allocator.PerformOperation(h.output.ColVecs(), func() {
				for curOutputIdx < h.output.Capacity() && h.bucketIdx < len(h.buckets) {
					if !isInnerJoin || h.bucketMatched[h.bucketIdx] {
						bucket := h.buckets[h.bucketIdx]
						for fnIdx, fn := range bucket.fns {
							fn.SetOutput(h.output.ColVec(fnIdx))
							fn.Flush(curOutputIdx)
						}
						curOutputIdx++
					}
					h.bucketIdx++
				}
			})
			if h.bucketIdx == len(h.buckets) {
				h.state = hgjDone
			}
			h.output.SetLength(curOutputIdx)
			return h.output

		case hgjDone:
			return coldata.ZeroBatch

		default:
			colexecerror.InternalError(errors.AssertionFailedf("unknown hashGroupJoinerState %d", h.state))
			// Unreachable code.
			return nil
		}
	}
}

func (h *hashGroupJoiner) ChildCount(verbose bool) int {
	return h.hj.ChildCount(verbose)
}

func (h *hashGroupJoiner) Child(nth int, verbose bool) execinfra.OpNode {
	return h.hj.Child(nth, verbose)
}

func (h *hashGroupJoiner) Close() error {
	return h.toClose.Close()
}
