// Copyright 2022 The Cockroach Authors.
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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecagg"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexechash"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type hashGroupJoinerState int

const (
	hgjBuilding hashGroupJoinerState = iota
	hgjProbing
	hgjProcessingUnmatched
	hgjOutputting
	hgjDone
)

func NewHashGroupJoiner(
	ctx context.Context,
	unlimitedAllocator *colmem.Allocator,
	spec colexecargs.HashJoinerSpec,
	leftSource, rightSource colexecop.Operator,
	joinOutputProjection []uint32,
	args *colexecagg.NewHashAggregatorArgs,
) colexecop.Operator {
	switch spec.JoinType {
	case descpb.InnerJoin, descpb.RightOuterJoin, descpb.FullOuterJoin:
	default:
		colexecerror.InternalError(errors.AssertionFailedf("hash group join is only supported for inner, right/full outer joins"))
	}
	var da tree.DatumAlloc
	helper := newAggregatorHelper(args.NewAggregatorArgs, &da, true /* isHashAgg */, coldata.BatchSize())
	if _, ok := helper.(*defaultAggregatorHelper); !ok {
		// TODO(yuzefovich): lift this restriction.
		colexecerror.InternalError(errors.AssertionFailedf("only non-distinct and non-filtering aggregations are supported"))
	}
	for _, aggFn := range args.Spec.Aggregations {
		if !colexecagg.IsAggOptimized(aggFn.Func) {
			// TODO(yuzefovich): lift this restriction.
			colexecerror.InternalError(errors.AssertionFailedf("only optimized aggregate functions are supported"))
		}
	}
	aggFnsAlloc, _, toClose, err := colexecagg.NewAggregateFuncsAlloc(
		ctx, args.NewAggregatorArgs, args.Spec.Aggregations, hashAggregatorAllocSize, colexecagg.HashAggKind,
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
	return &hashGroupJoiner{
		TwoInputInitHelper: colexecop.MakeTwoInputInitHelper(leftSource, rightSource),
		allocator:          unlimitedAllocator,
		hashTableAllocator: args.HashTableAllocator,
		probeState: colexechash.JoinProbeState{
			Type:   spec.JoinType,
			EqCols: spec.Left.EqCols,
		},
		hjSpec:               spec,
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
	colexecop.TwoInputInitHelper

	state              hashGroupJoinerState
	allocator          *colmem.Allocator
	hashTableAllocator *colmem.Allocator

	ht         *colexechash.HashTable
	probeState colexechash.JoinProbeState

	hjSpec               colexecargs.HashJoinerSpec
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
	if !h.TwoInputInitHelper.Init(ctx) {
		return
	}
	// TODO: tune these.
	const hashTableLoadFactor = 1.0
	const hashTableInitialNumHashBuckets = 256
	h.ht = colexechash.NewHashTable(
		ctx,
		h.hashTableAllocator,
		hashTableLoadFactor,
		hashTableInitialNumHashBuckets,
		h.hjSpec.Right.SourceTypes,
		h.hjSpec.Right.EqCols,
		true, /* allowNullEquality */
		colexechash.HashTableFullBuildMode,
		colexechash.HashTableDefaultProbeMode,
	)
}

func (h *hashGroupJoiner) Next() coldata.Batch {
	isInnerJoin := h.hjSpec.JoinType == descpb.InnerJoin
	for {
		switch h.state {
		case hgjBuilding:
			h.ht.FullBuild(h.InputTwo, true /* storeHashCodes */)
			// TODO(yuzefovich): check whether the hash table is empty and
			// optimize the behavior if so.
			if h.ht.Vals.Length() == 0 {
				colexecerror.InternalError(errors.AssertionFailedf("empty right input is not supported"))
			}

			h.aggInputVecsScratch = make([]coldata.Vec, len(h.joinOutputProjection))
			for i, joinOutColIdx := range h.joinOutputProjection {
				if int(joinOutColIdx) >= len(h.hjSpec.Left.SourceTypes) {
					h.aggInputVecsScratch[i] = h.ht.Vals.ColVec(int(joinOutColIdx) - len(h.hjSpec.Left.SourceTypes))
				}
			}

			// Divide up all tuples from the right input into buckets. All
			// tuples non-distinct on the equality columns will have the same
			// headID value which equals the position of the earliest tuple from
			// this set in h.ht.Vals. By construction, whenever a larger headID
			// is seen, it is the first tuple of the new bucket.
			// TODO: this is wrong for h.ht.Vals > coldata.MaxBatchSize.
			h.ht.FindBuckets(
				h.ht.Vals, h.ht.Keys, h.ht.BuildScratch.First,
				h.ht.BuildScratch.Next, h.ht.CheckProbeForDistinct,
				false /* zeroHeadIDForDistinctTuple */, true, /* probingAgainstItself */
			)
			// TODO: memory accounting.
			h.rightTupleIdxToBucketIdx = make([]int, h.ht.Vals.Length())
			numBuckets := 1
			largestHeadID := uint32(1)
			h.rightTupleIdxToBucketIdx[0] = 0
			for i := 1; i < h.ht.Vals.Length(); i++ {
				curHeadID := h.ht.ProbeScratch.HeadID[i]
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
			h.rightTuplesArrangedByBuckets = make([]int, h.ht.Vals.Length())
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
			h.bucketIdxToBucketStart[numBuckets] = h.ht.Vals.Length()

			// TODO: consider using aggBucketAlloc here.
			h.buckets = make([]aggBucket, numBuckets)
			h.bucketMatched = make([]bool, numBuckets)
			h.state = hgjProbing
			continue

		case hgjProbing:
			batch := h.InputOne.Next()
			batchSize := batch.Length()
			sel := batch.Selection()
			if batchSize == 0 {
				if isInnerJoin {
					h.state = hgjOutputting
				} else {
					h.state = hgjProcessingUnmatched
				}
				continue
			}
			for i, joinOutColIdx := range h.joinOutputProjection {
				if int(joinOutColIdx) < len(h.hjSpec.Left.SourceTypes) {
					h.aggInputVecsScratch[i] = batch.ColVec(int(joinOutColIdx))
				}
			}

			// First, we compute the hash values for all tuples in the batch.
			h.probeState.PrepareForNewBatch(h.ht, batch)

			// Then, we initialize ToCheckID with the initial hash buckets and
			// ToCheck with all applicable indices.
			//
			// Early bounds checks.
			toCheckIDs := h.ht.ProbeScratch.ToCheckID
			_ = toCheckIDs[batchSize-1]
			var nToCheck uint32
			for i, bucket := range h.probeState.Buckets[:batchSize] {
				f := h.ht.BuildScratch.First[bucket]
				//gcassert:bce
				toCheckIDs[i] = f
			}
			copy(h.ht.ProbeScratch.ToCheck, colexechash.HashTableInitialToCheck[:batchSize])
			nToCheck = uint32(batchSize)

			// Now we collect all matches that we can emit in the probing phase
			// in a single batch.
			h.probeState.PrepareForCollecting(batchSize)
			for nToCheck > 0 {
				// Continue searching along the hash table next chains for the corresponding
				// buckets. If the key is found or end of next chain is reached, the key is
				// removed from the ToCheck array.
				nToCheck = h.ht.DistinctCheck(nToCheck, sel)
				// TODO(yuzefovich): check whether we can omit the 'toCheck'
				// tuple if the new ToCheckID is 0.
				for _, toCheck := range h.ht.ProbeScratch.ToCheck[:nToCheck] {
					h.ht.ProbeScratch.ToCheckID[toCheck] = h.ht.BuildScratch.Next[h.ht.ProbeScratch.ToCheckID[toCheck]]
				}
			}

			nResults := h.ht.DistinctCollect(&h.probeState, batch, batchSize, sel)
			if nResults > 0 {
				// TODO(yuzefovich): pay attention to
				// h.probeState.ProbeRowUnmatched for full outer joins.
				for i := 0; i < nResults; {
					firstMatchIdx := i
					rightSideTupleIdx := h.probeState.BuildIdx[i]
					i++
					// All matches with the same tuple on the right side are
					// contiguous.
					for ; i < nResults && h.probeState.BuildIdx[i] == rightSideTupleIdx; i++ {
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
								sel = h.probeState.ProbeIdx[firstMatchIdx:firstNonMatchIdx]
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
							// TODO: think through this.
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
			// TODO: use SetAccountingHelper here.
			h.output, _ = h.allocator.ResetMaybeReallocateNoMemLimit(h.outputTypes, h.output, len(h.buckets))
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

func (h *hashGroupJoiner) Close(ctx context.Context) error {
	return h.toClose.Close(ctx)
}

// hgj currently is the naive implementation of hash group-join
// operation which simply uses the hash joiner and the hash aggregator directly.
// TODO(yuzefovich): add optimized implementation.
type hgj struct {
	colexecop.TwoInputInitHelper

	hjLeftSource *copyingOperator
	hj           colexecop.BufferingInMemoryOperator
	ha           *hashAggregator
}

var _ colexecop.BufferingInMemoryOperator = &hgj{}
var _ colexecop.Closer = &hgj{}

// newHashGroupJoiner creates a new hash group-join operator.
func newHashGroupJoiner(
	ctx context.Context,
	leftSource, rightSource colexecop.Operator,
	// Hide the complexity of creating the hash joiner behind a constructor
	// function in order to not make colexec package depend on colexecargs.
	hjConstructor func(leftSource colexecop.Operator) colexecop.BufferingInMemoryOperator,
	numJoinOutputCols int,
	hjProjection []uint32,
	haArgs *colexecagg.NewHashAggregatorArgs,
	leftSourceSQArgs *colexecutils.NewSpillingQueueArgs,
) colexecop.BufferingInMemoryOperator {
	// We wrap the left input with a copying operator so that we could spill to
	// disk in case the memory limit is exceeded during the aggregation.
	// Exporting buffered tuples from the right input is handled by the hash
	// joiner, so no need to do this for the right input.
	// TODO(yuzefovich): remove this copying operator if
	// HashAggregationDiskSpillingEnabled is false.
	hjLeftSource := newCopyingOperator(leftSource, leftSourceSQArgs)
	hj := hjConstructor(hjLeftSource)

	aggInput := hj.(colexecop.Operator)
	if len(hjProjection) > 0 {
		aggInput = colexecbase.NewSimpleProjectOp(hj, numJoinOutputCols, hjProjection)
	}

	haArgs.Input = aggInput
	ha := NewHashAggregator(
		ctx,
		haArgs,
		// We don't need the aggregator to track input tuples, so we pass nil
		// here.
		nil, /* newSpillingQueueArgs */
	)

	return &hgj{
		TwoInputInitHelper: colexecop.MakeTwoInputInitHelper(leftSource, rightSource),
		hjLeftSource:       hjLeftSource,
		hj:                 hj,
		ha:                 ha.(*hashAggregator),
	}
}

// Init implements the colexecop.Operator interface.
func (h *hgj) Init(ctx context.Context) {
	if !h.TwoInputInitHelper.Init(ctx) {
		return
	}
	h.hj.Init(ctx)
	h.ha.Init(ctx)
}

// Next implements the colexecop.Operator interface.
func (h *hgj) Next() coldata.Batch {
	return h.ha.Next()
}

// ExportBuffered implements the colexecop.BufferingInMemoryOperator interface.
//
// The memory limit can be reached either during the join phase or the
// aggregation phase. If it's the former, then it must be while building the
// hash table based on the right input without having read anything from the
// left input. In this case, we only need to export tuples that are buffered in
// the hash joiner. If it's the latter, then it must be while handling the new
// bucket. In this case, in addition to tuples buffered in the hash joiner we
// also need to export all tuples we have read from the left input (due to not
// being able to spill the intermediate aggregation state). Thus, we currently
// always instantiate a copyingOperator around the left input which allows us to
// perform the export.
func (h *hgj) ExportBuffered(input colexecop.Operator) coldata.Batch {
	if h.ha.ht != nil {
		// This is the first call to ExportBuffered - release the hash table of
		// the hash aggregator since we no longer need it.
		h.ha.ht.Release()
		h.ha.ht = nil
	}
	if h.InputTwo == input {
		// When exporting from the right input, simply delegate to the hash
		// joiner.
		return h.hj.ExportBuffered(input)
	}
	if !h.hjLeftSource.zeroBatchEnqueued {
		h.hjLeftSource.sq.Enqueue(h.Ctx, coldata.ZeroBatch)
		h.hjLeftSource.zeroBatchEnqueued = true
	}
	b, err := h.hjLeftSource.sq.Dequeue(h.Ctx)
	if err != nil {
		colexecerror.InternalError(err)
	}
	return b
}

// Close implements the colexecop.Closer interface.
func (h *hgj) Close(ctx context.Context) error {
	lastErr := h.ha.Close(ctx)
	if err := h.hjLeftSource.Close(ctx); err != nil {
		lastErr = err
	}
	return lastErr
}

// copyingOperator is a utility operator that copies all the batches from the
// input into the spilling queue first before propagating the batch further.
type copyingOperator struct {
	colexecop.OneInputInitCloserHelper
	colexecop.NonExplainable

	sq                *colexecutils.SpillingQueue
	zeroBatchEnqueued bool
}

var _ colexecop.ClosableOperator = &copyingOperator{}

func newCopyingOperator(
	input colexecop.Operator, args *colexecutils.NewSpillingQueueArgs,
) *copyingOperator {
	return &copyingOperator{
		OneInputInitCloserHelper: colexecop.MakeOneInputInitCloserHelper(input),
		sq:                       colexecutils.NewSpillingQueue(args),
	}
}

// Next implements the colexecop.Operator interface.
func (c *copyingOperator) Next() coldata.Batch {
	b := c.Input.Next()
	c.sq.Enqueue(c.Ctx, b)
	c.zeroBatchEnqueued = b.Length() == 0
	return b
}

// Close implements the colexecop.Closer interface.
func (c *copyingOperator) Close(ctx context.Context) error {
	if !c.CloserHelper.Close() {
		return nil
	}
	err := c.sq.Close(ctx)
	c.sq = nil
	return err
}
