// Copyright 2019 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexechash"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// hashAggregatorState represents the state of the hash aggregator operator.
type hashAggregatorState int

const (
	// hashAggregatorBuffering is the state in which the hashAggregator reads
	// the batches from the input and buffers them up. Once the number of
	// buffered tuples reaches maxBuffered or the input has been fully exhausted,
	// the hashAggregator transitions to hashAggregatorAggregating state.
	hashAggregatorBuffering hashAggregatorState = iota

	// hashAggregatorAggregating is the state in which the hashAggregator is
	// performing the aggregation on the buffered tuples. If the input has been
	// fully exhausted and the buffer is empty, the hashAggregator transitions
	// to hashAggregatorOutputting state.
	hashAggregatorAggregating

	// hashAggregatorOutputting is the state in which the hashAggregator is
	// writing its aggregation results to the output buffer.
	hashAggregatorOutputting

	// hashAggregatorDone is the state in which the hashAggregator has finished
	// writing to the output buffer.
	hashAggregatorDone
)

// hashAggregator is an operator that performs aggregation based on the
// specified grouping columns. This operator performs aggregation in online
// fashion. It reads from the input one batch at a time, groups all tuples into
// the equality chains, probes heads of those chains against already existing
// buckets and creates new buckets for new groups. After the input is
// exhausted, the operator begins to write the result into an output buffer.
// The output row ordering of this operator is arbitrary.
// Note that throughout this file "buckets" and "groups" mean the same thing
// and are used interchangeably.
type hashAggregator struct {
	// Note that we don't use colexecop.OneInputInitCloserHelper here instead of
	// the three options below because we need a custom behavior for Init() and
	// Close().
	colexecop.OneInputNode
	colexecop.InitHelper
	colexecop.CloserHelper

	allocator *colmem.Allocator
	spec      *execinfrapb.AggregatorSpec

	aggHelper          aggregatorHelper
	inputTypes         []*types.T
	outputTypes        []*types.T
	inputArgsConverter *colconv.VecToDatumConverter

	// maxBuffered determines the maximum number of tuples that are buffered up
	// for aggregation at once.
	maxBuffered    int
	bufferingState struct {
		// tuples contains the tuples that we have buffered up for aggregation.
		// Its length will not exceed maxBuffered.
		tuples *colexecutils.AppendOnlyBufferedBatch
		// pendingBatch stores the last read batch from the input that hasn't
		// been fully processed yet.
		pendingBatch coldata.Batch
		// unprocessedIdx is the index of the first tuple in pendingBatch that
		// hasn't been processed yet.
		unprocessedIdx int
	}

	// numPreviouslyCreatedBuckets tracks the maximum number of buckets that
	// have been created throughout the lifetime of this hashAggregator. This
	// matters if the hashAggregator is reset - we reuse the same buckets on the
	// next run.
	// If non-zero, all buckets available to use are in
	// buckets[len(buckets):numPreviouslyCreatedBuckets] range. Note that
	// cap(buckets) might be higher than this number, but all buckets past
	// numPreviouslyCreatedBuckets haven't been instantiated properly, so
	// cap(buckets) should be ignored.
	numPreviouslyCreatedBuckets int
	// buckets contains all aggregation groups that we have so far. There is
	// 1-to-1 mapping between buckets[i] and ht.Vals[i].
	buckets []*aggBucket
	// ht stores tuples that are "heads" of the corresponding aggregation
	// groups ("head" here means the tuple that was first seen from the group).
	ht *colexechash.HashTable

	// state stores the current state of hashAggregator.
	state hashAggregatorState

	scratch struct {
		// eqChains stores the chains of tuples from the current batch that are
		// equal on the grouping columns (meaning that all tuples from the
		// batch will be included into one of these chains). These chains must
		// be set to zero length once the batch has been processed so that the
		// memory could be reused.
		eqChains [][]int

		// intSlice and anotherIntSlice are simply scratch int slices that are
		// reused for several purposes by the hashAggregator.
		intSlice        []int
		anotherIntSlice []int
	}

	// inputTrackingState tracks all the input tuples which is needed in order
	// to fallback to the external hash aggregator.
	inputTrackingState struct {
		tuples            *colexecutils.SpillingQueue
		zeroBatchEnqueued bool
	}

	// curOutputBucketIdx tracks the index in buckets to be flushed next when
	// populating the output.
	curOutputBucketIdx int

	output coldata.Batch

	aggFnsAlloc *colexecagg.AggregateFuncsAlloc
	hashAlloc   aggBucketAlloc
	datumAlloc  rowenc.DatumAlloc
	toClose     colexecop.Closers
}

var _ colexecop.ResettableOperator = &hashAggregator{}
var _ colexecop.BufferingInMemoryOperator = &hashAggregator{}
var _ colexecop.ClosableOperator = &hashAggregator{}

// hashAggregatorAllocSize determines the allocation size used by the hash
// aggregator's allocators. This number was chosen after running benchmarks of
// 'sum' aggregation on ints and decimals with varying group sizes (powers of 2
// from 1 to 4096).
const hashAggregatorAllocSize = 128

// NewHashAggregator creates a hash aggregator on the given grouping columns.
// The input specifications to this function are the same as that of the
// NewOrderedAggregator function.
// newSpillingQueueArgs - when non-nil - specifies the arguments to
// instantiate a SpillingQueue with which will be used to keep all of the
// input tuples in case the in-memory hash aggregator needs to fallback to
// the disk-backed operator. Pass in nil in order to not track all input
// tuples.
func NewHashAggregator(
	args *colexecagg.NewAggregatorArgs, newSpillingQueueArgs *colexecutils.NewSpillingQueueArgs,
) (colexecop.ResettableOperator, error) {
	aggFnsAlloc, inputArgsConverter, toClose, err := colexecagg.NewAggregateFuncsAlloc(
		args, hashAggregatorAllocSize, true, /* isHashAgg */
	)
	// We want this number to be coldata.MaxBatchSize, but then we would lose
	// some test coverage due to disabling of the randomization of the batch
	// size, so we, instead, use 4 x coldata.BatchSize() (which ends up being
	// coldata.MaxBatchSize in non-test environment).
	maxBuffered := 4 * coldata.BatchSize()
	if maxBuffered > coldata.MaxBatchSize {
		// When randomizing coldata.BatchSize() in tests we might exceed
		// coldata.MaxBatchSize, so we need to shrink it.
		maxBuffered = coldata.MaxBatchSize
	}
	hashAgg := &hashAggregator{
		OneInputNode:       colexecop.NewOneInputNode(args.Input),
		allocator:          args.Allocator,
		spec:               args.Spec,
		state:              hashAggregatorBuffering,
		inputTypes:         args.InputTypes,
		outputTypes:        args.OutputTypes,
		inputArgsConverter: inputArgsConverter,
		maxBuffered:        maxBuffered,
		toClose:            toClose,
		aggFnsAlloc:        aggFnsAlloc,
		hashAlloc:          aggBucketAlloc{allocator: args.Allocator},
	}
	hashAgg.bufferingState.tuples = colexecutils.NewAppendOnlyBufferedBatch(args.Allocator, args.InputTypes, nil /* colsToStore */)
	hashAgg.datumAlloc.AllocSize = hashAggregatorAllocSize
	hashAgg.aggHelper = newAggregatorHelper(args, &hashAgg.datumAlloc, true /* isHashAgg */, hashAgg.maxBuffered)
	if newSpillingQueueArgs != nil {
		hashAgg.inputTrackingState.tuples = colexecutils.NewSpillingQueue(newSpillingQueueArgs)
	}
	return hashAgg, err
}

func (op *hashAggregator) Init(ctx context.Context) {
	if !op.InitHelper.Init(ctx) {
		return
	}
	op.Input.Init(op.Ctx)
	// These numbers were chosen after running the micro-benchmarks and relevant
	// TPCH queries using tpchvec/bench.
	const hashTableLoadFactor = 0.1
	const hashTableNumBuckets = 256
	op.ht = colexechash.NewHashTable(
		op.Ctx,
		op.allocator,
		hashTableLoadFactor,
		hashTableNumBuckets,
		op.inputTypes,
		op.spec.GroupCols,
		true, /* allowNullEquality */
		colexechash.HashTableDistinctBuildMode,
		colexechash.HashTableDefaultProbeMode,
	)
}

func (op *hashAggregator) Next() coldata.Batch {
	for {
		switch op.state {
		case hashAggregatorBuffering:
			if op.bufferingState.pendingBatch != nil && op.bufferingState.unprocessedIdx < op.bufferingState.pendingBatch.Length() {
				op.allocator.PerformAppend(op.bufferingState.tuples, func() {
					op.bufferingState.tuples.AppendTuples(
						op.bufferingState.pendingBatch, op.bufferingState.unprocessedIdx, op.bufferingState.pendingBatch.Length(),
					)
				})
			}
			op.bufferingState.pendingBatch, op.bufferingState.unprocessedIdx = op.Input.Next(), 0
			n := op.bufferingState.pendingBatch.Length()
			if op.inputTrackingState.tuples != nil {
				op.inputTrackingState.tuples.Enqueue(op.Ctx, op.bufferingState.pendingBatch)
				op.inputTrackingState.zeroBatchEnqueued = n == 0
			}
			if n == 0 {
				// This is the last input batch.
				if op.bufferingState.tuples.Length() == 0 {
					// There are currently no buffered tuples to perform the
					// aggregation on.
					if len(op.buckets) == 0 {
						// We don't have any buckets which means that there were
						// no input tuples whatsoever, so we can transition to
						// finished state right away.
						op.state = hashAggregatorDone
					} else {
						// There are some buckets, so we proceed to the
						// outputting state.
						op.state = hashAggregatorOutputting
					}
				} else {
					// There are some buffered tuples on which we need to run
					// the aggregation.
					op.state = hashAggregatorAggregating
				}
				continue
			}
			toBuffer := n
			if op.bufferingState.tuples.Length()+toBuffer > op.maxBuffered {
				toBuffer = op.maxBuffered - op.bufferingState.tuples.Length()
			}
			if toBuffer > 0 {
				op.allocator.PerformAppend(op.bufferingState.tuples, func() {
					op.bufferingState.tuples.AppendTuples(op.bufferingState.pendingBatch, 0 /* startIdx */, toBuffer)
				})
				op.bufferingState.unprocessedIdx = toBuffer
			}
			if op.bufferingState.tuples.Length() == op.maxBuffered {
				op.state = hashAggregatorAggregating
				continue
			}

		case hashAggregatorAggregating:
			op.inputArgsConverter.ConvertBatch(op.bufferingState.tuples)
			op.onlineAgg(op.bufferingState.tuples)
			if op.bufferingState.pendingBatch.Length() == 0 {
				if len(op.buckets) == 0 {
					op.state = hashAggregatorDone
				} else {
					op.state = hashAggregatorOutputting
				}
				continue
			}
			op.bufferingState.tuples.ResetInternalBatch()
			op.state = hashAggregatorBuffering

		case hashAggregatorOutputting:
			// Note that ResetMaybeReallocate truncates the requested capacity
			// at coldata.BatchSize(), so we can just try asking for
			// len(op.buckets) capacity. Note that in hashAggregatorOutputting
			// state we always have at least 1 bucket.
			//
			// For now, we don't enforce any footprint-based memory limit.
			// TODO(yuzefovich): refactor this.
			const maxBatchMemSize = math.MaxInt64
			op.output, _ = op.allocator.ResetMaybeReallocate(
				op.outputTypes, op.output, len(op.buckets), maxBatchMemSize,
			)
			curOutputIdx := 0
			op.allocator.PerformOperation(op.output.ColVecs(), func() {
				for curOutputIdx < op.output.Capacity() && op.curOutputBucketIdx < len(op.buckets) {
					bucket := op.buckets[op.curOutputBucketIdx]
					for fnIdx, fn := range bucket.fns {
						fn.SetOutput(op.output.ColVec(fnIdx))
						fn.Flush(curOutputIdx)
					}
					curOutputIdx++
					op.curOutputBucketIdx++
				}
			})
			if op.curOutputBucketIdx >= len(op.buckets) {
				op.state = hashAggregatorDone
			}
			op.output.SetLength(curOutputIdx)
			return op.output

		case hashAggregatorDone:
			return coldata.ZeroBatch

		default:
			colexecerror.InternalError(errors.AssertionFailedf("hash aggregator in unhandled state"))
			// This code is unreachable, but the compiler cannot infer that.
			return nil
		}
	}
}

func (op *hashAggregator) setupScratchSlices(numBuffered int) {
	if len(op.scratch.eqChains) < numBuffered {
		op.scratch.eqChains = make([][]int, numBuffered)
		op.scratch.intSlice = make([]int, numBuffered)
		op.scratch.anotherIntSlice = make([]int, numBuffered)
	}
}

// onlineAgg groups all tuples in b into equality chains, then probes the
// heads of those chains against already existing groups, aggregates matched
// chains into the corresponding buckets and creates new buckets for new
// aggregation groups.
//
// Let's go through an example of how this function works: our input stream
// contains the following tuples:
//   {-3}, {-3}, {-2}, {-1}, {-4}, {-1}, {-1}, {-4}.
// (Note that negative values are chosen in order to visually distinguish them
// from the IDs that we'll be working with below.)
// We will use coldata.BatchSize() == 4 and let's assume that we will use a
// simple hash function h(i) = i % 2 with two buckets in the hash table.
//
// I. we get a batch [-3, -3, -2, -1].
//   1. a) compute hash buckets: ProbeScratch.next = [reserved, 1, 1, 0, 1]
//      b) build 'next' chains between hash buckets:
//           ProbeScratch.first = [3, 1] (length of first == # of hash buckets)
//           ProbeScratch.next = [reserved, 2, 4, 0, 0]
//         (Note that we have a hash collision in the bucket with hash 1.)
//      c) find "equality" buckets (populate HeadID):
//           ProbeScratch.HeadID = [1, 1, 3, 4]
//         (This means that tuples at position 0 and 1 are the same, and the
//          tuple at position HeadID-1 is the head of the equality chain.)
//   2. divide all tuples into the equality chains based on HeadID:
//        eqChains[0] = [0, 1]
//        eqChains[1] = [2]
//        eqChains[2] = [3]
//      The special "heads of equality chains" selection vector is [0, 2, 3].
//   3. we don't have any existing buckets yet, so this step is a noop.
//   4. each of the three equality chains contains tuples from a separate
//      aggregation group, so we perform aggregation on each of them in turn.
//   After we do so, we will have three buckets and the hash table will contain
//   three tuples (with buckets and tuples corresponding to each other):
//     buckets = [<bucket for -3>, <bucket for -2>, <bucket for -1>]
//     ht.Vals = [-3, -2, -1].
//   We have fully processed the first batch.
//
// II. we get a batch [-4, -1, -1, -4].
//   1. a) compute hash buckets: ProbeScratch.next = [reserved, 0, 1, 1, 0]
//      b) build 'next' chains between hash buckets:
//           ProbeScratch.first = [1, 2]
//           ProbeScratch.next = [reserved, 4, 3, 0, 0]
//      c) find "equality" buckets:
//           ProbeScratch.HeadID = [1, 2, 2, 1]
//   2. divide all tuples into the equality chains based on HeadID:
//        eqChains[0] = [0, 3]
//        eqChains[1] = [1, 2]
//      The special "heads of equality chains" selection vector is [0, 1].
//   3. probe that special "heads" selection vector against the tuples already
//      present in the hash table:
//        ProbeScratch.HeadID = [0, 3]
//      Value 0 indicates that the first equality chain doesn't have an
//      existing bucket, but the second chain does and the ID of its bucket is
//      HeadID-1 = 2. We aggregate the second equality chain into that bucket.
//   4. the first equality chain contains tuples from a new aggregation group,
//      so we create a new bucket for it and perform the aggregation.
//   After we do so, we will have four buckets and the hash table will contain
//   four tuples:
//     buckets = [<bucket for -3>, <bucket for -2>, <bucket for -1>, <bucket for -4>]
//     ht.Vals = [-3, -2, -1, -4].
//   We have fully processed the second batch.
//
//  We have processed the input fully, so we're ready to emit the output.
//
// NOTE: b *must* be non-zero length batch.
func (op *hashAggregator) onlineAgg(b coldata.Batch) {
	op.setupScratchSlices(b.Length())
	inputVecs := b.ColVecs()
	// Step 1: find "equality" buckets: we compute the hash buckets for all
	// tuples, build 'next' chains between them, and then find equality buckets
	// for the tuples.
	op.ht.ComputeHashAndBuildChains(b)
	op.ht.FindBuckets(
		b, op.ht.Keys, op.ht.ProbeScratch.First, op.ht.ProbeScratch.Next, op.ht.CheckProbeForDistinct,
	)

	// Step 2: now that we have op.ht.ProbeScratch.HeadID populated we can
	// populate the equality chains.
	eqChainsCount, eqChainsHeadsSel := op.populateEqChains(b)
	b.SetLength(eqChainsCount)

	// Make a copy of the selection vector that contains heads of the
	// corresponding equality chains because the underlying memory will be
	// modified below.
	eqChainsHeads := op.scratch.intSlice[:eqChainsCount]
	copy(eqChainsHeads, eqChainsHeadsSel)

	// Step 3: if we have any existing buckets, we need to probe the heads of
	// the equality chains (which the selection vector on b currently contains)
	// against the heads of the existing groups.
	if len(op.buckets) > 0 {
		op.ht.FindBuckets(
			b, op.ht.Keys, op.ht.BuildScratch.First, op.ht.BuildScratch.Next, op.ht.CheckBuildForAggregation,
		)
		for eqChainsSlot, HeadID := range op.ht.ProbeScratch.HeadID[:eqChainsCount] {
			if HeadID != 0 {
				// Tuples in this equality chain belong to an already existing
				// group.
				eqChain := op.scratch.eqChains[eqChainsSlot]
				bucket := op.buckets[HeadID-1]
				op.aggHelper.performAggregation(
					op.Ctx, inputVecs, len(eqChain), eqChain, bucket, nil, /* groups */
				)
				// We have fully processed this equality chain, so we need to
				// reset its length.
				op.scratch.eqChains[eqChainsSlot] = op.scratch.eqChains[eqChainsSlot][:0]
			}
		}
	}

	// Step 4: now we go over all equality chains and check whether there are
	// any that haven't been processed yet (they will be of non-zero length).
	// If we find any, we'll create a new bucket for each.
	newGroupsHeadsSel := op.scratch.anotherIntSlice[:0]
	newGroupCount := 0
	for eqChainSlot, eqChain := range op.scratch.eqChains[:eqChainsCount] {
		if len(eqChain) > 0 {
			// Tuples in this equality chain belong to a new aggregation group,
			// so we'll use a new bucket and make sure that the head of this
			// equality chain is appended to the hash table in the
			// corresponding position.
			var bucket *aggBucket
			if nextBucketIdx := len(op.buckets); op.numPreviouslyCreatedBuckets > nextBucketIdx {
				// We still have a bucket created on the previous run of the
				// hash aggregator. Increase the length of op.buckets, using
				// previously-allocated capacity, and then reset the bucket for
				// reuse.
				op.buckets = op.buckets[:nextBucketIdx+1]
				bucket = op.buckets[nextBucketIdx]
				bucket.reset()
			} else {
				// Need to allocate a new bucket.
				bucket = op.hashAlloc.newAggBucket()
				op.buckets = append(op.buckets, bucket)
				// We know that all selected tuples belong to the same single
				// group, so we can pass 'nil' for the 'groups' argument.
				bucket.init(
					op.aggFnsAlloc.MakeAggregateFuncs(), op.aggHelper.makeSeenMaps(), nil, /* groups */
				)
			}
			op.aggHelper.performAggregation(
				op.Ctx, inputVecs, len(eqChain), eqChain, bucket, nil, /* groups */
			)
			newGroupsHeadsSel = append(newGroupsHeadsSel, eqChainsHeads[eqChainSlot])
			// We need to compact the hash buffer according to the new groups
			// head tuples selection vector we're building.
			op.ht.ProbeScratch.HashBuffer[newGroupCount] = op.ht.ProbeScratch.HashBuffer[eqChainSlot]
			newGroupCount++
			op.scratch.eqChains[eqChainSlot] = op.scratch.eqChains[eqChainSlot][:0]
		}
	}

	if newGroupCount > 0 {
		// We have created new buckets, so we need to append the heads of those
		// buckets to the hash table.
		copy(b.Selection(), newGroupsHeadsSel)
		b.SetLength(newGroupCount)
		op.ht.AppendAllDistinct(b)
	}
}

func (op *hashAggregator) ExportBuffered(input colexecop.Operator) coldata.Batch {
	if !op.inputTrackingState.zeroBatchEnqueued {
		// Per the contract of the spilling queue, we need to append a
		// zero-length batch.
		op.inputTrackingState.tuples.Enqueue(op.Ctx, coldata.ZeroBatch)
		op.inputTrackingState.zeroBatchEnqueued = true
	}
	batch, err := op.inputTrackingState.tuples.Dequeue(op.Ctx)
	if err != nil {
		colexecerror.InternalError(err)
	}
	return batch
}

func (op *hashAggregator) Reset(ctx context.Context) {
	if r, ok := op.Input.(colexecop.Resetter); ok {
		r.Reset(ctx)
	}
	op.bufferingState.tuples.ResetInternalBatch()
	op.bufferingState.pendingBatch = nil
	op.bufferingState.unprocessedIdx = 0
	if op.numPreviouslyCreatedBuckets < len(op.buckets) {
		op.numPreviouslyCreatedBuckets = len(op.buckets)
	}
	// Set up buckets for reuse.
	op.buckets = op.buckets[:0]
	op.ht.Reset(ctx)
	if op.inputTrackingState.tuples != nil {
		op.inputTrackingState.tuples.Reset(ctx)
		op.inputTrackingState.zeroBatchEnqueued = false
	}
	op.curOutputBucketIdx = 0
	op.state = hashAggregatorBuffering
}

func (op *hashAggregator) Close() error {
	if !op.CloserHelper.Close() {
		return nil
	}
	var retErr error
	if op.inputTrackingState.tuples != nil {
		retErr = op.inputTrackingState.tuples.Close(op.EnsureCtx())
	}
	if err := op.toClose.Close(); err != nil {
		retErr = err
	}
	return retErr
}
