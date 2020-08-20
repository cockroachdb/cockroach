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
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// hashAggregatorState represents the state of the hash aggregator operator.
type hashAggregatorState int

const (
	// hashAggregatorAggregating is the state in which the hashAggregator is
	// reading the batches from the input and performing aggregation on them,
	// one at a time. After the input has been fully exhausted, hashAggregator
	// transitions to hashAggregatorOutputting state.
	hashAggregatorAggregating hashAggregatorState = iota

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
	OneInputNode

	allocator *colmem.Allocator
	spec      *execinfrapb.AggregatorSpec

	aggHelper          hashAggregatorHelper
	inputTypes         []*types.T
	outputTypes        []*types.T
	inputArgsConverter *vecToDatumConverter

	// buckets contains all aggregation groups that we have so far. There is
	// 1-to-1 mapping between buckets[i] and ht.vals[i]. Once the output from
	// the buckets has been flushed, buckets will be sliced up accordingly.
	buckets []*hashAggBucket
	// ht stores tuples that are "heads" of the corresponding aggregation
	// groups ("head" here means the tuple that was first seen from the group).
	ht *hashTable

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

	output coldata.Batch

	testingKnobs struct {
		// numOfHashBuckets is the number of hash buckets that each tuple will be
		// assigned to. When it is 0, hash aggregator will not enforce maximum
		// number of hash buckets. It is used to test hash collision.
		numOfHashBuckets uint64
	}

	aggFnsAlloc *aggregateFuncsAlloc
	hashAlloc   hashAggBucketAlloc
	datumAlloc  sqlbase.DatumAlloc
	toClose     Closers
}

var _ closableOperator = &hashAggregator{}

// hashAggregatorAllocSize determines the allocation size used by the hash
// aggregator's allocators. This number was chosen after running benchmarks of
// 'sum' aggregation on ints and decimals with varying group sizes (powers of 2
// from 1 to 4096).
const hashAggregatorAllocSize = 128

// NewHashAggregator creates a hash aggregator on the given grouping columns.
// The input specifications to this function are the same as that of the
// NewOrderedAggregator function.
// memAccount should be the same as the one used by allocator and will be used
// by hashAggregatorHelper to handle DISTINCT clause.
func NewHashAggregator(
	allocator *colmem.Allocator,
	memAccount *mon.BoundAccount,
	input colexecbase.Operator,
	inputTypes []*types.T,
	spec *execinfrapb.AggregatorSpec,
	evalCtx *tree.EvalContext,
	constructors []execinfrapb.AggregateConstructor,
	constArguments []tree.Datums,
	outputTypes []*types.T,
) (colexecbase.Operator, error) {
	aggFnsAlloc, inputArgsConverter, toClose, err := newAggregateFuncsAlloc(
		allocator, inputTypes, spec, evalCtx, constructors, constArguments,
		outputTypes, hashAggregatorAllocSize, true, /* isHashAgg */
	)
	hashAgg := &hashAggregator{
		OneInputNode:       NewOneInputNode(input),
		allocator:          allocator,
		spec:               spec,
		state:              hashAggregatorAggregating,
		inputTypes:         inputTypes,
		outputTypes:        outputTypes,
		inputArgsConverter: inputArgsConverter,
		toClose:            toClose,
		aggFnsAlloc:        aggFnsAlloc,
		hashAlloc:          hashAggBucketAlloc{allocator: allocator},
	}
	hashAgg.datumAlloc.AllocSize = hashAggregatorAllocSize
	hashAgg.aggHelper = newHashAggregatorHelper(allocator, memAccount, inputTypes, spec, &hashAgg.datumAlloc)
	return hashAgg, err
}

func (op *hashAggregator) Init() {
	op.input.Init()
	// Note that we use a batch with fixed capacity because aggregate functions
	// hold onto the vectors passed in into their Init method, so we cannot
	// simply reallocate the output batch.
	// TODO(yuzefovich): consider changing aggregateFunc interface to allow for
	// updating the output vector.
	op.output = op.allocator.NewMemBatchWithFixedCapacity(op.outputTypes, coldata.BatchSize())
	op.scratch.eqChains = make([][]int, coldata.BatchSize())
	op.scratch.intSlice = make([]int, coldata.BatchSize())
	op.scratch.anotherIntSlice = make([]int, coldata.BatchSize())
	// TODO(yuzefovich): tune this.
	numOfHashBuckets := uint64(HashTableNumBuckets)
	if op.testingKnobs.numOfHashBuckets != 0 {
		numOfHashBuckets = op.testingKnobs.numOfHashBuckets
	}
	op.ht = newHashTable(
		op.allocator,
		numOfHashBuckets,
		op.inputTypes,
		op.spec.GroupCols,
		true, /* allowNullEquality */
		hashTableDistinctBuildMode,
		hashTableDefaultProbeMode,
	)
}

func (op *hashAggregator) Next(ctx context.Context) coldata.Batch {
	for {
		switch op.state {
		case hashAggregatorAggregating:
			b := op.input.Next(ctx)
			if b.Length() == 0 {
				// TODO(yuzefovich): we no longer need the hash table, so we
				// could be releasing its memory here.
				op.state = hashAggregatorOutputting
				continue
			}
			// TODO(yuzefovich): benchmark whether it is beneficial to buffer
			// up several batches from the input before proceeding to online
			// aggregation.
			op.inputArgsConverter.convertBatch(b)
			op.onlineAgg(ctx, b)
		case hashAggregatorOutputting:
			op.output.ResetInternalBatch()
			curOutputIdx := 0
			for curOutputIdx < op.output.Capacity() && curOutputIdx < len(op.buckets) {
				bucket := op.buckets[curOutputIdx]
				for _, fn := range bucket.fns {
					fn.Flush(curOutputIdx)
				}
				curOutputIdx++
			}
			op.buckets = op.buckets[curOutputIdx:]
			if len(op.buckets) == 0 {
				op.state = hashAggregatorDone
			}
			op.output.SetLength(curOutputIdx)
			return op.output
		case hashAggregatorDone:
			return coldata.ZeroBatch
		default:
			colexecerror.InternalError("hash aggregator in unhandled state")
			// This code is unreachable, but the compiler cannot infer that.
			return nil
		}
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
//   1. a) compute hash buckets: probeScratch.next = [reserved, 1, 1, 0, 1]
//      b) build 'next' chains between hash buckets:
//           probeScratch.first = [3, 1] (length of first == # of hash buckets)
//           probeScratch.next = [reserved, 2, 4, 0, 0]
//         (Note that we have a hash collision in the bucket with hash 1.)
//      c) find "equality" buckets (populate headID):
//           probeScratch.headID = [1, 1, 3, 4]
//         (This means that tuples at position 0 and 1 are the same, and the
//          tuple at position headID-1 is the head of the equality chain.)
//   2. divide all tuples into the equality chains based on headID:
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
//     ht.vals = [-3, -2, -1].
//   We have fully processed the first batch.
//
// II. we get a batch [-4, -1, -1, -4].
//   1. a) compute hash buckets: probeScratch.next = [reserved, 0, 1, 1, 0]
//      b) build 'next' chains between hash buckets:
//           probeScratch.first = [1, 2]
//           probeScratch.next = [reserved, 4, 3, 0, 0]
//      c) find "equality" buckets:
//           probeScratch.headID = [1, 2, 2, 1]
//   2. divide all tuples into the equality chains based on headID:
//        eqChains[0] = [0, 3]
//        eqChains[1] = [1, 2]
//      The special "heads of equality chains" selection vector is [0, 1].
//   3. probe that special "heads" selection vector against the tuples already
//      present in the hash table:
//        probeScratch.headID = [0, 3]
//      Value 0 indicates that the first equality chain doesn't have an
//      existing bucket, but the second chain does and the ID of its bucket is
//      headID-1 = 2. We aggregate the second equality chain into that bucket.
//   4. the first equality chain contains tuples from a new aggregation group,
//      so we create a new bucket for it and perform the aggregation.
//   After we do so, we will have four buckets and the hash table will contain
//   four tuples:
//     buckets = [<bucket for -3>, <bucket for -2>, <bucket for -1>, <bucket for -4>]
//     ht.vals = [-3, -2, -1, -4].
//   We have fully processed the second batch.
//
//  We have processed the input fully, so we're ready to emit the output.
func (op *hashAggregator) onlineAgg(ctx context.Context, b coldata.Batch) {
	inputVecs := b.ColVecs()
	// Step 1: find "equality" buckets: we compute the hash buckets for all
	// tuples, build 'next' chains between them, and then find equality buckets
	// for the tuples.
	op.ht.computeHashAndBuildChains(ctx, b)
	op.ht.findBuckets(
		b, op.ht.probeScratch.keys, op.ht.probeScratch.first,
		op.ht.probeScratch.next, op.ht.checkProbeForDistinct,
	)

	// Step 2: now that we have op.ht.probeScratch.headID populated we can
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
		op.ht.findBuckets(
			b, op.ht.probeScratch.keys, op.ht.buildScratch.first,
			op.ht.buildScratch.next, op.ht.checkBuildForAggregation,
		)
		for eqChainsSlot, headID := range op.ht.probeScratch.headID[:eqChainsCount] {
			if headID != 0 {
				// Tuples in this equality chain belong to an already existing
				// group.
				eqChain := op.scratch.eqChains[eqChainsSlot]
				bucket := op.buckets[headID-1]
				op.aggHelper.performAggregation(ctx, inputVecs, len(eqChain), eqChain, bucket)
				// We have fully processed this equality chain, so we need to
				// reset its length.
				op.scratch.eqChains[eqChainsSlot] = op.scratch.eqChains[eqChainsSlot][:0]
			}
		}
		copy(op.ht.probeScratch.headID[:eqChainsCount], zeroUint64Column)
		copy(op.ht.probeScratch.differs[:eqChainsCount], zeroBoolColumn)
	}

	// Step 4: now we go over all equality chains and check whether there are
	// any that haven't been processed yet (they will be of non-zero length).
	// If we find any, we'll create a new bucket for each.
	newGroupsHeadsSel := op.scratch.anotherIntSlice[:0]
	newGroupCount := 0
	for eqChainSlot, eqChain := range op.scratch.eqChains[:eqChainsCount] {
		if len(eqChain) > 0 {
			// Tuples in this equality chain belong to a new aggregation group,
			// so we'll create a new bucket and make sure that the head of this
			// equality chain is appended to the hash table in the
			// corresponding position.
			bucket := op.hashAlloc.newHashAggBucket()
			op.buckets = append(op.buckets, bucket)
			bucket.init(op.output, op.aggFnsAlloc.makeAggregateFuncs(), op.aggHelper.makeSeenMaps())
			op.aggHelper.performAggregation(ctx, inputVecs, len(eqChain), eqChain, bucket)
			newGroupsHeadsSel = append(newGroupsHeadsSel, eqChainsHeads[eqChainSlot])
			// We need to compact the hash buffer according to the new groups
			// head tuples selection vector we're building.
			op.ht.probeScratch.hashBuffer[newGroupCount] = op.ht.probeScratch.hashBuffer[eqChainSlot]
			newGroupCount++
			op.scratch.eqChains[eqChainSlot] = op.scratch.eqChains[eqChainSlot][:0]
		}
	}

	if newGroupCount > 0 {
		// We have created new buckets, so we need to append the heads of those
		// buckets to the hash table.
		copy(b.Selection(), newGroupsHeadsSel)
		b.SetLength(newGroupCount)
		op.ht.appendAllDistinct(ctx, b)
	}
}

// reset resets the hashAggregator for another run. Primarily used for
// benchmarks.
func (op *hashAggregator) reset(ctx context.Context) {
	if r, ok := op.input.(resetter); ok {
		r.reset(ctx)
	}
	op.buckets = op.buckets[:0]
	op.state = hashAggregatorAggregating
	op.ht.reset(ctx)
	op.output.ResetInternalBatch()
	op.output.SetLength(0)
}

func (op *hashAggregator) Close(ctx context.Context) error {
	return op.toClose.Close(ctx)
}

// hashAggBucket stores the aggregation functions for the corresponding
// aggregation group as well as other utility information.
type hashAggBucket struct {
	fns []aggregateFunc
	// seen is a slice of maps used to handle distinct aggregation. A
	// corresponding entry in the slice is nil if the function doesn't have a
	// DISTINCT clause. The slice itself will be nil whenever no aggregate
	// function has a DISTINCT clause.
	seen []map[string]struct{}
}

const sizeOfHashAggBucket = unsafe.Sizeof(hashAggBucket{})

func (v *hashAggBucket) init(b coldata.Batch, fns []aggregateFunc, seen []map[string]struct{}) {
	v.fns = fns
	for fnIdx, fn := range v.fns {
		// We know that all selected tuples in b belong to the same single
		// group, so we can pass 'nil' for the first argument.
		fn.Init(nil /* groups */, b.ColVec(fnIdx))
	}
	v.seen = seen
}

// hashAggBucketAlloc is a utility struct that batches allocations of
// hashAggBuckets.
type hashAggBucketAlloc struct {
	allocator *colmem.Allocator
	buf       []hashAggBucket
}

func (a *hashAggBucketAlloc) newHashAggBucket() *hashAggBucket {
	if len(a.buf) == 0 {
		a.allocator.AdjustMemoryUsage(int64(hashAggregatorAllocSize * sizeOfHashAggBucket))
		a.buf = make([]hashAggBucket, hashAggregatorAllocSize)
	}
	ret := &a.buf[0]
	a.buf = a.buf[1:]
	return ret
}
