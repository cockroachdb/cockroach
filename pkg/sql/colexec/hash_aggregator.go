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
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
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
	// writing its aggregation results to output buffer after.
	hashAggregatorOutputting

	// hashAggregatorDone is the state in which the hashAggregator has finished
	// writing to the output buffer.
	hashAggregatorDone
)

// hashAggregator is an operator that performs aggregation based on specified
// grouping columns. This operator performs aggregation in online fashion. It
// reads the input one batch at a time, hashes each tuple from the batch and
// groups the tuples with same hash code into same group. Then aggregation
// function is lazily created for each group. The tuples in that group will be
// then passed into the aggregation function. After the input is exhausted, the
// operator begins to write the result into an output buffer. The output row
// ordering of this operator is arbitrary.
type hashAggregator struct {
	OneInputNode

	allocator *colmem.Allocator

	aggCols  [][]uint32
	aggTypes [][]*types.T
	aggFuncs []execinfrapb.AggregatorSpec_Func

	inputTypes  []*types.T
	outputTypes []*types.T

	// aggFuncMap stores the mapping from hash code to a vector of aggregation
	// functions. Each aggregation function is stored along with keys that
	// corresponds to the group the aggregation function operates on. This is to
	// handle hash collisions.
	aggFuncMap hashAggFuncMap

	// state stores the current state of hashAggregator.
	state hashAggregatorState

	scratch struct {
		// sels stores the intermediate selection vector for each hash code. It
		// is maintained in such a way that when for a particular hashCode
		// there are no tuples in the batch, the corresponding int slice is of
		// length 0. Also, onlineAgg() method will reset all modified slices to
		// have zero length once it is done processing all tuples in the batch,
		// this allows us to not reset the slices for all possible hash codes.
		//
		// Instead of having a map from hashCode to []int (which could result
		// in having many int slices), we are using a constant number of such
		// slices and have a "map" from hashCode to a "slot" in sels that does
		// the "translation." The key insight here is that we will have at most
		// coldata.BatchSize() different hashCodes at once.
		sels [][]int
		// hashCodeForSelsSlot stores the hashCode that corresponds to a slot
		// in sels slice. For example, if we have tuples with the following
		// hashCodes = {0, 2, 0, 0, 1, 2, 1}, then we will have:
		//   hashCodeForSelsSlot = {0, 2, 1}
		//   sels[0] = {0, 2, 3}
		//   sels[1] = {1, 5}
		//   sels[2] = {4, 6}
		// Note that we're not using Golang's map for this purpose because
		// although, in theory, it has O(1) amortized lookup cost, in practice,
		// it is faster to do a linear search for a particular hashCode in this
		// slice: given that we have at most coldata.BatchSize() number of
		// different hashCodes - which is a constant - we get
		// O(coldata.BatchSize()) = O(1) lookup cost. And now we have two cases
		// to consider:
		// 1. we have few distinct hashCodes (large group sizes), then the
		// overhead of linear search will be significantly lower than of a
		// lookup in map
		// 2. we have many distinct hashCodes (small group sizes), then the
		// map *might* outperform the linear search, but the time spent in
		// other parts of the hash aggregator will dominate the total runtime,
		// so this would not matter.
		hashCodeForSelsSlot []uint64

		// group is a boolean vector where "true" represent the beginning of a group
		// in the column. It is shared among all aggregation functions. Since
		// hashAggregator manually manages mapping between input groups and their
		// corresponding aggregation functions, group is set to all false to prevent
		// premature materialization of aggregation result in the aggregation
		// function. However, aggregation function expects at least one group in its
		// input batches, (that is, at least one "true" in the group vector
		// corresponding to the selection vector). Therefore, before the first
		// invocation of .Compute() method, the element in group vector which
		// corresponds to the first value of the selection vector is set to true so
		// that aggregation function will initialize properly. Then after .Compute()
		// finishes, it is set back to false so the same group vector can be reused
		// by other aggregation functions.
		group []bool
	}

	// keyMapping stores the key values for each aggregation group. It is a
	// bufferedBatch because in the worst case where all keys in the grouping
	// columns are distinct, we need to store every single key in the input.
	keyMapping *appendOnlyBufferedBatch

	output struct {
		coldata.Batch

		// pendingOutput indicates if there is more data that needs to be returned.
		pendingOutput bool

		// resumeHashCode is the hash code that hashAggregator should start reading
		// from on the next iteration of Next().
		resumeHashCode uint64

		// resumeIdx is the index of the vector corresponding to the resumeHashCode
		// that hashAggregator should start reading from on the next iteration of Next().
		resumeIdx int
	}

	testingKnobs struct {
		// numOfHashBuckets is the number of hash buckets that each tuple will be
		// assigned to. When it is 0, hash aggregator will not enforce maximum
		// number of hash buckets. It is used to test hash collision.
		numOfHashBuckets uint64
	}

	// groupCols stores the indices of the grouping columns.
	groupCols []uint32

	// groupTypes stores the types of the grouping columns.
	groupTypes                 []*types.T
	groupCanonicalTypeFamilies []types.Family

	// hashBuffer stores hash values for each tuple in the buffered batch.
	hashBuffer []uint64

	alloc          hashAggFuncsAlloc
	cancelChecker  CancelChecker
	decimalScratch decimalOverloadScratch
}

var _ colexecbase.Operator = &hashAggregator{}

// NewHashAggregator creates a hash aggregator on the given grouping columns.
// The input specifications to this function are the same as that of the
// NewOrderedAggregator function.
func NewHashAggregator(
	allocator *colmem.Allocator,
	input colexecbase.Operator,
	typs []*types.T,
	aggFns []execinfrapb.AggregatorSpec_Func,
	groupCols []uint32,
	aggCols [][]uint32,
) (colexecbase.Operator, error) {
	aggTyps := extractAggTypes(aggCols, typs)
	outputTypes, err := makeAggregateFuncsOutputTypes(aggTyps, aggFns)
	if err != nil {
		return nil, errors.AssertionFailedf(
			"this error should have been checked in isAggregateSupported\n%+v", err,
		)
	}

	groupTypes := make([]*types.T, len(groupCols))
	for i, colIdx := range groupCols {
		groupTypes[i] = typs[colIdx]
	}

	return &hashAggregator{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,

		aggCols:    aggCols,
		aggFuncs:   aggFns,
		aggTypes:   aggTyps,
		aggFuncMap: make(hashAggFuncMap),

		state:       hashAggregatorAggregating,
		inputTypes:  typs,
		outputTypes: outputTypes,

		groupCols:                  groupCols,
		groupTypes:                 groupTypes,
		groupCanonicalTypeFamilies: typeconv.ToCanonicalTypeFamilies(groupTypes),

		alloc: hashAggFuncsAlloc{allocator: allocator},
	}, err
}

func (op *hashAggregator) Init() {
	op.input.Init()
	op.output.Batch = op.allocator.NewMemBatch(op.outputTypes)

	op.scratch.sels = make([][]int, coldata.BatchSize())
	op.scratch.hashCodeForSelsSlot = make([]uint64, coldata.BatchSize())
	op.scratch.group = make([]bool, coldata.BatchSize())
	// Eventually, op.keyMapping will contain as many tuples as there are
	// groups in the input, but we don't know that number upfront, so we
	// allocate it with some reasonably sized constant capacity.
	op.keyMapping = newAppendOnlyBufferedBatch(
		op.allocator, op.groupTypes, coldata.BatchSize(),
	)
	op.hashBuffer = make([]uint64, coldata.BatchSize())
}

func (op *hashAggregator) Next(ctx context.Context) coldata.Batch {
	for {
		switch op.state {
		case hashAggregatorAggregating:
			b := op.input.Next(ctx)
			if b.Length() == 0 {
				op.state = hashAggregatorOutputting
				continue
			}
			op.buildSelectionForEachHashCode(ctx, b)
			op.onlineAgg(b)
		case hashAggregatorOutputting:
			curOutputIdx := 0
			op.output.ResetInternalBatch()

			// If there is pending output, we try to finish outputting the aggregation
			// result in the same bucket. If we cannot finish, we update resumeIdx and
			// return the current batch.
			if op.output.pendingOutput {
				remainingAggFuncs := op.aggFuncMap[op.output.resumeHashCode][op.output.resumeIdx:]
				for groupIdx, aggFunc := range remainingAggFuncs {
					if curOutputIdx < coldata.BatchSize() {
						for _, fn := range aggFunc.fns {
							fn.SetOutputIndex(curOutputIdx)
							fn.Flush()
						}
					} else {
						op.output.resumeIdx = op.output.resumeIdx + groupIdx
						op.output.SetLength(curOutputIdx)

						return op.output
					}
					curOutputIdx++
				}
				delete(op.aggFuncMap, op.output.resumeHashCode)
			}

			op.output.pendingOutput = false

			for aggHashCode, aggFuncs := range op.aggFuncMap {
				for groupIdx, aggFunc := range aggFuncs {
					if curOutputIdx < coldata.BatchSize() {
						for _, fn := range aggFunc.fns {
							fn.SetOutputIndex(curOutputIdx)
							fn.Flush()
						}
					} else {
						// If current batch is filled, we record where we left off
						// and then return the current batch.
						op.output.resumeIdx = groupIdx
						op.output.resumeHashCode = aggHashCode
						op.output.pendingOutput = true
						op.output.SetLength(curOutputIdx)

						return op.output
					}
					curOutputIdx++
				}
				delete(op.aggFuncMap, aggHashCode)
			}

			op.state = hashAggregatorDone
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

func (op *hashAggregator) buildSelectionForEachHashCode(ctx context.Context, b coldata.Batch) {
	nKeys := b.Length()
	hashBuffer := op.hashBuffer[:nKeys]

	initHash(hashBuffer, nKeys, defaultInitHashValue)

	for _, colIdx := range op.groupCols {
		rehash(ctx,
			hashBuffer,
			b.ColVec(int(colIdx)),
			nKeys,
			b.Selection(),
			op.cancelChecker,
			op.decimalScratch,
		)
	}

	if op.testingKnobs.numOfHashBuckets != 0 {
		finalizeHash(hashBuffer, nKeys, op.testingKnobs.numOfHashBuckets)
	}

	op.populateSels(b, hashBuffer)
}

// onlineAgg probes aggFuncMap using the built sels map and lazily creates
// aggFunctions for each group if it doesn't not exist. Then it calls Compute()
// on each aggregation function to perform aggregation.
func (op *hashAggregator) onlineAgg(b coldata.Batch) {
	for selsSlot, hashCode := range op.scratch.hashCodeForSelsSlot {
		remaining := op.scratch.sels[selsSlot]

		var anyMatched bool

		// Stage 1: Probe aggregate functions for each hash code and perform
		//          aggregation.
		if aggFuncs, ok := op.aggFuncMap[hashCode]; ok {
			for _, aggFunc := range aggFuncs {
				// We write the selection vector of matched tuples directly
				// into the selection vector of b and selection vector of
				// unmatched tuples into 'remaining'.'remaining' will reuse the
				// underlying memory allocated for 'sel' to avoid extra
				// allocation and copying.
				anyMatched, remaining = aggFunc.match(
					remaining, b, op.groupCols, op.groupTypes,
					op.groupCanonicalTypeFamilies, op.keyMapping,
					op.scratch.group[:len(remaining)], false, /* firstDefiniteMatch */
				)
				if anyMatched {
					aggFunc.compute(b, op.aggCols)
				}
			}
		} else {
			// No aggregate functions exist for this hashCode, create one. Since we
			// don't expect a lot of hash collisions we only allocate small amount of
			// memory here.
			op.aggFuncMap[hashCode] = make([]*hashAggFuncs, 0, 1)
		}

		// Stage 2: Build aggregate function that doesn't exist, then perform
		//          aggregation on the newly created aggregate function.
		for len(remaining) > 0 {
			// Record the selection vector index of the beginning of the group.
			groupStartIdx := remaining[0]

			// Build new agg functions.
			keyIdx := op.keyMapping.Length()
			aggFunc := op.alloc.newHashAggFuncs()
			aggFunc.keyIdx = keyIdx

			// Store the key of the current aggregating group into keyMapping.
			op.allocator.PerformOperation(op.keyMapping.ColVecs(), func() {
				for keyIdx, colIdx := range op.groupCols {
					// TODO(azhng): Try to preallocate enough memory so instead of
					// .Append() we can use execgen.SET to improve the
					// performance.
					op.keyMapping.ColVec(keyIdx).Append(coldata.SliceArgs{
						Src:         b.ColVec(int(colIdx)),
						DestIdx:     aggFunc.keyIdx,
						SrcStartIdx: groupStartIdx,
						SrcEndIdx:   groupStartIdx + 1,
					})
				}
				op.keyMapping.SetLength(keyIdx + 1)
			})

			aggFunc.fns, _ = makeAggregateFuncs(op.allocator, op.aggTypes, op.aggFuncs)
			op.aggFuncMap[hashCode] = append(op.aggFuncMap[hashCode], aggFunc)

			// Select rest of the tuples that matches the current key. We don't need
			// to check if there is any match since 'remaining[0]' will always be
			// matched.
			_, remaining = aggFunc.match(
				remaining, b, op.groupCols, op.groupTypes,
				op.groupCanonicalTypeFamilies, op.keyMapping,
				op.scratch.group[:len(remaining)], true, /* firstDefiniteMatch */
			)

			// Hack required to get aggregation function working. See '.scratch.group'
			// field comment in hashAggregator for more details.
			op.scratch.group[groupStartIdx] = true
			aggFunc.init(op.scratch.group, op.output.Batch)
			aggFunc.compute(b, op.aggCols)
			op.scratch.group[groupStartIdx] = false
		}

		// We have processed all tuples with this hashCode, so we should reset
		// the length of the corresponding slice.
		op.scratch.sels[selsSlot] = op.scratch.sels[selsSlot][:0]
	}
}

// reset resets the hashAggregator for another run. Primarily used for
// benchmarks.
func (op *hashAggregator) reset(ctx context.Context) {
	if r, ok := op.input.(resetter); ok {
		r.reset(ctx)
	}

	op.aggFuncMap = hashAggFuncMap{}
	op.state = hashAggregatorAggregating

	op.output.ResetInternalBatch()
	op.output.SetLength(0)
	op.output.pendingOutput = false

	op.keyMapping.ResetInternalBatch()
	op.keyMapping.SetLength(0)
}

// hashAggFuncs stores the aggregation functions for the corresponding
// aggregating group.
type hashAggFuncs struct {
	// keyIdx is the index of key of the current aggregating group, which is
	// stored in the hashAggregator keyMapping batch.
	keyIdx int

	fns []aggregateFunc
}

const sizeOfHashAggFuncs = unsafe.Sizeof(hashAggFuncs{})

// TODO(yuzefovich): we need to account for memory used by this map. It is
// likely that we will replace Golang's map with our vectorized hash table, so
// we might hold off with fixing the accounting until then.
type hashAggFuncMap map[uint64][]*hashAggFuncs

func (v *hashAggFuncs) init(group []bool, b coldata.Batch) {
	for fnIdx, fn := range v.fns {
		fn.Init(group, b.ColVec(fnIdx))
	}
}

func (v *hashAggFuncs) compute(b coldata.Batch, aggCols [][]uint32) {
	for fnIdx, fn := range v.fns {
		fn.Compute(b, aggCols[fnIdx])
	}
}

// hashAggFuncsAllocSize determines the allocation size used by
// hashAggFuncsAlloc. This number was chosen after running benchmarks of 'sum'
// aggregation on ints and decimals with varying group sizes (powers of 2 from
// 1 to 4096).
const hashAggFuncsAllocSize = 64

// hashAggFuncsAlloc is a utility struct that batches allocations of
// hashAggFuncs.
type hashAggFuncsAlloc struct {
	allocator *colmem.Allocator
	buf       []hashAggFuncs
}

func (a *hashAggFuncsAlloc) newHashAggFuncs() *hashAggFuncs {
	if len(a.buf) == 0 {
		a.allocator.AdjustMemoryUsage(int64(hashAggFuncsAllocSize * sizeOfHashAggFuncs))
		a.buf = make([]hashAggFuncs, hashAggFuncsAllocSize)
	}
	ret := &a.buf[0]
	a.buf = a.buf[1:]
	return ret
}
