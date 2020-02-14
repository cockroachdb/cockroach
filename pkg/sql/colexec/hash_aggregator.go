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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// hashAggregatorState represents the state of the hash aggregator operator.
type hashAggregatorState int

const (
	// hashAggregatorBuffering represents the state the hashAggregator is
	// buffering up its inputs.
	hashAggregatorBuffering hashAggregatorState = iota

	// hashAggregatorAggregating represents the state the hashAggregator is
	// performing aggregation on its buffered inputs. After aggregation is done,
	// the input buffer used in hashAggregatorBuffering phase is reset and ready
	// to be reused.
	hashAggregatorAggregating

	// hashAggregatorOutputting representing the state the hashAggregator is
	// writing its aggregation results to output buffer after it has exhausted all
	// inputs and finished aggregating.
	hashAggregatorOutputting

	// hashAggregatorDone represents the state the hashAggregator has finished
	// writing to the output buffer.
	hashAggregatorDone
)

// hashAggregator is an operator that performs aggregation based on specified
// grouping columns. This operator performs aggregation in online fashion. It
// buffers the input up to batchTupleLimit. Then the aggregator hashes each
// tuple and groups the tuples with same hash code into same group. Then
// aggregation function is lazily created for each group. The tuples in that
// group will be then passed into the aggregation function. After all input is
// exhausted, the operator begins to write the result into an output buffer. The
// output row ordering of this operator is arbitrary.
type hashAggregator struct {
	OneInputNode

	allocator *Allocator

	aggCols  [][]uint32
	aggTypes [][]coltypes.T
	aggFuncs []execinfrapb.AggregatorSpec_Func

	outputTypes []coltypes.T

	// aggFuncMap stores the mapping from hash code to a vector of aggregation
	// functions. Each aggregation function is stored along with keys that
	// corresponds to the group the aggregation function operates on. This is to
	// handle hash collisions.
	aggFuncMap hashAggFuncMap

	// valTypes stores the corresponding types of the val columns.
	valTypes []coltypes.T

	// valCols stores the column indices of grouping columns and aggregating
	// columns.
	valCols []uint32

	// batchTupleLimit limits the number of tuples the aggregator will buffer
	// before starts to perform aggregation. The maximum value of this field
	// is math.MaxUint16 - coldata.BatchSize().
	batchTupleLimit int

	// state stores the current state of hashAggregator.
	state hashAggregatorState

	scratch struct {
		coldata.Batch

		// sels stores the intermediate selection vector for each hash code
		sels map[uint64][]uint16

		// group is a boolean vector where "true" represent the beginning of a group
		// in the column. It is shared among all aggregation functions. Since
		// hashAggregator manually manage mapping between input groups and their
		// corresponding aggregation functions, group is set to all false to prevent
		// premature materialization of aggregation result in the aggregation
		// function. However, aggregation function expect at least one group in its
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
	keyMapping struct {
		*bufferedBatch

		// nextAvailableIdx is the next available slot inside the key mapping buffer.
		nextAvailableIdx uint64
	}

	output struct {
		coldata.Batch

		// pendingOutput indicates if there are more data need to be returned.
		pendingOutput bool

		// resumeHashCode is the hash code that hashAggregator should start reading
		// from on the next iteration of Next().
		resumeHashCode uint64

		// resumeIdx is the index of the vector corresponding to the resumeHashCode
		// that hashAggregator should start reading from on the next iteration of Next().
		resumeIdx uint64
	}

	testingKnobs struct {
		numOfHashBucket uint64
	}

	// groupCols stores the indices of the grouping columns.
	groupCols []uint32

	// groupCols stores the types of the grouping columns.
	groupTypes []coltypes.T

	// hashBuffer stores hash values for each tuple in the buffered batch.
	hashBuffer []uint64

	cancelChecker CancelChecker
}

// NewHashAggregator creates a hash aggregator on the given grouping columns.
// The input specifications to this function are the same as that of the
// NewOrderedAggregator function.
func NewHashAggregator(
	allocator *Allocator,
	input Operator,
	colTypes []coltypes.T,
	aggFns []execinfrapb.AggregatorSpec_Func,
	groupCols []uint32,
	aggCols [][]uint32,
) (Operator, error) {
	aggTyps := extractAggTypes(aggCols, colTypes)

	// Only keep relevant output columns, those that are used as input to an
	// aggregation.
	nCols := uint32(len(colTypes))
	var keepCol util.FastIntSet

	// compressed represents a mapping between each original column and its index
	// in the new compressed columns set. This is required since we are
	// effectively compressing the original list of columns by only keeping the
	// columns used as input to an aggregation function and for grouping.
	compressed := make([]uint32, nCols)
	for _, cols := range aggCols {
		for _, col := range cols {
			keepCol.Add(int(col))
		}
	}

	for _, col := range groupCols {
		keepCol.Add(int(col))
	}

	// Map the corresponding aggCols to the new output column indices.
	nOutCols := uint32(0)
	compressedInputCols := make([]uint32, 0)
	compressedValTypes := make([]coltypes.T, 0)
	keepCol.ForEach(func(i int) {
		compressedInputCols = append(compressedInputCols, uint32(i))
		compressedValTypes = append(compressedValTypes, colTypes[i])
		compressed[i] = nOutCols
		nOutCols++
	})

	mappedAggCols := make([][]uint32, len(aggCols))
	for aggIdx := range aggCols {
		mappedAggCols[aggIdx] = make([]uint32, len(aggCols[aggIdx]))
		for i := range mappedAggCols[aggIdx] {
			mappedAggCols[aggIdx][i] = compressed[aggCols[aggIdx][i]]
		}
	}

	_, outputTypes, err := makeAggregateFuncs(allocator, aggTyps, aggFns)
	if err != nil {
		return nil, errors.AssertionFailedf(
			"this error should have been checked in isAggregateSupported\n%+v", err,
		)
	}

	groupTypes := make([]coltypes.T, len(groupCols))
	for i, colIdx := range groupCols {
		groupTypes[i] = colTypes[colIdx]
	}

	tupleLimit := int(coldata.BatchSize())

	return &hashAggregator{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,

		aggCols:    mappedAggCols,
		aggFuncs:   aggFns,
		aggTypes:   aggTyps,
		aggFuncMap: make(hashAggFuncMap),

		batchTupleLimit: tupleLimit,

		state:       hashAggregatorBuffering,
		outputTypes: outputTypes,

		valTypes: compressedValTypes,
		valCols:  compressedInputCols,

		groupCols:  groupCols,
		groupTypes: groupTypes,

		hashBuffer: make([]uint64, tupleLimit+int(coldata.BatchSize())),
	}, nil

}
func (op *hashAggregator) Init() {
	op.input.Init()

	op.output.Batch = op.allocator.NewMemBatch(op.outputTypes)

	op.scratch.Batch =
		op.allocator.NewMemBatchWithSize(op.valTypes, op.batchTupleLimit+int(coldata.BatchSize()))
	op.scratch.sels = make(map[uint64][]uint16)
	op.scratch.group = make([]bool, op.batchTupleLimit+int(coldata.BatchSize()))

	op.keyMapping.bufferedBatch =
		newBufferedBatch(op.allocator, op.groupTypes, op.batchTupleLimit)
	op.keyMapping.nextAvailableIdx = 0
}

func (op *hashAggregator) Next(ctx context.Context) coldata.Batch {
	for {
		switch op.state {
		case hashAggregatorBuffering:
			op.scratch.ResetInternalBatch()
			op.scratch.SetLength(0)

			// Buffering up input batches.
			if done := op.bufferBatch(ctx); done {
				op.state = hashAggregatorOutputting
				continue
			}

			op.buildSelectionForEachHashCode(ctx)
			op.state = hashAggregatorAggregating
		case hashAggregatorAggregating:
			op.scratch.SetSelection(true)
			op.onlineAgg()
			op.state = hashAggregatorBuffering
		case hashAggregatorOutputting:
			curOutputIdx := uint16(0)
			op.output.ResetInternalBatch()

			// If there is pending output, we try to finish outputting the aggregation
			// result in the same bucket. If we cannot finish, we update resumeIdx and
			// return the current batch.
			if op.output.pendingOutput {
				remainingAggFuncs := op.aggFuncMap[op.output.resumeHashCode][op.output.resumeIdx:]
				for aggFuncIdx, aggFunc := range remainingAggFuncs {
					if curOutputIdx < coldata.BatchSize() {
						for fnIdx, fn := range aggFunc.fns {
							fn.SetOutputIndex(int(curOutputIdx))
							// Passing a zero batch into an aggregation function causing it to
							// flush the agg result to the output batch at curOutputIdx.
							fn.Compute(coldata.ZeroBatch, op.aggCols[fnIdx])
						}
					} else {
						op.output.resumeIdx = op.output.resumeIdx + uint64(aggFuncIdx)
						op.output.SetLength(curOutputIdx)

						return op.output
					}
					curOutputIdx++
				}
				delete(op.aggFuncMap, op.output.resumeHashCode)
			}

			op.output.pendingOutput = false

			for aggHashCode, aggFuncs := range op.aggFuncMap {
				for aggFuncIdx, aggFunc := range aggFuncs {
					if curOutputIdx < coldata.BatchSize() {
						for fnIdx, fn := range aggFunc.fns {
							fn.SetOutputIndex(int(curOutputIdx))
							fn.Compute(coldata.ZeroBatch, op.aggCols[fnIdx])
						}
					} else {
						// If current batch is filled, we record where we left off
						// and then return the current batch.
						op.output.resumeIdx = uint64(aggFuncIdx)
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
			execerror.VectorizedInternalPanic("hash aggregator in unhandled state")
			// This code is unreachable, but the compiler cannot infer that.
			return nil
		}
	}
}

// bufferBatch buffers up batches from input sources until number of tuples
// reaches batchTupleLimit. It returns true when the hash aggregator has
// consumed all batches from input.
func (op *hashAggregator) bufferBatch(ctx context.Context) bool {
	bufferedTupleCount := 0

	for bufferedTupleCount < op.batchTupleLimit {
		b := op.input.Next(ctx)
		batchSize := b.Length()
		if b.Length() == 0 {
			break
		}
		bufferedTupleCount += int(batchSize)
		op.allocator.PerformOperation(op.scratch.ColVecs(), func() {
			for i, colIdx := range op.valCols {
				op.scratch.ColVec(i).Append(
					coldata.SliceArgs{
						ColType:   op.valTypes[i],
						Src:       b.ColVec(int(colIdx)),
						Sel:       b.Selection(),
						DestIdx:   uint64(op.scratch.Length()),
						SrcEndIdx: uint64(batchSize),
					},
				)
			}
		})
		op.scratch.SetLength(uint16(bufferedTupleCount))
	}

	return bufferedTupleCount == 0
}

func (op *hashAggregator) buildSelectionForEachHashCode(ctx context.Context) {
	nKeys := op.scratch.Length()
	hashBuffer := op.hashBuffer[:nKeys]

	initHash(hashBuffer, uint64(nKeys), 1)

	for _, colIdx := range op.groupCols {
		rehash(ctx,
			hashBuffer,
			op.valTypes[colIdx],
			op.scratch.ColVec(int(colIdx)),
			uint64(nKeys),
			nil, /* sel */
			op.cancelChecker)
	}

	if op.testingKnobs.numOfHashBucket != 0 {
		finalizeHash(hashBuffer, uint64(nKeys), op.testingKnobs.numOfHashBucket)
	}

	// Resets the selection vectors to reuse the memory allocated.
	for hashCode := range op.scratch.sels {
		op.scratch.sels[hashCode] = op.scratch.sels[hashCode][:0]
	}

	// We can use selIdx to index into op.scratch since op.scratch never has a
	// a selection vector.
	for selIdx, hashCode := range hashBuffer {
		if _, ok := op.scratch.sels[hashCode]; !ok {
			op.scratch.sels[hashCode] = make([]uint16, 0)
		}
		op.scratch.sels[hashCode] = append(op.scratch.sels[hashCode], uint16(selIdx))
	}
}

// onlineAgg probes aggFuncMap using the built sels map and
// lazily create aggFunctions for each group if it doesn not
// exist. Then it calls Compute() on each aggregation function
// to perform aggregation.
func (op *hashAggregator) onlineAgg() {
	for hashCode, sel := range op.scratch.sels {
		if len(sel) == 0 {
			continue
		}

		remaining := sel

		var anyMatched bool

		// Stage 1: Probe aggregate functions for each hash code and perform
		//          aggregation.
		if aggFuncs, ok := op.aggFuncMap[hashCode]; ok {
			for _, aggFunc := range aggFuncs {
				// We write the selection vector of matched tuples directly into
				// 'matched' and selection vector of unmatched tuples into 'remaining'.
				// 'remaining' will reuse the underlying memory allocated for 'sel' to
				// avoid extra allocation and copying.
				remaining, anyMatched = aggFunc.match(
					remaining, op.scratch, op.groupCols, op.groupTypes, op.keyMapping,
					op.scratch.group[:len(remaining)],
				)
				if anyMatched {
					aggFunc.compute(op.scratch, op.aggCols)
				}
			}
		} else {
			// No aggregate functions exist for this hashCode, create one. Since we
			// don't expect a lot of hash collisions we only allocate small amount of
			// memory here.
			op.aggFuncMap[hashCode] = make([]*hashAggFuncs, 0, 1)
		}

		// Stage 2: Build aggregate function that does not exist then perform
		//          aggregation on the newly created aggregate function.
		for len(remaining) > 0 {
			// Record the selection vector index of the beginning of the group.
			groupStartIdx := remaining[0]

			// Build new agg functions.
			aggFunc := &hashAggFuncs{keyIdx: op.keyMapping.nextAvailableIdx}
			op.keyMapping.nextAvailableIdx++

			// Store the key of the current aggregating group into keyMapping.
			op.allocator.PerformOperation(op.keyMapping.ColVecs(), func() {
				for keyIdx, colIdx := range op.groupCols {
					op.keyMapping.ColVec(keyIdx).Append(coldata.SliceArgs{
						Src:         op.scratch.ColVec(int(colIdx)),
						ColType:     op.valTypes[colIdx],
						DestIdx:     aggFunc.keyIdx,
						SrcStartIdx: uint64(remaining[0]),
						SrcEndIdx:   uint64(remaining[0] + 1),
					})
				}
			})

			aggFunc.fns, _, _ =
				makeAggregateFuncs(op.allocator, op.aggTypes, op.aggFuncs)
			op.aggFuncMap[hashCode] = append(op.aggFuncMap[hashCode], aggFunc)

			// Select rest of the tuples that matches the current key. We don't need
			// to check if there is any match since 'remaining[0]' will always be
			// matched.
			remaining, _ = aggFunc.match(
				remaining, op.scratch, op.groupCols, op.groupTypes, op.keyMapping,
				op.scratch.group[:len(remaining)],
			)

			// Hack required to get aggregation function working. See '.scratch.group'
			// field comment in hashAggregator for more details.
			op.scratch.group[groupStartIdx] = true
			aggFunc.init(op.scratch.group, op.output)
			aggFunc.compute(op.scratch, op.aggCols)
			op.scratch.group[groupStartIdx] = false
		}
	}
}

// reset resets the hashAggregator for another run. Primarily used for
// benchmarks.
func (op *hashAggregator) reset() {
	if r, ok := op.input.(resetter); ok {
		r.reset()
	}

	op.aggFuncMap = hashAggFuncMap{}
	op.state = hashAggregatorBuffering

	op.output.ResetInternalBatch()
	op.output.SetLength(0)
	op.output.pendingOutput = false

	op.scratch.ResetInternalBatch()
	op.scratch.SetLength(0)

	op.keyMapping.reset()

	op.scratch.sels = make(map[uint64][]uint16)
}

var _ Operator = &hashAggregator{}

// hashAggFuncs stores the aggregation functions for the corresponding
// aggregating group.
type hashAggFuncs struct {

	// keyIdx is the index of key of the current aggregating group, which is
	// stored in the hashAggregator keyMapping batch.
	keyIdx uint64

	fns []aggregateFunc
}

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
