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

const (
	hashAggregatorBatchBufferLimit        = uint16(1)
	hashAggregatorMaximumBatchBufferLimit = uint16(63) // 2^16 / BatchSize() - 1

	hashAggregatorBuffering = iota
	hashAggregatorOutputting
	hashAggregatorDone
)

type hashAggregatorState int

type hashAggregator struct {
	OneInputNode

	allocator *Allocator

	aggCols    [][]uint32
	aggTypes   [][]coltypes.T
	aggFuncs   []execinfrapb.AggregatorSpec_Func
	aggFuncMap hashAggFuncMap

	// type of each column
	valTypes []coltypes.T
	valCols  []uint32

	outputTypes []coltypes.T
	isScalar    bool

	// number of tuples to buffer before we can perform aggregation
	batchBufferTupleLimit uint16

	needToResetBuffer bool
	state             hashAggregatorState
	buffer            coldata.Batch

	pendingOutput  bool
	resumeHashCode uint64
	resumeIdx      uint64
	output         coldata.Batch

	// columns used for grouping
	groupCols  []uint32
	groupTypes []coltypes.T
	group      []bool

	// buffer to store hash values for each tuple
	hashBuffer []uint64

	cancelChecker CancelChecker
}

// HashAggregator is an operator that performs aggregation based on
// specified grouping columns.
// This operator performs aggregation in online fashion. It buffers the
// input up to a certain threshold. Then each group in input is hash to
// a bucket and the operator lazily creates aggregation functions for
// each group. After all input is exhausted, the operator begin to write
// the result into an output buffer. The output row ordering of this
// operator is arbitrary.
func NewHashAggregator(
	allocator *Allocator,
	input Operator,
	colTypes []coltypes.T,
	aggFns []execinfrapb.AggregatorSpec_Func,
	groupCols []uint32,
	aggCols [][]uint32,
	isScalar bool,
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

	bufferLimit := hashAggregatorBatchBufferLimit
	if bufferLimit > hashAggregatorMaximumBatchBufferLimit {
		bufferLimit = hashAggregatorMaximumBatchBufferLimit
	}

	batchBufferTupleLimit := bufferLimit * coldata.BatchSize()

	return &hashAggregator{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,

		aggCols:    mappedAggCols,
		aggFuncs:   aggFns,
		aggTypes:   aggTyps,
		aggFuncMap: make(hashAggFuncMap),

		batchBufferTupleLimit: batchBufferTupleLimit,

		state:       hashAggregatorBuffering,
		isScalar:    isScalar,
		outputTypes: outputTypes,

		valTypes: compressedValTypes,
		valCols:  compressedInputCols,

		groupCols:  groupCols,
		groupTypes: groupTypes,
		group:      make([]bool, (bufferLimit+1)*coldata.BatchSize()),

		hashBuffer: make([]uint64, batchBufferTupleLimit),
	}, nil
}
func (op *hashAggregator) Init() {
	op.input.Init()
	op.output = op.allocator.NewMemBatch(op.outputTypes)
	op.buffer = op.allocator.NewMemBatchWithSize(op.valTypes, int(op.batchBufferTupleLimit))
}

func (op *hashAggregator) Next(ctx context.Context) coldata.Batch {
	for {
		switch op.state {
		case hashAggregatorBuffering:
			if op.needToResetBuffer {
				op.buffer = op.allocator.NewMemBatchWithSize(op.valTypes, int(op.batchBufferTupleLimit))
			}

			// Buffering up input batches.
			bufferedBatchCount := op.bufferBatch(ctx)

			if bufferedBatchCount == 0 {
				op.state = hashAggregatorOutputting
				continue
			}

			sels := op.buildSelectionForHashCode(ctx)
			op.onlineAgg(sels)
			op.needToResetBuffer = true
		case hashAggregatorOutputting:
			curOutputIdx := uint16(0)
			op.output.ResetInternalBatch()

			// If there is pending output, we try to finish outputting the aggregation result
			// in the same bucket. If we cannot finish, we update resumeIdx and return the
			// current batch.
			if op.pendingOutput {
				for aggFuncIdx, aggFunc := range op.aggFuncMap[op.resumeHashCode][op.resumeIdx:] {
					if curOutputIdx < coldata.BatchSize() {
						for fnIdx, fn := range aggFunc.fns {
							fn.SetOutputIndex(int(curOutputIdx))
							fn.Compute(coldata.ZeroBatch, op.aggCols[fnIdx])
						}
					} else {
						op.resumeIdx = op.resumeIdx + uint64(aggFuncIdx)
						op.output.SetLength(curOutputIdx)

						return op.output
					}
					curOutputIdx++
				}
				delete(op.aggFuncMap, op.resumeHashCode)
			}

			op.pendingOutput = false

			for aggHashCode, aggFuncs := range op.aggFuncMap {
				for aggFuncIdx, aggFunc := range aggFuncs {
					if curOutputIdx < coldata.BatchSize() {
						for fnIdx, fn := range aggFunc.fns {
							fn.SetOutputIndex(int(curOutputIdx))
							fn.Compute(coldata.ZeroBatch, op.aggCols[fnIdx])
						}
					} else {
						// If current batch is filled, we record where we left off
						// and then return the current batch
						op.resumeIdx = uint64(aggFuncIdx)
						op.resumeHashCode = aggHashCode
						op.pendingOutput = true
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

func (op *hashAggregator) bufferBatch(ctx context.Context) uint16 {
	bufferedBatchCount := uint16(0)

	for bufferedTupleCount := uint16(0); bufferedTupleCount < op.batchBufferTupleLimit; {
		b := op.input.Next(ctx)
		batchSize := b.Length()
		if b.Length() == 0 {
			break
		}
		bufferedTupleCount += batchSize
		bufferedBatchCount++
		op.allocator.PerformOperation(op.buffer.ColVecs(), func() {
			for i, colIdx := range op.valCols {
				op.buffer.ColVec(i).Append(
					coldata.SliceArgs{
						ColType:   op.valTypes[i],
						Src:       b.ColVec(int(colIdx)),
						Sel:       b.Selection(),
						DestIdx:   uint64(op.buffer.Length()),
						SrcEndIdx: uint64(batchSize),
					},
				)
			}
		})
		op.buffer.SetLength(bufferedTupleCount)
	}

	return bufferedBatchCount
}

// Build selection vector for each hash code.
func (op *hashAggregator) buildSelectionForHashCode(ctx context.Context) map[uint64][]uint16 {
	nKeys := op.buffer.Length()
	initHash(op.hashBuffer, uint64(nKeys), 1)

	for _, colIdx := range op.groupCols {
		rehash(ctx,
			op.hashBuffer,
			op.valTypes[colIdx],
			op.buffer.ColVec(int(colIdx)),
			uint64(nKeys),
			nil,
			op.cancelChecker)
	}

	hashBuffer := op.hashBuffer[:op.buffer.Length()]

	sels := make(map[uint64][]uint16)
	for selIdx, hashCode := range hashBuffer {
		if _, ok := sels[hashCode]; !ok {
			sels[hashCode] = make([]uint16, 0)
		}
		sels[hashCode] = append(sels[hashCode], uint16(selIdx))
	}

	return sels
}

// onlineAgg probes aggFuncMap using the built sels map and
// lazily create aggFunctions for each group if it doesn not
// exist. Then it calls Compute() on each aggregation function
// to perform aggregation.
func (op *hashAggregator) onlineAgg(sels map[uint64][]uint16) {
	for hashCode, sel := range sels {

		var matched, remaining []uint16

		// stage 1: probe aggregate functions for each bucket and
		//          perform aggregation
		if aggFuncs, ok := op.aggFuncMap[hashCode]; ok {
			for _, aggFunc := range aggFuncs {
				matched, remaining = aggFunc.match(sel, op.buffer, op.groupCols, op.groupTypes)
				if len(matched) > 0 {
					aggFunc.compute(op.buffer, op.aggCols, matched)
				}
			}
		} else {
			// since we don't expect a lot of hash collisions
			// we only allocate small amount of memory here
			op.aggFuncMap[hashCode] = make([]*hashAggFunc, 0, 1)
		}

		if remaining == nil {
			remaining = sel
		}

		// stage 2: build aggregate function that does not exist.
		//          then perform aggregation on the newly created
		//          aggregate function.
		for len(remaining) > 0 {
			groupSel := make([]uint16, 1, len(sel))
			groupSel[0] = remaining[0]

			keys := make([]coldata.Vec, len(op.groupCols))
			for keyIdx, colIdx := range op.groupCols {
				keys[keyIdx] = op.buffer.ColVec(int(colIdx)).Window(op.valTypes[colIdx], uint64(groupSel[0]), uint64(groupSel[0]+1))
			}

			remaining = remaining[1:]

			// build new agg functions.
			aggFunc := &hashAggFunc{keys: keys}
			aggFunc.fns, _, _ = makeAggregateFuncs(op.allocator, op.aggTypes, op.aggFuncs)
			op.aggFuncMap[hashCode] = append(op.aggFuncMap[hashCode], aggFunc)

			// select rest of the tuples that matches the current key.
			matched, remaining = aggFunc.match(remaining, op.buffer, op.groupCols, op.groupTypes)
			groupSel = append(groupSel, matched...)

			// this groupCol hack is required since aggregate functions
			// expect at least one group in all of its input batches,
			// otherwise it will cause index out of bound error later
			// when aggregate functions outputs the result.
			op.group[groupSel[0]] = true
			aggFunc.init(op.group, op.output)
			aggFunc.compute(op.buffer, op.aggCols, groupSel)
			op.group[groupSel[0]] = false
		}
	}
}

// reset resets the hashAggregator for another run. Primarily used for
// benchmarks.
func (op *hashAggregator) reset() {
	if resetter, ok := op.input.(resetter); ok {
		resetter.reset()
	}

	op.aggFuncMap = hashAggFuncMap{}
	op.state = hashAggregatorBuffering
	op.pendingOutput = false
	op.needToResetBuffer = false
	op.resumeIdx = 0
	op.output.ResetInternalBatch()
	op.buffer = op.allocator.NewMemBatchWithSize(op.valTypes, int(op.batchBufferTupleLimit))
}

var _ Operator = &hashAggregator{}

type hashAggFunc struct {
	keys []coldata.Vec
	fns  []aggregateFunc
}

type hashAggFuncMap map[uint64][]*hashAggFunc

func (v *hashAggFunc) init(group []bool, b coldata.Batch) {
	for fnIdx, fn := range v.fns {
		fn.Init(group, b.ColVec(fnIdx))
	}
}

func (v *hashAggFunc) compute(b coldata.Batch, aggCols [][]uint32, sel []uint16) {
	for fnIdx, fn := range v.fns {
		b.SetSelection(true)
		b.SetLength(uint16(len(sel)))
		s := b.Selection()
		copy(s, sel)
		fn.Compute(b, aggCols[fnIdx])
	}
}
