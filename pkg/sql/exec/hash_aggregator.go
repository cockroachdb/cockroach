// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package exec

import (
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// hashAggregator is an operator that performs an aggregation based on
// specified grouping columns. This operator performs a hash table build at the
// first call to `Next()` which consumes the entire build source. After the
// build phase, the state of the hashAggregator is changed to isAggregating and
// the grouped hash table buckets are then passed as input to an
// orderedAggregator. The output row ordering of this aggregator is arbitrary.
type hashAggregator struct {
	spec aggregatorSpec

	// isAggregating represents whether the hashAggregator is in the building or
	// aggregating phase.
	buildFinished bool

	ht      *hashTable
	builder *hashJoinBuilder

	// orderedAgg is the underlying orderedAggregator used by this
	// hashAggregator once the bucket-groups of the input has been determined.
	orderedAgg *orderedAggregator
}

// NewHashAggregator creates a hashed aggregator on the given grouping
// columns. The input specifications to this function are the same as that of
// the NewOrderedAggregator function.
func NewHashAggregator(
	input Operator,
	colTypes []types.T,
	aggFns []distsqlpb.AggregatorSpec_Func,
	groupCols []uint32,
	aggCols [][]uint32,
) (Operator, error) {
	aggTyps := extractAggTypes(aggCols, colTypes)

	// Only keep relevant output columns, those that are used as input to an
	// aggregation.
	nCols := uint32(len(colTypes))
	keepCol := make([]bool, nCols)

	// compressed represents a mapping between each original column and its index
	// in the new compressed columns set. This is required since we are
	// effectively compressing the original list of columns by only keeping the
	// columns used as input to an aggregation function,
	compressed := make([]uint32, nCols)
	for _, cols := range aggCols {
		for _, col := range cols {
			keepCol[col] = true
		}
	}

	// Map the corresponding aggCols to the new output column indices.
	nOutCols := uint32(0)
	outCols := make([]uint32, 0)
	for i := range keepCol {
		if keepCol[i] {
			outCols = append(outCols, uint32(i))
			compressed[i] = nOutCols
			nOutCols++
		}
	}

	mappedAggCols := make([][]uint32, len(aggCols))
	for aggIdx := range aggCols {
		mappedAggCols[aggIdx] = make([]uint32, len(aggCols[aggIdx]))
		for i := range mappedAggCols[aggIdx] {
			mappedAggCols[aggIdx][i] = compressed[aggCols[aggIdx][i]]
		}
	}

	ht := makeHashTable(
		hashTableBucketSize,
		colTypes,
		groupCols,
		outCols,
	)

	builder := makeHashJoinBuilder(
		ht,
		hashJoinerSourceSpec{
			source:      input,
			eqCols:      groupCols,
			outCols:     outCols,
			sourceTypes: colTypes,
		},
	)

	funcs, outTyps, err := makeAggregateFuncs(aggTyps, aggFns)
	if err != nil {
		return nil, err
	}

	distinctCol := make([]bool, ColBatchSize)

	// op is the underlying batching operator used as input by the orderedAgg to
	// create the batches of ColBatchSize from built hashTable.
	op := makeHashAggregatorBatchOp(ht, distinctCol)

	orderedAgg := &orderedAggregator{
		input:          op,
		aggCols:        mappedAggCols,
		aggTyps:        aggTyps,
		groupCol:       distinctCol,
		aggregateFuncs: funcs,
		outputTyps:     outTyps,
	}

	return &hashAggregator{
		spec: aggregatorSpec{
			input:     input,
			colTypes:  colTypes,
			aggFns:    aggFns,
			groupCols: groupCols,
			aggCols:   aggCols,
		},

		ht:      ht,
		builder: builder,

		orderedAgg: orderedAgg,
	}, nil
}

func (ag *hashAggregator) Init() {
	ag.spec.input.Init()
	ag.orderedAgg.Init()
}

func (ag *hashAggregator) Next() ColBatch {
	if !ag.buildFinished {
		ag.build()
	}

	return ag.orderedAgg.Next()
}

// Reset resets the hashAggregator for another run. Primarily used for
// benchmarks.
func (ag *hashAggregator) reset() {
	ag.ht.size = 0
	ag.buildFinished = false
	ag.orderedAgg.reset()
}

func (ag *hashAggregator) build() {
	ag.builder.exec()
	ag.buildFinished = true
}

var _ Operator = &hashAggregator{}

// aggregatorSpec represents the specifications for the various aggregation
// operators.
type aggregatorSpec struct {
	// input is the source Operator consumed by the aggregator.
	input Operator
	// colTypes represents the column types corresponding to the input columns.
	colTypes []types.T

	// aggFns represents the list of aggregation functions to be performed.
	aggFns []distsqlpb.AggregatorSpec_Func

	// aggCols is a slice where each index represents a new aggregation function.
	// The slice at each index maps to the input column indices used as input to
	// each corresponding aggregation function.
	aggCols [][]uint32
	// groupCols holds the list of group column indices.
	groupCols []uint32
}

// hashAggregatorBatchOp is a helper operator used by the hashAggregator as
// input to its orderedAggregator. Once the building of the hashTable is
// completed, this batch operator exists to return the next batch of input to
// the orderedAggregator based on the results of the pre-built hashTable.
type hashAggregatorBatchOp struct {
	ht *hashTable

	// sel is an ordered list of indices to select representing the input rows.
	// This selection vector is much bigger than ColBatchSize and should be
	// batched with the hashAggregatorBatchOp operator.
	sel []uint64
	// distinct represents whether each corresponding row is part of a new group.
	distinct []bool

	// distinctCol is a reference to the slice that aggregateFuncs use to
	// determine whether a value is part of the current aggregation group. See
	// aggregateFunc.Init for more information.
	distinctCol []bool

	batchStart uint64
	batch      ColBatch
}

func makeHashAggregatorBatchOp(ht *hashTable, distinctCol []bool) Operator {
	return &hashAggregatorBatchOp{
		distinctCol: distinctCol,
		ht:          ht,
		batch:       NewMemBatch(ht.outTypes),
	}
}

func (op *hashAggregatorBatchOp) Init() {}

func (op *hashAggregatorBatchOp) Next() ColBatch {
	// The selection vector needs to be populated before any batching can be
	// done.
	if op.sel == nil {
		// After the entire hashTable is built, we want to construct the selection
		// vector. This vector would be an ordered list of indices indicating the
		// ordering of the bucket-grouped rows of input. The same linked list is
		// traversed from each head to form this ordered list.
		headID := uint64(0)
		curID := uint64(0)

		// Since next is no longer useful and pre-allocated to the appropriate size,
		// we can use it as the selection vector. This way we don't have to
		// reallocate a huge array.
		op.sel = op.ht.next
		// Since visited is no longer used and pre-allocated to the appropriate
		// size, we can use it for the distinct vector.
		op.distinct = op.ht.visited

		for i := uint64(0); i < op.ht.size; i++ {
			op.distinct[i] = false
			for !op.ht.head[headID] || curID == 0 {
				op.distinct[i] = true
				headID++
				curID = headID
			}
			op.sel[i] = curID - 1
			curID = op.ht.same[curID]
		}
	}

	// Create and return the next batch of input to a maximum size of
	// ColBatchSize. The rows in the new batch is specified by the corresponding
	// slice in the selection vector.
	nSelected := uint16(0)

	batchEnd := op.batchStart + uint64(ColBatchSize)
	if batchEnd > op.ht.size {
		batchEnd = op.ht.size
	}
	nSelected = uint16(batchEnd - op.batchStart)

	copy(op.distinctCol, op.distinct[op.batchStart:batchEnd])

	for i, colIdx := range op.ht.outCols {
		toCol := op.batch.ColVec(i)
		fromCol := op.ht.vals[colIdx]
		toCol.CopyWithSelInt64(fromCol, op.sel[op.batchStart:batchEnd], nSelected, op.ht.valTypes[op.ht.outCols[i]])
	}

	op.batchStart = batchEnd

	op.batch.SetLength(nSelected)
	return op.batch
}

func (op *hashAggregatorBatchOp) reset() {
	op.batchStart = 0
}

var _ Operator = &hashAggregatorBatchOp{}
