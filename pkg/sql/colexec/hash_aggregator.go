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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// HashAggregator is an operator chain that performs an aggregation based on
// specified grouping columns. It's composed of two operators:
// 1. hashGrouper
// 2. orderedAggregator
// This operator chain performs a hash table build at the first call to Next()
// which consumes the entire build source. After the build phase, the state of
// the hashGrouper is changed to buildFinished and the grouped hash table
// buckets are then passed as input to an orderedAggregator. The output row
// ordering of this aggregator is arbitrary.
//
// The high level way this works is:
// 1. build a hash table of its entire input, throwing away non-distinct inputs
// 2. for every key, probe the hash table, populating a linked list array that
//    keeps track of which indexes of the hash table are identical
// 3. traverse the input keys again, determining a single unique key for each
//    group of identical keys. for each unique key, traverse the linked list,
//    marking the index of each identical key in the selection vector
// 4. point the ordered aggregator at the selection vector, so it gets groups of
//    rows with identical equality columns.
//
// TODO(jordan,sqlexec): This works fine, but it's kind of slow. I think the
// reason it's slow is that it has to make a lot of passes over the input, and a
// lot of random access jumps through the large linked list array.
//
// One idea to improve the performance of this is to change the algorithm so
// that the aggregation is performed on demand. This would require reworking the
// individual aggregator functions to be a bit more general, so they don't
// assume usage in the ordered aggregator paradigm, which is why it hasn't been
// tried yet.

// NewHashAggregator creates a hash aggregator on the given grouping
// columns. The input specifications to this function are the same as that of
// the NewOrderedAggregator function.
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
	// columns used as input to an aggregation function,
	compressed := make([]uint32, nCols)
	for _, cols := range aggCols {
		for _, col := range cols {
			keepCol.Add(int(col))
		}
	}

	// Map the corresponding aggCols to the new output column indices.
	nOutCols := uint32(0)
	outCols := make([]uint32, 0)
	keepCol.ForEach(func(i int) {
		outCols = append(outCols, uint32(i))
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

	ht := makeHashTable(
		allocator,
		hashTableBucketSize,
		colTypes,
		groupCols,
		outCols,
		true, /* allowNullEquality */
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

	funcs, outTyps, err := makeAggregateFuncs(allocator, aggTyps, aggFns)
	if err != nil {
		return nil, errors.AssertionFailedf(
			"this error should have been checked in isAggregateSupported\n%+v", err,
		)
	}

	distinctCol := make([]bool, coldata.BatchSize())

	grouper := &hashGrouper{
		builder:     builder,
		ht:          ht,
		distinctCol: distinctCol,
		batch:       allocator.NewMemBatch(ht.outTypes),
	}

	orderedAgg := &orderedAggregator{
		OneInputNode:   NewOneInputNode(grouper),
		allocator:      allocator,
		aggCols:        mappedAggCols,
		aggTypes:       aggTyps,
		groupCol:       distinctCol,
		aggregateFuncs: funcs,
		outputTypes:    outTyps,
		isScalar:       isScalar,
	}

	return orderedAgg, nil
}

// hashGrouper performs grouping using a hashTable, returning batches
// of the grouped hash table buckets as its output. Once the building of the
// hashTable is completed, this operator returns the next batch of input
// to the orderedAggregator based on the results of the pre-built hashTable.
// See the description at the top of this file for more information.
type hashGrouper struct {
	builder *hashJoinBuilder
	ht      *hashTable

	// sel is an ordered list of indices to select representing the input rows.
	// This selection vector is much bigger than coldata.BatchSize() and should be
	// batched with the hashGrouper operator.
	sel []uint64
	// distinct represents whether each corresponding row is part of a new group.
	distinct []bool

	// distinctCol is a reference to the slice that aggregateFuncs use to
	// determine whether a value is part of the current aggregation group. See
	// aggregateFunc.Init for more information.
	distinctCol []bool

	batchStart uint64
	batch      coldata.Batch

	buildFinished bool
}

var _ execinfra.OpNode = &hashGrouper{}

func (op *hashGrouper) ChildCount(verbose bool) int {
	return 1
}

func (op *hashGrouper) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		return op.builder.spec.source
	}
	execerror.VectorizedInternalPanic(fmt.Sprintf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

func (op *hashGrouper) Init() {
	op.builder.spec.source.Init()
}

func (op *hashGrouper) Next(ctx context.Context) coldata.Batch {
	op.batch.ResetInternalBatch()
	// First, build the hash table.
	if !op.buildFinished {
		op.buildFinished = true
		op.builder.exec(ctx)
	}

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
	// coldata.BatchSize(). The rows in the new batch is specified by the corresponding
	// slice in the selection vector.
	nSelected := uint16(0)

	batchEnd := op.batchStart + uint64(coldata.BatchSize())
	if batchEnd > op.ht.size {
		batchEnd = op.ht.size
	}
	nSelected = uint16(batchEnd - op.batchStart)

	copy(op.distinctCol, op.distinct[op.batchStart:batchEnd])

	for i, colIdx := range op.ht.outCols {
		toCol := op.batch.ColVec(i)
		fromCol := op.ht.vals[colIdx]
		op.ht.allocator.Copy(
			toCol,
			coldata.CopySliceArgs{
				SliceArgs: coldata.SliceArgs{
					ColType:     op.ht.valTypes[op.ht.outCols[i]],
					Src:         fromCol,
					SrcStartIdx: op.batchStart,
					SrcEndIdx:   batchEnd,
				},
				Sel64: op.sel,
			},
		)
	}

	op.batchStart = batchEnd

	op.batch.SetLength(nSelected)
	return op.batch
}

// Reset resets the hashGrouper for another run. Primarily used for
// benchmarks.
func (op *hashGrouper) reset() {
	op.batchStart = 0
	op.ht.size = 0
	op.buildFinished = false
}

var _ Operator = &hashGrouper{}
