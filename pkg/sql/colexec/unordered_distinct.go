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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// NewUnorderedDistinct creates an unordered distinct on the given distinct
// columns.
// numHashBuckets determines the number of buckets that the hash table is
// created with.
func NewUnorderedDistinct(
	allocator *colmem.Allocator,
	input colexecbase.Operator,
	distinctCols []uint32,
	typs []*types.T,
	numHashBuckets uint64,
) colexecbase.Operator {
	ht := newHashTable(
		allocator,
		numHashBuckets,
		typs,
		distinctCols,
		true, /* allowNullEquality */
		hashTableDistinctBuildMode,
		hashTableDefaultProbeMode,
	)

	return &unorderedDistinct{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,
		ht:           ht,
		output:       allocator.NewMemBatch(typs),
	}
}

// unorderedDistinct performs a DISTINCT operation using a hashTable. Once the
// building of the hashTable is completed, this operator iterates over all of
// the tuples to check whether the tuple is the "head" of a linked list that
// contain all of the tuples that are equal on distinct columns. Only the
// "head" is included into the big selection vector. Once the big selection
// vector is populated, the operator proceeds to returning the batches
// according to a chunk of the selection vector.
type unorderedDistinct struct {
	OneInputNode

	allocator     *colmem.Allocator
	ht            *hashTable
	buildFinished bool

	distinctCount int

	output           coldata.Batch
	outputBatchStart int
}

var _ colexecbase.Operator = &unorderedDistinct{}

func (op *unorderedDistinct) Init() {
	op.input.Init()
}

func (op *unorderedDistinct) Next(ctx context.Context) coldata.Batch {
	op.output.ResetInternalBatch()
	// First, build the hash table and populate the selection vector that
	// includes only distinct tuples.
	if !op.buildFinished {
		op.buildFinished = true
		op.ht.build(ctx, op.input)

		// We're using the hashTable in distinct mode, so it buffers only distinct
		// tuples, as a result, we will be simply returning all buffered tuples.
		op.distinctCount = op.ht.vals.Length()
	}

	// Create and return the next batch of input to a maximum size of
	// coldata.BatchSize().
	nSelected := 0
	batchEnd := op.outputBatchStart + coldata.BatchSize()
	if batchEnd > op.distinctCount {
		batchEnd = op.distinctCount
	}
	nSelected = batchEnd - op.outputBatchStart

	op.allocator.PerformOperation(op.output.ColVecs(), func() {
		for colIdx, fromCol := range op.ht.vals.ColVecs() {
			toCol := op.output.ColVec(colIdx)
			toCol.Copy(
				coldata.CopySliceArgs{
					SliceArgs: coldata.SliceArgs{
						Src:         fromCol,
						SrcStartIdx: op.outputBatchStart,
						SrcEndIdx:   batchEnd,
					},
				},
			)
		}
	})

	op.outputBatchStart = batchEnd
	op.output.SetLength(nSelected)
	return op.output
}

// reset resets the unorderedDistinct.
func (op *unorderedDistinct) reset(ctx context.Context) {
	if r, ok := op.input.(resetter); ok {
		r.reset(ctx)
	}
	op.ht.vals.ResetInternalBatch()
	op.ht.vals.SetLength(0)
	op.buildFinished = false
	op.ht.reset(ctx)
	op.distinctCount = 0
	op.outputBatchStart = 0
}
