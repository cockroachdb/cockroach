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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// NewUnorderedDistinct creates an unordered distinct on the given distinct
// columns.
// numHashBuckets determines the number of buckets that the hash table is
// created with.
func NewUnorderedDistinct(
	allocator *colmem.Allocator, input colexecbase.Operator, distinctCols []uint32, typs []*types.T,
) colexecbase.Operator {
	// This number was chosen after running the micro-benchmarks.
	const hashTableLoadFactor = 2.0
	ht := newHashTable(
		allocator,
		hashTableLoadFactor,
		typs,
		distinctCols,
		// Store all columns from the source since the unordered distinct
		// doesn't change the schema.
		nil,  /* colsToStore */
		true, /* allowNullEquality */
		hashTableDistinctBuildMode,
		hashTableDefaultProbeMode,
	)

	return &unorderedDistinct{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,
		ht:           ht,
		typs:         typs,
	}
}

// unorderedDistinct performs a DISTINCT operation using a hashTable. It
// populates the hash table in an iterative fashion by appending only the
// distinct tuples from each input batch. Once at least one tuple is appended,
// all of the distinct tuples from the batch are emitted in the output.
type unorderedDistinct struct {
	OneInputNode

	allocator *colmem.Allocator
	ht        *hashTable
	typs      []*types.T

	output coldata.Batch
	// htIdx indicates the number of tuples from ht we have already emitted in
	// the output.
	htIdx int
}

var _ colexecbase.Operator = &unorderedDistinct{}

func (op *unorderedDistinct) Init() {
	op.input.Init()
}

func (op *unorderedDistinct) Next(ctx context.Context) coldata.Batch {
	for {
		batch := op.input.Next(ctx)
		if batch.Length() == 0 {
			return coldata.ZeroBatch
		}
		op.ht.distinctBuild(ctx, batch)
		if op.ht.vals.Length() > op.htIdx {
			// We've just appended some distinct tuples to the hash table, so we
			// will emit all of them as the output.
			outputLength := op.ht.vals.Length() - op.htIdx
			// For now, we don't enforce any footprint-based memory limit.
			// TODO(yuzefovich): refactor this.
			const maxBatchMemSize = math.MaxInt64
			op.output, _ = op.allocator.ResetMaybeReallocate(op.typs, op.output, outputLength, maxBatchMemSize)
			op.allocator.PerformOperation(op.output.ColVecs(), func() {
				for colIdx, fromCol := range op.ht.vals.ColVecs() {
					toCol := op.output.ColVec(colIdx)
					toCol.Copy(
						coldata.CopySliceArgs{
							SliceArgs: coldata.SliceArgs{
								Src:         fromCol,
								SrcStartIdx: op.htIdx,
								SrcEndIdx:   op.htIdx + outputLength,
							},
						},
					)
				}
				op.output.SetLength(outputLength)
			})
			op.htIdx += outputLength
			return op.output
		}
	}
}

// reset resets the unorderedDistinct.
func (op *unorderedDistinct) reset(ctx context.Context) {
	if r, ok := op.input.(resetter); ok {
		r.reset(ctx)
	}
	op.ht.reset(ctx)
	op.htIdx = 0
}
