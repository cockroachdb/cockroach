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
func NewUnorderedDistinct(
	allocator *colmem.Allocator, input colexecbase.Operator, distinctCols []uint32, typs []*types.T,
) ResettableOperator {
	// These numbers were chosen after running the micro-benchmarks.
	const hashTableLoadFactor = 2.0
	const hashTableNumBuckets = 128
	ht := newHashTable(
		allocator,
		hashTableLoadFactor,
		hashTableNumBuckets,
		typs,
		distinctCols,
		true, /* allowNullEquality */
		hashTableDistinctBuildMode,
		hashTableDefaultProbeMode,
	)

	return &unorderedDistinct{
		OneInputNode: NewOneInputNode(input),
		ht:           ht,
	}
}

// unorderedDistinct performs a DISTINCT operation using a hashTable. It
// populates the hash table in an iterative fashion by appending only the
// distinct tuples from each input batch. Once at least one tuple is appended,
// all of the distinct tuples from the batch are emitted in the output.
type unorderedDistinct struct {
	OneInputNode

	ht             *hashTable
	lastInputBatch coldata.Batch
}

var _ colexecbase.BufferingInMemoryOperator = &unorderedDistinct{}
var _ ResettableOperator = &unorderedDistinct{}

func (op *unorderedDistinct) Init() {
	op.input.Init()
}

func (op *unorderedDistinct) Next(ctx context.Context) coldata.Batch {
	for {
		op.lastInputBatch = op.input.Next(ctx)
		if op.lastInputBatch.Length() == 0 {
			return coldata.ZeroBatch
		}
		// Note that distinctBuild might panic with a memory budget exceeded
		// error, in which case no tuples from the last input batch are output.
		// In such scenario, we don't know at which point of distinctBuild that
		// happened, but it doesn't matter - we will export the last input batch
		// when falling back to disk.
		op.ht.distinctBuild(ctx, op.lastInputBatch)
		if op.lastInputBatch.Length() > 0 {
			// We've just appended some distinct tuples to the hash table, so we
			// will emit all of them as the output. Note that the selection
			// vector on batch is set in such a manner that only the distinct
			// tuples are selected, so we can just emit batch directly.
			return op.lastInputBatch
		}
	}
}

func (op *unorderedDistinct) ExportBuffered(colexecbase.Operator) coldata.Batch {
	// We have output all the distinct tuples except for the ones that are part
	// of the last input batch, so we only need to export that batch, and then
	// we're done exporting.
	if op.lastInputBatch != nil {
		batch := op.lastInputBatch
		op.lastInputBatch = nil
		return batch
	}
	return coldata.ZeroBatch
}

// reset resets the unorderedDistinct.
func (op *unorderedDistinct) reset(ctx context.Context) {
	if r, ok := op.input.(resetter); ok {
		r.reset(ctx)
	}
	op.ht.reset(ctx)
}
