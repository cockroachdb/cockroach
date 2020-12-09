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
	allocator *colmem.Allocator, input colexecbase.Operator, distinctCols []uint32, typs []*types.T,
) colexecbase.Operator {
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

	ht *hashTable
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
		if batch.Length() > 0 {
			// We've just appended some distinct tuples to the hash table, so we
			// will emit all of them as the output. Note that the selection
			// vector on batch is set in such a manner that only the distinct
			// tuples are selected, so we can just emit batch directly.
			return batch
		}
	}
}

// reset resets the unorderedDistinct.
func (op *unorderedDistinct) reset(ctx context.Context) {
	if r, ok := op.input.(resetter); ok {
		r.reset(ctx)
	}
	op.ht.reset(ctx)
}
