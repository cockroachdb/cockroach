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
// NOTE: it takes in an *unlimited* allocator, and the operator itself is
// responsible for complying with the specified limit.
func NewUnorderedDistinct(
	unlimitedAllocator *colmem.Allocator,
	input colexecbase.Operator,
	distinctCols []uint32,
	typs []*types.T,
	memoryLimit int64,
) ResettableOperator {
	// These numbers were chosen after running the micro-benchmarks.
	const hashTableLoadFactor = 2.0
	const hashTableNumBuckets = 128
	ht := newHashTable(
		unlimitedAllocator,
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
		memoryLimit:  memoryLimit,
	}
}

// unorderedDistinct performs a DISTINCT operation using a hashTable. It
// populates the hash table in an iterative fashion by appending only the
// distinct tuples from each input batch. Once at least one tuple is appended,
// all of the distinct tuples from the batch are emitted in the output.
type unorderedDistinct struct {
	OneInputNode

	ht *hashTable
	// memoryLimit indicates the maximum amount of memory that can be used by
	// the hash table.
	memoryLimit int64
}

var _ colexecbase.BufferingInMemoryOperator = &unorderedDistinct{}
var _ ResettableOperator = &unorderedDistinct{}

func (op *unorderedDistinct) Init() {
	op.input.Init()
}

func (op *unorderedDistinct) Next(ctx context.Context) coldata.Batch {
	// We first check whether the hash table has already exceeded the memory
	// limit.
	if op.ht.allocator.Used() > op.memoryLimit {
		// We adjust the allocator to request the amount of memory that should
		// exceed the limit of the bound memory account, so this call will
		// result in an internal OOM error that will be caught by the disk
		// spiller.
		// We use a fraction of math.MaxInt64 to avoid possible integer
		// overflows.
		op.ht.allocator.AdjustMemoryUsage(math.MaxInt64 / 4)
	}
	for {
		batch := op.input.Next(ctx)
		if batch.Length() == 0 {
			return coldata.ZeroBatch
		}
		// Note that distinctBuild will never panic with an OOM error because
		// the hash table is using an unlimited allocator. This ensures that the
		// hash table never ends up in an inconsistent state (like when some
		// tuples are appended to ht.vals, but the internal slices 'first' and
		// 'next' are not updated accordingly).
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

func (op *unorderedDistinct) ExportBuffered(colexecbase.Operator) coldata.Batch {
	// We don't export any of the buffered data (instead, the external
	// distinct will be planned with unorderedDistinctFilterer as its input), so
	// there is nothing to export.
	return coldata.ZeroBatch
}

// reset resets the unorderedDistinct.
func (op *unorderedDistinct) reset(ctx context.Context) {
	if r, ok := op.input.(resetter); ok {
		r.reset(ctx)
	}
	op.ht.reset(ctx)
}

// unorderedDistinctFilterer filters out tuples that are duplicates of the
// tuples already emitted by the unordered distinct.
type unorderedDistinctFilterer struct {
	OneInputNode
	NonExplainable

	ht *hashTable
}

var _ colexecbase.Operator = &unorderedDistinctFilterer{}

func (f *unorderedDistinctFilterer) Init() {
	f.input.Init()
}

func (f *unorderedDistinctFilterer) Next(ctx context.Context) coldata.Batch {
	for {
		batch := f.input.Next(ctx)
		if batch.Length() == 0 {
			return coldata.ZeroBatch
		}
		if f.ht.vals.Length() > 0 {
			// The unordered distinct has emitted some tuples, so we need to
			// check all tuples in batch against the hash table.
			f.ht.computeHashAndBuildChains(ctx, batch)
			// Remove the duplicates within batch itself.
			f.ht.removeDuplicates(batch, f.ht.keys, f.ht.probeScratch.first, f.ht.probeScratch.next, f.ht.checkProbeForDistinct)
			// Remove the duplicates of already emitted distinct tuples.
			f.ht.removeDuplicates(batch, f.ht.keys, f.ht.buildScratch.first, f.ht.buildScratch.next, f.ht.checkBuildForDistinct)
		}
		if batch.Length() > 0 {
			return batch
		}
	}
}
