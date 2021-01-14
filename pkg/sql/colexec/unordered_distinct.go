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

	ht *hashTable
	// lastInputBatch tracks the last input batch read from the input and not
	// emitted into the output. It is the only batch that we need to export when
	// spilling to disk, and it will contain only the distinct tuples that need
	// to be emitted into the output.
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
		// distinctBuild call might result in an OOM error after lastInputBatch
		// is updated in-place to include only the new distinct tuples. If an
		// OOM occurs, we are careful not to filter them out (since the
		// filtering has already been performed); if an OOM doesn't occur, we
		// will emit the updated last input batch here.
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

func (op *unorderedDistinct) ExportBuffered(context.Context, colexecbase.Operator) coldata.Batch {
	if op.lastInputBatch != nil {
		batch := op.lastInputBatch
		op.lastInputBatch = nil
		return batch
	}
	// We only need to export the last input batch because the buffered in the
	// hash table data is used by the unorderedDistinctFilterer (which is
	// planned by the external distinct).
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
	// seenBatch tracks whether the operator has already read at least one
	// batch.
	seenBatch bool
}

var _ colexecbase.Operator = &unorderedDistinctFilterer{}

func (f *unorderedDistinctFilterer) Init() {
	f.input.Init()
}

func (f *unorderedDistinctFilterer) Next(ctx context.Context) coldata.Batch {
	if f.ht.vals.Length() == 0 {
		// The hash table is empty, so there is nothing to filter against.
		return f.input.Next(ctx)
	}
	for {
		batch := f.input.Next(ctx)
		if batch.Length() == 0 {
			return coldata.ZeroBatch
		}
		if !f.seenBatch {
			// This is the first batch we received from bufferExportingOperator
			// and the hash table is not empty; therefore, this batch must be
			// the last input batch coming from the in-memory unordered
			// distinct.
			//
			// That batch has already been updated in-place to contain only
			// distinct tuples all of which have been appended to the hash
			// table, so we don't need to perform filtering on it. However, we
			// might need to repair the hash table in case the OOM error
			// occurred when tuples were being appended to f.ht.vals.
			//
			// See https://github.com/cockroachdb/cockroach/pull/58006#pullrequestreview-565859919
			// for all the gory details.
			f.ht.maybeRepairAfterDistinctBuild(ctx)
			f.seenBatch = true
			return batch
		}
		// The unordered distinct has emitted some tuples, so we need to check
		// all tuples in batch against the hash table.
		f.ht.computeHashAndBuildChains(ctx, batch)
		// Remove the duplicates within batch itself.
		f.ht.removeDuplicates(batch, f.ht.keys, f.ht.probeScratch.first, f.ht.probeScratch.next, f.ht.checkProbeForDistinct)
		// Remove the duplicates of already emitted distinct tuples.
		f.ht.removeDuplicates(batch, f.ht.keys, f.ht.buildScratch.first, f.ht.buildScratch.next, f.ht.checkBuildForDistinct)
		if batch.Length() > 0 {
			return batch
		}
	}
}
