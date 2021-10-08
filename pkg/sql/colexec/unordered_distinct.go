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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexechash"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// NewUnorderedDistinct creates an unordered distinct on the given distinct
// columns.
func NewUnorderedDistinct(
	allocator *colmem.Allocator,
	input colexecop.Operator,
	distinctCols []uint32,
	typs []*types.T,
	nullsAreDistinct bool,
	errorOnDup string,
) colexecop.ResettableOperator {
	return &unorderedDistinct{
		OneInputNode:         colexecop.NewOneInputNode(input),
		allocator:            allocator,
		distinctCols:         distinctCols,
		typs:                 typs,
		nullsAreDistinct:     nullsAreDistinct,
		UpsertDistinctHelper: colexecbase.UpsertDistinctHelper{ErrorOnDup: errorOnDup},
	}
}

// unorderedDistinct performs a DISTINCT operation using a HashTable. It
// populates the hash table in an iterative fashion by appending only the
// distinct tuples from each input batch. Once at least one tuple is appended,
// all of the distinct tuples from the batch are emitted in the output.
type unorderedDistinct struct {
	colexecop.InitHelper
	colexecop.OneInputNode
	colexecbase.UpsertDistinctHelper

	allocator        *colmem.Allocator
	distinctCols     []uint32
	typs             []*types.T
	nullsAreDistinct bool

	ht *colexechash.HashTable
	// lastInputBatch tracks the last input batch read from the input and not
	// emitted into the output. It is the only batch that we need to export when
	// spilling to disk, and it will contain only the distinct tuples that need
	// to be emitted into the output.
	lastInputBatch coldata.Batch
	// lastInputBatchOrigLen tracks the length of lastInputBatch before
	// performing a distinct operation on it.
	lastInputBatchOrigLen int
}

var _ colexecop.BufferingInMemoryOperator = &unorderedDistinct{}
var _ colexecop.ResettableOperator = &unorderedDistinct{}

func (op *unorderedDistinct) Init(ctx context.Context) {
	if !op.InitHelper.Init(ctx) {
		return
	}
	op.Input.Init(op.Ctx)
	// These numbers were chosen after running the micro-benchmarks.
	const hashTableLoadFactor = 2.0
	const hashTableNumBuckets = 128
	op.ht = colexechash.NewHashTable(
		op.Ctx,
		op.allocator,
		hashTableLoadFactor,
		hashTableNumBuckets,
		op.typs,
		op.distinctCols,
		!op.nullsAreDistinct, /* allowNullEquality */
		colexechash.HashTableDistinctBuildMode,
		colexechash.HashTableDefaultProbeMode,
	)
}

func (op *unorderedDistinct) Next() coldata.Batch {
	for {
		op.lastInputBatch = op.Input.Next()
		op.lastInputBatchOrigLen = op.lastInputBatch.Length()
		if op.lastInputBatchOrigLen == 0 {
			return coldata.ZeroBatch
		}
		// DistinctBuild call might result in an OOM error after lastInputBatch
		// is updated in-place to include only the new distinct tuples. If an
		// OOM occurs, we are careful not to filter them out (since the
		// filtering has already been performed); if an OOM doesn't occur, we
		// will emit the updated last input batch here.
		op.ht.DistinctBuild(op.lastInputBatch)
		updatedLen := op.lastInputBatch.Length()
		op.MaybeEmitErrorOnDup(op.lastInputBatchOrigLen, updatedLen)
		if updatedLen > 0 {
			// We've just appended some distinct tuples to the hash table, so we
			// will emit all of them as the output. Note that the selection
			// vector on batch is set in such a manner that only the distinct
			// tuples are selected, so we can just emit batch directly.
			return op.lastInputBatch
		}
	}
}

func (op *unorderedDistinct) ExportBuffered(colexecop.Operator) coldata.Batch {
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

// Reset resets the unorderedDistinct.
func (op *unorderedDistinct) Reset(ctx context.Context) {
	if r, ok := op.Input.(colexecop.Resetter); ok {
		r.Reset(ctx)
	}
	op.ht.Reset(ctx)
}

// unorderedDistinctFilterer filters out tuples that are duplicates of the
// tuples already emitted by the unordered distinct.
type unorderedDistinctFilterer struct {
	colexecop.OneInputHelper
	colexecop.NonExplainable

	ud *unorderedDistinct
	// seenBatch tracks whether the operator has already read at least one
	// batch.
	seenBatch bool
}

var _ colexecop.Operator = &unorderedDistinctFilterer{}

func (f *unorderedDistinctFilterer) Next() coldata.Batch {
	if f.ud.ht.Vals.Length() == 0 {
		// The hash table is empty, so there is nothing to filter against.
		return f.Input.Next()
	}
	for {
		batch := f.Input.Next()
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
			// occurred when tuples were being appended to f.ht.Vals.
			//
			// See https://github.com/cockroachdb/cockroach/pull/58006#pullrequestreview-565859919
			// for all the gory details.
			f.ud.ht.MaybeRepairAfterDistinctBuild()
			f.ud.MaybeEmitErrorOnDup(f.ud.lastInputBatchOrigLen, batch.Length())
			f.seenBatch = true
			return batch
		}
		origLen := batch.Length()
		// The unordered distinct has emitted some tuples, so we need to check
		// all tuples in batch against the hash table.
		f.ud.ht.ComputeHashAndBuildChains(batch)
		// Remove the duplicates within batch itself.
		f.ud.ht.RemoveDuplicates(batch, f.ud.ht.Keys, f.ud.ht.ProbeScratch.First, f.ud.ht.ProbeScratch.Next, f.ud.ht.CheckProbeForDistinct)
		// Remove the duplicates of already emitted distinct tuples.
		f.ud.ht.RemoveDuplicates(batch, f.ud.ht.Keys, f.ud.ht.BuildScratch.First, f.ud.ht.BuildScratch.Next, f.ud.ht.CheckBuildForDistinct)
		f.ud.MaybeEmitErrorOnDup(origLen, batch.Length())
		if batch.Length() > 0 {
			return batch
		}
	}
}
