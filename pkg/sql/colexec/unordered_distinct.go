// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	return &UnorderedDistinct{
		OneInputNode:         colexecop.NewOneInputNode(input),
		hashTableAllocator:   allocator,
		distinctCols:         distinctCols,
		typs:                 typs,
		nullsAreDistinct:     nullsAreDistinct,
		UpsertDistinctHelper: colexecbase.UpsertDistinctHelper{ErrorOnDup: errorOnDup},
		// This number was chosen after running the micro-benchmarks.
		hashTableNumBuckets: 128,
	}
}

// UnorderedDistinct performs a DISTINCT operation using a HashTable. It
// populates the hash table in an iterative fashion by appending only the
// distinct tuples from each input batch. Once at least one tuple is appended,
// all of the distinct tuples from the batch are emitted in the output.
type UnorderedDistinct struct {
	colexecop.InitHelper
	colexecop.OneInputNode
	colexecbase.UpsertDistinctHelper

	hashTableAllocator *colmem.Allocator
	distinctCols       []uint32
	typs               []*types.T
	nullsAreDistinct   bool

	Ht                  *colexechash.HashTable
	hashTableNumBuckets uint32
	// lastInputBatch tracks the last input batch read from the input and not
	// emitted into the output. It is the only batch that we need to export when
	// spilling to disk, and it will contain only the distinct tuples that need
	// to be emitted into the output.
	lastInputBatch coldata.Batch
	// LastInputBatchOrigLen tracks the length of lastInputBatch before
	// performing a distinct operation on it.
	LastInputBatchOrigLen int
}

var _ colexecop.BufferingInMemoryOperator = &UnorderedDistinct{}
var _ colexecop.ResettableOperator = &UnorderedDistinct{}

// Init implements the colexecop.Operator interface.
func (op *UnorderedDistinct) Init(ctx context.Context) {
	if !op.InitHelper.Init(ctx) {
		return
	}
	op.Input.Init(op.Ctx)
	// This number was chosen after running the micro-benchmarks.
	const hashTableLoadFactor = 2.0
	op.Ht = colexechash.NewHashTable(
		op.Ctx,
		op.hashTableAllocator,
		coldata.BatchSize(),
		hashTableLoadFactor,
		op.hashTableNumBuckets,
		op.typs,
		op.distinctCols,
		!op.nullsAreDistinct, /* allowNullEquality */
		colexechash.HashTableDistinctBuildMode,
		colexechash.HashTableDefaultProbeMode,
	)
}

// Next implements the colexecop.Operator interface.
func (op *UnorderedDistinct) Next() coldata.Batch {
	for {
		op.lastInputBatch = op.Input.Next()
		op.LastInputBatchOrigLen = op.lastInputBatch.Length()
		if op.LastInputBatchOrigLen == 0 {
			return coldata.ZeroBatch
		}
		// DistinctBuild call might result in an OOM error after lastInputBatch
		// is updated in-place to include only the new distinct tuples. If an
		// OOM occurs, we are careful not to filter them out (since the
		// filtering has already been performed); if an OOM doesn't occur, we
		// will emit the updated last input batch here.
		op.Ht.DistinctBuild(op.lastInputBatch)
		updatedLen := op.lastInputBatch.Length()
		op.MaybeEmitErrorOnDup(op.LastInputBatchOrigLen, updatedLen)
		if updatedLen > 0 {
			// We've just appended some distinct tuples to the hash table, so we
			// will emit all of them as the output. Note that the selection
			// vector on batch is set in such a manner that only the distinct
			// tuples are selected, so we can just emit batch directly.
			return op.lastInputBatch
		}
	}
}

// ExportBuffered implements the colexecop.BufferingInMemoryOperator interface.
func (op *UnorderedDistinct) ExportBuffered(colexecop.Operator) coldata.Batch {
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

// ReleaseBeforeExport implements the colexecop.BufferingInMemoryOperator
// interface.
func (op *UnorderedDistinct) ReleaseBeforeExport() {
	// We need to hold onto the hash table to perform the filtering in the
	// unorderedDistinctFilterer.
}

// ReleaseAfterExport implements the colexecop.BufferingInMemoryOperator
// interface.
func (op *UnorderedDistinct) ReleaseAfterExport(colexecop.Operator) {
	// We need to hold onto the hash table to perform the filtering in the
	// unorderedDistinctFilterer.
}

// Reset resets the UnorderedDistinct.
func (op *UnorderedDistinct) Reset(ctx context.Context) {
	if r, ok := op.Input.(colexecop.Resetter); ok {
		r.Reset(ctx)
	}
	op.Ht.Reset(ctx)
}
