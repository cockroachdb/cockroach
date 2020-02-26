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
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
)

// NewUnorderedDistinct creates an unordered distinct on the given distinct
// columns.
func NewUnorderedDistinct(
	allocator *Allocator, input Operator, distinctCols []uint32, colTypes []coltypes.T,
) Operator {
	outCols := make([]uint32, len(colTypes))
	for i := range outCols {
		outCols[i] = uint32(i)
	}
	ht := newHashTable(
		allocator,
		hashTableNumBuckets,
		colTypes,
		distinctCols,
		outCols,
		true, /* allowNullEquality */
		hashTableDistinctMode,
	)

	return &unorderedDistinct{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,
		ht:           ht,
		output:       allocator.NewMemBatch(ht.outTypes),
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

	allocator     *Allocator
	ht            *hashTable
	buildFinished bool

	// sel is a list of indices to select representing the distinct rows.
	sel           []uint64
	distinctCount uint64

	output           coldata.Batch
	outputBatchStart uint64
}

var _ Operator = &unorderedDistinct{}

func (op *unorderedDistinct) Init() {
	op.input.Init()
}

func (op *unorderedDistinct) Next(ctx context.Context) coldata.Batch {
	op.output.ResetInternalBatch()
	// First, build the hash table.
	if !op.buildFinished {
		op.buildFinished = true
		op.ht.build(ctx, op.input)
		op.ht.findTupleGroups(ctx)
	}

	// The selection vector needs to be populated before any batching can be
	// done.
	if op.sel == nil {
		// Since next is no longer useful and pre-allocated to the appropriate
		// size, we can use it as the selection vector. This way we don't have to
		// reallocate a huge array.
		op.sel = op.ht.next
		// We calculate keyID for tuple at index i as "i+1," so we start from
		// position 1.
		for i, isHead := range op.ht.head[1:] {
			if isHead {
				// The tuple at index i is the "head" of the linked list of tuples that
				// are the same on the distinct columns, so we will include it while
				// all other tuples from the linked list will be skipped.
				op.sel[op.distinctCount] = uint64(i)
				op.distinctCount++
			}
		}
	}

	// Create and return the next batch of input to a maximum size of
	// coldata.BatchSize(). The rows in the new batch are specified by the
	// corresponding slice in the selection vector.
	nSelected := uint16(0)
	batchEnd := op.outputBatchStart + uint64(coldata.BatchSize())
	if batchEnd > op.distinctCount {
		batchEnd = op.distinctCount
	}
	nSelected = uint16(batchEnd - op.outputBatchStart)

	op.allocator.PerformOperation(op.output.ColVecs(), func() {
		for i, colIdx := range op.ht.outCols {
			toCol := op.output.ColVec(i)
			fromCol := op.ht.vals.colVecs[colIdx]
			toCol.Copy(
				coldata.CopySliceArgs{
					SliceArgs: coldata.SliceArgs{
						ColType:     op.ht.valTypes[op.ht.outCols[i]],
						Src:         fromCol,
						SrcStartIdx: op.outputBatchStart,
						SrcEndIdx:   batchEnd,
					},
					Sel64: op.sel,
				},
			)
		}
	})

	op.outputBatchStart = batchEnd
	op.output.SetLength(nSelected)
	return op.output
}

// Reset resets the unorderedDistinct for another run. Primarily used for
// benchmarks.
func (op *unorderedDistinct) reset() {
	op.outputBatchStart = 0
	op.ht.vals.reset()
	op.buildFinished = false
}
