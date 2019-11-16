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
	ht := makeHashTable(
		allocator,
		hashTableBucketSize,
		colTypes,
		distinctCols,
		outCols,
		true, /* allowNullEquality */
	)

	builder := makeHashJoinBuilder(
		ht,
		hashJoinerSourceSpec{
			source:      input,
			eqCols:      distinctCols,
			outCols:     outCols,
			sourceTypes: colTypes,
		},
	)

	return &unorderedDistinct{
		builder: builder,
		ht:      ht,
		output:  allocator.NewMemBatch(ht.outTypes),
	}
}

// unorderedDistinct performs a DISTINCT operation using a hashTable. Once the
// building of the hashTable is completed, this operator iterates over all
// possible hash values, checks whether there is a bucket corresponding to the
// hash, and if so, selects an arbitrary row from the bucket into the output.
type unorderedDistinct struct {
	builder       *hashJoinBuilder
	ht            *hashTable
	buildFinished bool

	// sel is a list of indices to select representing the distinct rows.
	sel           []uint64
	distinctCount uint64

	output           coldata.Batch
	outputBatchStart uint64
}

var _ Operator = &unorderedDistinct{}

func (op *unorderedDistinct) ChildCount(bool) int {
	return 1
}

func (op *unorderedDistinct) Child(nth int, _ bool) execinfra.OpNode {
	if nth == 0 {
		return op.builder.spec.source
	}
	execerror.VectorizedInternalPanic(fmt.Sprintf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

func (op *unorderedDistinct) Init() {
	op.builder.spec.source.Init()
}

func (op *unorderedDistinct) Next(ctx context.Context) coldata.Batch {
	op.output.ResetInternalBatch()
	// First, build the hash table.
	if !op.buildFinished {
		op.buildFinished = true
		op.builder.exec(ctx)
	}

	// The selection vector needs to be populated before any batching can be
	// done.
	if op.sel == nil {
		// Since next is no longer useful and pre-allocated to the appropriate
		// size, we can use it as the selection vector. This way we don't have to
		// reallocate a huge array.
		op.sel = op.ht.next

		for _, keyID := range op.ht.first {
			if keyID != 0 {
				op.sel[op.distinctCount] = keyID - 1
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

	for i, colIdx := range op.ht.outCols {
		toCol := op.output.ColVec(i)
		fromCol := op.ht.vals[colIdx]
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

	op.outputBatchStart = batchEnd
	op.output.SetLength(nSelected)
	return op.output
}

// Reset resets the unorderedDistinct for another run. Primarily used for
// benchmarks.
func (op *unorderedDistinct) reset() {
	op.outputBatchStart = 0
	op.ht.size = 0
	op.buildFinished = false
}
