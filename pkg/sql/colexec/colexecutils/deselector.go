// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecutils

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// deselectorOp consumes the input operator, and if resulting batches have a
// selection vector, it coalesces them (meaning that tuples will be reordered
// or omitted according to the selection vector). If the batches come with no
// selection vector, it is a noop.
type deselectorOp struct {
	colexecop.OneInputHelper
	colexecop.NonExplainable
	unlimitedAllocator *colmem.Allocator
	inputTypes         []*types.T

	output coldata.Batch
}

var _ colexecop.Operator = &deselectorOp{}

// NewDeselectorOp creates a new deselector operator on the given input
// operator with the given column types.
//
// The provided allocator must be derived from an unlimited memory monitor since
// the deselectorOp cannot spill to disk and a "memory budget exceeded" error
// might be caught by the higher-level diskSpiller which would result in losing
// some query results.
func NewDeselectorOp(
	unlimitedAllocator *colmem.Allocator, input colexecop.Operator, typs []*types.T,
) colexecop.Operator {
	return &deselectorOp{
		OneInputHelper:     colexecop.MakeOneInputHelper(input),
		unlimitedAllocator: unlimitedAllocator,
		inputTypes:         typs,
	}
}

func (p *deselectorOp) Next() coldata.Batch {
	batch := p.Input.Next()
	if batch.Selection() == nil || batch.Length() == 0 {
		return batch
	}
	// deselectorOp should *not* limit the capacities of the returned batches,
	// so we don't use a memory limit here. It is up to the wrapped operator to
	// limit the size of batches based on the memory footprint.
	p.output, _ = p.unlimitedAllocator.ResetMaybeReallocateNoMemLimit(
		p.inputTypes, p.output, batch.Length(),
	)
	sel := batch.Selection()
	p.unlimitedAllocator.PerformOperation(p.output.ColVecs(), func() {
		for i := range p.inputTypes {
			toCol := p.output.ColVec(i)
			fromCol := batch.ColVec(i)
			toCol.Copy(
				coldata.SliceArgs{
					Src:       fromCol,
					Sel:       sel,
					SrcEndIdx: batch.Length(),
				},
			)
		}
	})
	p.output.SetLength(batch.Length())
	return p.output
}
