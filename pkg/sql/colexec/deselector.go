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

// deselectorOp consumes the input operator, and if resulting batches have a
// selection vector, it coalesces them (meaning that tuples will be reordered
// or omitted according to the selection vector). If the batches come with no
// selection vector, it is a noop.
type deselectorOp struct {
	OneInputNode
	NonExplainable
	allocator  *colmem.Allocator
	inputTypes []*types.T

	output coldata.Batch
}

var _ colexecbase.Operator = &deselectorOp{}

// NewDeselectorOp creates a new deselector operator on the given input
// operator with the given column types.
func NewDeselectorOp(
	allocator *colmem.Allocator, input colexecbase.Operator, typs []*types.T,
) colexecbase.Operator {
	return &deselectorOp{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,
		inputTypes:   typs,
	}
}

func (p *deselectorOp) Init() {
	p.input.Init()
}

func (p *deselectorOp) Next(ctx context.Context) coldata.Batch {
	p.resetOutput()
	batch := p.input.Next(ctx)
	if batch.Selection() == nil {
		return batch
	}

	sel := batch.Selection()
	p.allocator.PerformOperation(p.output.ColVecs(), func() {
		for i := range p.inputTypes {
			toCol := p.output.ColVec(i)
			fromCol := batch.ColVec(i)
			toCol.Copy(
				coldata.CopySliceArgs{
					SliceArgs: coldata.SliceArgs{
						Src:       fromCol,
						Sel:       sel,
						SrcEndIdx: batch.Length(),
					},
				},
			)
		}
	})
	p.output.SetLength(batch.Length())
	return p.output
}

func (p *deselectorOp) resetOutput() {
	if p.output == nil {
		p.output = p.allocator.NewMemBatch(p.inputTypes)
	} else {
		p.output.ResetInternalBatch()
	}
}
