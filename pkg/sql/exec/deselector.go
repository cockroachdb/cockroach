// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// deselectorOp consumes the input operator, and if resulting batches have a
// selection vector, it coalesces them (meaning that tuples will be reordered
// or omitted according to the selection vector). If the batches come with no
// selection vector, it is a noop.
type deselectorOp struct {
	input      Operator
	inputTypes []types.T

	output coldata.Batch
}

var _ Operator = &deselectorOp{}

// NewDeselectorOp creates a new deselector operator on the given input
// operator with the given column types.
func NewDeselectorOp(input Operator, colTypes []types.T) Operator {
	return &deselectorOp{
		input:      input,
		inputTypes: colTypes,
	}
}

func (p *deselectorOp) Init() {
	p.input.Init()
	p.output = coldata.NewMemBatch(p.inputTypes)
}

func (p *deselectorOp) Next(ctx context.Context) coldata.Batch {
	batch := p.input.Next(ctx)
	if batch.Selection() == nil {
		return batch
	}

	p.output.SetLength(batch.Length())
	sel := batch.Selection()
	for i, t := range p.inputTypes {
		toCol := p.output.ColVec(i)
		fromCol := batch.ColVec(i)
		toCol.Copy(
			coldata.CopyArgs{
				ColType:   t,
				Src:       fromCol,
				Sel:       sel,
				SrcEndIdx: uint64(batch.Length()),
			},
		)
	}
	return p.output
}
