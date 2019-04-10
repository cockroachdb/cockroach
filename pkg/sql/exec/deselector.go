// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
		toCol.CopyWithSelInt16(fromCol, sel, batch.Length(), t)
	}
	return p.output
}
