// Copyright 2018 The Cockroach Authors.
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

import "github.com/cockroachdb/cockroach/pkg/sql/exec/types"

// coalescerOp consumes the input operator and coalesces the resulting batches
// to return full batches of ColBatchSize.
type coalescerOp struct {
	input      Operator
	inputTypes []types.T

	group  ColBatch
	buffer ColBatch
}

var _ Operator = &coalescerOp{}

// NewCoalescerOp creates a new coalescer operator on the given input operator
// with the given column types.
func NewCoalescerOp(input Operator, colTypes []types.T) Operator {
	return &coalescerOp{
		input:      input,
		inputTypes: colTypes,
	}
}

func (p *coalescerOp) Init() {
	p.input.Init()
	p.group = NewMemBatch(p.inputTypes)
	p.buffer = NewMemBatch(p.inputTypes)
}

func (p *coalescerOp) Next() ColBatch {
	tempBatch := p.group
	p.group = p.buffer

	p.buffer = tempBatch
	p.buffer.SetLength(0)

	for p.group.Length() < ColBatchSize {
		leftover := ColBatchSize - p.group.Length()
		batch := p.input.Next()
		batchSize := batch.Length()

		if batchSize == 0 {
			break
		}

		sel := batch.Selection()

		for i, t := range p.inputTypes {
			toCol := p.group.ColVec(i)
			fromCol := batch.ColVec(i)

			if batchSize <= leftover {
				if sel != nil {
					toCol.AppendWithSel(fromCol, sel, batchSize, t, uint64(p.group.Length()))
				} else {
					toCol.Append(fromCol, t, uint64(p.group.Length()), batchSize)
				}
			} else {
				bufferCol := p.buffer.ColVec(i)
				if sel != nil {
					toCol.AppendWithSel(fromCol, sel, leftover, t, uint64(p.group.Length()))
					bufferCol.CopyWithSelInt16(fromCol, sel[leftover:batchSize], batchSize-leftover, t)
				} else {
					toCol.Append(fromCol, t, uint64(p.group.Length()), leftover)
					bufferCol.Copy(fromCol, uint64(leftover), uint64(batchSize), t)
				}
			}
		}

		if batchSize <= leftover {
			p.group.SetLength(p.group.Length() + batchSize)
		} else {
			p.group.SetLength(ColBatchSize)
			p.buffer.SetLength(batchSize - leftover)
		}
	}

	return p.group
}
