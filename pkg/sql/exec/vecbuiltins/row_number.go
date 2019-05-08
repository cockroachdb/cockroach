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

package vecbuiltins

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// TODO(yuzefovich): add randomized tests.
type rowNumberOp struct {
	input           exec.Operator
	outputColIdx    int
	partitionColIdx int

	rowNumber int64
}

var _ exec.Operator = &rowNumberOp{}

// NewRowNumberOperator creates a new exec.Operator that computes window
// function ROW_NUMBER. outputColIdx specifies in which exec.Vec the operator
// should put its output (if there is no such column, a new column is
// appended).
func NewRowNumberOperator(
	input exec.Operator, outputColIdx int, partitionColIdx int,
) exec.Operator {
	return &rowNumberOp{input: input, outputColIdx: outputColIdx, partitionColIdx: partitionColIdx}
}

func (r *rowNumberOp) Init() {
	r.input.Init()
	// ROW_NUMBER starts counting from 1.
	r.rowNumber = 1
}

func (r *rowNumberOp) Next(ctx context.Context) coldata.Batch {
	b := r.input.Next(ctx)
	if b.Length() == 0 {
		return b
	}
	// TODO(yuzefovich): template partition out.
	if r.partitionColIdx != -1 {
		if r.partitionColIdx == b.Width() {
			b.AppendCol(types.Bool)
		} else if r.partitionColIdx > b.Width() {
			panic("unexpected: column partitionColIdx is neither present nor the next to be appended")
		}
		if r.outputColIdx == b.Width() {
			b.AppendCol(types.Int64)
		} else if r.outputColIdx > b.Width() {
			panic("unexpected: column outputColIdx is neither present nor the next to be appended")
		}
		partitionCol := b.ColVec(r.partitionColIdx).Bool()
		rowNumberCol := b.ColVec(r.outputColIdx).Int64()
		sel := b.Selection()
		if sel != nil {
			for i := uint16(0); i < b.Length(); i++ {
				if partitionCol[sel[i]] {
					r.rowNumber = 1
				}
				rowNumberCol[sel[i]] = r.rowNumber
				r.rowNumber++
			}
		} else {
			for i := uint16(0); i < b.Length(); i++ {
				if partitionCol[i] {
					r.rowNumber = 1
				}
				rowNumberCol[i] = r.rowNumber
				r.rowNumber++
			}
		}
	} else {
		if r.outputColIdx == b.Width() {
			b.AppendCol(types.Int64)
		} else if r.outputColIdx > b.Width() {
			panic("unexpected: column outputColIdx is neither present nor the next to be appended")
		}
		rowNumberCol := b.ColVec(r.outputColIdx).Int64()
		sel := b.Selection()
		if sel != nil {
			for i := uint16(0); i < b.Length(); i++ {
				rowNumberCol[sel[i]] = r.rowNumber
				r.rowNumber++
			}
		} else {
			for i := uint16(0); i < b.Length(); i++ {
				rowNumberCol[i] = r.rowNumber
				r.rowNumber++
			}
		}
	}
	return b
}
