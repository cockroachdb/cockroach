// Copyright 2018 The Cockroach Authors.
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

// coalescerOp consumes the input operator and coalesces the resulting batches
// to return full batches of coldata.BatchSize().
type coalescerOp struct {
	OneInputNode
	NonExplainable

	inputTypes []coltypes.T

	group  coldata.Batch
	buffer coldata.Batch
}

var _ StaticMemoryOperator = &coalescerOp{}

// NewCoalescerOp creates a new coalescer operator on the given input operator
// with the given column types.
func NewCoalescerOp(input Operator, colTypes []coltypes.T) Operator {
	return &coalescerOp{
		OneInputNode: NewOneInputNode(input),
		inputTypes:   colTypes,
	}
}

func (p *coalescerOp) EstimateStaticMemoryUsage() int {
	return 2 * EstimateBatchSizeBytes(p.inputTypes, int(coldata.BatchSize()))
}

func (p *coalescerOp) Init() {
	p.input.Init()
	p.group = coldata.NewMemBatch(p.inputTypes)
	p.buffer = coldata.NewMemBatch(p.inputTypes)
}

func (p *coalescerOp) Next(ctx context.Context) coldata.Batch {
	p.group.ResetInternalBatch()
	p.buffer.ResetInternalBatch()
	tempBatch := p.group
	p.group = p.buffer

	p.buffer = tempBatch
	p.buffer.SetLength(0)

	for p.group.Length() < coldata.BatchSize() {
		leftover := coldata.BatchSize() - p.group.Length()
		batch := p.input.Next(ctx)
		batchSize := batch.Length()

		if batchSize == 0 {
			break
		}

		sel := batch.Selection()

		for i, t := range p.inputTypes {
			toCol := p.group.ColVec(i)
			fromCol := batch.ColVec(i)

			if batchSize <= leftover {
				toCol.Append(
					coldata.SliceArgs{
						ColType:   t,
						Src:       fromCol,
						Sel:       sel,
						DestIdx:   uint64(p.group.Length()),
						SrcEndIdx: uint64(batchSize),
					},
				)
			} else {
				bufferCol := p.buffer.ColVec(i)
				toCol.Append(
					coldata.SliceArgs{
						ColType:   t,
						Src:       fromCol,
						Sel:       sel,
						DestIdx:   uint64(p.group.Length()),
						SrcEndIdx: uint64(leftover),
					},
				)
				bufferCol.Copy(
					coldata.CopySliceArgs{
						SliceArgs: coldata.SliceArgs{
							ColType:     t,
							Src:         fromCol,
							Sel:         sel,
							SrcStartIdx: uint64(leftover),
							SrcEndIdx:   uint64(batchSize),
						},
					},
				)
			}
		}

		if batchSize <= leftover {
			p.group.SetLength(p.group.Length() + batchSize)
		} else {
			p.group.SetLength(coldata.BatchSize())
			p.buffer.SetLength(batchSize - leftover)
		}
	}

	return p.group
}
