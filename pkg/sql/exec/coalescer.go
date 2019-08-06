// Copyright 2018 The Cockroach Authors.
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

// coalescerOp consumes the input operator and coalesces the resulting batches
// to return full batches of col.BatchSize.
type coalescerOp struct {
	OneInputNode

	inputTypes []types.T

	group  coldata.Batch
	buffer coldata.Batch
}

var _ StaticMemoryOperator = &coalescerOp{}

// NewCoalescerOp creates a new coalescer operator on the given input operator
// with the given column types.
func NewCoalescerOp(input Operator, colTypes []types.T) Operator {
	return &coalescerOp{
		OneInputNode: NewOneInputNode(input),
		inputTypes:   colTypes,
	}
}

func (p *coalescerOp) EstimateStaticMemoryUsage() int {
	return 2 * EstimateBatchSizeBytes(p.inputTypes, coldata.BatchSize)
}

func (p *coalescerOp) Init() {
	p.input.Init()
	p.group = coldata.NewMemBatch(p.inputTypes)
	p.buffer = coldata.NewMemBatch(p.inputTypes)
}

func (p *coalescerOp) Next(ctx context.Context) coldata.Batch {
	tempBatch := p.group
	p.group = p.buffer

	p.buffer = tempBatch
	p.buffer.SetLength(0)

	for p.group.Length() < coldata.BatchSize {
		leftover := coldata.BatchSize - p.group.Length()
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
					coldata.AppendArgs{
						ColType:   t,
						Src:       fromCol,
						Sel:       sel,
						DestIdx:   uint64(p.group.Length()),
						SrcEndIdx: batchSize,
					},
				)
			} else {
				bufferCol := p.buffer.ColVec(i)
				toCol.Append(
					coldata.AppendArgs{
						ColType:   t,
						Src:       fromCol,
						Sel:       sel,
						DestIdx:   uint64(p.group.Length()),
						SrcEndIdx: leftover,
					},
				)
				bufferCol.Copy(
					coldata.CopyArgs{
						ColType:     t,
						Src:         fromCol,
						Sel:         sel,
						SrcStartIdx: uint64(leftover),
						SrcEndIdx:   uint64(batchSize),
					},
				)
			}
		}

		if batchSize <= leftover {
			p.group.SetLength(p.group.Length() + batchSize)
		} else {
			p.group.SetLength(coldata.BatchSize)
			p.buffer.SetLength(batchSize - leftover)
		}
	}

	return p.group
}
