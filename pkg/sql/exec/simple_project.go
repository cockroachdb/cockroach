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

// simpleProjectOp is an operator that implements "simple projection" - removal of
// columns that aren't needed by later operators.
type simpleProjectOp struct {
	input Operator

	batch *projectingBatch
}

var _ Operator = &simpleProjectOp{}

// projectingBatch is a Batch that applies a simple projection to another,
// underlying batch, discarding all columns but the ones in its projection
// slice, in order.
type projectingBatch struct {
	coldata.Batch

	projection []uint32
}

func newProjectionBatch(projection []uint32) *projectingBatch {
	return &projectingBatch{
		projection: projection,
	}
}

func (b *projectingBatch) ColVec(i int) coldata.Vec {
	return b.Batch.ColVec(int(b.projection[i]))
}

func (b *projectingBatch) ColVecs() []coldata.Vec {
	panic("projectingBatch doesn't support ColVecs()")
}

func (b *projectingBatch) Width() int {
	return len(b.projection)
}

func (b *projectingBatch) AppendCol(t types.T) {
	b.Batch.AppendCol(t)
	b.projection = append(b.projection, uint32(b.Batch.Width())-1)
}

// NewSimpleProjectOp returns a new simpleProjectOp that applies a simple
// projection on the columns in its input batch, returning a new batch with
// only the columns in the projection slice, in order. In a degenerate case
// when input already outputs batches that satisfy the projection, a
// simpleProjectOp is not planned and input is returned.
func NewSimpleProjectOp(input Operator, numInputCols int, projection []uint32) Operator {
	if numInputCols == len(projection) {
		projectionIsRedundant := true
		for i := range projection {
			if projection[i] != uint32(i) {
				projectionIsRedundant = false
			}
		}
		if projectionIsRedundant {
			return input
		}
	}
	return &simpleProjectOp{
		input: input,
		batch: newProjectionBatch(projection),
	}
}

func (d *simpleProjectOp) Init() {
	d.input.Init()
}

func (d *simpleProjectOp) Next(ctx context.Context) coldata.Batch {
	batch := d.input.Next(ctx)
	d.batch.Batch = batch

	return d.batch
}
