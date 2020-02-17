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
)

// simpleProjectOp is an operator that implements "simple projection" - removal of
// columns that aren't needed by later operators.
type simpleProjectOp struct {
	OneInputNode
	NonExplainable

	batch *projectingBatch
}

var _ Operator = &simpleProjectOp{}

// projectingBatch is a Batch that applies a simple projection to another,
// underlying batch, discarding all columns but the ones in its projection
// slice, in order.
type projectingBatch struct {
	coldata.Batch

	projection []uint32
	// colVecs is a lazily populated slice of coldata.Vecs to support returning
	// these in ColVecs().
	colVecs []coldata.Vec
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
	if b.Batch == coldata.ZeroBatch {
		return nil
	}
	if b.colVecs == nil {
		b.colVecs = make([]coldata.Vec, len(b.projection))
	}
	for i := range b.colVecs {
		b.colVecs[i] = b.Batch.ColVec(int(b.projection[i]))
	}
	return b.colVecs
}

func (b *projectingBatch) Width() int {
	return len(b.projection)
}

func (b *projectingBatch) AppendCol(col coldata.Vec) {
	b.Batch.AppendCol(col)
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
		OneInputNode: NewOneInputNode(input),
		batch:        newProjectionBatch(projection),
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
