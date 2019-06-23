// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package vecbuiltins

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
)

// TODO(yuzefovich): add randomized tests.
// TODO(yuzefovich): add benchmarks.
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
	batch := r.input.Next(ctx)
	if batch.Length() == 0 {
		return batch
	}
	if r.partitionColIdx != -1 {
		r.nextBodyWithPartition(batch)
	} else {
		r.nextBodyNoPartition(batch)
	}
	return batch
}
