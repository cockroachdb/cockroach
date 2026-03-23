// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
)

// fnOp is an operator that executes an arbitrary function for its side-effects,
// once per input batch, passing the input batch unmodified along.
type fnOp struct {
	colexecop.OneInputHelper
	colexecop.NonExplainable

	fn func()
}

var _ colexecop.ResettableOperator = &fnOp{}

func (f *fnOp) Next() (coldata.Batch, *execinfrapb.ProducerMetadata) {
	batch, meta := f.Input.Next()
	if meta != nil {
		return nil, meta
	}
	f.fn()
	return batch, nil
}

func (f *fnOp) Reset(ctx context.Context) {
	if resettableOp, ok := f.Input.(colexecop.Resetter); ok {
		resettableOp.Reset(ctx)
	}
}
