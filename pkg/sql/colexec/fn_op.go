// Copyright 2019 The Cockroach Authors.
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

// fnOp is an operator that executes an arbitrary function for its side-effects,
// once per input batch, passing the input batch unmodified along.
type fnOp struct {
	OneInputNode
	NonExplainable

	fn func()
}

var _ ResettableOperator = fnOp{}

func (f fnOp) Init() {
	f.input.Init()
}

func (f fnOp) Next(ctx context.Context) coldata.Batch {
	batch := f.input.Next(ctx)
	f.fn()
	return batch
}

func (f fnOp) reset(ctx context.Context) {
	if resettableOp, ok := f.input.(resetter); ok {
		resettableOp.reset(ctx)
	}
}
