// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
)

// fnOp is an operator that executes an arbitrary function for its side-effects,
// once per input batch, passing the input batch unmodified along.
type fnOp struct {
	colexecop.OneInputHelper
	colexecop.NonExplainable

	fn func()
}

var _ colexecop.ResettableOperator = &fnOp{}

func (f *fnOp) Next() coldata.Batch {
	batch := f.Input.Next()
	f.fn()
	return batch
}

func (f *fnOp) Reset(ctx context.Context) {
	if resettableOp, ok := f.Input.(colexecop.Resetter); ok {
		resettableOp.Reset(ctx)
	}
}
