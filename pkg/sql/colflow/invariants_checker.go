// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colflow

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// invariantsChecker is a helper Operator that will check that invariants that
// are present in the vectorized engine are maintained on all batches. It
// should be planned between other Operators in tests.
type invariantsChecker struct {
	colexecop.OneInputHelper
}

var _ colexecop.Operator = &invariantsChecker{}

// newInvariantsChecker creates a new invariantsChecker.
func newInvariantsChecker(input colexecop.Operator) colexecop.Operator {
	return &invariantsChecker{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
	}
}

func (i *invariantsChecker) Init(ctx context.Context) {
	if !i.InitHelper.Init(ctx) {
		colexecerror.InternalError(errors.AssertionFailedf("Init is called for the second time"))
	}
	i.Input.Init(ctx)
}

func (i *invariantsChecker) Next() coldata.Batch {
	if i.Ctx == nil {
		colexecerror.InternalError(errors.AssertionFailedf("Ctx is nil, was Init() called?"))
	}
	b := i.Input.Next()
	n := b.Length()
	if n == 0 {
		return b
	}
	for colIdx := 0; colIdx < b.Width(); colIdx++ {
		v := b.ColVec(colIdx)
		if v.CanonicalTypeFamily() == types.BytesFamily {
			v.Bytes().AssertOffsetsAreNonDecreasing(n)
		}
	}
	if sel := b.Selection(); sel != nil {
		for i := 1; i < n; i++ {
			if sel[i] <= sel[i-1] {
				colexecerror.InternalError(errors.AssertionFailedf(
					"unexpectedly selection vector is not an increasing sequence "+
						"at position %d: %v", i, sel[:n],
				))
			}
		}
	}
	return b
}
