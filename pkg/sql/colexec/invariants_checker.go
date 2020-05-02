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
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// invariantsChecker is a helper Operator that will check that invariants that
// are present in the vectorized engine are maintained on all batches. It
// should be planned between other Operators in tests.
type invariantsChecker struct {
	OneInputNode
}

var _ execinfra.Operator = invariantsChecker{}

// NewInvariantsChecker creates a new invariantsChecker.
func NewInvariantsChecker(input execinfra.Operator) execinfra.Operator {
	return &invariantsChecker{
		OneInputNode: OneInputNode{input: input},
	}
}

func (i invariantsChecker) Init() {
	i.input.Init()
}

func (i invariantsChecker) Next(ctx context.Context) coldata.Batch {
	b := i.input.Next(ctx)
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
	return b
}
