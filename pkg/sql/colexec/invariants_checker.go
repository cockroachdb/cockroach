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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
)

// invariantsChecker is a helper Operator that will check that invariants that
// are present in the vectorized engine are maintained on all batches. It
// should be planned between other Operators in tests.
type invariantsChecker struct {
	OneInputNode

	expectedBatchWidth int
}

var _ Operator = invariantsChecker{}

// NewInvariantsChecker creates a new invariantsChecker.
func NewInvariantsChecker(input Operator, expectedBatchWidth int) Operator {
	return &invariantsChecker{
		OneInputNode:       OneInputNode{input: input},
		expectedBatchWidth: expectedBatchWidth,
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
	if i.expectedBatchWidth != b.Width() {
		panic(
			fmt.Sprintf("unexpected batch width: expected %d, got %d",
				i.expectedBatchWidth, b.Width(),
			))
	}
	for colIdx := 0; colIdx < b.Width(); colIdx++ {
		v := b.ColVec(colIdx)
		if v.Type() == coltypes.Bytes {
			v.Bytes().AssertOffsetsAreNonDecreasing(n)
		}
	}
	return b
}
