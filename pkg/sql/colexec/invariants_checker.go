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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// InvariantsChecker is a helper Operator that will check that invariants that
// are present in the vectorized engine are maintained on all batches. It
// should be planned between other Operators in tests.
type InvariantsChecker struct {
	colexecop.OneInputNode
	colexecop.NonExplainable

	initStatus     colexecop.OperatorInitStatus
	metadataSource execinfrapb.MetadataSource
}

var _ colexecop.Operator = &InvariantsChecker{}
var _ execinfrapb.MetadataSource

// NewInvariantsChecker creates a new InvariantsChecker.
func NewInvariantsChecker(input colexecop.Operator) *InvariantsChecker {
	c := &InvariantsChecker{
		OneInputNode: colexecop.OneInputNode{Input: input},
	}
	if ms, ok := input.(execinfrapb.MetadataSource); ok {
		c.metadataSource = ms
	}
	return c
}

// Init implements the colexecop.Operator interface.
func (i *InvariantsChecker) Init() {
	i.initStatus = colexecop.OperatorInitialized
	i.Input.Init()
}

// Next implements the colexecop.Operator interface.
func (i *InvariantsChecker) Next(ctx context.Context) coldata.Batch {
	b := i.Input.Next(ctx)
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

// DrainMeta implements the execinfrapb.MetadataSource interface.
func (i *InvariantsChecker) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	if i.initStatus != colexecop.OperatorInitialized {
		if c, ok := i.Input.(*Columnarizer); ok {
			if c.removedFromFlow {
				// This is a special case in which we allow for the operator to
				// not be initialized.
				return nil
			}
		}
		colexecerror.InternalError(errors.AssertionFailedf("DrainMeta called before Init, input is %T", i.Input))
	}
	if i.metadataSource == nil {
		return nil
	}
	return i.metadataSource.DrainMeta(ctx)
}
