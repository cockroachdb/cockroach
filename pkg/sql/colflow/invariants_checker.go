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
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// invariantsChecker is a helper Operator that will check that invariants that
// are present in the vectorized engine are maintained on all batches. It
// should be planned between other Operators in tests.
type invariantsChecker struct {
	colexecop.OneInputNode

	initStatus     colexecop.OperatorInitStatus
	metadataSource execinfrapb.MetadataSource
}

var _ colexecop.Operator = &invariantsChecker{}
var _ execinfrapb.MetadataSource

// newInvariantsChecker creates a new invariantsChecker.
func newInvariantsChecker(input colexecop.Operator) colexecop.Operator {
	c := &invariantsChecker{
		OneInputNode: colexecop.OneInputNode{Input: input},
	}
	if ms, ok := input.(execinfrapb.MetadataSource); ok {
		c.metadataSource = ms
	}
	return c
}

func (i *invariantsChecker) Init() {
	i.initStatus = colexecop.OperatorInitialized
	i.Input.Init()
}

func (i *invariantsChecker) Next(ctx context.Context) coldata.Batch {
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

func (i *invariantsChecker) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	if i.initStatus != colexecop.OperatorInitialized {
		colexecerror.InternalError(errors.AssertionFailedf("DrainMeta called before Init"))
	}
	if i.metadataSource == nil {
		return nil
	}
	return i.metadataSource.DrainMeta(ctx)
}
