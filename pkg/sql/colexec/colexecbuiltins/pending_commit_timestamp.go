// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecbuiltins

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
)

// pendingCommitTimestampOp implements the vectorized projection of
// pending_commit_timestamp(). The function call has no inputs and a fixed
// TIMESTAMPTZ result type, but the value it returns
// (tree.DPendingCommitTimestampDatum) isn't a *DTimestampTZ -- so the
// generic defaultBuiltinFuncOperator can't fan it out into a TIMESTAMPTZ
// vec slot via colconv.GetDatumToPhysicalFn.
//
// Instead we set every output slot to NULL and flip the per-Vec
// AllPendingCommitTimestamp flag, which the materializer's vec→datum
// conversion (colconv.VecToDatumConverter) reads to substitute the marker
// datum back in. The marker then flows to the row writer (row.Updater /
// row.Inserter) which special-cases it through valueside.Encode.
//
// CASE-style merging works because Vec.Copy / Vec.CopyWithReorderedSource
// propagate the flag (see the OR-semantics comment there). The optbuilder
// only permits pending_commit_timestamp() in assignment positions, so the
// flag isn't expected to feed into casts, comparisons, or anything else
// that interprets the underlying vec data. If that ever changes, the flag
// needs to be downgraded from "all rows in this vec" to a per-row mask.
type pendingCommitTimestampOp struct {
	colexecop.OneInputHelper
	outputIdx int
}

var _ colexecop.Operator = (*pendingCommitTimestampOp)(nil)

func newPendingCommitTimestampOp(
	outputIdx int, input colexecop.Operator,
) colexecop.Operator {
	return &pendingCommitTimestampOp{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		outputIdx:      outputIdx,
	}
}

func (p *pendingCommitTimestampOp) Next() (coldata.Batch, *execinfrapb.ProducerMetadata) {
	batch, meta := p.Input.Next()
	if meta != nil {
		return nil, meta
	}
	if batch.Length() == 0 {
		return coldata.ZeroBatch, nil
	}
	outVec := batch.ColVec(p.outputIdx)
	outVec.Nulls().SetNulls()
	outVec.SetAllPendingCommitTimestamp()
	return batch, nil
}
