// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

// PlanAndRunCTAS plans and runs the CREATE TABLE AS command.
func PlanAndRunCTAS(
	ctx context.Context,
	dsp *DistSQLPlanner,
	planner *planner,
	txn *kv.Txn,
	isLocal bool,
	in planNode,
	out execinfrapb.ProcessorCoreUnion,
	recv *DistSQLReceiver,
) {
	planCtx := dsp.NewPlanningCtx(ctx, planner.ExtendedEvalContext(), txn)
	planCtx.isLocal = isLocal
	planCtx.planner = planner
	planCtx.stmtType = tree.Rows

	p, err := dsp.createPlanForNode(planCtx, in)
	if err != nil {
		recv.SetError(errors.Wrapf(err, "constructing distSQL plan"))
		return
	}

	p.AddNoGroupingStage(
		out, execinfrapb.PostProcessSpec{}, rowexec.CTASPlanResultTypes, execinfrapb.Ordering{},
	)

	// The bulk row writers will emit a binary encoded BulkOpSummary.
	p.PlanToStreamColMap = []int{0}
	p.ResultTypes = rowexec.CTASPlanResultTypes

	// Make copy of evalCtx as Run might modify it.
	evalCtxCopy := planner.ExtendedEvalContextCopy()
	dsp.FinalizePlan(planCtx, &p)
	dsp.Run(planCtx, txn, &p, recv, evalCtxCopy, nil /* finishedSetupFn */)()
}
