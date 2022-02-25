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
	"github.com/cockroachdb/errors"
)

// PlanAndRunCTAS plans and runs the CREATE TABLE AS command.
func PlanAndRunCTAS(
	ctx context.Context,
	dsp *DistSQLPlanner,
	planner *planner,
	txn *kv.Txn,
	isLocal bool,
	in planMaybePhysical,
	out execinfrapb.ProcessorCoreUnion,
	recv *DistSQLReceiver,
) {
	distribute := DistributionType(DistributionTypeNone)
	if !isLocal {
		distribute = DistributionTypeSystemTenantOnly
	}
	planCtx := dsp.NewPlanningCtx(ctx, planner.ExtendedEvalContext(), planner,
		txn, distribute)
	planCtx.stmtType = tree.Rows

	physPlan, cleanup, err := dsp.createPhysPlan(planCtx, in)
	defer cleanup()
	if err != nil {
		recv.SetError(errors.Wrapf(err, "constructing distSQL plan"))
		return
	}
	physPlan.AddNoGroupingStage(
		out, execinfrapb.PostProcessSpec{}, rowexec.CTASPlanResultTypes, execinfrapb.Ordering{},
	)

	// The bulk row writers will emit a binary encoded BulkOpSummary.
	physPlan.PlanToStreamColMap = []int{0}

	// Make copy of evalCtx as Run might modify it.
	evalCtxCopy := planner.ExtendedEvalContextCopy()
	dsp.FinalizePlan(planCtx, physPlan)
	dsp.Run(planCtx, txn, physPlan, recv, evalCtxCopy, nil /* finishedSetupFn */)()
}
