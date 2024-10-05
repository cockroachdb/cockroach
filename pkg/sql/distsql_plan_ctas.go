// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	distribute := DistributionType(LocalDistribution)
	if !isLocal {
		distribute = FullDistribution
	}
	planCtx := dsp.NewPlanningCtx(ctx, planner.ExtendedEvalContext(), planner,
		txn, distribute)
	planCtx.stmtType = tree.Rows

	physPlan, cleanup, err := dsp.createPhysPlan(ctx, planCtx, in)
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
	FinalizePlan(ctx, planCtx, physPlan)
	finishedSetupFn, cleanup := getFinishedSetupFn(planner)
	defer cleanup()
	dsp.Run(ctx, planCtx, txn, physPlan, recv, evalCtxCopy, finishedSetupFn)
}
