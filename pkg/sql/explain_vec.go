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
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/colflow"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// explainVecNode is a planNode that wraps a plan and returns
// information related to running that plan with the vectorized engine.
type explainVecNode struct {
	optColumnsSlot

	options *tree.ExplainOptions
	plan    planComponents

	run struct {
		lines []string
		// The current row returned by the node.
		values tree.Datums
	}
}

func (n *explainVecNode) startExec(params runParams) error {
	n.run.values = make(tree.Datums, 1)
	distSQLPlanner := params.extendedEvalCtx.DistSQLPlanner
	distribution := getPlanDistribution(
		params.ctx, params.p, params.extendedEvalCtx.ExecCfg.NodeInfo.NodeID,
		params.extendedEvalCtx.SessionData().DistSQLMode, n.plan.main,
	)
	outerSubqueries := params.p.curPlan.subqueryPlans
	planCtx := newPlanningCtxForExplainPurposes(distSQLPlanner, params, n.plan.subqueryPlans, distribution)
	defer func() {
		planCtx.planner.curPlan.subqueryPlans = outerSubqueries
	}()
	physPlan, cleanup, err := newPhysPlanForExplainPurposes(params.ctx, planCtx, distSQLPlanner, n.plan.main)
	defer cleanup()
	if err != nil {
		if len(n.plan.subqueryPlans) > 0 {
			return errors.New("running EXPLAIN (VEC) on this query is " +
				"unsupported because of the presence of subqueries")
		}
		return err
	}

	distSQLPlanner.finalizePlanWithRowCount(params.ctx, planCtx, physPlan, n.plan.mainRowCount)
	flows := physPlan.GenerateFlowSpecs()
	flowCtx, cleanup := newFlowCtxForExplainPurposes(params.ctx, planCtx, params.p)
	defer cleanup()

	// We want to get the vectorized plan which would be executed with the
	// current 'vectorize' option. If 'vectorize' is set to 'off', then the
	// vectorized engine is disabled, and we will return an error in such case.
	// With all other options, we don't change the setting to the
	// most-inclusive option as we used to because the plan can be different
	// based on 'vectorize' setting.
	if flowCtx.EvalCtx.SessionData().VectorizeMode == sessiondatapb.VectorizeOff {
		return errors.New("vectorize is set to 'off'")
	}
	verbose := n.options.Flags[tree.ExplainFlagVerbose]
	willDistribute := physPlan.Distribution.WillDistribute()
	n.run.lines, err = colflow.ExplainVec(
		params.ctx, flowCtx, flows, physPlan.LocalProcessors, nil, /* opChains */
		distSQLPlanner.gatewaySQLInstanceID, verbose, willDistribute,
	)
	if err != nil {
		return err
	}
	return nil
}

func newFlowCtxForExplainPurposes(
	ctx context.Context, planCtx *PlanningCtx, p *planner,
) (_ *execinfra.FlowCtx, cleanup func()) {
	monitor := mon.NewMonitor(
		"explain", /* name */
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		p.execCfg.Settings,
	)
	monitor.StartNoReserved(ctx, p.Mon())
	cleanup = func() {
		monitor.Stop(ctx)
	}
	return &execinfra.FlowCtx{
		NodeID:  planCtx.EvalContext().NodeID,
		EvalCtx: planCtx.EvalContext(),
		Mon:     monitor,
		Cfg: &execinfra.ServerConfig{
			Settings:         p.execCfg.Settings,
			LogicalClusterID: p.DistSQLPlanner().distSQLSrv.ServerConfig.LogicalClusterID,
			VecFDSemaphore:   p.execCfg.DistSQLSrv.VecFDSemaphore,
			PodNodeDialer:    p.DistSQLPlanner().podNodeDialer,
		},
		Descriptors: p.Descriptors(),
		DiskMonitor: &mon.BytesMonitor{},
	}, cleanup
}

func newPlanningCtxForExplainPurposes(
	distSQLPlanner *DistSQLPlanner,
	params runParams,
	subqueryPlans []subquery,
	distribution physicalplan.PlanDistribution,
) *PlanningCtx {
	distribute := DistributionType(DistributionTypeNone)
	if distribution.WillDistribute() {
		distribute = DistributionTypeAlways
	}
	planCtx := distSQLPlanner.NewPlanningCtx(params.ctx, params.extendedEvalCtx,
		params.p, params.p.txn, distribute)
	planCtx.planner.curPlan.subqueryPlans = subqueryPlans
	for i := range planCtx.planner.curPlan.subqueryPlans {
		p := &planCtx.planner.curPlan.subqueryPlans[i]
		// Fake subquery results - they're not important for our explain output.
		p.started = true
		p.result = tree.DNull
	}
	return planCtx
}

func (n *explainVecNode) Next(runParams) (bool, error) {
	if len(n.run.lines) == 0 {
		return false, nil
	}
	n.run.values[0] = tree.NewDString(n.run.lines[0])
	n.run.lines = n.run.lines[1:]
	return true, nil
}

func (n *explainVecNode) Values() tree.Datums { return n.run.values }
func (n *explainVecNode) Close(ctx context.Context) {
	n.plan.close(ctx)
}
