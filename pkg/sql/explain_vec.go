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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
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
		params.ctx, params.p, params.extendedEvalCtx.ExecCfg.NodeID,
		params.extendedEvalCtx.SessionData.DistSQLMode, n.plan.main,
	)
	willDistribute := distribution.WillDistribute()
	outerSubqueries := params.p.curPlan.subqueryPlans
	planCtx := newPlanningCtxForExplainPurposes(distSQLPlanner, params, n.plan.subqueryPlans, distribution)
	defer func() {
		planCtx.planner.curPlan.subqueryPlans = outerSubqueries
	}()
	physPlan, err := newPhysPlanForExplainPurposes(planCtx, distSQLPlanner, n.plan.main)
	if err != nil {
		if len(n.plan.subqueryPlans) > 0 {
			return errors.New("running EXPLAIN (VEC) on this query is " +
				"unsupported because of the presence of subqueries")
		}
		return err
	}

	distSQLPlanner.FinalizePlan(planCtx, physPlan)
	flows := physPlan.GenerateFlowSpecs()
	flowCtx := newFlowCtxForExplainPurposes(planCtx, params.p, &distSQLPlanner.rpcCtx.ClusterID)

	// We want to get the vectorized plan which would be executed with the
	// current 'vectorize' option. If 'vectorize' is set to 'off', then the
	// vectorized engine is disabled, and we will return an error in such case.
	// With all other options, we don't change the setting to the
	// most-inclusive option as we used to because the plan can be different
	// based on 'vectorize' setting.
	if flowCtx.EvalCtx.SessionData.VectorizeMode == sessiondatapb.VectorizeOff {
		return errors.New("vectorize is set to 'off'")
	}
	verbose := n.options.Flags[tree.ExplainFlagVerbose]
	n.run.lines, err = colflow.ExplainVec(
		params.ctx, flowCtx, flows, physPlan.LocalProcessors, nil, /* opChains */
		distSQLPlanner.gatewayNodeID, verbose, willDistribute,
	)
	if err != nil {
		return err
	}
	return nil
}

func newFlowCtxForExplainPurposes(
	planCtx *PlanningCtx, p *planner, clusterID *base.ClusterIDContainer,
) *execinfra.FlowCtx {
	return &execinfra.FlowCtx{
		NodeID:  planCtx.EvalContext().NodeID,
		EvalCtx: planCtx.EvalContext(),
		Cfg: &execinfra.ServerConfig{
			Settings:       p.execCfg.Settings,
			ClusterID:      clusterID,
			VecFDSemaphore: p.execCfg.DistSQLSrv.VecFDSemaphore,
		},
		TypeResolverFactory: &descs.DistSQLTypeResolverFactory{
			Descriptors: p.Descriptors(),
		},
		DiskMonitor: &mon.BytesMonitor{},
	}
}

func newPlanningCtxForExplainPurposes(
	distSQLPlanner *DistSQLPlanner,
	params runParams,
	subqueryPlans []subquery,
	distribution physicalplan.PlanDistribution,
) *PlanningCtx {
	planCtx := distSQLPlanner.NewPlanningCtx(params.ctx, params.extendedEvalCtx, params.p, params.p.txn, distribution.WillDistribute())
	planCtx.ignoreClose = true
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
