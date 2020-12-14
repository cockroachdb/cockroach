// Copyright 2017 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// explainDistSQLNode is a planNode that wraps a plan and returns
// information related to running that plan under DistSQL.
type explainDistSQLNode struct {
	optColumnsSlot

	options *tree.ExplainOptions
	plan    planComponents

	stmtType tree.StatementType

	run explainDistSQLRun
}

// explainDistSQLRun contains the run-time state of explainDistSQLNode during local execution.
type explainDistSQLRun struct {
	// The single row returned by the node.
	values tree.Datums

	// done is set if Next() was called.
	done bool
}

func (n *explainDistSQLNode) startExec(params runParams) error {
	distSQLPlanner := params.extendedEvalCtx.DistSQLPlanner
	distribution := getPlanDistribution(
		params.ctx, params.p, params.extendedEvalCtx.ExecCfg.NodeID,
		params.extendedEvalCtx.SessionData.DistSQLMode, n.plan.main,
	)
	willDistribute := distribution.WillDistribute()
	planCtx := distSQLPlanner.NewPlanningCtx(params.ctx, params.extendedEvalCtx, params.p, params.p.txn, willDistribute)
	planCtx.ignoreClose = true
	planCtx.stmtType = n.stmtType
	planCtx.noEvalSubqueries = true

	physPlan, err := newPhysPlanForExplainPurposes(planCtx, distSQLPlanner, n.plan.main)
	if err != nil {
		if len(n.plan.subqueryPlans) > 0 {
			return errors.New("running EXPLAIN (DISTSQL) on this query is " +
				"unsupported because of the presence of subqueries")
		}
		return err
	}
	distSQLPlanner.FinalizePlan(planCtx, physPlan)

	flows := physPlan.GenerateFlowSpecs()
	flags := execinfrapb.DiagramFlags{
		ShowInputTypes: n.options.Flags[tree.ExplainFlagTypes],
	}
	diagram, err := execinfrapb.GeneratePlanDiagram(params.p.stmt.String(), flows, flags)
	if err != nil {
		return err
	}

	planJSON, planURL, err := diagram.ToURL()
	if err != nil {
		return err
	}

	n.run.values = tree.Datums{
		tree.MakeDBool(tree.DBool(willDistribute)),
		tree.NewDString(planURL.String()),
		tree.NewDString(planJSON),
	}
	return nil
}

func (n *explainDistSQLNode) Next(runParams) (bool, error) {
	if n.run.done {
		return false, nil
	}
	n.run.done = true
	return true, nil
}

func (n *explainDistSQLNode) Values() tree.Datums { return n.run.values }
func (n *explainDistSQLNode) Close(ctx context.Context) {
	n.plan.close(ctx)
}

func newPhysPlanForExplainPurposes(
	planCtx *PlanningCtx, distSQLPlanner *DistSQLPlanner, plan planMaybePhysical,
) (*PhysicalPlan, error) {
	if plan.isPhysicalPlan() {
		return plan.physPlan.PhysicalPlan, nil
	}
	return distSQLPlanner.createPhysPlanForPlanNode(planCtx, plan.planNode)
}
