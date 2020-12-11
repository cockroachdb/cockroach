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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// explainDistSQLNode is a planNode that wraps a plan and returns
// information related to running that plan under DistSQL.
type explainDistSQLNode struct {
	optColumnsSlot

	options *tree.ExplainOptions
	plan    planComponents

	stmtType tree.StatementType

	// If analyze is set, plan will be executed with tracing enabled and a url
	// pointing to a visual query plan with statistics will be in the row
	// returned by the node.
	analyze bool

	run explainDistSQLRun
}

// explainDistSQLRun contains the run-time state of explainDistSQLNode during local execution.
type explainDistSQLRun struct {
	// The single row returned by the node.
	values tree.Datums

	// done is set if Next() was called.
	done bool

	// executedStatement is set if EXPLAIN ANALYZE was active and finished
	// executing the query, regardless of query success or failure.
	executedStatement bool
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

	if n.analyze && len(n.plan.cascades) > 0 {
		return errors.New("running EXPLAIN ANALYZE (DISTSQL) on this query is " +
			"unsupported because of the presence of cascades")
	}

	// In EXPLAIN ANALYZE mode, we need subqueries to be evaluated as normal.
	// In EXPLAIN mode, we don't evaluate subqueries, and instead display their
	// original text in the plan.
	planCtx.noEvalSubqueries = !n.analyze

	if n.analyze && len(n.plan.subqueryPlans) > 0 {
		outerSubqueries := planCtx.planner.curPlan.subqueryPlans
		defer func() {
			planCtx.planner.curPlan.subqueryPlans = outerSubqueries
		}()
		planCtx.planner.curPlan.subqueryPlans = n.plan.subqueryPlans

		// Discard rows that are returned.
		rw := newCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
			return nil
		})
		execCfg := params.p.ExecCfg()
		recv := MakeDistSQLReceiver(
			planCtx.ctx,
			rw,
			tree.Rows,
			execCfg.RangeDescriptorCache,
			params.p.txn,
			execCfg.Clock,
			params.extendedEvalCtx.Tracing,
		)
		if !distSQLPlanner.PlanAndRunSubqueries(
			planCtx.ctx,
			params.p,
			params.extendedEvalCtx.copy,
			n.plan.subqueryPlans,
			recv,
		) {
			if err := rw.Err(); err != nil {
				return err
			}
			return recv.commErr
		}
	}

	physPlan, err := newPhysPlanForExplainPurposes(planCtx, distSQLPlanner, n.plan.main)
	if err != nil {
		if len(n.plan.subqueryPlans) > 0 {
			return errors.New("running EXPLAIN (DISTSQL) on this query is " +
				"unsupported because of the presence of subqueries")
		}
		return err
	}
	distSQLPlanner.FinalizePlan(planCtx, physPlan)

	var diagram execinfrapb.FlowDiagram
	if n.analyze {
		// TODO(andrei): We don't create a child span if the parent is already
		// recording because we don't currently have a good way to ask for a
		// separate recording for the child such that it's also guaranteed that we
		// don't get a noopSpan.
		var sp *tracing.Span
		if parentSp := tracing.SpanFromContext(params.ctx); parentSp != nil &&
			!parentSp.IsRecording() {
			tracer := parentSp.Tracer()
			sp = tracer.StartSpan(
				"explain-distsql", tracing.WithForceRealSpan(),
				tracing.WithParentAndAutoCollection(parentSp),
				tracing.WithCtxLogTags(params.ctx))
		} else {
			tracer := params.extendedEvalCtx.ExecCfg.AmbientCtx.Tracer
			sp = tracer.StartSpan(
				"explain-distsql", tracing.WithForceRealSpan(),
				tracing.WithCtxLogTags(params.ctx))
		}
		sp.StartRecording(tracing.SnowballRecording)
		ctx := tracing.ContextWithSpan(params.ctx, sp)
		planCtx.ctx = ctx
		// Make a copy of the evalContext with the recording span in it; we can't
		// change the original.
		newEvalCtx := params.extendedEvalCtx.copy()
		newEvalCtx.Context = ctx
		newParams := params
		newParams.extendedEvalCtx = newEvalCtx

		// Discard rows that are returned.
		rw := newCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
			return nil
		})
		execCfg := newParams.p.ExecCfg()
		const stmtType = tree.Rows
		recv := MakeDistSQLReceiver(
			planCtx.ctx,
			rw,
			stmtType,
			execCfg.RangeDescriptorCache,
			newParams.p.txn,
			execCfg.Clock,
			newParams.extendedEvalCtx.Tracing,
		)
		defer recv.Release()

		planCtx.saveFlows = func(flows map[roachpb.NodeID]*execinfrapb.FlowSpec) error {
			d, err := planCtx.flowSpecsToDiagram(ctx, flows)
			if err != nil {
				return err
			}
			diagram = d
			return nil
		}
		planCtx.saveDiagramFlags = execinfrapb.DiagramFlags{
			ShowInputTypes:    n.options.Flags[tree.ExplainFlagTypes],
			MakeDeterministic: execCfg.TestingKnobs.DeterministicExplainAnalyze,
		}

		distSQLPlanner.Run(
			planCtx, newParams.p.txn, physPlan, recv, newParams.extendedEvalCtx, nil, /* finishedSetupFn */
		)()

		n.run.executedStatement = true

		sp.Finish()
		spans := sp.GetRecording()

		if err := rw.Err(); err != nil {
			return err
		}
		diagram.AddSpans(spans)
	} else {
		flows := physPlan.GenerateFlowSpecs()
		flags := execinfrapb.DiagramFlags{
			ShowInputTypes: n.options.Flags[tree.ExplainFlagTypes],
		}
		diagram, err = execinfrapb.GeneratePlanDiagram(params.p.stmt.String(), flows, flags)
		if err != nil {
			return err
		}
	}

	if n.analyze && len(n.plan.checkPlans) > 0 {
		outerChecks := planCtx.planner.curPlan.checkPlans
		defer func() {
			planCtx.planner.curPlan.checkPlans = outerChecks
		}()
		planCtx.planner.curPlan.checkPlans = n.plan.checkPlans

		// Discard rows that are returned.
		rw := newCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
			return nil
		})
		execCfg := params.p.ExecCfg()
		recv := MakeDistSQLReceiver(
			planCtx.ctx,
			rw,
			tree.Rows,
			execCfg.RangeDescriptorCache,
			params.p.txn,
			execCfg.Clock,
			params.extendedEvalCtx.Tracing,
		)
		if !distSQLPlanner.PlanAndRunCascadesAndChecks(
			planCtx.ctx,
			params.p,
			params.extendedEvalCtx.copy,
			&n.plan,
			recv,
		) {
			if err := rw.Err(); err != nil {
				return err
			}
			return recv.commErr
		}
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
