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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
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

// distSQLExplainable is an interface used for local plan nodes that create
// distributed jobs. The plan node should implement this interface so that
// EXPLAIN (DISTSQL) will show the DistSQL plan instead of the local plan node.
type distSQLExplainable interface {
	// newPlanForExplainDistSQL returns the DistSQL physical plan that can be
	// used by the explainDistSQLNode to generate flow specs (and run in the case
	// of EXPLAIN ANALYZE).
	newPlanForExplainDistSQL(*PlanningCtx, *DistSQLPlanner) (*PhysicalPlan, error)
}

// getPlanDistributionForExplainPurposes returns the PlanDistribution that plan
// will have. It is similar to getPlanDistribution but also pays attention to
// whether the logical plan will be handled as a distributed job. It should
// *only* be used in EXPLAIN variants.
func getPlanDistributionForExplainPurposes(
	ctx context.Context,
	nodeID *base.SQLIDContainer,
	distSQLMode sessiondata.DistSQLExecMode,
	plan planMaybePhysical,
) physicalplan.PlanDistribution {
	if plan.isPhysicalPlan() {
		return plan.physPlan.Distribution
	}
	switch p := plan.planNode.(type) {
	case *explainDistSQLNode:
		if p.plan.main.isPhysicalPlan() {
			return p.plan.main.physPlan.Distribution
		}
	case *explainVecNode:
		if p.plan.isPhysicalPlan() {
			return p.plan.physPlan.Distribution
		}
	case *explainPlanNode:
		if p.plan.main.isPhysicalPlan() {
			return p.plan.main.physPlan.Distribution
		}
	}
	if _, ok := plan.planNode.(distSQLExplainable); ok {
		// This is a special case for plans that will be actually distributed
		// but are represented using local plan nodes (for example, "create
		// statistics" is handled by the jobs framework which is responsible
		// for setting up the correct DistSQL infrastructure).
		return physicalplan.FullyDistributedPlan
	}
	return getPlanDistribution(ctx, nodeID, distSQLMode, plan)
}

func (n *explainDistSQLNode) startExec(params runParams) error {
	distSQLPlanner := params.extendedEvalCtx.DistSQLPlanner
	distribution := getPlanDistributionForExplainPurposes(
		params.ctx, params.extendedEvalCtx.ExecCfg.NodeID,
		params.extendedEvalCtx.SessionData.DistSQLMode, n.plan.main,
	)
	willDistribute := distribution.WillDistribute()
	planCtx := distSQLPlanner.NewPlanningCtx(params.ctx, params.extendedEvalCtx, params.p.txn, willDistribute)
	planCtx.ignoreClose = true
	planCtx.planner = params.p
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
			func(ts hlc.Timestamp) {
				execCfg.Clock.Update(ts)
			},
			params.extendedEvalCtx.Tracing,
		)
		if !distSQLPlanner.PlanAndRunSubqueries(
			planCtx.ctx,
			params.p,
			params.extendedEvalCtx.copy,
			n.plan.subqueryPlans,
			recv,
			willDistribute,
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
		var sp opentracing.Span
		if parentSp := opentracing.SpanFromContext(params.ctx); parentSp != nil &&
			!tracing.IsRecording(parentSp) {
			tracer := parentSp.Tracer()
			sp = tracer.StartSpan(
				"explain-distsql", tracing.Recordable,
				opentracing.ChildOf(parentSp.Context()),
				tracing.LogTagsFromCtx(params.ctx))
		} else {
			tracer := params.extendedEvalCtx.ExecCfg.AmbientCtx.Tracer
			sp = tracer.StartSpan(
				"explain-distsql", tracing.Recordable,
				tracing.LogTagsFromCtx(params.ctx))
		}
		tracing.StartRecording(sp, tracing.SnowballRecording)
		ctx := opentracing.ContextWithSpan(params.ctx, sp)
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
			func(ts hlc.Timestamp) {
				execCfg.Clock.Update(ts)
			},
			newParams.extendedEvalCtx.Tracing,
		)
		defer recv.Release()

		planCtx.saveDiagram = func(d execinfrapb.FlowDiagram) {
			diagram = d
		}
		planCtx.saveDiagramShowInputTypes = n.options.Flags[tree.ExplainFlagTypes]

		distSQLPlanner.Run(
			planCtx, newParams.p.txn, physPlan, recv, newParams.extendedEvalCtx, nil, /* finishedSetupFn */
		)()

		n.run.executedStatement = true

		sp.Finish()
		spans := tracing.GetRecording(sp)

		if err := rw.Err(); err != nil {
			return err
		}
		diagram.AddSpans(spans)
	} else {
		flows := physPlan.GenerateFlowSpecs()
		showInputTypes := n.options.Flags[tree.ExplainFlagTypes]
		diagram, err = execinfrapb.GeneratePlanDiagram(params.p.stmt.String(), flows, showInputTypes)
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
			func(ts hlc.Timestamp) {
				execCfg.Clock.Update(ts)
			},
			params.extendedEvalCtx.Tracing,
		)
		if !distSQLPlanner.PlanAndRunCascadesAndChecks(
			planCtx.ctx,
			params.p,
			params.extendedEvalCtx.copy,
			&n.plan,
			recv,
			willDistribute,
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
	var physPlan *PhysicalPlan
	var err error
	if planNode, ok := plan.planNode.(distSQLExplainable); ok {
		physPlan, err = planNode.newPlanForExplainDistSQL(planCtx, distSQLPlanner)
	} else {
		physPlan, err = distSQLPlanner.createPhysPlanForPlanNode(planCtx, plan.planNode)
	}
	if err != nil {
		return nil, err
	}
	return physPlan, nil
}
