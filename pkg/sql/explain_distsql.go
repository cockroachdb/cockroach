// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	opentracing "github.com/opentracing/opentracing-go"
)

// explainDistSQLNode is a planNode that wraps a plan and returns
// information related to running that plan under DistSQL.
type explainDistSQLNode struct {
	optColumnsSlot

	plan          planNode
	subqueryPlans []subquery

	stmtType tree.StatementType

	// If analyze is set, plan will be executed with tracing enabled and a url
	// pointing to a visual query plan with statistics will be in the row
	// returned by the node.
	analyze bool

	// optimizeSubqueries indicates whether to invoke optimizeSubquery and
	// setUnlimited on the subqueries.
	optimizeSubqueries bool

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
	// makePlanForExplainDistSQL returns the DistSQL physical plan that can be
	// used by the explainDistSQLNode to generate flow specs (and run in the case
	// of EXPLAIN ANALYZE).
	makePlanForExplainDistSQL(*PlanningCtx, *DistSQLPlanner) (PhysicalPlan, error)
}

func (n *explainDistSQLNode) startExec(params runParams) error {
	if n.analyze && params.SessionData().DistSQLMode == sessiondata.DistSQLOff {
		return pgerror.Newf(
			pgcode.ObjectNotInPrerequisiteState,
			"cannot run EXPLAIN ANALYZE while distsql is disabled",
		)
	}

	// Trigger limit propagation.
	params.p.prepareForDistSQLSupportCheck()

	distSQLPlanner := params.extendedEvalCtx.DistSQLPlanner

	var recommendation distRecommendation
	if _, ok := n.plan.(distSQLExplainable); ok {
		recommendation = shouldDistribute
	} else {
		recommendation, _ = distSQLPlanner.checkSupportForNode(n.plan)
	}

	planCtx := distSQLPlanner.NewPlanningCtx(params.ctx, params.extendedEvalCtx, params.p.txn)
	planCtx.isLocal = !shouldDistributeGivenRecAndMode(recommendation, params.SessionData().DistSQLMode)
	planCtx.ignoreClose = true
	planCtx.planner = params.p
	planCtx.stmtType = n.stmtType

	// In EXPLAIN ANALYZE mode, we need subqueries to be evaluated as normal.
	// In EXPLAIN mode, we don't evaluate subqueries, and instead display their
	// original text in the plan.
	planCtx.noEvalSubqueries = !n.analyze

	if n.analyze && len(n.subqueryPlans) > 0 {
		outerSubqueries := planCtx.planner.curPlan.subqueryPlans
		defer func() {
			planCtx.planner.curPlan.subqueryPlans = outerSubqueries
		}()
		planCtx.planner.curPlan.subqueryPlans = n.subqueryPlans

		if n.optimizeSubqueries {
			// The sub-plan's subqueries have been captured local to the
			// explainDistSQLNode node so that they would not be automatically
			// started for execution by planTop.start(). But this also means
			// they were not yet processed by makePlan()/optimizePlan(). Do it
			// here.
			for i := range n.subqueryPlans {
				if err := params.p.optimizeSubquery(params.ctx, &n.subqueryPlans[i]); err != nil {
					return err
				}

				// Trigger limit propagation. This would be done otherwise when
				// starting the plan. However we do not want to start the plan.
				params.p.setUnlimited(n.subqueryPlans[i].plan)
			}
		}

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
			execCfg.LeaseHolderCache,
			params.p.txn,
			func(ts hlc.Timestamp) {
				_ = execCfg.Clock.Update(ts)
			},
			params.extendedEvalCtx.Tracing,
		)
		if !distSQLPlanner.PlanAndRunSubqueries(
			planCtx.ctx,
			params.p,
			params.extendedEvalCtx.copy,
			n.subqueryPlans,
			recv,
			true,
		) {
			if err := rw.Err(); err != nil {
				return err
			}
			return recv.commErr
		}
	}

	var plan PhysicalPlan
	var err error
	if planNode, ok := n.plan.(distSQLExplainable); ok {
		plan, err = planNode.makePlanForExplainDistSQL(planCtx, distSQLPlanner)
	} else {
		plan, err = distSQLPlanner.createPlanForNode(planCtx, n.plan)
	}
	if err != nil {
		return err
	}

	distSQLPlanner.FinalizePlan(planCtx, &plan)

	flows := plan.GenerateFlowSpecs(params.extendedEvalCtx.NodeID)
	diagram, err := distsqlpb.GeneratePlanDiagram(flows)
	if err != nil {
		return err
	}

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
			execCfg.LeaseHolderCache,
			newParams.p.txn,
			func(ts hlc.Timestamp) {
				_ = execCfg.Clock.Update(ts)
			},
			newParams.extendedEvalCtx.Tracing,
		)
		defer recv.Release()
		distSQLPlanner.Run(
			planCtx, newParams.p.txn, &plan, recv, newParams.extendedEvalCtx, nil /* finishedSetupFn */)

		n.run.executedStatement = true

		sp.Finish()
		spans := tracing.GetRecording(sp)

		if err := rw.Err(); err != nil {
			return err
		}
		diagram.AddSpans(spans)
	}

	planJSON, planURL, err := diagram.ToURL()
	if err != nil {
		return err
	}

	n.run.values = tree.Datums{
		tree.MakeDBool(tree.DBool(recommendation == shouldDistribute)),
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
	n.plan.Close(ctx)
	for i := range n.subqueryPlans {
		// Once a subquery plan has been evaluated, it already closes its
		// plan.
		if n.subqueryPlans[i].plan != nil {
			n.subqueryPlans[i].plan.Close(ctx)
			n.subqueryPlans[i].plan = nil
		}
	}
}
