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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
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
	distSQLPlanner := params.extendedEvalCtx.DistSQLPlanner
	shouldPlanDistribute, recommendation := willDistributePlan(distSQLPlanner, n.plan, params)
	planCtx := distSQLPlanner.NewPlanningCtx(params.ctx, params.extendedEvalCtx, params.p.txn)
	planCtx.isLocal = !shouldPlanDistribute
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

	plan, err := makePhysicalPlan(planCtx, distSQLPlanner, n.plan)
	if err != nil {
		if len(n.subqueryPlans) > 0 {
			return errors.New("running EXPLAIN (DISTSQL) on this query is " +
				"unsupported because of the presence of subqueries")
		}
		return err
	}

	distSQLPlanner.FinalizePlan(planCtx, &plan)
	flows := plan.GenerateFlowSpecs(params.extendedEvalCtx.NodeID)
	diagram, err := execinfrapb.GeneratePlanDiagram(params.p.stmt.String(), flows)
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
			planCtx, newParams.p.txn, &plan, recv, newParams.extendedEvalCtx, nil, /* finishedSetupFn */
		)()

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

// willDistributePlan checks if the given plan will run with distributed
// execution. It takes into account whether a distSQL plan can be made at all
// and the session setting for distSQL.
func willDistributePlan(
	distSQLPlanner *DistSQLPlanner, plan planNode, params runParams,
) (bool, distRecommendation) {
	// Trigger limit propagation.
	params.p.prepareForDistSQLSupportCheck()
	var recommendation distRecommendation
	if _, ok := plan.(distSQLExplainable); ok {
		recommendation = shouldDistribute
	} else {
		recommendation, _ = distSQLPlanner.checkSupportForNode(plan)
	}
	shouldDistribute := shouldDistributeGivenRecAndMode(recommendation, params.SessionData().DistSQLMode)
	return shouldDistribute, recommendation
}

func makePhysicalPlan(
	planCtx *PlanningCtx, distSQLPlanner *DistSQLPlanner, plan planNode,
) (PhysicalPlan, error) {
	var physPlan PhysicalPlan
	var err error
	if planNode, ok := plan.(distSQLExplainable); ok {
		physPlan, err = planNode.makePlanForExplainDistSQL(planCtx, distSQLPlanner)
	} else {
		physPlan, err = distSQLPlanner.createPlanForNode(planCtx, plan)
	}
	if err != nil {
		return PhysicalPlan{}, err
	}
	return physPlan, nil
}
