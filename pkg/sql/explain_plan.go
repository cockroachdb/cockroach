// Copyright 2016 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

const (
	// explainSubqueryFmtFlags is the format for subqueries within `EXPLAIN SQL` statements.
	// Since these are individually run, we don't need to scrub any data from subqueries.
	explainSubqueryFmtFlags = tree.FmtSimple

	// sampledLogicalPlanFmtFlags is the format for sampled logical plans. Because these exposed
	// in the Admin UI, sampled plans should be scrubbed of sensitive information.
	sampledLogicalPlanFmtFlags = tree.FmtHideConstants
)

// explainPlanNode wraps the logic for EXPLAIN as a planNode.
type explainPlanNode struct {
	explainer explainer

	plan planComponents

	run explainPlanRun
}

// makeExplainPlanNodeWithPlan instantiates a planNode that EXPLAINs an
// underlying plan.
func (p *planner) makeExplainPlanNodeWithPlan(
	ctx context.Context, opts *tree.ExplainOptions, plan planComponents,
) (planNode, error) {
	flags := explain.MakeFlags(opts)

	columns := sqlbase.ExplainPlanColumns
	if flags.Verbose {
		columns = sqlbase.ExplainPlanVerboseColumns
	}
	// Make a copy (to allow changes through planMutableColumns).
	columns = append(sqlbase.ResultColumns(nil), columns...)

	node := &explainPlanNode{
		plan: plan,
		run: explainPlanRun{
			results: p.newContainerValuesNode(columns, 0),
		},
	}

	node.explainer.init(flags, !p.ExecCfg().Codec.ForSystemTenant())

	noPlaceholderFlags := tree.FmtExpr(
		tree.FmtSymbolicSubqueries, flags.ShowTypes, false /* symbolicVars */, flags.Verbose,
	)
	node.explainer.fmtFlags = noPlaceholderFlags
	node.explainer.showPlaceholderValues = func(ctx *tree.FmtCtx, placeholder *tree.Placeholder) {
		d, err := placeholder.Eval(p.EvalContext())
		if err != nil {
			// Disable the placeholder formatter so that
			// we don't recurse infinitely trying to evaluate.
			//
			// We also avoid calling ctx.FormatNode because when
			// types are visible, this would cause the type information
			// to be printed twice.
			ctx.WithPlaceholderFormat(nil, func() {
				placeholder.Format(ctx)
			})
			return
		}
		ctx.FormatNode(d)
	}
	return node, nil
}

// explainPlanRun is the run-time state of explainPlanNode during local execution.
type explainPlanRun struct {
	// results is the container for EXPLAIN's output.
	results *valuesNode
}

func (e *explainPlanNode) startExec(params runParams) error {
	return populateExplain(params, &e.explainer, e.run.results, &e.plan)
}

func (e *explainPlanNode) Next(params runParams) (bool, error) { return e.run.results.Next(params) }
func (e *explainPlanNode) Values() tree.Datums                 { return e.run.results.Values() }

func (e *explainPlanNode) Close(ctx context.Context) {
	e.plan.main.Close(ctx)
	for i := range e.plan.subqueryPlans {
		e.plan.subqueryPlans[i].plan.Close(ctx)
	}
	for i := range e.plan.checkPlans {
		e.plan.checkPlans[i].plan.Close(ctx)
	}
	e.run.results.Close(ctx)
}

// explainFlags represents the run-time state of the EXPLAIN logic.
type explainer struct {
	flags explain.Flags

	// fmtFlags is the formatter to use for pretty-printing expressions.
	// This can change during the execution of EXPLAIN.
	fmtFlags tree.FmtFlags

	// showPlaceholderValues is a formatting overload function
	// that will try to evaluate the placeholders if possible.
	// Meant for use with FmtCtx.WithPlaceholderFormat().
	showPlaceholderValues func(ctx *tree.FmtCtx, placeholder *tree.Placeholder)

	ob *explain.OutputBuilder

	// fieldsToSkipInSpans is how many fields to skip when pretty-printing spans.
	// Usually 2, but can be 4 when running EXPLAIN from a tenant since there will
	// be an extra tenant prefix and ID. For example:
	//  - /51/1/1 is a key read as a system tenant where the first two values are
	//    the table ID and the index ID.
	//  - /Tenant/10/51/1/1 is a key read as a non-system tenant where the first
	//    four values are the special tenant prefix byte and tenant ID, followed
	//    by the table ID and the index ID.
	fieldsToSkipInSpans int
}

// init initializes an explainer with the given explain.Flags. Additionally,
// isTenantPlan specifies whether the plan that will be explained is executed on
// behalf of a tenant. This lets the explainer know that the tenant prefix
// should be stripped when printing spans.
func (e *explainer) init(flags explain.Flags, isTenantPlan bool) {
	*e = explainer{
		flags: flags,
		// fieldsToSkipInSpans is 2 by default because the explainer will skip the
		// table and index ID.
		fieldsToSkipInSpans: 2,
	}
	if isTenantPlan {
		e.fieldsToSkipInSpans = 4
	}
	e.ob = explain.NewOutputBuilder(flags)
}

// populateExplain walks the plan and generates rows in a valuesNode.
// The subquery plans, if any are known to the planner, are printed
// at the bottom.
func populateExplain(params runParams, e *explainer, v *valuesNode, plan *planComponents) error {
	// Determine the "distributed" and "vectorized" values and emit them as
	// special rows.
	distribution, willVectorize := explainGetDistributedAndVectorized(params, plan)
	e.ob.AddField("distribution", distribution.String())
	e.ob.AddField("vectorized", fmt.Sprintf("%t", willVectorize))

	e.populate(params.ctx, plan, explainSubqueryFmtFlags)
	rows := e.ob.BuildExplainRows()
	for _, r := range rows {
		if _, err := v.rows.AddRow(params.ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func (e *explainer) populate(
	ctx context.Context, plan *planComponents, subqueryFmtFlags tree.FmtFlags,
) {
	observer := planObserver{
		enterNode: e.enterNode,
		expr:      e.expr,
		attr:      e.attr,
		spans:     e.spans,
		leaveNode: e.leaveNode,
	}
	// observePlan never returns an error when returnError is false.
	_ = observePlan(ctx, plan, observer, false /* returnError */, subqueryFmtFlags)
}

// observePlan walks the plan tree, executing the appropriate functions in the
// planObserver.
func observePlan(
	ctx context.Context,
	plan *planComponents,
	observer planObserver,
	returnError bool,
	subqueryFmtFlags tree.FmtFlags,
) error {
	if plan.main.isPhysicalPlan() {
		return errors.AssertionFailedf(
			"EXPLAIN of a query with opt-driven DistSQL planning is not supported",
		)
	}
	// If there are any subqueries, cascades, or checks in the plan, we
	// enclose everything as children of a virtual "root" node.
	if len(plan.subqueryPlans) > 0 || len(plan.cascades) > 0 || len(plan.checkPlans) > 0 {
		if _, err := observer.enterNode(ctx, "root", plan.main.planNode); err != nil && returnError {
			return err
		}
	}

	// Explain the main plan.
	if err := walkPlan(ctx, plan.main.planNode, observer); err != nil && returnError {
		return err
	}

	// Explain the subqueries.
	for i := range plan.subqueryPlans {
		s := &plan.subqueryPlans[i]
		if _, err := observer.enterNode(ctx, "subquery", nil /* plan */); err != nil && returnError {
			return err
		}
		observer.attr("subquery", "id", fmt.Sprintf("@S%d", i+1))
		// This field contains the original subquery (which could have been modified
		// by optimizer transformations).
		observer.attr(
			"subquery",
			"original sql",
			tree.AsStringWithFlags(s.subquery, subqueryFmtFlags),
		)
		observer.attr("subquery", "exec mode", rowexec.SubqueryExecModeNames[s.execMode])
		if s.plan.planNode != nil {
			if err := walkPlan(ctx, s.plan.planNode, observer); err != nil && returnError {
				return err
			}
		} else if s.started {
			observer.expr(observeAlways, "subquery", "result", -1, s.result)
		}
		if err := observer.leaveNode("subquery", nil /* plan */); err != nil && returnError {
			return err
		}
	}

	// Explain the cascades.
	for i := range plan.cascades {
		if _, err := observer.enterNode(ctx, "fk-cascade", nil); err != nil && returnError {
			return err
		}
		observer.attr("cascade", "fk", plan.cascades[i].FKName)
		observer.attr("cascade", "input", plan.cascades[i].Buffer.(*bufferNode).label)
		if err := observer.leaveNode("cascade", nil); err != nil && returnError {
			return err
		}
	}

	// Explain the checks.
	for i := range plan.checkPlans {
		if _, err := observer.enterNode(ctx, "fk-check", nil /* plan */); err != nil && returnError {
			return err
		}
		if plan.checkPlans[i].plan.planNode != nil {
			if err := walkPlan(ctx, plan.checkPlans[i].plan.planNode, observer); err != nil && returnError {
				return err
			}
		}
		if err := observer.leaveNode("fk-check", nil /* plan */); err != nil && returnError {
			return err
		}
	}

	if len(plan.subqueryPlans) > 0 || len(plan.cascades) > 0 || len(plan.checkPlans) > 0 {
		if err := observer.leaveNode("root", plan.main.planNode); err != nil && returnError {
			return err
		}
	}

	return nil
}

// planToString builds a string representation of a plan using the EXPLAIN
// infrastructure.
func planToString(ctx context.Context, p *planTop) string {
	var e explainer
	e.init(explain.Flags{Verbose: true, ShowTypes: true}, p.flags.IsSet(planFlagTenant))
	e.fmtFlags = tree.FmtExpr(tree.FmtSymbolicSubqueries, true, true, true)

	e.populate(ctx, &p.planComponents, explainSubqueryFmtFlags)
	return e.ob.BuildString()
}

func getAttrForSpansAll(hardLimitSet bool) string {
	if hardLimitSet {
		return "LIMITED SCAN"
	}
	return "FULL SCAN"
}

// spans implements the planObserver interface.
func (e *explainer) spans(
	nodeName, fieldName string,
	index *descpb.IndexDescriptor,
	spans []roachpb.Span,
	hardLimitSet bool,
) {
	spanss := sqlbase.PrettySpans(index, spans, e.fieldsToSkipInSpans)
	if spanss != "" {
		if spanss == "-" {
			spanss = getAttrForSpansAll(hardLimitSet)
		}
		e.attr(nodeName, fieldName, spanss)
	}
}

// expr implements the planObserver interface.
func (e *explainer) expr(v observeVerbosity, nodeName, fieldName string, n int, expr tree.Expr) {
	if expr != nil {
		if !e.flags.Verbose && v == observeMetadata {
			return
		}
		if nodeName == "join" {
			qualifySave := e.fmtFlags
			e.fmtFlags.SetFlags(tree.FmtShowTableAliases)
			defer func(e *explainer, f tree.FmtFlags) { e.fmtFlags = f }(e, qualifySave)
		}
		if n >= 0 {
			fieldName = fmt.Sprintf("%s %d", fieldName, n)
		}

		f := tree.NewFmtCtx(e.fmtFlags)
		f.SetPlaceholderFormat(e.showPlaceholderValues)
		f.FormatNode(expr)
		e.attr(nodeName, fieldName, f.CloseAndGetString())
	}
}

// enterNode implements the planObserver interface.
func (e *explainer) enterNode(_ context.Context, name string, plan planNode) (bool, error) {
	if plan == nil {
		e.ob.EnterMetaNode(name)
	} else {
		e.ob.EnterNode(name, planColumns(plan), planReqOrdering(plan))
	}
	return true, nil
}

// attr implements the planObserver interface.
func (e *explainer) attr(nodeName, fieldName, attr string) {
	e.ob.AddField(fieldName, attr)
}

// leaveNode implements the planObserver interface.
func (e *explainer) leaveNode(name string, _ planNode) error {
	e.ob.LeaveNode()
	return nil
}

// explainGetDistributedAndVectorized determines the "distributed" and
// "vectorized" properties for EXPLAIN.
func explainGetDistributedAndVectorized(
	params runParams, plan *planComponents,
) (distribution physicalplan.PlanDistribution, willVectorize bool) {
	// Determine the "distributed" and "vectorized" values, which we will emit as
	// special rows.
	distSQLPlanner := params.extendedEvalCtx.DistSQLPlanner
	distribution = getPlanDistributionForExplainPurposes(
		params.ctx, params.p, params.extendedEvalCtx.ExecCfg.NodeID,
		params.extendedEvalCtx.SessionData.DistSQLMode, plan.main,
	)
	willDistribute := distribution.WillDistribute()
	outerSubqueries := params.p.curPlan.subqueryPlans
	planCtx := newPlanningCtxForExplainPurposes(distSQLPlanner, params, plan.subqueryPlans, distribution)
	defer func() {
		planCtx.planner.curPlan.subqueryPlans = outerSubqueries
	}()
	physicalPlan, err := newPhysPlanForExplainPurposes(planCtx, distSQLPlanner, plan.main)
	if err == nil {
		// There might be an issue making the physical plan, but that should not
		// cause an error or panic, so swallow the error. See #40677 for example.
		distSQLPlanner.FinalizePlan(planCtx, physicalPlan)
		flows := physicalPlan.GenerateFlowSpecs()
		flowCtx := newFlowCtxForExplainPurposes(planCtx, params)
		flowCtx.Cfg.ClusterID = &distSQLPlanner.rpcCtx.ClusterID

		ctxSessionData := flowCtx.EvalCtx.SessionData
		vectorizedThresholdMet := physicalPlan.MaxEstimatedRowCount >= ctxSessionData.VectorizeRowCountThreshold
		if ctxSessionData.VectorizeMode == sessiondata.VectorizeOff {
			willVectorize = false
		} else if !vectorizedThresholdMet && (ctxSessionData.VectorizeMode == sessiondata.Vectorize201Auto || ctxSessionData.VectorizeMode == sessiondata.VectorizeOn) {
			willVectorize = false
		} else {
			willVectorize = true
			thisNodeID, _ := params.extendedEvalCtx.NodeID.OptionalNodeID()
			for scheduledOnNodeID, flow := range flows {
				scheduledOnRemoteNode := scheduledOnNodeID != thisNodeID
				if _, err := colflow.SupportsVectorized(
					params.ctx, flowCtx, flow.Processors, !willDistribute, nil /* output */, scheduledOnRemoteNode,
				); err != nil {
					willVectorize = false
					break
				}
			}
		}
	}
	return distribution, willVectorize
}
