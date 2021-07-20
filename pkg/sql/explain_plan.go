// Copyright 2020 The Cockroach Authors.
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
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/errors"
)

// explainPlanNode implements EXPLAIN (PLAN) and EXPLAIN (DISTSQL); it produces
// the output of EXPLAIN given an explain.Plan.
type explainPlanNode struct {
	optColumnsSlot

	options *tree.ExplainOptions

	flags explain.Flags
	plan  *explain.Plan
	run   explainPlanNodeRun
}

type explainPlanNodeRun struct {
	results *valuesNode
}

func (e *explainPlanNode) startExec(params runParams) error {
	ob := explain.NewOutputBuilder(e.flags)
	plan := e.plan.WrappedPlan.(*planComponents)

	// Determine the "distribution" and "vectorized" values, which we will emit as
	// special rows.

	distribution := getPlanDistribution(
		params.ctx, params.p, params.extendedEvalCtx.ExecCfg.NodeID,
		params.extendedEvalCtx.SessionData.DistSQLMode, plan.main,
	)
	ob.AddDistribution(distribution.String())

	outerSubqueries := params.p.curPlan.subqueryPlans
	distSQLPlanner := params.extendedEvalCtx.DistSQLPlanner
	planCtx := newPlanningCtxForExplainPurposes(distSQLPlanner, params, plan.subqueryPlans, distribution)
	defer func() {
		planCtx.planner.curPlan.subqueryPlans = outerSubqueries
	}()
	physicalPlan, err := newPhysPlanForExplainPurposes(planCtx, distSQLPlanner, plan.main)
	var diagramURL url.URL
	var diagramJSON string
	if err != nil {
		if e.options.Mode == tree.ExplainDistSQL {
			if len(plan.subqueryPlans) > 0 {
				return errors.New("running EXPLAIN (DISTSQL) on this query is " +
					"unsupported because of the presence of subqueries")
			}
			return err
		}
		// For regular EXPLAIN, simply skip emitting the "vectorized" information.
	} else {
		// There might be an issue making the physical plan, but that should not
		// cause an error or panic, so swallow the error. See #40677 for example.
		distSQLPlanner.FinalizePlan(planCtx, physicalPlan)
		flows := physicalPlan.GenerateFlowSpecs()
		flowCtx := newFlowCtxForExplainPurposes(planCtx, params.p, &distSQLPlanner.rpcCtx.ClusterID)

		ctxSessionData := flowCtx.EvalCtx.SessionData
		var willVectorize bool
		if ctxSessionData.VectorizeMode == sessiondatapb.VectorizeOff {
			willVectorize = false
		} else {
			willVectorize = true
			for _, flow := range flows {
				if err := colflow.IsSupported(ctxSessionData.VectorizeMode, flow); err != nil {
					willVectorize = false
					break
				}
			}
		}
		ob.AddVectorized(willVectorize)

		if e.options.Mode == tree.ExplainDistSQL {
			flags := execinfrapb.DiagramFlags{
				ShowInputTypes: e.options.Flags[tree.ExplainFlagTypes],
			}
			diagram, err := execinfrapb.GeneratePlanDiagram(params.p.stmt.String(), flows, flags)
			if err != nil {
				return err
			}

			diagramJSON, diagramURL, err = diagram.ToURL()
			if err != nil {
				return err
			}
		}
	}

	var rows []string
	if e.options.Flags[tree.ExplainFlagJSON] {
		// For the JSON flag, we only want to emit the diagram JSON.
		rows = []string{diagramJSON}
	} else {
		if err := emitExplain(ob, params.EvalContext(), params.p.ExecCfg().Codec, e.plan); err != nil {
			return err
		}
		rows = ob.BuildStringRows()
		if e.options.Mode == tree.ExplainDistSQL {
			rows = append(rows, "", fmt.Sprintf("Diagram: %s", diagramURL.String()))
		}
	}
	v := params.p.newContainerValuesNode(colinfo.ExplainPlanColumns, 0)
	datums := make([]tree.DString, len(rows))
	for i, row := range rows {
		datums[i] = tree.DString(row)
		if _, err := v.rows.AddRow(params.ctx, tree.Datums{&datums[i]}); err != nil {
			return err
		}
	}
	e.run.results = v

	return nil
}

func emitExplain(
	ob *explain.OutputBuilder,
	evalCtx *tree.EvalContext,
	codec keys.SQLCodec,
	explainPlan *explain.Plan,
) (err error) {
	// Guard against bugs in the explain code.
	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate internal and runtime errors without
			// having to add error checks everywhere throughout the code. This is only
			// possible because the code does not update shared state and does not
			// manipulate locks.
			// Note that we don't catch anything in debug builds, so that failures are
			// more visible.
			if ok, e := errorutil.ShouldCatch(r); ok && !util.CrdbTestBuild {
				err = e
			} else {
				// Other panic objects can't be considered "safe" and thus are
				// propagated as crashes that terminate the session.
				panic(r)
			}
		}
	}()

	if explainPlan == nil {
		return errors.AssertionFailedf("no plan")
	}

	spanFormatFn := func(table cat.Table, index cat.Index, scanParams exec.ScanParams) string {
		if table.IsVirtualTable() {
			return "<virtual table spans>"
		}
		tabDesc := table.(*optTable).desc
		idx := index.(*optIndex).idx
		spans, err := generateScanSpans(evalCtx, codec, tabDesc, idx, scanParams)
		if err != nil {
			return err.Error()
		}
		// skip is how many fields to skip when pretty-printing spans.
		// Usually 2, but can be 4 when running EXPLAIN from a tenant since there
		// will be an extra tenant prefix and ID. For example:
		//  - /51/1/1 is a key read as a system tenant where the first two values
		//    are the table ID and the index ID.
		//  - /Tenant/10/51/1/1 is a key read as a non-system tenant where the first
		//    four values are the special tenant prefix byte and tenant ID, followed
		//    by the table ID and the index ID.
		skip := 2
		if !codec.ForSystemTenant() {
			skip = 4
		}
		return catalogkeys.PrettySpans(idx, spans, skip)
	}

	return explain.Emit(explainPlan, ob, spanFormatFn)
}

func (e *explainPlanNode) Next(params runParams) (bool, error) { return e.run.results.Next(params) }
func (e *explainPlanNode) Values() tree.Datums                 { return e.run.results.Values() }

func (e *explainPlanNode) Close(ctx context.Context) {
	closeNode := func(n exec.Node) {
		switch n := n.(type) {
		case planNode:
			n.Close(ctx)
		case planMaybePhysical:
			n.Close(ctx)
		default:
			panic(errors.AssertionFailedf("unknown plan node type %T", n))
		}
	}
	// The wrapped node can be planNode or planMaybePhysical.
	closeNode(e.plan.Root.WrappedNode())
	for i := range e.plan.Subqueries {
		closeNode(e.plan.Subqueries[i].Root.(*explain.Node).WrappedNode())
	}
	for i := range e.plan.Checks {
		closeNode(e.plan.Checks[i].WrappedNode())
	}
	if e.run.results != nil {
		e.run.results.Close(ctx)
	}
}

func newPhysPlanForExplainPurposes(
	planCtx *PlanningCtx, distSQLPlanner *DistSQLPlanner, plan planMaybePhysical,
) (*PhysicalPlan, error) {
	if plan.isPhysicalPlan() {
		return plan.physPlan.PhysicalPlan, nil
	}
	return distSQLPlanner.createPhysPlanForPlanNode(planCtx, plan.planNode)
}
