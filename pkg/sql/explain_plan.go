// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/indexrec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/errors"
)

// explainPlanNode implements EXPLAIN (PLAN) and EXPLAIN (DISTSQL); it produces
// the output of EXPLAIN given an explain.Plan.
type explainPlanNode struct {
	zeroInputPlanNode
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

	var rows []string
	if e.options.Mode == tree.ExplainGist {
		rows = []string{e.plan.Gist.String()}
	} else {
		// Determine the "distribution" and "vectorized" values, which we will emit as
		// special rows.

		// Note that we delay adding the annotation about the distribution until
		// after the plan is finalized (when the physical plan is successfully
		// created).
		distribution, _ := getPlanDistribution(
			params.ctx, params.p.Descriptors().HasUncommittedTypes(),
			params.extendedEvalCtx.SessionData(), plan.main, &params.p.distSQLVisitor,
		)

		outerSubqueries := params.p.curPlan.subqueryPlans
		distSQLPlanner := params.extendedEvalCtx.DistSQLPlanner
		planCtx := newPlanningCtxForExplainPurposes(distSQLPlanner, params, plan.subqueryPlans, distribution)
		defer func() {
			planCtx.planner.curPlan.subqueryPlans = outerSubqueries
		}()
		physicalPlan, cleanup, err := newPhysPlanForExplainPurposes(params.ctx, planCtx, distSQLPlanner, plan.main)
		defer cleanup()
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
			ob.AddDistribution(distribution.String())
			// For regular EXPLAIN, simply skip emitting the "vectorized" information.
		} else {
			// There might be an issue making the physical plan, but that should not
			// cause an error or panic, so swallow the error. See #40677 for example.
			finalizePlanWithRowCount(params.ctx, planCtx, physicalPlan, plan.mainRowCount)
			ob.AddDistribution(physicalPlan.Distribution.String())
			flows, flowsCleanup := physicalPlan.GenerateFlowSpecs()
			defer flowsCleanup(flows)

			ctxSessionData := planCtx.EvalContext().SessionData()
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
					ShowInputTypes:    e.options.Flags[tree.ExplainFlagTypes],
					MakeDeterministic: e.flags.Deflake.HasAny(explain.DeflakeAll) || params.p.execCfg.TestingKnobs.DeterministicExplain,
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

		if e.options.Flags[tree.ExplainFlagJSON] {
			// For the JSON flag, we only want to emit the diagram JSON.
			rows = []string{diagramJSON}
		} else {
			// We want to fully expand all post-queries in vanilla EXPLAIN. This
			// should be safe because:
			// 1. we created all separate exec.Factory objects (in
			// execFactory.ConstructExplain and execbuilder.Builder.buildExplain),
			// so there is no concern about factories being reset after the
			// "main" optimizer plan was created.
			// 2. the txn in which the EXPLAIN statement runs is still open
			// since we're in the middle of the execution of the
			// explainPlanNode.
			const createPostQueryPlanIfMissing = true
			if err := emitExplain(params.ctx, ob, params.EvalContext(), params.p.ExecCfg().Codec, e.plan, createPostQueryPlanIfMissing); err != nil {
				return err
			}
			rows = ob.BuildStringRows()
			if e.options.Mode == tree.ExplainDistSQL {
				rows = append(rows, "", fmt.Sprintf("Diagram: %s", diagramURL.String()))
			}
		}
	}
	// Add index recommendations to output, if they exist.
	if recs := params.p.instrumentation.explainIndexRecs; recs != nil {
		// First add empty row.
		rows = append(rows, "")
		rows = append(rows, fmt.Sprintf("index recommendations: %d", len(recs)))
		for i := range recs {
			plural := ""
			recType := ""
			switch recs[i].RecType {
			case indexrec.TypeCreateIndex:
				recType = "index creation"
			case indexrec.TypeReplaceIndex:
				recType = "index replacement"
				plural = "s"
			case indexrec.TypeAlterIndex:
				recType = "index alteration"
			default:
				return errors.New("unexpected index recommendation type")
			}
			rows = append(rows, fmt.Sprintf("%d. type: %s", i+1, recType))
			rows = append(rows, fmt.Sprintf("   SQL command%s: %s", plural, recs[i].SQL))
		}
	}
	v := params.p.newContainerValuesNode(colinfo.ExplainPlanColumns, len(rows))
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
	ctx context.Context,
	ob *explain.OutputBuilder,
	evalCtx *eval.Context,
	codec keys.SQLCodec,
	explainPlan *explain.Plan,
	createPostQueryPlanIfMissing bool,
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
			if ok, e := errorutil.ShouldCatch(r); ok && !buildutil.CrdbTestBuild {
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
		spans, err := generateScanSpans(ctx, evalCtx, codec, tabDesc, idx, scanParams)
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

	return explain.Emit(ctx, evalCtx, explainPlan, ob, spanFormatFn, createPostQueryPlanIfMissing)
}

func (e *explainPlanNode) Next(params runParams) (bool, error) { return e.run.results.Next(params) }
func (e *explainPlanNode) Values() tree.Datums                 { return e.run.results.Values() }

// closeExplainNode closes the given node which can either be planNode or
// planMaybePhysical.
func closeExplainNode(ctx context.Context, n exec.Node) {
	switch n := n.(type) {
	case planNode:
		n.Close(ctx)
	case planMaybePhysical:
		n.Close(ctx)
	default:
		panic(errors.AssertionFailedf("unknown plan node type %T", n))
	}
}

// closeExplainPlan closes the provided explain plan.
func closeExplainPlan(ctx context.Context, ep *explain.Plan) {
	closeExplainNode(ctx, ep.Root.WrappedNode())
	for i := range ep.Subqueries {
		closeExplainNode(ctx, ep.Subqueries[i].Root.(*explain.Node).WrappedNode())
	}
	for _, cascade := range ep.Cascades {
		// We don't want to create new plans if they haven't been cached - all
		// necessary plans must have been created already in explain.Emit call.
		const createPlanIfMissing = false
		if cp, _ := cascade.GetExplainPlan(ctx, createPlanIfMissing); cp != nil {
			closeExplainPlan(ctx, cp.(*explain.Plan))
		}
	}
	for i := range ep.Checks {
		closeExplainNode(ctx, ep.Checks[i].WrappedNode())
	}
	for _, trigger := range ep.Triggers {
		// We don't want to create new plans if they haven't been cached - all
		// necessary plans must have been created already in explain.Emit call.
		const createPlanIfMissing = false
		if tp, _ := trigger.GetExplainPlan(ctx, createPlanIfMissing); tp != nil {
			closeExplainPlan(ctx, tp.(*explain.Plan))
		}
	}
}

func (e *explainPlanNode) Close(ctx context.Context) {
	closeExplainPlan(ctx, e.plan)
	if e.run.results != nil {
		e.run.results.Close(ctx)
	}
}

func newPhysPlanForExplainPurposes(
	ctx context.Context, planCtx *PlanningCtx, distSQLPlanner *DistSQLPlanner, plan planMaybePhysical,
) (_ *PhysicalPlan, cleanup func(), _ error) {
	if plan.isPhysicalPlan() {
		return plan.physPlan.PhysicalPlan, func() {}, nil
	}
	physPlan, err := distSQLPlanner.createPhysPlanForPlanNode(ctx, planCtx, plan.planNode)
	return physPlan, planCtx.getCleanupFunc(), err
}
