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
	"reflect"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
	"github.com/cockroachdb/errors"
)

// explainVecNode is a planNode that wraps a plan and returns
// information related to running that plan with the vectorized engine.
type explainVecNode struct {
	optColumnsSlot

	options *tree.ExplainOptions
	plan    planMaybePhysical

	stmtType tree.StatementType

	run struct {
		lines []string
		// The current row returned by the node.
		values tree.Datums
	}
	subqueryPlans []subquery
}

type flowWithNode struct {
	nodeID roachpb.NodeID
	flow   *execinfrapb.FlowSpec
}

func (n *explainVecNode) startExec(params runParams) error {
	n.run.values = make(tree.Datums, 1)
	distSQLPlanner := params.extendedEvalCtx.DistSQLPlanner
	distribution := getPlanDistributionForExplainPurposes(
		params.ctx, params.extendedEvalCtx.ExecCfg.NodeID,
		params.extendedEvalCtx.SessionData.DistSQLMode, n.plan,
	)
	willDistribute := distribution.WillDistribute()
	outerSubqueries := params.p.curPlan.subqueryPlans
	planCtx := newPlanningCtxForExplainPurposes(distSQLPlanner, params, n.stmtType, n.subqueryPlans, distribution)
	defer func() {
		planCtx.planner.curPlan.subqueryPlans = outerSubqueries
	}()
	physPlan, err := newPhysPlanForExplainPurposes(planCtx, distSQLPlanner, n.plan)
	if err != nil {
		if len(n.subqueryPlans) > 0 {
			return errors.New("running EXPLAIN (VEC) on this query is " +
				"unsupported because of the presence of subqueries")
		}
		return err
	}

	distSQLPlanner.FinalizePlan(planCtx, physPlan)
	flows := physPlan.GenerateFlowSpecs()
	flowCtx := newFlowCtxForExplainPurposes(planCtx, params)
	flowCtx.Cfg.ClusterID = &distSQLPlanner.rpcCtx.ClusterID

	// We want to get the vectorized plan which would be executed with the
	// current 'vectorize' option. If 'vectorize' is set to 'off', then the
	// vectorized engine is disabled, and we will return an error in such case.
	// With all other options, we don't change the setting to the
	// most-inclusive option as we used to because the plan can be different
	// based on 'vectorize' setting.
	if flowCtx.EvalCtx.SessionData.VectorizeMode == sessiondata.VectorizeOff {
		return errors.New("vectorize is set to 'off'")
	}

	sortedFlows := make([]flowWithNode, 0, len(flows))
	for nodeID, flow := range flows {
		sortedFlows = append(sortedFlows, flowWithNode{nodeID: nodeID, flow: flow})
	}
	// Sort backward, since the first thing you add to a treeprinter will come last.
	sort.Slice(sortedFlows, func(i, j int) bool { return sortedFlows[i].nodeID < sortedFlows[j].nodeID })
	tp := treeprinter.NewWithIndent(false /* leftPad */, true /* rightPad */, 0 /* edgeLength */)
	root := tp.Child("")
	verbose := n.options.Flags[tree.ExplainFlagVerbose]
	thisNodeID, _ := params.extendedEvalCtx.NodeID.OptionalNodeID()
	for _, flow := range sortedFlows {
		node := root.Childf("Node %d", flow.nodeID)
		scheduledOnRemoteNode := flow.nodeID != thisNodeID
		opChains, err := colflow.SupportsVectorized(params.ctx, flowCtx, flow.flow.Processors, !willDistribute, nil /* output */, scheduledOnRemoteNode)
		if err != nil {
			return err
		}
		// It is possible that when iterating over execinfra.OpNodes we will hit
		// a panic (an input that doesn't implement OpNode interface), so we're
		// catching such errors.
		if err := colexecerror.CatchVectorizedRuntimeError(func() {
			for _, op := range opChains {
				formatOpChain(op, node, verbose)
			}
		}); err != nil {
			return err
		}
	}
	n.run.lines = tp.FormattedRows()
	return nil
}

func newFlowCtxForExplainPurposes(planCtx *PlanningCtx, params runParams) *execinfra.FlowCtx {
	return &execinfra.FlowCtx{
		NodeID:  planCtx.EvalContext().NodeID,
		EvalCtx: planCtx.EvalContext(),
		Cfg: &execinfra.ServerConfig{
			Settings:       params.p.execCfg.Settings,
			DiskMonitor:    &mon.BytesMonitor{},
			VecFDSemaphore: params.p.execCfg.DistSQLSrv.VecFDSemaphore,
		},
	}
}

func newPlanningCtxForExplainPurposes(
	distSQLPlanner *DistSQLPlanner,
	params runParams,
	stmtType tree.StatementType,
	subqueryPlans []subquery,
	distribution physicalplan.PlanDistribution,
) *PlanningCtx {
	planCtx := distSQLPlanner.NewPlanningCtx(params.ctx, params.extendedEvalCtx, params.p.txn, distribution.WillDistribute())
	planCtx.ignoreClose = true
	planCtx.planner = params.p
	planCtx.stmtType = stmtType
	planCtx.planner.curPlan.subqueryPlans = subqueryPlans
	for i := range planCtx.planner.curPlan.subqueryPlans {
		p := &planCtx.planner.curPlan.subqueryPlans[i]
		// Fake subquery results - they're not important for our explain output.
		p.started = true
		p.result = tree.DNull
	}
	return planCtx
}

func shouldOutput(operator execinfra.OpNode, verbose bool) bool {
	_, nonExplainable := operator.(colexec.NonExplainable)
	return !nonExplainable || verbose
}

func formatOpChain(operator execinfra.OpNode, node treeprinter.Node, verbose bool) {
	seenOps := make(map[reflect.Value]struct{})
	if shouldOutput(operator, verbose) {
		doFormatOpChain(operator, node.Child(reflect.TypeOf(operator).String()), verbose, seenOps)
	} else {
		doFormatOpChain(operator, node, verbose, seenOps)
	}
}
func doFormatOpChain(
	operator execinfra.OpNode,
	node treeprinter.Node,
	verbose bool,
	seenOps map[reflect.Value]struct{},
) {
	for i := 0; i < operator.ChildCount(verbose); i++ {
		child := operator.Child(i, verbose)
		childOpValue := reflect.ValueOf(child)
		childOpName := reflect.TypeOf(child).String()
		if _, seenOp := seenOps[childOpValue]; seenOp {
			// We have already seen this operator, so in order to not repeat the full
			// chain again, we will simply print out this operator's name and will
			// not recurse into its children. Note that we print out the name
			// unequivocally.
			node.Child(childOpName)
			continue
		}
		seenOps[childOpValue] = struct{}{}
		if shouldOutput(child, verbose) {
			doFormatOpChain(child, node.Child(childOpName), verbose, seenOps)
		} else {
			doFormatOpChain(child, node, verbose, seenOps)
		}
	}
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
	n.plan.Close(ctx)
}
