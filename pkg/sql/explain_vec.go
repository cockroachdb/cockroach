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
	"github.com/cockroachdb/cockroach/pkg/sql/colflow"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
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
	plan    planNode

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
	willDistributePlan, _ := willDistributePlan(distSQLPlanner, n.plan, params)
	outerSubqueries := params.p.curPlan.subqueryPlans
	planCtx := makeExplainVecPlanningCtx(distSQLPlanner, params, n.stmtType, n.subqueryPlans, willDistributePlan)
	defer func() {
		planCtx.planner.curPlan.subqueryPlans = outerSubqueries
	}()
	plan, err := makePhysicalPlan(planCtx, distSQLPlanner, n.plan)
	if err != nil {
		if len(n.subqueryPlans) > 0 {
			return errors.New("running EXPLAIN (VEC) on this query is " +
				"unsupported because of the presence of subqueries")
		}
		return err
	}

	distSQLPlanner.FinalizePlan(planCtx, &plan)
	flows := plan.GenerateFlowSpecs(params.extendedEvalCtx.NodeID)
	flowCtx := makeFlowCtx(planCtx, plan, params)
	flowCtx.Cfg.ClusterID = &distSQLPlanner.rpcCtx.ClusterID

	// Temporarily set vectorize to on so that we can get the whole plan back even
	// if we wouldn't support it due to lack of streaming.
	origMode := flowCtx.EvalCtx.SessionData.VectorizeMode
	flowCtx.EvalCtx.SessionData.VectorizeMode = sessiondata.VectorizeExperimentalOn
	defer func() { flowCtx.EvalCtx.SessionData.VectorizeMode = origMode }()

	sortedFlows := make([]flowWithNode, 0, len(flows))
	for nodeID, flow := range flows {
		sortedFlows = append(sortedFlows, flowWithNode{nodeID: nodeID, flow: flow})
	}
	// Sort backward, since the first thing you add to a treeprinter will come last.
	sort.Slice(sortedFlows, func(i, j int) bool { return sortedFlows[i].nodeID < sortedFlows[j].nodeID })
	tp := treeprinter.NewWithIndent(false /* leftPad */, true /* rightPad */, 0 /* edgeLength */)
	root := tp.Child("")
	verbose := n.options.Flags.Contains(tree.ExplainFlagVerbose)
	thisNodeID := distSQLPlanner.nodeDesc.NodeID
	for _, flow := range sortedFlows {
		node := root.Childf("Node %d", flow.nodeID)
		fuseOpt := flowinfra.FuseNormally
		if flow.nodeID == thisNodeID && !willDistributePlan {
			fuseOpt = flowinfra.FuseAggressively
		}
		opChains, err := colflow.SupportsVectorized(params.ctx, flowCtx, flow.flow.Processors, fuseOpt, nil /* output */)
		if err != nil {
			return err
		}
		for _, op := range opChains {
			formatOpChain(op, node, verbose)
		}
	}
	n.run.lines = tp.FormattedRows()
	return nil
}

func makeFlowCtx(planCtx *PlanningCtx, plan PhysicalPlan, params runParams) *execinfra.FlowCtx {
	flowCtx := &execinfra.FlowCtx{
		NodeID:  planCtx.EvalContext().NodeID,
		EvalCtx: planCtx.EvalContext(),
		Cfg: &execinfra.ServerConfig{
			Settings:    params.p.execCfg.Settings,
			DiskMonitor: &mon.BytesMonitor{},
		},
	}
	return flowCtx
}

func makeExplainVecPlanningCtx(
	distSQLPlanner *DistSQLPlanner,
	params runParams,
	stmtType tree.StatementType,
	subqueryPlans []subquery,
	willDistributePlan bool,
) *PlanningCtx {
	planCtx := distSQLPlanner.NewPlanningCtx(params.ctx, params.extendedEvalCtx, params.p.txn)
	planCtx.isLocal = !willDistributePlan
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
