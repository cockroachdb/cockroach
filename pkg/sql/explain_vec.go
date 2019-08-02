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
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// explainVecNode is a planNode that wraps a plan and returns
// information related to running that plan with the vectorized engine.
type explainVecNode struct {
	optColumnsSlot

	plan planNode

	stmtType tree.StatementType

	run struct {
		lines []string
		// The current row returned by the node.
		values tree.Datums
	}
}

type flowWithNode struct {
	nodeID roachpb.NodeID
	flow   *distsqlpb.FlowSpec
}

func (n *explainVecNode) startExec(params runParams) error {
	// Trigger limit propagation.
	params.p.prepareForDistSQLSupportCheck()
	n.run.values = make(tree.Datums, 1)
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
	planCtx.noEvalSubqueries = true

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

	var localState distsqlrun.LocalState
	localState.EvalContext = planCtx.EvalContext()
	if planCtx.isLocal {
		localState.IsLocal = true
		localState.LocalProcs = plan.LocalProcessors
	}
	flowCtx := &distsqlrun.FlowCtx{
		NodeID:  planCtx.EvalContext().NodeID,
		EvalCtx: planCtx.EvalContext(),
		Cfg:     &distsqlrun.ServerConfig{},
	}

	sortedFlows := make([]flowWithNode, 0, len(flows))
	for nodeID, flow := range flows {
		sortedFlows = append(sortedFlows, flowWithNode{nodeID: nodeID, flow: flow})
	}
	// Sort backward, since the first thing you add to a treeprinter will come last.
	sort.Slice(sortedFlows, func(i, j int) bool { return sortedFlows[i].nodeID < sortedFlows[j].nodeID })
	tp := treeprinter.New()
	root := tp.Child("")
	for _, flow := range sortedFlows {
		node := root.Childf("Node %d", flow.nodeID)
		opChains, err := distsqlrun.SupportsVectorized(params.ctx, flowCtx, flow.flow.Processors)
		if err != nil {
			return err
		}
		for _, op := range opChains {
			formatOpChain(op, node)
		}
	}
	n.run.lines = tp.FormattedRows()
	return nil
}

func formatOpChain(operator exec.OpNode, node treeprinter.Node) {
	doFormatOpChain(operator, node.Child(reflect.TypeOf(operator).String()))
}
func doFormatOpChain(operator exec.OpNode, node treeprinter.Node) {
	for i := 0; i < operator.ChildCount(); i++ {
		child := operator.Child(i)
		doFormatOpChain(child, node.Child(reflect.TypeOf(child).String()))
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
