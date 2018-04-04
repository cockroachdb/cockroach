// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// explainDistSQLNode is a planNode that wraps a plan and returns
// information related to running that plan under DistSQL.
type explainDistSQLNode struct {
	optColumnsSlot

	plan planNode

	run explainDistSQLRun
}

// explainDistSQLRun contains the run-time state of explainDistSQLNode during local execution.
type explainDistSQLRun struct {
	// The single row returned by the node.
	values tree.Datums

	// done is set if Next() was called.
	done bool
}

func (n *explainDistSQLNode) startExec(params runParams) error {
	// Trigger limit propagation.
	params.p.setUnlimited(n.plan)

	distSQLPlanner := params.extendedEvalCtx.DistSQLPlanner

	auto, err := distSQLPlanner.CheckSupport(n.plan)
	if err != nil {
		return err
	}

	planCtx := distSQLPlanner.newPlanningCtx(params.ctx, params.extendedEvalCtx, params.p.txn)
	plan, err := distSQLPlanner.createPlanForNode(&planCtx, n)
	if err != nil {
		return err
	}
	distSQLPlanner.FinalizePlan(&planCtx, &plan)
	flows := plan.GenerateFlowSpecs(params.extendedEvalCtx.NodeID)
	planJSON, planURL, err := distsqlrun.GeneratePlanDiagramWithURL(flows)
	if err != nil {
		return err
	}

	n.run.values = tree.Datums{
		tree.MakeDBool(tree.DBool(auto)),
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

func (n *explainDistSQLNode) Values() tree.Datums       { return n.run.values }
func (n *explainDistSQLNode) Close(ctx context.Context) { n.plan.Close(ctx) }

var explainDistSQLColumns = sqlbase.ResultColumns{
	{Name: "Automatic", Typ: types.Bool},
	{Name: "URL", Typ: types.String},
	{Name: "JSON", Typ: types.String, Hidden: true},
}
