// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// distSQLNode is a planNode that runs the wrapped node through distSQL physical
// planning and execution. The plan is assumed to be supported by distSQL.
// Rows are discarded and not returned.
type distSQLNode struct {
	executed bool
	// plan is the wrapped execution plan that will be executed in distSQL.
	plan     planNode
	stmtType tree.StatementType
}

// makeDistSQLNode creates a new distSQLNode.
//
// Args:
// plan: The wrapped execution plan to be physically planned and executed.
// stmtType: The statement type of the statement represented by plan.
func (p *planner) makeDistSQLNode(plan planNode, stmtType tree.StatementType) planNode {
	return &distSQLNode{
		plan:     plan,
		stmtType: stmtType,
	}
}

func (n *distSQLNode) Next(params runParams) (bool, error) {
	if !n.executed {
		// For now, the distSQLNode discards all result rows since it is only
		// used in show trace. Keeping rows could potentially lead to a memory
		// blowup.
		rr := newCallbackResultWriter(func(_ context.Context, _ tree.Datums) error {
			return nil
		})
		execCfg := params.p.ExecCfg()
		recv := makeDistSQLReceiver(
			params.ctx,
			&rr,
			n.stmtType,
			execCfg.RangeDescriptorCache,
			execCfg.LeaseHolderCache,
			params.p.txn,
			func(ts hlc.Timestamp) {
				_ = execCfg.Clock.Update(ts)
			},
		)
		execCfg.DistSQLPlanner.PlanAndRun(
			params.ctx, params.p.txn, n.plan, recv, params.p.ExtendedEvalContext(),
		)
		n.executed = true
		return false, rr.Err()
	}

	return false, nil
}

func (n *distSQLNode) Values() tree.Datums {
	return nil
}

func (n *distSQLNode) Close(ctx context.Context) {
	if n.plan != nil {
		n.plan.Close(ctx)
	}
}
