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
	"github.com/pkg/errors"
)

const distSQLWrapperBufferSize = 1024

// distSQLWrapper is a planNode that runs the wrapped node through distSQL physical
// planning and execution. The plan is assumed to be supported by distSQL.
type distSQLWrapper struct {
	// plan is the wrapped execution plan that will be executed in distSQL.
	plan     planNode
	stmtType tree.StatementType

	resultWriter rowResultWriter
	currentRow   tree.Datums
	// resultRows is a buffered channel to which we write the distSQL results.
	// Once the buffer fills up, the next write will block, allowing us to push
	// back on distSQL processing and avoid running out of memory.
	resultRows chan tree.Datums
}

// newDistSQLWrapper creates a new distSQLWrapper.
//
// Args:
// plan: The wrapped execution plan to be physically planned and executed.
// stmtType: The statement type of the statement represented by plan.
func (p *planner) newDistSQLWrapper(plan planNode, stmtType tree.StatementType) planNode {
	n := &distSQLWrapper{
		plan:       plan,
		stmtType:   stmtType,
		resultRows: make(chan tree.Datums, distSQLWrapperBufferSize),
	}
	n.resultWriter = newCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
		rowCopy := make(tree.Datums, len(row))
		copy(rowCopy, row)
		select {
		case n.resultRows <- rowCopy:
			return nil
		case <-ctx.Done():
			err := errors.Wrap(ctx.Err(), "context canceled while writing distSQLWrapper result")
			n.resultWriter.SetError(err)
			return err
		}
	})
	return n
}

func (n *distSQLWrapper) startExec(params runParams) error {
	execCfg := params.p.ExecCfg()
	recv := makeDistSQLReceiver(
		params.ctx,
		n.resultWriter,
		n.stmtType,
		execCfg.RangeDescriptorCache,
		execCfg.LeaseHolderCache,
		params.p.txn,
		func(ts hlc.Timestamp) {
			_ = execCfg.Clock.Update(ts)
		},
	)
	go func() {
		execCfg.DistSQLPlanner.PlanAndRun(
			params.ctx, params.p.txn, n.plan, recv, params.p.ExtendedEvalContext(),
		)
		close(n.resultRows)
	}()
	return nil
}

func (n *distSQLWrapper) Next(params runParams) (bool, error) {
	if n.currentRow = <-n.resultRows; n.currentRow == nil {
		return false, n.resultWriter.Err()
	}
	return true, nil
}

func (n *distSQLWrapper) Values() tree.Datums {
	return n.currentRow
}

func (n *distSQLWrapper) Close(ctx context.Context) {
	// Make sure the channel is drained and closed.
	for range n.resultRows {
	}
	n.plan.Close(ctx)
}
