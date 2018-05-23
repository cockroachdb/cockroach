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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// explainDistSQLNode is a planNode that wraps a plan and returns
// information related to running that plan under DistSQL.
type explainDistSQLNode struct {
	optColumnsSlot

	plan planNode

	// If analyze is set, plan will be executed with tracing enabled and a url
	// pointing to a visual query plan with statistics will be in the row
	// returned by the node.
	analyze bool
	// stmtType is the StatementType of the plan. It is needed by the
	// distSQLWrapper when analyzing a statement.
	stmtType tree.StatementType

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
	// Check for subqueries and trigger limit propagation.
	if _, err := params.p.prepareForDistSQLSupportCheck(
		params.ctx, true, /* returnError */
	); err != nil {
		return err
	}

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

	var spans []tracing.RecordedSpan
	if n.analyze {
		// If tracing is already enabled, don't call StopTracing at the end.
		shouldStopTracing := !params.extendedEvalCtx.Tracing.Enabled()

		// Start tracing. KV tracing is not enabled because we are only interested
		// in stats present on the spans. Noop if tracing is already enabled.
		if err := params.extendedEvalCtx.Tracing.StartTracing(
			tracing.SnowballRecording, false, /* kvTracingEnabled */
		); err != nil {
			return err
		}

		// Discard rows that are returned.
		rw := newCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
			return nil
		})
		execCfg := params.p.ExecCfg()
		recv := makeDistSQLReceiver(
			params.ctx,
			rw,
			n.stmtType,
			execCfg.RangeDescriptorCache,
			execCfg.LeaseHolderCache,
			params.p.txn,
			func(ts hlc.Timestamp) {
				_ = execCfg.Clock.Update(ts)
			},
		)
		distSQLPlanner.Run(&planCtx, params.p.txn, &plan, recv, params.p.ExtendedEvalContext())

		spans = params.extendedEvalCtx.Tracing.getRecording()
		if shouldStopTracing {
			if err := params.extendedEvalCtx.Tracing.StopTracing(); err != nil {
				return err
			}
		}
	}

	flows := plan.GenerateFlowSpecs(params.extendedEvalCtx.NodeID)
	planJSON, planURL, err := distsqlrun.GeneratePlanDiagramURLWithSpans(flows, spans)
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

func (n *explainDistSQLNode) Values() tree.Datums { return n.run.values }
func (n *explainDistSQLNode) Close(ctx context.Context) {
	// If we analyzed the statement, we relinquished ownership of the plan to a
	// distSQLWrapper.
	if !n.analyze {
		n.plan.Close(ctx)
	}
}
