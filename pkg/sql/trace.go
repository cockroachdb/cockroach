// Copyright 2016 The Cockroach Authors.
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
	"errors"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// traceNode is a planNode that wraps another node and uses session
// tracing to report all the database events that occur during its
// execution.
// It is used as the top-level node for SHOW TRACE FOR statements.
type traceNode struct {
	// plan is the wrapped execution plan that will be traced.
	plan    planNode
	columns sqlbase.ResultColumns

	// If set, the trace will also include "KV trace" messages - verbose messages
	// around the interaction of SQL with KV. Some of the messages are per-row.
	kvTracingEnabled bool

	run traceRun
}

// traceRun contains the run-time state of traceNode during local execution.
type traceRun struct {
	execDone bool

	traceRows []traceRow
	curRow    int

	// stopTracing is set if this node started tracing on the
	// session. If it is set, then Close() must call it.
	stopTracing func() error
}

var sessionTraceTableName = tree.TableName{
	DatabaseName: tree.Name("crdb_internal"),
	TableName:    tree.Name("session_trace"),
}

// makeTraceNode creates a new traceNode.
//
// Args:
// plan: The wrapped execution plan to be traced.
// kvTrancingEnabled: If set, the trace will also include "KV trace" messages -
//   verbose messages around the interaction of SQL with KV. Some of the
//   messages are per-row.
func (p *planner) makeTraceNode(plan planNode, kvTracingEnabled bool) (planNode, error) {
	desc, err := p.getVirtualTabler().getVirtualTableDesc(&sessionTraceTableName)
	if err != nil {
		return nil, err
	}
	return &traceNode{
		plan:             plan,
		columns:          sqlbase.ResultColumnsFromColDescs(desc.Columns),
		kvTracingEnabled: kvTracingEnabled,
	}, nil
}

var errTracingAlreadyEnabled = errors.New(
	"cannot run SHOW TRACE FOR on statement while session tracing is enabled" +
		" - did you mean SHOW TRACE FOR SESSION?")

func (n *traceNode) Start(params runParams) error {
	if params.p.session.Tracing.Enabled() {
		return errTracingAlreadyEnabled
	}
	if err := params.p.session.Tracing.StartTracing(
		tracing.SnowballRecording, n.kvTracingEnabled,
	); err != nil {
		return err
	}
	session := params.p.session
	n.run.stopTracing = func() error { return stopTracing(session) }

	startCtx, sp := tracing.ChildSpan(params.ctx, "starting plan")
	defer sp.Finish()
	params.ctx = startCtx
	return n.plan.Start(params)
}

func (n *traceNode) Close(ctx context.Context) {
	if n.plan != nil {
		n.plan.Close(ctx)
	}
	n.run.traceRows = nil
	if n.run.stopTracing != nil {
		if err := n.run.stopTracing(); err != nil {
			log.Errorf(ctx, "error stopping tracing at end of SHOW TRACE FOR: %v", err)
		}
	}
}

func (n *traceNode) Next(params runParams) (bool, error) {
	if !n.run.execDone {
		// We need to run the entire statement upfront. Subsequent
		// invocations of Next() will merely return the trace.

		func() {
			consumeCtx, sp := tracing.ChildSpan(params.ctx, "consuming rows")
			defer sp.Finish()

			slowPath := true
			if a, ok := n.plan.(planNodeFastPath); ok {
				if count, res := a.FastPathResults(); res {
					log.VEventf(consumeCtx, 2, "fast path - rows affected: %d", count)
					slowPath = false
				}
			}
			if slowPath {
				for {
					hasNext, err := n.plan.Next(params)
					if err != nil {
						log.VEventf(consumeCtx, 2, "execution failed: %v", err)
						break
					}
					if !hasNext {
						break
					}

					values := n.plan.Values()
					if n.kvTracingEnabled {
						log.VEventf(consumeCtx, 2, "output row: %s", values)
					}
				}
			}
			log.VEventf(consumeCtx, 2, "plan completed execution")

			// Release the plan's resources early.
			n.plan.Close(consumeCtx)
			n.plan = nil

			log.VEventf(consumeCtx, 2, "resources released, stopping trace")
		}()

		if err := stopTracing(params.p.session); err != nil {
			return false, err
		}
		n.run.stopTracing = nil

		var err error
		n.run.traceRows, err = params.p.session.Tracing.generateSessionTraceVTable()
		if err != nil {
			return false, err
		}
		n.run.execDone = true
	}

	if n.run.curRow >= len(n.run.traceRows) {
		return false, nil
	}
	n.run.curRow++
	return true, nil
}

func (n *traceNode) Values() tree.Datums {
	return n.run.traceRows[n.run.curRow-1][:]
}
