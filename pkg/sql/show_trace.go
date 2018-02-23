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
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// showTraceNode is a planNode that wraps another node and uses session
// tracing to report all the database events that occur during its
// execution.
// It is used as the top-level node for SHOW TRACE FOR statements.
type showTraceNode struct {
	// plan is the wrapped execution plan that will be traced.
	plan    planNode
	columns sqlbase.ResultColumns

	// If set, the trace will also include "KV trace" messages - verbose messages
	// around the interaction of SQL with KV. Some of the messages are per-row.
	kvTracingEnabled bool

	run traceRun
}

// ShowTrace shows the current stored session trace.
// Privileges: None.
func (p *planner) ShowTrace(ctx context.Context, n *tree.ShowTrace) (planNode, error) {
	if n.TraceType == tree.ShowTraceReplica {
		return p.ShowTraceReplica(ctx, n)
	}

	const fullSelection = `
       timestamp,
       timestamp-first_value(timestamp) OVER (ORDER BY timestamp) AS age,
       message, tag, loc, operation, span`
	const compactSelection = `
       timestamp-first_value(timestamp) OVER (ORDER BY timestamp) AS age,
       IF(length(loc)=0,message,loc || ' ' || message) AS message,
       tag, operation`

	const traceClause = `
SELECT %s
  FROM (SELECT timestamp,
               message,
               tag,
               loc,
               first_value(operation) OVER (PARTITION BY span_idx ORDER BY message_idx) as operation,
               span_idx AS span
          FROM crdb_internal.session_trace)
 %s
 ORDER BY timestamp
`

	renderClause := fullSelection
	if n.Compact {
		renderClause = compactSelection
	}

	whereClause := ""
	if n.TraceType == tree.ShowTraceKV {
		whereClause = `
WHERE message LIKE 'fetched: %'
   OR message LIKE 'CPut %'
   OR message LIKE 'Put %'
   OR message LIKE 'DelRange %'
   OR message LIKE 'ClearRange %'
   OR message LIKE 'Del %'
   OR message LIKE 'Get %'
   OR message LIKE 'Scan %'
   OR message = 'consuming rows'
   OR message = 'starting plan'
   OR message LIKE 'fast path - %'
   OR message LIKE 'querying next range at %'
   OR message LIKE 'output row: %'
   OR message LIKE 'execution failed: %'
   OR message LIKE 'r%: sending batch %'
   OR message LIKE 'cascading %'
`
	}

	plan, err := p.delegateQuery(ctx, "SHOW TRACE",
		fmt.Sprintf(traceClause, renderClause, whereClause), nil, nil)
	if err != nil {
		return nil, err
	}

	if n.Statement == nil {
		// SHOW TRACE FOR SESSION ...
		return plan, nil
	}

	// SHOW TRACE FOR SELECT ...
	stmtPlan, err := p.newPlan(ctx, n.Statement, nil)
	if err != nil {
		plan.Close(ctx)
		return nil, err
	}
	tracePlan, err := p.makeShowTraceNode(
		stmtPlan, n.TraceType == tree.ShowTraceKV /* kvTracingEnabled */)
	if err != nil {
		plan.Close(ctx)
		stmtPlan.Close(ctx)
		return nil, err
	}

	// inject the tracePlan inside the SHOW query plan.

	// Suggestion from Radu:
	// This is not very elegant and would be cleaner if the showTraceNode
	// could process the statement source node first, let it emit its
	// trace messages, and only then query the session_trace vtable.
	// Alternatively, the outer SHOW could use a UNION between the
	// showTraceNode and the query on session_trace.

	// Unfortunately, neither is currently possible because the plan for
	// the query on session_trace cannot be constructed until the trace
	// is complete. If we want to enable EXPLAIN [SHOW TRACE FOR ...],
	// this limitation of the vtable session_trace must be lifted first.
	// TODO(andrei): make this code more elegant.
	if s, ok := plan.(*sortNode); ok {
		if w, ok := s.plan.(*windowNode); ok {
			if r, ok := w.plan.(*renderNode); ok {
				subPlan := r.source.plan
				if f, ok := subPlan.(*filterNode); ok {
					// The filter layer is only present when the KV modifier is
					// specified.
					subPlan = f.source.plan
				}
				if w, ok := subPlan.(*windowNode); ok {
					if r, ok := w.plan.(*renderNode); ok {
						if _, ok := r.source.plan.(*delayedNode); ok {
							r.source.plan.Close(ctx)
							r.source.plan = tracePlan
							return plan, nil
						}
					}
				}
			}
		}
	}

	// We failed to substitute; this is an internal error.
	err = pgerror.NewErrorf(pgerror.CodeInternalError,
		"invalid logical plan structure:\n%s", planToString(ctx, plan, nil),
	).SetDetailf(
		"while inserting:\n%s", planToString(ctx, tracePlan, nil))
	plan.Close(ctx)
	stmtPlan.Close(ctx)
	tracePlan.Close(ctx)
	return nil, err
}

// makeShowTraceNode creates a new showTraceNode.
//
// Args:
// plan: The wrapped execution plan to be traced.
// kvTrancingEnabled: If set, the trace will also include "KV trace" messages -
//   verbose messages around the interaction of SQL with KV. Some of the
//   messages are per-row.
func (p *planner) makeShowTraceNode(plan planNode, kvTracingEnabled bool) (planNode, error) {
	desc, err := p.getVirtualTabler().getVirtualTableDesc(&sessionTraceTableName)
	if err != nil {
		return nil, err
	}
	return &showTraceNode{
		plan:             plan,
		columns:          sqlbase.ResultColumnsFromColDescs(desc.Columns),
		kvTracingEnabled: kvTracingEnabled,
	}, nil
}

// traceRun contains the run-time state of showTraceNode during local execution.
type traceRun struct {
	execDone bool

	traceRows []traceRow
	curRow    int

	// stopTracing is set if this node started tracing on the
	// session. If it is set, then Close() must call it.
	stopTracing func() error
}

func (n *showTraceNode) startExec(params runParams) error {
	if params.extendedEvalCtx.Tracing.Enabled() {
		return errTracingAlreadyEnabled
	}
	if err := params.extendedEvalCtx.SessionMutator.StartSessionTracing(
		tracing.SnowballRecording, n.kvTracingEnabled,
	); err != nil {
		return err
	}
	n.run.stopTracing = func() error { return stopTracing(params.extendedEvalCtx.SessionMutator) }

	startCtx, sp := tracing.ChildSpan(params.ctx, "starting plan")
	defer sp.Finish()
	params.ctx = startCtx
	return startExec(params, n.plan)
}

func (n *showTraceNode) Next(params runParams) (bool, error) {
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

		if err := stopTracing(params.extendedEvalCtx.SessionMutator); err != nil {
			return false, err
		}
		n.run.stopTracing = nil

		var err error
		n.run.traceRows, err = params.extendedEvalCtx.Tracing.getRecording()
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

func (n *showTraceNode) Values() tree.Datums {
	return n.run.traceRows[n.run.curRow-1][:]
}

func (n *showTraceNode) Close(ctx context.Context) {
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

var sessionTraceTableName = tree.MakeTableNameWithSchema("", "crdb_internal", "session_trace")

var errTracingAlreadyEnabled = errors.New(
	"cannot run SHOW TRACE FOR on statement while session tracing is enabled" +
		" - did you mean SHOW TRACE FOR SESSION?")
