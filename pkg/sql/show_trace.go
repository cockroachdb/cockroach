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
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// showTraceNode is a planNode that processes session trace data; it can
// retrieve data from the current session trace, or it can trace the execution
// of an inner plan.
//
// It is used as the top-level node for SHOW TRACE FOR statements.
type showTraceNode struct {
	// plan is the wrapped execution plan that will be traced; nil if we are
	// just showing the session trace.
	plan planNode

	// stmtType represents the statement type of the wrapped execution plan. Only
	// set if plan is not nil.
	stmtType tree.StatementType

	columns sqlbase.ResultColumns
	compact bool

	// If set, the trace will also include "KV trace" messages - verbose messages
	// around the interaction of SQL with KV. Some of the messages are per-row.
	kvTracingEnabled bool

	run traceRun
}

// ShowTrace shows the current stored session trace, or the trace of a given
// query.
// Privileges: None.
func (p *planner) ShowTrace(ctx context.Context, n *tree.ShowTrace) (planNode, error) {
	// For the SHOW TRACE FOR <query> version, build a plan for the inner
	// statement.
	// For the SHOW TRACE FOR SESSION version, stmtPlan stays nil.
	var stmtPlan planNode
	var stmtType tree.StatementType
	if n.Statement != nil {
		stmtType = n.Statement.StatementType()
		var err error
		stmtPlan, err = p.newPlan(ctx, n.Statement, nil)
		if err != nil {
			return nil, err
		}
	}

	node := p.makeShowTraceNode(stmtPlan, stmtType, n.Compact, n.TraceType == tree.ShowTraceKV)

	if n.TraceType == tree.ShowTraceReplica {
		// Wrap the showTraceNode in a showTraceReplicaNode.
		return p.makeShowTraceReplicaNode(node), nil
	}
	return node, nil
}

// makeShowTraceNode creates a new showTraceNode.
//
// Args:
// plan: The wrapped execution plan to be traced.
// stmtType: The statement type of the wrapped execution plan.
// kvTrancingEnabled: If set, the trace will also include "KV trace" messages -
//   verbose messages around the interaction of SQL with KV. Some of the
//   messages are per-row.
func (p *planner) makeShowTraceNode(
	plan planNode, stmtType tree.StatementType, compact bool, kvTracingEnabled bool,
) *showTraceNode {
	n := &showTraceNode{
		plan:             plan,
		stmtType:         stmtType,
		kvTracingEnabled: kvTracingEnabled,
		compact:          compact,
	}
	if compact {
		// We make a copy here because n.columns can be mutated to rename columns.
		n.columns = append(n.columns, showCompactTraceColumns...)
	} else {
		n.columns = append(n.columns, showTraceColumns...)
	}
	return n
}

// traceRun contains the run-time state of showTraceNode during local execution.
type traceRun struct {
	execDone bool

	resultRows []tree.Datums
	curRow     int

	// stopTracing is set if this node started tracing on the
	// session. If it is set, then Close() must call it.
	stopTracing func() error
}

func (n *showTraceNode) startExec(params runParams) error {
	if n.plan == nil {
		return nil
	}
	if params.extendedEvalCtx.Tracing.Enabled() {
		return errTracingAlreadyEnabled
	}
	if err := params.extendedEvalCtx.Tracing.StartTracing(
		tracing.SnowballRecording, n.kvTracingEnabled,
	); err != nil {
		return err
	}
	n.run.stopTracing = params.extendedEvalCtx.Tracing.StopTracing

	startCtx, sp := tracing.ChildSpan(params.ctx, "starting plan")
	defer sp.Finish()
	params.ctx = startCtx
	return startExec(params, n.plan)
}

func (n *showTraceNode) Next(params runParams) (bool, error) {
	if !n.run.execDone {
		// Get all the data upfront and process the traces. Subsequent
		// invocations of Next() will merely return the results.
		if n.plan != nil {
			n.runChildPlan(params)

			if err := params.extendedEvalCtx.Tracing.StopTracing(); err != nil {
				return false, err
			}
			n.run.stopTracing = nil
		}

		traceRows, err := params.extendedEvalCtx.Tracing.getRecording()
		if err != nil {
			return false, err
		}
		n.processTraceRows(params.EvalContext(), traceRows)
		n.run.execDone = true
	}

	if n.run.curRow >= len(n.run.resultRows) {
		return false, nil
	}
	n.run.curRow++
	return true, nil
}

// runChildPlan runs the enclosed plan inside a child span.
func (n *showTraceNode) runChildPlan(params runParams) {
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
}

// processTraceRows populates n.resultRows.
func (n *showTraceNode) processTraceRows(evalCtx *tree.EvalContext, traceRows []traceRow) {
	// The schema of the trace rows mirrors that of the
	// crdb_internal.session_trace table:
	const (
		// span_idx    INT NOT NULL,        -- The span's index.
		spanIdxCol = iota
		// message_idx INT NOT NULL,        -- The message's index within its span.
		_
		// timestamp   TIMESTAMPTZ NOT NULL,-- The message's timestamp.
		timestampCol
		// duration    INTERVAL,            -- The span's duration. Set only on the first
		//                                  -- (dummy) message on a span.
		//                                  -- NULL if the span was not finished at the time
		//                                  -- the trace has been collected.
		_
		// operation   STRING NULL,         -- The span's operation. Set only on
		//                                  -- the first (dummy) message in a span.
		opCol
		// loc         STRING NOT NULL,     -- The file name / line number prefix, if any.
		locCol
		// tag         STRING NOT NULL,     -- The logging tag, if any.
		tagCol
		// message     STRING NOT NULL      -- The logged message.
		msgCol
	)

	// Get the operation ID for each spanIdx (it is present only in the first
	// message of a span).
	opMap := make(map[tree.DInt]*tree.DString)
	for _, r := range traceRows {
		if r[opCol] != tree.DNull {
			spanIdx := *r[spanIdxCol].(*tree.DInt)
			opMap[spanIdx] = r[opCol].(*tree.DString)
		}
	}

	// Filter trace rows based on the message (in the SHOW KV TRACE case)
	if n.kvTracingEnabled {
		res := traceRows[:0]
		for _, r := range traceRows {
			msg := r[msgCol].(*tree.DString)
			if kvMsgRegexp.MatchString(string(*msg)) {
				res = append(res, r)
			}
		}
		traceRows = res
	}
	if len(traceRows) == 0 {
		return
	}

	// Sort the trace rows based on timestamp.
	sort.Slice(traceRows, func(i, j int) bool {
		return traceRows[i][timestampCol].Compare(
			evalCtx,
			traceRows[j][timestampCol],
		) < 0
	})

	// Render the final rows.
	n.run.resultRows = make([]tree.Datums, len(traceRows))
	minTs := traceRows[0][timestampCol].(*tree.DTimestampTZ)
	for i, r := range traceRows {
		ts := r[timestampCol].(*tree.DTimestampTZ)
		age := &tree.DInterval{Duration: duration.Duration{Nanos: ts.Sub(minTs.Time).Nanoseconds()}}
		loc := r[locCol]
		tag := r[tagCol]
		msg := r[msgCol]
		spanIdx := r[spanIdxCol]
		op := tree.DNull
		if opStr, ok := opMap[*(spanIdx.(*tree.DInt))]; ok {
			op = opStr
		}

		if !n.compact {
			n.run.resultRows[i] = tree.Datums{ts, age, msg, tag, loc, op, spanIdx}
		} else {
			msgStr := msg.(*tree.DString)
			if locStr := string(*loc.(*tree.DString)); locStr != "" {
				msgStr = tree.NewDString(fmt.Sprintf("%s %s", locStr, string(*msgStr)))
			}
			n.run.resultRows[i] = tree.Datums{age, msgStr, tag, op}
		}
	}
}

func (n *showTraceNode) Values() tree.Datums {
	return n.run.resultRows[n.run.curRow-1]
}

func (n *showTraceNode) Close(ctx context.Context) {
	if n.plan != nil {
		n.plan.Close(ctx)
	}
	n.run.resultRows = nil
	if n.run.stopTracing != nil {
		if err := n.run.stopTracing(); err != nil {
			log.Errorf(ctx, "error stopping tracing at end of SHOW TRACE FOR: %v", err)
		}
	}
}

var errTracingAlreadyEnabled = errors.New(
	"cannot run SHOW TRACE FOR on statement while session tracing is enabled" +
		" - did you mean SHOW TRACE FOR SESSION?")

// kvMsgRegexp is the message filter used for SHOW KV TRACE.
var kvMsgRegexp = regexp.MustCompile(
	strings.Join([]string{
		"^fetched: ",
		"^CPut ",
		"^Put ",
		"^DelRange ",
		"^ClearRange ",
		"^Del ",
		"^Get ",
		"^Scan ",
		"^fast path - ",
		"^querying next range at ",
		"^output row: ",
		"^execution failed: ",
		"^r.*: sending batch ",
		"^cascading ",
		"^consuming rows$",
		"^starting plan$",
	}, "|"),
)

var showTraceColumns = sqlbase.ResultColumns{
	{Name: "timestamp", Typ: types.TimestampTZ},
	{Name: "age", Typ: types.Interval},
	{Name: "message", Typ: types.String},
	{Name: "tag", Typ: types.String},
	{Name: "loc", Typ: types.String},
	{Name: "operation", Typ: types.String},
	{Name: "span", Typ: types.Int},
}

var showCompactTraceColumns = sqlbase.ResultColumns{
	{Name: "age", Typ: types.Interval},
	{Name: "message", Typ: types.String},
	{Name: "tag", Typ: types.String},
	{Name: "operation", Typ: types.String},
}
