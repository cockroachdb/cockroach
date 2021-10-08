// Copyright 2016 The Cockroach Authors.
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
	"fmt"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// showTraceNode is a planNode that processes session trace data.
type showTraceNode struct {
	columns colinfo.ResultColumns
	compact bool

	// If set, the trace will also include "KV trace" messages - verbose messages
	// around the interaction of SQL with KV. Some of the messages are per-row.
	kvTracingEnabled bool

	run traceRun
}

// ShowTrace shows the current stored session trace, or the trace of a given
// query.
// Privileges: None.
func (p *planner) ShowTrace(ctx context.Context, n *tree.ShowTraceForSession) (planNode, error) {
	var node planNode = p.makeShowTraceNode(n.Compact, n.TraceType == tree.ShowTraceKV)

	// Ensure the messages are sorted in age order, so that the user
	// does not get confused.
	ageColIdx := colinfo.GetTraceAgeColumnIdx(n.Compact)
	node = &sortNode{
		plan: node,
		ordering: colinfo.ColumnOrdering{
			colinfo.ColumnOrderInfo{ColIdx: ageColIdx, Direction: encoding.Ascending},
		},
	}

	if n.TraceType == tree.ShowTraceReplica {
		node = &showTraceReplicaNode{plan: node}
	}
	return node, nil
}

// makeShowTraceNode creates a new showTraceNode.
//
// Args:
// kvTracingEnabled: If set, the trace will also include "KV trace" messages -
//   verbose messages around the interaction of SQL with KV. Some of the
//   messages are per-row.
func (p *planner) makeShowTraceNode(compact bool, kvTracingEnabled bool) *showTraceNode {
	n := &showTraceNode{
		kvTracingEnabled: kvTracingEnabled,
		compact:          compact,
	}
	if compact {
		// We make a copy here because n.columns can be mutated to rename columns.
		n.columns = append(n.columns, colinfo.ShowCompactTraceColumns...)
	} else {
		n.columns = append(n.columns, colinfo.ShowTraceColumns...)
	}
	return n
}

// traceRun contains the run-time state of showTraceNode during local execution.
type traceRun struct {
	resultRows []tree.Datums
	curRow     int
}

func (n *showTraceNode) startExec(params runParams) error {
	// Get all the data upfront and process the traces. Subsequent
	// invocations of Next() will merely return the results.
	traceRows, err := params.extendedEvalCtx.Tracing.getSessionTrace()
	if err != nil {
		return err
	}
	n.processTraceRows(params.EvalContext(), traceRows)
	return nil
}

// Next implements the planNode interface
func (n *showTraceNode) Next(params runParams) (bool, error) {
	if n.run.curRow >= len(n.run.resultRows) {
		return false, nil
	}
	n.run.curRow++
	return true, nil
}

// processTraceRows populates n.resultRows.
// This code must be careful not to overwrite traceRows,
// because this is a shared slice which will be reused
// by subsequent SHOW TRACE FOR SESSION statements.
func (n *showTraceNode) processTraceRows(evalCtx *tree.EvalContext, traceRows []traceRow) {
	// Filter trace rows based on the message (in the SHOW KV TRACE case)
	if n.kvTracingEnabled {
		res := make([]traceRow, 0, len(traceRows))
		for _, r := range traceRows {
			msg := r[traceMsgCol].(*tree.DString)
			if kvMsgRegexp.MatchString(string(*msg)) {
				res = append(res, r)
			}
		}
		traceRows = res
	}
	if len(traceRows) == 0 {
		return
	}

	// Render the final rows.
	n.run.resultRows = make([]tree.Datums, len(traceRows))
	for i, r := range traceRows {
		ts := r[traceTimestampCol].(*tree.DTimestampTZ)
		loc := r[traceLocCol]
		tag := r[traceTagCol]
		msg := r[traceMsgCol]
		spanIdx := r[traceSpanIdxCol]
		op := r[traceOpCol]
		age := r[traceAgeCol]

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
	n.run.resultRows = nil
}

// kvMsgRegexp is the message filter used for SHOW KV TRACE.
var kvMsgRegexp = regexp.MustCompile(
	strings.Join([]string{
		"^fetched: ",
		"^CPut ",
		"^Put ",
		"^InitPut ",
		"^DelRange ",
		"^ClearRange ",
		"^Del ",
		"^Get ",
		"^Scan ",
		"^querying next range at ",
		"^output row: ",
		"^rows affected: ",
		"^execution failed after ",
		"^r.*: sending batch ",
		"^cascading ",
		"^fast path completed",
	}, "|"),
)
