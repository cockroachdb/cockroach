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
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package sql

import (
	"fmt"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
)

// explainTraceNode is a planNode that wraps another node and converts DebugValues() results to a
// row of Values(). It is used as the top-level node for EXPLAIN (TRACE) statements.
type explainTraceNode struct {
	plan planNode
	txn  *client.Txn
	// Internal state, not to be initialized.
	earliest  time.Time
	exhausted bool
	rows      []parser.DTuple
	lastTS    time.Time
	lastPos   int
}

var traceColumns = append([]ResultColumn{
	{Name: "Cumulative Time", Typ: parser.TypeString},
	{Name: "Duration", Typ: parser.TypeString},
	{Name: "Span Pos", Typ: parser.TypeInt},
	{Name: "Operation", Typ: parser.TypeString},
	{Name: "Event", Typ: parser.TypeString},
}, debugColumns...)

var traceOrdering = sqlbase.ColumnOrdering{
	{ColIdx: len(traceColumns), Direction: encoding.Ascending}, /* Start time */
	{ColIdx: 2, Direction: encoding.Ascending},                 /* Span pos */
}

func makeTraceNode(plan planNode, txn *client.Txn) planNode {
	return &selectTopNode{
		source: &explainTraceNode{
			plan: plan,
			txn:  txn,
		},
		sort: &sortNode{
			// Don't use the planner context: this sort node is sorting the
			// trace events themselves; we don't want any events from this sort
			// node to show up in the EXPLAIN TRACE output.
			ctx:      context.Background(),
			ordering: traceOrdering,
			columns:  traceColumns,
		},
	}
}

func (*explainTraceNode) Columns() []ResultColumn { return traceColumns }
func (*explainTraceNode) Ordering() orderingInfo  { return orderingInfo{} }

func (n *explainTraceNode) expandPlan() error {
	if err := n.plan.expandPlan(); err != nil {
		return err
	}

	n.plan.MarkDebug(explainDebug)
	return nil
}

func (n *explainTraceNode) Start() error { return n.plan.Start() }

func (n *explainTraceNode) Next() (bool, error) {
	first := n.rows == nil
	if first {
		n.rows = []parser.DTuple{}
	}
	for !n.exhausted && len(n.rows) <= 1 {
		var vals debugValues
		if next, err := n.plan.Next(); !next {
			n.exhausted = true
			sp := opentracing.SpanFromContext(n.txn.Context)
			if err != nil {
				sp.LogEvent(err.Error())
				return false, err
			}
			sp.LogEvent("tracing completed")
			sp.Finish()
			sp = nil
			n.txn.Context = opentracing.ContextWithSpan(n.txn.Context, nil)
		} else {
			vals = n.plan.DebugValues()
		}
		var basePos int
		if len(n.txn.CollectedSpans) == 0 {
			if !n.exhausted {
				n.txn.CollectedSpans = append(n.txn.CollectedSpans, basictracer.RawSpan{
					Logs: []opentracing.LogData{{Timestamp: n.lastTS}},
				})
			}
			basePos = n.lastPos + 1
		}

		// Iterate through once to determine earliest timestamp.
		var earliest time.Time
		for _, sp := range n.txn.CollectedSpans {
			for _, entry := range sp.Logs {
				if n.earliest.IsZero() || entry.Timestamp.Before(earliest) {
					n.earliest = entry.Timestamp
				}
			}
		}

		for _, sp := range n.txn.CollectedSpans {
			for i, entry := range sp.Logs {
				commulativeDuration := fmt.Sprintf("%.3fms", entry.Timestamp.Sub(n.earliest).Seconds()*1000)
				var duration string
				if i > 0 {
					duration = fmt.Sprintf("%.3fms", entry.Timestamp.Sub(n.lastTS).Seconds()*1000)
				}
				cols := append(parser.DTuple{
					parser.NewDString(commulativeDuration),
					parser.NewDString(duration),
					parser.NewDInt(parser.DInt(basePos + i)),
					parser.NewDString(sp.Operation),
					parser.NewDString(entry.Event),
				}, vals.AsRow()...)

				// Timestamp is added for sorting, but will be removed after sort.
				n.rows = append(n.rows, append(cols, parser.MakeDTimestamp(entry.Timestamp, time.Nanosecond)))
				n.lastTS, n.lastPos = entry.Timestamp, i
			}
		}
		n.txn.CollectedSpans = nil
	}

	if first {
		return len(n.rows) > 0, nil
	}
	if len(n.rows) <= 1 {
		return false, nil
	}
	n.rows = n.rows[1:]
	return true, nil
}

func (n *explainTraceNode) ExplainPlan(v bool) (name, description string, children []planNode) {
	return "explain", "trace", []planNode{n.plan}
}

func (n *explainTraceNode) ExplainTypes(fn func(string, string)) {}

func (n *explainTraceNode) Values() parser.DTuple {
	return n.rows[0]
}

func (*explainTraceNode) MarkDebug(_ explainMode)      {}
func (*explainTraceNode) DebugValues() debugValues     { return debugValues{} }
func (*explainTraceNode) SetLimitHint(_ int64, _ bool) {}
