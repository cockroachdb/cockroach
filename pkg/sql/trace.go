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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
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

var traceColumns = append(ResultColumns{
	{Name: "Cumulative Time", Typ: parser.TypeString},
	{Name: "Duration", Typ: parser.TypeString},
	{Name: "Span Pos", Typ: parser.TypeInt},
	{Name: "Operation", Typ: parser.TypeString},
	{Name: "Event", Typ: parser.TypeString},
}, debugColumns...)

// Internally, the explainTraceNode also returns a timestamp column which is
// used during sorting.
var traceColumnsWithTS = append(traceColumns, ResultColumn{
	Name: "Timestamp", Typ: parser.TypeTimestamp,
})

var traceOrdering = sqlbase.ColumnOrdering{
	{ColIdx: len(traceColumns), Direction: encoding.Ascending}, /* Start time */
	{ColIdx: 2, Direction: encoding.Ascending},                 /* Span pos */
}

func (p *planner) makeTraceNode(plan planNode, txn *client.Txn) planNode {
	return &selectTopNode{
		source: &explainTraceNode{
			plan: plan,
			txn:  txn,
		},
		sort: &sortNode{
			// Generally, sortNode uses its ctx field to log sorting
			// details.  However the user of EXPLAIN(TRACE) only wants
			// details about the traced statement, not about the sortNode
			// that does work on behalf of the EXPLAIN statement itself.  So
			// we connect this sortNode to a different context, so its log
			// messages do not go to the planner's context which will be
			// responsible to collect the trace.

			// TODO(andrei): I think ideally we would also use the planner's
			// Span, but create a sub-span for the Sorting with a special
			// tag that is ignored by the EXPLAIN TRACE logic. This way,
			// this sorting would still appear in our debug tracing, it just
			// wouldn't be reported. Of course, currently statements under
			// EXPLAIN TRACE are not present in our normal debug tracing at
			// all since we override the tracer, but I'm hoping to stop
			// doing that. And even then I'm not completely sure how this
			// would work exactly, since the sort node "wraps" the inner
			// select node, but we can probably do something using
			// opentracing's "follows-from" spans as opposed to
			// "parent-child" spans when expressing this relationship
			// between the sort and the select.
			ctx:      opentracing.ContextWithSpan(p.ctx(), nil),
			p:        p,
			ordering: traceOrdering,
			// These are the columns that the sortNode (and thus the selectTopNode)
			// presents as the output.
			columns: traceColumns,
		},
	}
}

func (*explainTraceNode) Columns() ResultColumns { return traceColumnsWithTS }
func (*explainTraceNode) Ordering() orderingInfo { return orderingInfo{} }

func (n *explainTraceNode) expandPlan() error {
	if err := n.plan.expandPlan(); err != nil {
		return err
	}

	n.plan.MarkDebug(explainDebug)
	return nil
}

func (n *explainTraceNode) Start() error { return n.plan.Start() }
func (n *explainTraceNode) Close()       { n.plan.Close() }

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
					Logs: []opentracing.LogRecord{{Timestamp: n.lastTS}},
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
				// Extract the message of the event, which is either in an "event" or
				// "error" field.
				var msg string
				for _, f := range entry.Fields {
					key := f.Key()
					if key == "event" {
						msg = fmt.Sprint(f.Value())
						break
					}
					if key == "error" {
						msg = fmt.Sprint("error:", f.Value())
						break
					}
				}
				cols := append(parser.DTuple{
					parser.NewDString(commulativeDuration),
					parser.NewDString(duration),
					parser.NewDInt(parser.DInt(basePos + i)),
					parser.NewDString(sp.Operation),
					parser.NewDString(msg),
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
