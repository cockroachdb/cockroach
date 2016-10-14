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

	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"golang.org/x/net/context"
)

// explainTraceNode is a planNode that wraps another node and converts DebugValues() results to a
// row of Values(). It is used as the top-level node for EXPLAIN (TRACE) statements.
type explainTraceNode struct {
	plan planNode
	// Internal state, not to be initialized.
	earliest     time.Time
	exhausted    bool
	rows         []parser.DTuple
	lastTS       time.Time
	lastPos      int
	trace        *tracing.RecordedTrace
	p            *planner
	spanFinished bool

	// Initialized at Start() time. When called, restores the planner's context to
	// what it was before this node hijacked it.
	restorePlannerCtx func()
	tracingCtx        context.Context
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

func (p *planner) makeTraceNode(plan planNode) planNode {
	return &selectTopNode{
		source: &explainTraceNode{
			plan: plan,
			p:    p,
		},
		sort: &sortNode{
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

func (n *explainTraceNode) Start() error {
	tracingCtx, recorder, err := tracing.StartSnowballTrace(n.p.ctx(), "explain trace")
	if err != nil {
		return err
	}
	n.trace = recorder
	n.tracingCtx = tracingCtx
	// Everything running on the planner/session until Close() will be done with
	// the tracingCtx. More exactly, the inner n.plan will run with this hijacked
	// context.
	n.restorePlannerCtx = n.p.hijackCtx(tracingCtx)

	return n.plan.Start()
}

func (n *explainTraceNode) Close() {
	sp := opentracing.SpanFromContext(n.tracingCtx)
	if sp != nil && !n.spanFinished {
		n.unhijackCtx()
		sp.Finish()
	}
	n.plan.Close()
}

func (n *explainTraceNode) unhijackCtx() {
	// Restore the hijacked context on the planner.
	// restorePlannerCtx can be nil if Start() hasn't been called (in particular,
	// if this node is wrapped in an EXPLAIN(PLAN)).
	if n.restorePlannerCtx != nil {
		n.restorePlannerCtx()
		n.restorePlannerCtx = nil
	}
}

func (n *explainTraceNode) Next() (bool, error) {
	first := n.rows == nil
	if first {
		n.rows = []parser.DTuple{}
	}
	for !n.exhausted && len(n.rows) <= 1 {
		var vals debugValues
		if next, err := n.plan.Next(); !next {
			sp := opentracing.SpanFromContext(n.tracingCtx)
			n.exhausted = true
			// Finish the tracing span that we began in Start().
			if err != nil {
				sp.LogFields(otlog.String("event", err.Error()))
				return false, err
			}
			sp.LogFields(otlog.String("event", "tracing completed"))
			n.unhijackCtx()
			sp.Finish()
			n.spanFinished = true
		} else {
			vals = n.plan.DebugValues()
		}
		var basePos int
		if len(n.trace.GetSpans()) == 0 {
			if !n.exhausted {
				n.trace.AddDummySpan(basictracer.RawSpan{
					Logs: []opentracing.LogRecord{{Timestamp: n.lastTS}},
				})
			}
			basePos = n.lastPos + 1
		}

		// Iterate through once to determine earliest timestamp.
		var earliest time.Time
		for _, sp := range n.trace.GetSpans() {
			for _, entry := range sp.Logs {
				if n.earliest.IsZero() || entry.Timestamp.Before(earliest) {
					n.earliest = entry.Timestamp
				}
			}
		}

		for _, sp := range n.trace.GetSpans() {
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
		// Clear the spans that have been accumulated so far, so that we'll
		// associate new spans with the next "debug values".
		n.trace.ClearSpans()
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

func (n *explainTraceNode) Values() parser.DTuple {
	return n.rows[0]
}

func (*explainTraceNode) MarkDebug(_ explainMode)  {}
func (*explainTraceNode) DebugValues() debugValues { return debugValues{} }
