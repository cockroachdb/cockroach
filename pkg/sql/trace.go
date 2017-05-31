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

	opentracing "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// explainTraceNode is a planNode that wraps another node and converts DebugValues() results to a
// row of Values(). It is used as the top-level node for EXPLAIN (TRACE) statements.
type explainTraceNode struct {
	plan planNode
	// Internal state, not to be initialized.
	earliest    time.Time
	exhausted   bool
	rows        []parser.Datums
	lastTS      time.Time
	lastPos     int
	recordingSp opentracing.Span
	p           *planner

	// Initialized at Start() time. When called, restores the planner's context to
	// what it was before this node hijacked it.
	restorePlannerCtx func()
	tracingCtx        context.Context
}

var traceColumns = append(sqlbase.ResultColumns{
	{Name: "Cumulative Time", Typ: parser.TypeString},
	{Name: "Duration", Typ: parser.TypeString},
	{Name: "Span Pos", Typ: parser.TypeInt},
	{Name: "Operation", Typ: parser.TypeString},
	{Name: "Event", Typ: parser.TypeString},
}, debugColumns...)

// Internally, the explainTraceNode also returns a timestamp column which is
// used during sorting.
var traceColumnsWithTS = append(traceColumns, sqlbase.ResultColumn{
	Name: "Timestamp", Typ: parser.TypeTimestamp,
})

var traceOrdering = sqlbase.ColumnOrdering{
	{ColIdx: len(traceColumns), Direction: encoding.Ascending}, /* Start time */
	{ColIdx: 2, Direction: encoding.Ascending},                 /* Span pos */
}

func (p *planner) makeTraceNode(plan planNode) planNode {
	return &sortNode{
		plan: &explainTraceNode{
			plan: plan,
			p:    p,
		},
		p:        p,
		ordering: traceOrdering,
		// These are the columns that the sortNode (and thus the selectTopNode)
		// presents as the output.
		columns: traceColumns,
	}
}

func (n *explainTraceNode) Start(ctx context.Context) error {
	return n.plan.Start(ctx)
}

// hijackTxnContext hijacks the session's/txn's context, causing everything
// happening in the current txn before explainTraceNode.Close() to happen inside
// a recorded trace.
//
// TODO(andrei): This is currently called from Next(), which means that
// `n.plan.Start()` is not traced. That's a shame, but unfortunately we can't
// hijack in explainTraceNode.Start() because we need to only hijack after the
// sortNode wrapping the explainTraceNode has started execution. This is because
// the context that's passed to the first call of sortNode.Next() (the call
// that's responsible for exhausting the explainTraceNode) needs to be the
// un-hijacked one - otherwise, the hijacked ctx will be used by that call to
// sortNode.Next() after the explandPlanNode closes the tracing span (resulting
// in a span use-after-finish).
func (n *explainTraceNode) hijackTxnContext(ctx context.Context) error {
	tracer := n.p.session.execCfg.AmbientCtx.Tracer
	tracingCtx, sp, err := tracing.StartSnowballTrace(ctx, tracer, "explain trace")
	if err != nil {
		return err
	}
	n.recordingSp = sp
	n.tracingCtx = tracingCtx
	// Everything running on the planner/session until Close() will be done with
	// the tracingCtx. More exactly, the inner n.plan will run with this hijacked
	// context.
	n.restorePlannerCtx = n.p.session.hijackCtx(tracingCtx)
	return nil
}

func (n *explainTraceNode) Close(ctx context.Context) {
	if n.restorePlannerCtx != nil {
		n.unhijackCtx()
		sp := opentracing.SpanFromContext(n.tracingCtx)
		sp.Finish()
	}
	n.plan.Close(ctx)
}

func (n *explainTraceNode) unhijackCtx() {
	// Restore the hijacked context on the planner.
	n.restorePlannerCtx()
	n.restorePlannerCtx = nil
}

func (n *explainTraceNode) Next(ctx context.Context) (bool, error) {
	first := n.rows == nil
	if first {
		n.rows = []parser.Datums{}
		if err := n.hijackTxnContext(ctx); err != nil {
			return false, err
		}
		// After the call to hijackTxnContext, the current and future invocations of
		// explainTraceNode.Next() need to use n.tracingCtx instead of the method
		// argument.
		n.lastTS = timeutil.Now()
	}
	for !n.exhausted && len(n.rows) <= 1 {
		var vals debugValues
		if next, err := n.plan.Next(n.tracingCtx); !next {
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
		} else {
			vals = n.plan.DebugValues()
		}
		var basePos int
		recording := tracing.GetRecording(n.recordingSp)
		// Remove empty spans. We do this because we use ClearRecordedLogs which
		// clears logs but doesn't remove spans.
		for i := 0; i < len(recording); {
			if len(recording[i].Logs) == 0 {
				copy(recording[i:], recording[i+1:])
				recording = recording[:len(recording)-1]
			} else {
				i++
			}
		}
		if len(recording) == 0 {
			if !n.exhausted {
				err := tracing.ImportRemoteSpans(n.recordingSp, []tracing.RecordedSpan{{
					Logs: []tracing.RecordedSpan_LogRecord{{Time: n.lastTS}},
				}})
				if err != nil {
					return false, err
				}
				recording = tracing.GetRecording(n.recordingSp)
			}
			basePos = n.lastPos + 1
		}

		// Iterate through once to determine earliest timestamp.
		for _, sp := range recording {
			for _, entry := range sp.Logs {
				if n.earliest.IsZero() || entry.Time.Before(n.earliest) {
					n.earliest = entry.Time
				}
			}
		}

		for _, sp := range recording {
			for i, entry := range sp.Logs {
				commulativeDuration := fmt.Sprintf("%.3fms", entry.Time.Sub(n.earliest).Seconds()*1000)
				var duration string
				if i > 0 {
					duration = fmt.Sprintf("%.3fms", entry.Time.Sub(n.lastTS).Seconds()*1000)
				}
				// Extract the message of the event, which is either in an "event" or
				// "error" field.
				var msg string
				for _, f := range entry.Fields {
					key := f.Key
					if key == "event" {
						msg = f.Value
						break
					}
					if key == "error" {
						msg = fmt.Sprint("error:", f.Value)
						break
					}
				}
				cols := append(parser.Datums{
					parser.NewDString(commulativeDuration),
					parser.NewDString(duration),
					parser.NewDInt(parser.DInt(basePos + i)),
					parser.NewDString(sp.Operation),
					parser.NewDString(msg),
				}, vals.AsRow()...)

				// Timestamp is added for sorting, but will be removed after sort.
				n.rows = append(n.rows, append(cols, parser.MakeDTimestamp(entry.Time, time.Nanosecond)))
				n.lastTS, n.lastPos = entry.Time, i
			}
		}
		// Clear the logs that have been accumulated so far, so that we'll associate
		// new logs with the next "debug values".
		tracing.ClearRecordedLogs(n.recordingSp)
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

func (n *explainTraceNode) Values() parser.Datums {
	return n.rows[0]
}

func (*explainTraceNode) Columns() sqlbase.ResultColumns { return traceColumnsWithTS }

func (*explainTraceNode) MarkDebug(_ explainMode)  {}
func (*explainTraceNode) DebugValues() debugValues { return debugValues{} }
