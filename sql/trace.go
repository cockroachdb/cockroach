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

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
)

// explainTraceNode is a planNode that wraps another node and converts DebugValues() results to a
// row of Values(). It is used as the top-level node for EXPLAIN (TRACE) statements.
type explainTraceNode struct {
	plan planNode
	txn  *client.Txn
	// Internal state, not to be initialized.
	exhausted bool
	rows      []parser.DTuple
	lastTS    time.Time
	lastPos   int
}

var traceColumns = append([]ResultColumn{
	{Name: "Cumulative Time", Typ: parser.DummyString},
	{Name: "Duration", Typ: parser.DummyString},
	{Name: "Span Pos", Typ: parser.DummyInt},
	{Name: "Operation", Typ: parser.DummyString},
	{Name: "Event", Typ: parser.DummyString},
}, debugColumns...)

func (*explainTraceNode) Columns() []ResultColumn { return traceColumns }
func (*explainTraceNode) Ordering() orderingInfo  { return orderingInfo{} }

func (n *explainTraceNode) PErr() *roachpb.Error { return nil }
func (n *explainTraceNode) Next() bool {
	first := n.rows == nil
	if first {
		n.rows = []parser.DTuple{}
	}
	for !n.exhausted && len(n.rows) <= 1 {
		var vals debugValues
		if !n.plan.Next() {
			n.exhausted = true
			sp := opentracing.SpanFromContext(n.txn.Context)
			if pErr := n.PErr(); pErr != nil {
				sp.LogEvent(pErr.String())
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
					Context: basictracer.Context{},
					Logs:    []opentracing.LogData{{Timestamp: n.lastTS}},
				})
			}
			basePos = n.lastPos + 1
		}

		// Iterate through once to determine earliest timestamp.
		var earliest time.Time
		for _, sp := range n.txn.CollectedSpans {
			for _, entry := range sp.Logs {
				if earliest.IsZero() || entry.Timestamp.Before(earliest) {
					earliest = entry.Timestamp
				}
			}
		}

		for _, sp := range n.txn.CollectedSpans {
			for i, entry := range sp.Logs {
				commulativeDuration := fmt.Sprintf("%.3fms", time.Duration(entry.Timestamp.Sub(earliest)).Seconds()*1000)
				var duration string
				if i > 0 {
					duration = fmt.Sprintf("%.3fms", time.Duration(entry.Timestamp.Sub(n.lastTS)).Seconds()*1000)
				}
				cols := append(parser.DTuple{
					parser.DString(commulativeDuration),
					parser.DString(duration),
					parser.DInt(basePos + i),
					parser.DString(sp.Operation),
					parser.DString(entry.Event),
				}, vals.AsRow()...)

				// Timestamp is added for sorting, but will be removed after sort.
				n.rows = append(n.rows, append(cols, parser.DTimestamp{Time: entry.Timestamp}))
				n.lastTS, n.lastPos = entry.Timestamp, i
			}
		}
		n.txn.CollectedSpans = nil
	}

	if first {
		return len(n.rows) > 0
	}
	if len(n.rows) <= 1 {
		return false
	}
	n.rows = n.rows[1:]
	return true
}

func (n *explainTraceNode) ExplainPlan() (name, description string, children []planNode) {
	return n.plan.ExplainPlan()
}

func (n *explainTraceNode) Values() parser.DTuple {
	return n.rows[0]
}

func (*explainTraceNode) MarkDebug(_ explainMode) {
	panic("debug mode not implemented in explainDebugNode")
}

func (*explainTraceNode) DebugValues() debugValues {
	panic("debug mode not implemented in explainTraceNode")
}

func (*explainTraceNode) SetLimitHint(_ int64, _ bool) {}
