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
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/standardtracer"
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
	{Name: "Timestamp", Typ: parser.DummyTimestamp},
	{Name: "Duration", Typ: parser.DummyString},
	{Name: "Pos", Typ: parser.DummyInt},
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
			if err := n.PErr(); err != nil {
				n.txn.Trace.LogEvent(err.GoError().Error())
			}
			n.txn.Trace.LogEvent("tracing completed")
			n.txn.Trace.Finish()
		} else {
			vals = n.plan.DebugValues()
		}
		var basePos int
		if len(n.txn.CollectedSpans) == 0 {
			if !n.exhausted {
				n.txn.CollectedSpans = append(n.txn.CollectedSpans, standardtracer.RawSpan{
					StandardContext: &standardtracer.StandardContext{},
					Logs:            []*opentracing.LogData{{Timestamp: n.lastTS}},
				})
			}
			basePos = n.lastPos + 1
		}

		for _, sp := range n.txn.CollectedSpans {
			for i, entry := range sp.Logs {
				var timeVal string
				if i > 0 {
					timeVal = fmt.Sprintf("%s", time.Duration(entry.Timestamp.Sub(n.lastTS)))
				}

				n.rows = append(n.rows, append(parser.DTuple{
					parser.DTimestamp{Time: entry.Timestamp},
					parser.DString(timeVal),
					parser.DInt(basePos + i),
					parser.DString(sp.Operation),
					parser.DString(entry.Event),
				}, vals.AsRow()...))
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

func (*explainTraceNode) DebugValues() debugValues {
	panic("debug mode not implemented in explainTraceNode")
}
