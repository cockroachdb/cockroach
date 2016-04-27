// Copyright 2015 The Cockroach Authors.
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
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/tracing"
	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
)

type explainMode int

const (
	explainNone explainMode = iota
	explainDebug
	explainPlan
	explainTrace
)

// Explain executes the explain statement, providing debugging and analysis
// info about a DELETE, INSERT, SELECT or UPDATE statement.
//
// Privileges: the same privileges as the statement being explained.
func (p *planner) Explain(n *parser.Explain, autoCommit bool) (planNode, *roachpb.Error) {
	mode := explainNone
	verbose := false
	for _, opt := range n.Options {
		newMode := explainNone
		if strings.EqualFold(opt, "DEBUG") {
			newMode = explainDebug
		} else if strings.EqualFold(opt, "TRACE") {
			newMode = explainTrace
		} else if strings.EqualFold(opt, "PLAN") {
			newMode = explainPlan
		} else if strings.EqualFold(opt, "VERBOSE") {
			verbose = true
		} else {
			return nil, roachpb.NewUErrorf("unsupported EXPLAIN option: %s", opt)
		}
		if newMode != explainNone {
			if mode != explainNone {
				return nil, roachpb.NewUErrorf("cannot set EXPLAIN mode more than once: %s", opt)
			}
			mode = newMode
		}
	}
	if mode == explainNone {
		mode = explainPlan
	}

	if mode == explainTrace {
		sp, err := tracing.JoinOrNewSnowball("coordinator", nil, func(sp basictracer.RawSpan) {
			p.txn.CollectedSpans = append(p.txn.CollectedSpans, sp)
		})
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		p.txn.Context = opentracing.ContextWithSpan(p.txn.Context, sp)
	}

	plan, err := p.makePlan(n.Statement, nil, autoCommit)
	if err != nil {
		return nil, err
	}
	switch mode {
	case explainDebug:
		// Wrap the plan in an explainDebugNode.
		return &explainDebugNode{plan}, nil

	case explainPlan:
		v := &valuesNode{}
		v.columns = []ResultColumn{
			{Name: "Level", Typ: parser.DummyInt},
			{Name: "Type", Typ: parser.DummyString},
			{Name: "Description", Typ: parser.DummyString},
		}
		populateExplain(verbose, v, plan, 0)
		return v, nil

	case explainTrace:
		return (&sortNode{
			ordering: []columnOrderInfo{{len(traceColumns), encoding.Ascending}, {2, encoding.Ascending}},
			columns:  traceColumns,
		}).wrap(&explainTraceNode{plan: plan, txn: p.txn}), nil

	default:
		return nil, roachpb.NewUErrorf("unsupported EXPLAIN mode: %d", mode)
	}
}

func populateExplain(verbose bool, v *valuesNode, plan planNode, level int) {
	name, description, children := plan.ExplainPlan(verbose)

	row := parser.DTuple{
		parser.NewDInt(parser.DInt(level)),
		parser.NewDString(name),
		parser.NewDString(description),
	}
	v.rows = append(v.rows, row)

	for _, child := range children {
		populateExplain(verbose, v, child, level+1)
	}
}

type debugValueType int

const (
	// The debug values do not refer to a full result row.
	debugValuePartial debugValueType = iota

	// The debug values refer to a full result row but the row was filtered out.
	debugValueFiltered

	// The debug value refers to a full result row that has been stored in a buffer
	// and will be emitted later.
	debugValueBuffered

	// The debug values refer to a full result row.
	debugValueRow
)

func (t debugValueType) String() string {
	switch t {
	case debugValuePartial:
		return "PARTIAL"

	case debugValueFiltered:
		return "FILTERED"

	case debugValueBuffered:
		return "BUFFERED"

	case debugValueRow:
		return "ROW"

	default:
		panic(fmt.Sprintf("invalid debugValueType %d", t))
	}
}

// debugValues is a set of values used to implement EXPLAIN (DEBUG).
type debugValues struct {
	rowIdx int
	key    string
	value  string
	output debugValueType
}

func (vals *debugValues) AsRow() parser.DTuple {
	keyVal := parser.DNull
	if vals.key != "" {
		keyVal = parser.NewDString(vals.key)
	}

	// The "output" value is NULL for partial rows, or a DBool indicating if the row passed the
	// filtering.
	outputVal := parser.DNull

	switch vals.output {
	case debugValueFiltered:
		outputVal = parser.MakeDBool(false)

	case debugValueRow:
		outputVal = parser.MakeDBool(true)
	}

	return parser.DTuple{
		parser.NewDInt(parser.DInt(vals.rowIdx)),
		keyVal,
		parser.NewDString(vals.value),
		outputVal,
	}
}

// explainDebugNode is a planNode that wraps another node and converts DebugValues() results to a
// row of Values(). It is used as the top-level node for EXPLAIN (DEBUG) statements.
type explainDebugNode struct {
	plan planNode
}

// Columns for explainDebug mode.
var debugColumns = []ResultColumn{
	{Name: "RowIdx", Typ: parser.DummyInt},
	{Name: "Key", Typ: parser.DummyString},
	{Name: "Value", Typ: parser.DummyString},
	{Name: "Disposition", Typ: parser.DummyString},
}

func (*explainDebugNode) Columns() []ResultColumn { return debugColumns }
func (*explainDebugNode) Ordering() orderingInfo  { return orderingInfo{} }

func (n *explainDebugNode) PErr() *roachpb.Error { return n.plan.PErr() }
func (n *explainDebugNode) Start() *roachpb.Error {
	if pErr := n.plan.Start(); pErr != nil {
		return pErr
	}
	n.plan.MarkDebug(explainDebug)
	return nil
}

func (n *explainDebugNode) Next() bool { return n.plan.Next() }

func (n *explainDebugNode) ExplainPlan(v bool) (name, description string, children []planNode) {
	return n.plan.ExplainPlan(v)
}

func (n *explainDebugNode) Values() parser.DTuple {
	vals := n.plan.DebugValues()

	keyVal := parser.DNull
	if vals.key != "" {
		keyVal = parser.NewDString(vals.key)
	}

	return parser.DTuple{
		parser.NewDInt(parser.DInt(vals.rowIdx)),
		keyVal,
		parser.NewDString(vals.value),
		parser.NewDString(vals.output.String()),
	}
}

func (*explainDebugNode) MarkDebug(_ explainMode) {
	panic("debug mode not implemented in explainDebugNode")
}

func (*explainDebugNode) DebugValues() debugValues {
	panic("debug mode not implemented in explainDebugNode")
}

func (*explainDebugNode) SetLimitHint(_ int64, _ bool) {}
