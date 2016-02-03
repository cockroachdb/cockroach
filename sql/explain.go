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
	"strings"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
)

type explainMode int

const (
	explainNone explainMode = iota
	explainDebug
	explainPlan
)

// Explain executes the explain statement, providing debugging and analysis
// info about a DELETE, INSERT, SELECT or UPDATE statement.
//
// Privileges: the same privileges as the statement being explained.
func (p *planner) Explain(n *parser.Explain) (planNode, *roachpb.Error) {
	mode := explainNone
	if len(n.Options) == 1 && strings.EqualFold(n.Options[0], "DEBUG") {
		mode = explainDebug
	} else if len(n.Options) == 0 {
		mode = explainPlan
	}
	if mode == explainNone {
		return nil, roachpb.NewUErrorf("unsupported EXPLAIN options: %s", n)
	}

	plan, err := p.makePlan(n.Statement, false)
	if err != nil {
		return nil, err
	}
	switch mode {
	case explainDebug:
		plan, err = markDebug(plan, mode)
		if err != nil {
			return nil, roachpb.NewUErrorf("%v: %s", err, n)
		}
		// Wrap the plan in an explainDebugNode.
		return &explainDebugNode{plan}, nil

	case explainPlan:
		v := &valuesNode{}
		v.columns = []ResultColumn{
			{Name: "Level", Typ: parser.DummyInt},
			{Name: "Type", Typ: parser.DummyString},
			{Name: "Description", Typ: parser.DummyString},
		}
		populateExplain(v, plan, 0)
		return v, nil

	default:
		return nil, roachpb.NewUErrorf("unsupported EXPLAIN mode: %d", mode)
	}
}

func markDebug(plan planNode, mode explainMode) (planNode, *roachpb.Error) {
	switch t := plan.(type) {
	case *selectNode:
		t.explain = mode

		if _, ok := t.table.node.(*indexJoinNode); ok {
			// We will replace the indexJoinNode with the index node; we cannot
			// process filter or render expressions (we don't have all the values).
			// TODO(radu): this should go away once indexJoinNode properly
			// implements explainDebug.
			t.filter = nil
			t.render = nil
			t.qvals = nil
		} else if _, ok := t.table.node.(*scanNode); !ok {
			// TODO(radu): We don't support debug mode for selects with no table or with a
			// virtual table (subquery).
			return t, nil
		}
		// Mark the from node as debug (and potentially replace it).
		newNode, err := markDebug(t.table.node, mode)
		t.table.node = newNode
		return t, err

	case *scanNode:
		t.explain = mode
		return t, nil

	case *indexJoinNode:
		// Replace the indexJoinNode with the index node.
		return markDebug(t.index, mode)

	case *sortNode:
		// Replace the sort node with the node it wraps.
		return markDebug(t.plan, mode)

	case *groupNode:
		// Replace the group node with the node it wraps.
		return markDebug(t.plan, mode)

	default:
		return nil, roachpb.NewErrorf("TODO(pmattis): unimplemented %T", plan)
	}
}

func populateExplain(v *valuesNode, plan planNode, level int) {
	name, description, children := plan.ExplainPlan()

	row := parser.DTuple{
		parser.DInt(level),
		parser.DString(name),
		parser.DString(description),
	}
	v.rows = append(v.rows, row)

	for _, child := range children {
		populateExplain(v, child, level+1)
	}
}

// debugValues is a set of values used to implement EXPLAIN (DEBUG).
type debugValues struct {
	rowIdx int
	key    string
	value  parser.Datum
	output debugValueType
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
	{Name: "Output", Typ: parser.DummyBool},
}

func (*explainDebugNode) Columns() []ResultColumn { return debugColumns }
func (*explainDebugNode) Ordering() orderingInfo  { return orderingInfo{} }

func (n *explainDebugNode) PErr() *roachpb.Error { return n.plan.PErr() }
func (n *explainDebugNode) Next() bool           { return n.plan.Next() }

func (n *explainDebugNode) ExplainPlan() (name, description string, children []planNode) {
	return n.plan.ExplainPlan()
}

func (n *explainDebugNode) Values() parser.DTuple {
	vals := n.plan.DebugValues()

	// The "output" value is NULL for partial rows, or a DBool indicating if the row passed the
	// filtering.
	outputVal := parser.DNull

	switch vals.output {
	case debugValueFiltered:
		outputVal = parser.DBool(false)

	case debugValueRow:
		outputVal = parser.DBool(true)
	}

	return parser.DTuple{
		parser.DInt(vals.rowIdx),
		parser.DString(vals.key),
		vals.value,
		outputVal,
	}
}

func (*explainDebugNode) DebugValues() debugValues {
	panic("debug mode not implemented in explainDebugNode")
}
