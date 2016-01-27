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
		return plan, nil
	case explainPlan:
		v := &valuesNode{}
		v.columns = []ResultColumn{
			{Name: "Level", Typ: parser.DummyInt},
			{Name: "Type", Typ: parser.DummyString},
			{Name: "Description", Typ: parser.DummyString},
		}
		populateExplain(v, plan, 0)
		plan = v
	default:
		return nil, roachpb.NewUErrorf("unsupported EXPLAIN mode: %d", mode)
	}
	return plan, nil
}

func markDebug(plan planNode, mode explainMode) (planNode, *roachpb.Error) {
	switch t := plan.(type) {
	case *selectNode:
		return markDebug(t.from, mode)

	case *scanNode:
		// Mark the node as being explained.
		t.columns = []ResultColumn{
			{Name: "RowIdx", Typ: parser.DummyInt},
			{Name: "Key", Typ: parser.DummyString},
			{Name: "Value", Typ: parser.DummyString},
			{Name: "Output", Typ: parser.DummyBool},
		}
		t.explain = mode
		return t, nil

	case *indexJoinNode:
		return markDebug(t.index, mode)

	case *sortNode:
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
